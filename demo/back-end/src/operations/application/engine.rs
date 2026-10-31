//! The engine's views as the ports of the operations: the rows of their checkpoints ([`Checkpoints`]), the size of
//! the journal and how far a view is behind it ([`EventLedgerStats`]), the verification of each view
//! ([`VerifiedReadModel`]); and the services of a request built over them with what this node keeps
//! ([`RebuildService::of`], [`VerifyService::of`]).
use crate::{
    operations::application::{
        ledger::{EventLedgerStats, LedgerTotals},
        rebuild::{Checkpoints, ReadSideLimits, RebuildService, RebuildState, Replay},
        verify::{VerifiedReadModel, VerifyService},
    },
    shared::application::{StorageError, activity::ActivityHub},
};
use async_trait::async_trait;
use cqrsx::{
    engine::{Cqrs, views::ViewsError},
    journal::feed::Position,
    projection::checkpoint::{CheckpointError, CheckpointKey, CheckpointStatus, RebuildRequest},
    view::verify::Verification,
};
use std::{sync::Arc, time::Duration};

/// The views of the engine.
pub struct EngineViews(pub Arc<Cqrs>);

#[async_trait]
impl Checkpoints for EngineViews {
    async fn rows(&self) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        self.0.views().status().await
    }

    /// A view the engine does not have is a request nobody registered.
    async fn ask_rebuild(&self, key: &CheckpointKey, pace: Duration) -> Result<RebuildRequest, CheckpointError> {
        let requested = self.0.views().rebuild(key.projection(), pace).await;
        requested.map_err(|error| match error {
            ViewsError::Checkpoint(error) => error,
            _ => CheckpointError::Invalid("no such projection"),
        })
    }
}

#[async_trait]
impl EventLedgerStats for EngineViews {
    async fn totals(&self) -> Result<LedgerTotals, StorageError> {
        let totals = self.0.views().totals().await.map_err(StorageError::new)?;
        Ok(LedgerTotals {
            events: totals.events,
            aggregates: totals.aggregates,
        })
    }

    async fn count_after(&self, aggregate_type: &str, after: Position, cap: u64) -> Result<u64, StorageError> {
        let counted = self.0.views().count_after(aggregate_type, after, cap).await;
        counted.map_err(StorageError::new)
    }
}

/// One view of the engine, verified by the engine under its name.
pub struct EngineView {
    cqrs: Arc<Cqrs>,
    name: String,
}

impl EngineView {
    /// The view `name` of `cqrs`.
    pub fn new(cqrs: Arc<Cqrs>, name: impl Into<String>) -> Self {
        Self { cqrs, name: name.into() }
    }
}

#[async_trait]
impl VerifiedReadModel for EngineView {
    fn name(&self) -> &str {
        &self.name
    }

    async fn verify(&self) -> Result<Verification, StorageError> {
        self.cqrs.views().verify(&self.name).await.map_err(StorageError::new)
    }
}

impl RebuildService<EngineViews> {
    /// The service of one request over the engine: its views, the lag cap and the status timeout of its settings, the
    /// pace of Encore! ([`Replay`]), the activity log and what this node saw ([`RebuildState`]), which the `operations`
    /// module provides; a watcher of Encore! stops with the engine.
    pub fn of(cqrs: &Arc<Cqrs>) -> Result<Self, StorageError> {
        let missing = StorageError::new;
        let (replay, state) = (
            cqrs.dependency::<Replay>().map_err(missing)?,
            cqrs.dependency::<RebuildState>().map_err(missing)?,
        );
        let activity = cqrs.dependency::<ActivityHub>().map_err(missing)?;
        let settings = cqrs.settings();
        let limits = ReadSideLimits {
            lag_cap: settings.lag_cap(),
            max_pace: replay.max_pace,
            status_timeout: settings.status_timeout(),
        };
        let views = Arc::new(EngineViews(cqrs.clone()));
        let service = Self::new(views.clone(), cqrs.views().keys(), views, activity, limits, cqrs.stopping());
        Ok(service.with_state(state))
    }
}

impl VerifyService {
    /// Every view of the engine, verified by the engine.
    pub fn of(cqrs: &Arc<Cqrs>) -> Self {
        let views = cqrs.views().keys().into_iter().map(|key| {
            let view: Arc<dyn VerifiedReadModel> = Arc::new(EngineView::new(cqrs.clone(), key.projection()));
            view
        });
        Self::new(views.collect())
    }
}
