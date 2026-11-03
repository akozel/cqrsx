//! The framework's views as the ports of the operations: the rows of their checkpoints ([`Checkpoints`]), the size of
//! the journal and how far a view is behind it ([`JournalStats`]), the verification of each view
//! ([`VerifiedReadModel`]); and the services of a request built over them with what this node keeps
//! ([`ReadSideService::of`], [`VerifyService::of`]).
use crate::{
    operations::application::{
        journal::JournalStats,
        rebuild::{Checkpoints, ReadSideLimits, ReadSideService, ReadSideState, RebuildPace},
        verify::{VerifiedReadModel, VerifyService},
    },
    shared::application::{StorageError, activity::ActivityHub},
};
use async_trait::async_trait;
use cqrsx::{
    journal::feed::Position,
    projection::checkpoint::{CheckpointError, CheckpointKey, CheckpointStatus, RebuildReply},
    view::verify::Verification,
};
use cqrsx_framework::{
    Cqrsx,
    views::{JournalTotals, ViewsError},
};
use std::{sync::Arc, time::Duration};

/// The views of the framework.
pub struct FrameworkReadSide(pub Arc<Cqrsx>);

#[async_trait]
impl Checkpoints for FrameworkReadSide {
    async fn rows(&self) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        self.0.views().status().await
    }

    /// A view the framework does not have is a request nobody registered; a view whose processor has not started has no
    /// row yet.
    async fn request_rebuild(&self, key: &CheckpointKey, pace: Duration) -> Result<RebuildReply, CheckpointError> {
        let requested = self.0.views().request_rebuild(key.projection(), pace).await;
        requested.map_err(|error| match error {
            ViewsError::NotStarted => CheckpointError::NotRegistered,
            ViewsError::Checkpoint(error) => error,
            ViewsError::Unknown(_) | ViewsError::UnknownAggregateType(_) | ViewsError::Persistence(_) => {
                CheckpointError::Invalid("no such projection")
            }
        })
    }
}

#[async_trait]
impl JournalStats for FrameworkReadSide {
    async fn totals(&self) -> Result<JournalTotals, StorageError> {
        self.0.views().totals().await.map_err(StorageError::new)
    }

    async fn count_after(&self, aggregate_type: &str, after: Position, cap: u64) -> Result<u64, StorageError> {
        let counted = self.0.views().count_after(aggregate_type, after, cap).await;
        counted.map_err(StorageError::new)
    }
}

/// One view of the framework, verified by the framework under its name.
pub struct FrameworkView {
    cqrsx: Arc<Cqrsx>,
    name: String,
}

impl FrameworkView {
    /// The view `name` of `cqrsx`.
    pub fn new(cqrsx: Arc<Cqrsx>, name: impl Into<String>) -> Self {
        Self {
            cqrsx,
            name: name.into(),
        }
    }
}

#[async_trait]
impl VerifiedReadModel for FrameworkView {
    fn name(&self) -> &str {
        &self.name
    }

    async fn verify(&self) -> Result<Verification, StorageError> {
        self.cqrsx.views().verify(&self.name).await.map_err(StorageError::new)
    }
}

impl ReadSideService<FrameworkReadSide> {
    /// The service of one request over the framework: its views, the lag cap and the status timeout of its settings, the
    /// pace of Encore! ([`RebuildPace`]), the activity log and what this node saw ([`ReadSideState`]), which the `operations`
    /// module provides; a watcher of Encore! stops with the framework.
    pub fn of(cqrsx: &Arc<Cqrsx>) -> Result<Self, StorageError> {
        let missing = StorageError::new;
        let (rebuild_pace, state) = (
            cqrsx.dependency::<RebuildPace>().map_err(missing)?,
            cqrsx.dependency::<ReadSideState>().map_err(missing)?,
        );
        let activity = cqrsx.dependency::<ActivityHub>().map_err(missing)?;
        let settings = cqrsx.settings();
        let limits = ReadSideLimits {
            lag_cap: settings.lag_cap(),
            max_pace: rebuild_pace.max_pace,
            status_timeout: settings.status_timeout(),
        };
        let views = Arc::new(FrameworkReadSide(cqrsx.clone()));
        let service = Self::new(views.clone(), cqrsx.views().keys(), views, activity, limits, cqrsx.stopping());
        Ok(service.with_state(state))
    }
}

impl VerifyService {
    /// Every view of the framework, verified by the framework.
    pub fn of(cqrsx: &Arc<Cqrsx>) -> Self {
        let views = cqrsx.views().keys().into_iter().map(|key| {
            let view: Arc<dyn VerifiedReadModel> = Arc::new(FrameworkView::new(cqrsx.clone(), key.projection()));
            view
        });
        Self::new(views.collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::test_app;
    use cqrsx::projection::checkpoint::CheckpointStore;

    #[tokio::test]
    async fn a_rebuild_request_tells_a_row_not_registered_yet_from_a_view_nobody_has() {
        let (cqrsx, _) = test_app().await;
        let halls = cqrsx.views().keys().into_iter().next().unwrap();
        let read_side = FrameworkReadSide(Arc::new(cqrsx));
        let starting = read_side.request_rebuild(&halls, Duration::ZERO).await;
        assert!(matches!(starting, Err(CheckpointError::NotRegistered)), "{starting:?}");
        let nobody = read_side
            .request_rebuild(&CheckpointKey::new("nobody", "Concert"), Duration::ZERO)
            .await;
        assert!(
            matches!(nobody, Err(CheckpointError::Invalid("no such projection"))),
            "{nobody:?}"
        );
        read_side.0.checkpoints().unwrap().register(&halls).await.unwrap();
        let accepted = read_side.request_rebuild(&halls, Duration::ZERO).await;
        assert!(matches!(accepted, Ok(RebuildReply::Accepted)), "{accepted:?}");
    }
}
