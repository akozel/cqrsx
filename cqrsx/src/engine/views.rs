//! The views of an engine: the queries of one view ([`ViewQuery`]) and the operations over all of them ([`Views`]).
use crate::{
    cqrs::{
        aggregate::Aggregate,
        projection::{
            View,
            checkpoint::{CheckpointError, CheckpointKey, CheckpointStatus, CheckpointStore, RebuildRequest},
            repository::{StoredView, ViewRepository},
            verify::{Verification, verify},
        },
        store::{PersistenceError, feed::Position},
        testing::{mem_event_repository::MemEventRepository, mem_view_repository::MemViewRepository},
    },
    engine::Cqrs,
};
use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;

type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, PersistenceError>> + Send + 'a>>;

/// The queries of a view, its aggregate type erased.
trait Queries<V>: Send + Sync {
    fn get<'a>(&'a self, id: &'a str) -> Answer<'a, Option<StoredView<V>>>;
    fn list(&self) -> Answer<'_, Vec<StoredView<V>>>;
    fn range<'a>(&'a self, lower: &'a str, upper: &'a str) -> Answer<'a, Vec<StoredView<V>>>;
}

/// What the operations need of a view, its types erased.
trait Operations: Send + Sync {
    fn key(&self) -> CheckpointKey;
    fn verify(&self) -> Answer<'_, Verification>;
    fn lag(&self, after: Position) -> Answer<'_, u64>;
}

/// A view in memory: its documents and the journal of its aggregate.
struct Registered<V, A> {
    documents: Arc<MemViewRepository<V, A>>,
    journal: MemEventRepository,
}

impl<V: View<A> + 'static, A: Aggregate + 'static> Queries<V> for Registered<V, A> {
    fn get<'a>(&'a self, id: &'a str) -> Answer<'a, Option<StoredView<V>>> {
        Box::pin(async move { Ok(self.documents.load_with_context(id).await?.map(StoredView::from)) })
    }

    fn list(&self) -> Answer<'_, Vec<StoredView<V>>> {
        Box::pin(self.documents.list())
    }

    fn range<'a>(&'a self, lower: &'a str, upper: &'a str) -> Answer<'a, Vec<StoredView<V>>> {
        Box::pin(self.documents.range(lower, upper))
    }
}

impl<V: View<A> + PartialEq + 'static, A: Aggregate + 'static> Operations for Registered<V, A> {
    fn key(&self) -> CheckpointKey {
        CheckpointKey::of::<V, A>()
    }

    fn verify(&self) -> Answer<'_, Verification> {
        Box::pin(verify::<V, A>(&self.journal, &*self.documents))
    }

    fn lag(&self, after: Position) -> Answer<'_, u64> {
        Box::pin(self.journal.count_after::<A>(after, u64::MAX))
    }
}

/// The views of an engine: the queries by the view's type, the operations by its name.
#[derive(Default)]
pub(crate) struct Registry {
    queries: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    operations: Vec<Arc<dyn Operations>>,
}

impl Registry {
    pub(crate) fn register<V, A>(&mut self, documents: Arc<MemViewRepository<V, A>>, journal: MemEventRepository)
    where
        V: View<A> + PartialEq + 'static,
        A: Aggregate + 'static,
    {
        let view = Arc::new(Registered { documents, journal });
        let queries: Arc<dyn Queries<V>> = view.clone();
        self.queries.insert(TypeId::of::<V>(), Box::new(queries));
        self.operations.push(view);
    }

    pub(crate) fn names(&self) -> Vec<String> {
        self.operations
            .iter()
            .map(|view| view.key().projection().to_owned())
            .collect()
    }

    fn named(&self, name: &str) -> Result<&dyn Operations, ViewsError> {
        let view = self.operations.iter().find(|view| view.key().projection() == name);
        view.map(|view| &**view).ok_or_else(|| ViewsError::Unknown(name.to_owned()))
    }
}

/// The three queries of the view `V` ([`Cqrs::view`]): a document by id, every document and a range of ids, ordered by
/// the bytes of the ids. A view the engine does not have answers with an error.
pub struct ViewQuery<'a, V> {
    queries: Option<&'a dyn Queries<V>>,
}

impl<V: 'static> ViewQuery<'_, V> {
    /// The document of the aggregate `id` (its `Display`).
    pub async fn get(&self, id: &impl Display) -> Result<Option<StoredView<V>>, PersistenceError> {
        self.queries()?.get(&id.to_string()).await
    }

    pub async fn list(&self) -> Result<Vec<StoredView<V>>, PersistenceError> {
        self.queries()?.list().await
    }

    /// The documents whose id lies in `[lower, upper)`.
    pub async fn range(&self, lower: &str, upper: &str) -> Result<Vec<StoredView<V>>, PersistenceError> {
        self.queries()?.range(lower, upper).await
    }

    fn queries(&self) -> Result<&dyn Queries<V>, PersistenceError> {
        let unknown = || format!("the view `{}` is not registered", type_name::<V>()).into();
        self.queries.ok_or_else(|| PersistenceError::UnknownError(unknown()))
    }
}

/// An operation over the views that could not run.
#[derive(Debug, Error)]
pub enum ViewsError {
    #[error("the view `{0}` is not registered")]
    Unknown(String),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

/// The operations over the views of an engine ([`Cqrs::views`]), by a view's name: its checkpoint, the requests of a
/// rebuild its processor takes, the verification of its documents, how far it is behind.
pub struct Views<'a> {
    cqrs: &'a Cqrs,
}

impl Views<'_> {
    /// The checkpoint rows: positions, owners, leases, failures and rebuilds.
    pub async fn status(&self) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        self.cqrs.memory.checkpoints.status(None).await
    }

    /// Asks the processor of the view `name` to rebuild every document, `pace` between the events of a key.
    pub async fn rebuild(&self, name: &str, pace: Duration) -> Result<RebuildRequest, ViewsError> {
        let key = self.cqrs.views.named(name)?.key();
        Ok(self.cqrs.memory.checkpoints.request_rebuild(&key, pace).await?)
    }

    /// Asks the processor of the view `name` to rebuild the document of the aggregate `id`.
    pub async fn rebuild_key(&self, name: &str, id: &str) -> Result<(), ViewsError> {
        let key = self.cqrs.views.named(name)?.key();
        Ok(self.cqrs.memory.checkpoints.request_rebuild_key(&key, id).await?)
    }

    /// Every document of the view `name` against the fold of the part of its history it applied (D-20).
    pub async fn verify(&self, name: &str) -> Result<Verification, ViewsError> {
        Ok(self.cqrs.views.named(name)?.verify().await?)
    }

    /// The events of its aggregate type above the checkpoint of the view `name`.
    pub async fn lag(&self, name: &str) -> Result<u64, ViewsError> {
        let view = self.cqrs.views.named(name)?;
        let rows = self.cqrs.memory.checkpoints.status(Some(name)).await?;
        Ok(view.lag(rows.first().map_or(Position::START, |row| row.position)).await?)
    }
}

impl Cqrs {
    /// The queries of the view `V`.
    pub fn view<V: 'static>(&self) -> ViewQuery<'_, V> {
        let queries = self.views.queries.get(&TypeId::of::<V>());
        let queries = queries.and_then(|queries| queries.downcast_ref::<Arc<dyn Queries<V>>>());
        ViewQuery {
            queries: queries.map(|queries| &**queries),
        }
    }

    pub fn views(&self) -> Views<'_> {
        Views { cqrs: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{setup::Settings, test_support::*};

    async fn book_every(cqrs: &Cqrs, bookings: &[(&str, &str, &str)]) {
        for (show, seat, fan) in bookings {
            cqrs.from("ui")
                .execute::<Show>(&(*show).into(), book(seat, fan))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_view_answers_get_list_and_range_once_caught_up() {
        let (cqrs, _) = engine();
        book_every(&cqrs, &[("s-2", "A1", "fox"), ("s-1", "B2", "panda"), ("s-3", "C3", "fox")]).await;
        cqrs.catch_up().await;
        cqrs.catch_up().await;
        let halls = cqrs.view::<Hall>();
        let hall = halls.get(&"s-2").await.unwrap().unwrap();
        assert_eq!(
            (
                hall.id.as_str(),
                hall.last_sequence,
                hall.view.seats.get("A1").map(String::as_str)
            ),
            ("s-2", 1, Some("fox"))
        );
        assert!(halls.get(&"s-9").await.unwrap().is_none());
        let ids = |documents: Vec<StoredView<Hall>>| documents.into_iter().map(|document| document.id).collect::<Vec<_>>();
        assert_eq!(ids(halls.list().await.unwrap()), ["s-1", "s-2", "s-3"]);
        assert_eq!(ids(halls.range("s-1", "s-3").await.unwrap()), ["s-1", "s-2"]);
        let unknown = cqrs.view::<String>();
        let errors = [
            unknown.get(&"s-1").await.unwrap_err().to_string(),
            unknown.list().await.unwrap_err().to_string(),
            unknown.range("a", "b").await.unwrap_err().to_string(),
        ];
        assert!(
            errors
                .iter()
                .all(|error| error == "the view `alloc::string::String` is not registered"),
            "{errors:?}"
        );
    }

    #[tokio::test]
    async fn views_report_their_status_and_lag_verify_and_take_rebuild_requests() {
        let (cqrs, _) = engine();
        book_every(&cqrs, &[("s-1", "A1", "fox"), ("s-1", "A2", "panda"), ("s-2", "A1", "fox")]).await;
        let views = cqrs.views();
        assert_eq!(
            views.lag("halls").await.unwrap(),
            3,
            "nothing applied before the processors run"
        );
        assert!(views.status().await.unwrap().is_empty());
        cqrs.catch_up().await;
        assert_eq!(views.lag("halls").await.unwrap(), 0);
        let rows = views.status().await.unwrap();
        let named: Vec<(&str, &str, bool)> = rows
            .iter()
            .map(|row| {
                (
                    row.projection.as_str(),
                    row.aggregate_type.as_str(),
                    row.owner.as_deref() == Some(&cqrs.owner),
                )
            })
            .collect();
        assert_eq!(named, [("halls", "show", true), ("tickets", "wallet", true)]);
        let verified = views.verify("halls").await.unwrap();
        assert_eq!((verified.checked, verified.differences.len()), (2, 0));
        assert_eq!(
            views.rebuild("halls", Duration::ZERO).await.unwrap(),
            RebuildRequest::Accepted
        );
        assert_eq!(
            views.rebuild("halls", Duration::ZERO).await.unwrap(),
            RebuildRequest::AlreadyRunning
        );
        views.rebuild_key("halls", "s-1").await.unwrap();
        while views.status().await.unwrap()[0].rebuilt < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            views.verify("halls").await.unwrap().differences.len(),
            0,
            "the rebuild folds the history"
        );
        for refused in [
            views.rebuild("seats", Duration::ZERO).await.map(|_| ()),
            views.rebuild_key("seats", "s-1").await,
            views.verify("seats").await.map(|_| ()),
            views.lag("seats").await.map(|_| ()),
        ] {
            assert_eq!(refused.unwrap_err().to_string(), "the view `seats` is not registered");
        }
        let error: ViewsError = CheckpointError::Invalid("pace").into();
        assert_eq!(error.to_string(), "invalid checkpoint request: pace");
        let error: ViewsError = PersistenceError::OptimisticLockError.into();
        assert!(!error.to_string().is_empty());
    }

    /// G-09: a commit wakes the processors of its aggregate type, which read the journal before their poll.
    #[tokio::test(start_paused = true)]
    async fn a_commit_wakes_the_processors_of_its_type_before_their_poll() {
        let config = Settings::default().processor;
        let hour = crate::cqrs::projection::processor::config::ProcessorConfig::new(
            Duration::from_secs(3600),
            config.page_size(),
            config.lease(),
            config.delivery_limits(),
        )
        .unwrap();
        let cqrs = Cqrs::in_memory()
            .settings(Settings {
                processor: hour,
                ..Settings::default()
            })
            .module(shows)
            .build()
            .unwrap();
        cqrs.catch_up().await;
        let start = tokio::time::Instant::now();
        book_every(&cqrs, &[("s-1", "A1", "fox")]).await;
        while cqrs.view::<Hall>().get(&"s-1").await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "woken, not polled: {:?}",
            start.elapsed()
        );
    }
}
