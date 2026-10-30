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
        store::{PersistenceError, feed::Position, repository::PersistedEventRepository},
        testing::mem_event_repository::MemEventRepository,
    },
    engine::Cqrs,
};
use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    fmt::{self, Display},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;

type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, PersistenceError>> + Send + 'a>>;
type CheckpointAnswer<'a, T> = Pin<Box<dyn Future<Output = Result<T, CheckpointError>> + Send + 'a>>;

/// The size of the journal of the views' aggregate types: their events and the aggregates that hold them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JournalTotals {
    pub events: u64,
    pub aggregates: u64,
}

/// What a journal counts of one aggregate type: the events above a position (how far a view is behind) and its size.
pub(crate) trait Counts: Send + Sync {
    fn count_after<A: Aggregate>(&self, after: Position, cap: u64) -> impl Future<Output = Result<u64, PersistenceError>> + Send;
    fn totals<A: Aggregate>(&self) -> impl Future<Output = Result<JournalTotals, PersistenceError>> + Send;
}

impl Counts for MemEventRepository {
    fn count_after<A: Aggregate>(&self, after: Position, cap: u64) -> impl Future<Output = Result<u64, PersistenceError>> + Send {
        MemEventRepository::count_after::<A>(self, after, cap)
    }

    async fn totals<A: Aggregate>(&self) -> Result<JournalTotals, PersistenceError> {
        let events = MemEventRepository::count_after::<A>(self, Position::START, u64::MAX).await?;
        let (mut aggregates, mut after) = (0, String::new());
        loop {
            let page = self.aggregate_ids_after::<A>(&after, 100).await?;
            let Some(last) = page.last() else {
                return Ok(JournalTotals { events, aggregates });
            };
            after = last.clone();
            aggregates += page.len() as u64;
        }
    }
}

#[cfg(feature = "postgres")]
impl Counts for crate::postgres::event_repository::PostgresEventRepository {
    fn count_after<A: Aggregate>(&self, after: Position, cap: u64) -> impl Future<Output = Result<u64, PersistenceError>> + Send {
        crate::postgres::event_repository::PostgresEventRepository::count_after::<A>(self, after, cap)
    }

    async fn totals<A: Aggregate>(&self) -> Result<JournalTotals, PersistenceError> {
        let totals = crate::postgres::event_repository::PostgresEventRepository::totals::<A>(self).await?;
        Ok(JournalTotals {
            events: totals.events,
            aggregates: totals.aggregates,
        })
    }
}

/// What the operations over the views ask of the checkpoints, the store erased.
pub(crate) trait CheckpointOps: Send + Sync {
    fn status<'a>(&'a self, projection: Option<&'a str>) -> CheckpointAnswer<'a, Vec<CheckpointStatus>>;
    fn request_rebuild<'a>(&'a self, key: &'a CheckpointKey, pace: Duration) -> CheckpointAnswer<'a, RebuildRequest>;
    fn request_rebuild_key<'a>(&'a self, key: &'a CheckpointKey, id: &'a str) -> CheckpointAnswer<'a, ()>;
}

impl<C: CheckpointStore + 'static> CheckpointOps for C {
    fn status<'a>(&'a self, projection: Option<&'a str>) -> CheckpointAnswer<'a, Vec<CheckpointStatus>> {
        Box::pin(CheckpointStore::status(self, projection))
    }

    fn request_rebuild<'a>(&'a self, key: &'a CheckpointKey, pace: Duration) -> CheckpointAnswer<'a, RebuildRequest> {
        Box::pin(CheckpointStore::request_rebuild(self, key, pace))
    }

    fn request_rebuild_key<'a>(&'a self, key: &'a CheckpointKey, id: &'a str) -> CheckpointAnswer<'a, ()> {
        Box::pin(CheckpointStore::request_rebuild_key(self, key, id))
    }
}

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
    fn count_after(&self, after: Position, cap: u64) -> Answer<'_, u64>;
    fn totals(&self) -> Answer<'_, JournalTotals>;
}

/// A view: its documents, the journal its documents are verified against and its size counted on (`history`), and
/// the journal its lag is counted on (`counter`).
struct Registered<V, A, D, H, K> {
    documents: Arc<D>,
    history: H,
    counter: K,
    types: std::marker::PhantomData<fn() -> (V, A)>,
}

impl<V, A, D, H, K> Queries<V> for Registered<V, A, D, H, K>
where
    V: View<A> + 'static,
    A: Aggregate + 'static,
    D: ViewRepository<V, A> + 'static,
    H: Send + Sync,
    K: Send + Sync,
{
    fn get<'a>(&'a self, id: &'a str) -> Answer<'a, Option<StoredView<V>>> {
        Box::pin(async move { self.documents.load_with_context(id).await })
    }

    fn list(&self) -> Answer<'_, Vec<StoredView<V>>> {
        Box::pin(self.documents.list())
    }

    fn range<'a>(&'a self, lower: &'a str, upper: &'a str) -> Answer<'a, Vec<StoredView<V>>> {
        Box::pin(self.documents.range(lower, upper))
    }
}

impl<V, A, D, H, K> Operations for Registered<V, A, D, H, K>
where
    V: View<A> + PartialEq + 'static,
    A: Aggregate + 'static,
    D: ViewRepository<V, A> + 'static,
    H: PersistedEventRepository + Counts + 'static,
    K: Counts + 'static,
{
    fn key(&self) -> CheckpointKey {
        CheckpointKey::of::<V, A>()
    }

    fn verify(&self) -> Answer<'_, Verification> {
        Box::pin(verify::<V, A>(&self.history, &*self.documents))
    }

    fn count_after(&self, after: Position, cap: u64) -> Answer<'_, u64> {
        Box::pin(self.counter.count_after::<A>(after, cap))
    }

    fn totals(&self) -> Answer<'_, JournalTotals> {
        Box::pin(self.history.totals::<A>())
    }
}

/// The views of an engine: the queries by the view's type, the operations by its name.
#[derive(Default)]
pub(crate) struct Registry {
    queries: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    operations: Vec<Arc<dyn Operations>>,
}

impl Registry {
    pub(crate) fn register<V, A, D, H, K>(&mut self, documents: Arc<D>, history: H, counter: K)
    where
        V: View<A> + PartialEq + 'static,
        A: Aggregate + 'static,
        D: ViewRepository<V, A> + 'static,
        H: PersistedEventRepository + Counts + 'static,
        K: Counts + 'static,
    {
        let view = Arc::new(Registered::<V, A, D, H, K> {
            documents,
            history,
            counter,
            types: std::marker::PhantomData,
        });
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

// By hand: the queries are a trait object; what matters is whether the engine has the view.
impl<V> fmt::Debug for ViewQuery<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewQuery")
            .field("registered", &self.queries.is_some())
            .finish_non_exhaustive()
    }
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
    #[error("no view of the aggregate type `{0}` is registered")]
    UnknownAggregateType(String),
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

// By hand: the engine is not printed with its views, only their keys.
impl fmt::Debug for Views<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Views").field("keys", &self.keys()).finish_non_exhaustive()
    }
}

impl Views<'_> {
    /// The rows of the views' checkpoints, in the order the modules registered the views: the projection (the view's
    /// name) and the aggregate type of each.
    pub fn keys(&self) -> Vec<CheckpointKey> {
        self.cqrs.views.operations.iter().map(|view| view.key()).collect()
    }

    /// The checkpoint rows: positions, owners, leases, failures and rebuilds.
    pub async fn status(&self) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        self.cqrs.checkpoints.status(None).await
    }

    /// Asks the processor of the view `name` to rebuild every document, `pace` between the events of a key.
    pub async fn rebuild(&self, name: &str, pace: Duration) -> Result<RebuildRequest, ViewsError> {
        let key = self.cqrs.views.named(name)?.key();
        Ok(self.cqrs.checkpoints.request_rebuild(&key, pace).await?)
    }

    /// Asks the processor of the view `name` to rebuild the document of the aggregate `id`.
    pub async fn rebuild_key(&self, name: &str, id: &str) -> Result<(), ViewsError> {
        let key = self.cqrs.views.named(name)?.key();
        Ok(self.cqrs.checkpoints.request_rebuild_key(&key, id).await?)
    }

    /// Every document of the view `name` against the fold of the part of its history it applied (D-20).
    pub async fn verify(&self, name: &str) -> Result<Verification, ViewsError> {
        Ok(self.cqrs.views.named(name)?.verify().await?)
    }

    /// The events of its aggregate type above the checkpoint of the view `name`, counted up to the lag cap of the
    /// settings.
    pub async fn lag(&self, name: &str) -> Result<u64, ViewsError> {
        let view = self.cqrs.views.named(name)?;
        let rows = self.cqrs.checkpoints.status(Some(name)).await?;
        let after = rows.first().map_or(Position::START, |row| row.position);
        Ok(view.count_after(after, self.cqrs.settings.lag_cap()).await?)
    }

    /// The events of `aggregate_type` above `after`, counted up to `cap`: how far a view of the type whose checkpoint
    /// stands at `after` is behind.
    pub async fn count_after(&self, aggregate_type: &str, after: Position, cap: u64) -> Result<u64, ViewsError> {
        let view = self.of_type(aggregate_type)?;
        Ok(view.count_after(after, cap).await?)
    }

    /// The size of the journals of the views' aggregate types: their events and aggregates, each type once.
    pub async fn totals(&self) -> Result<JournalTotals, ViewsError> {
        let mut counted = Vec::new();
        let mut totals = JournalTotals::default();
        for view in &self.cqrs.views.operations {
            let aggregate_type = view.key().aggregate_type();
            if counted.contains(&aggregate_type) {
                continue;
            }
            counted.push(aggregate_type);
            let of_type = view.totals().await?;
            totals.events += of_type.events;
            totals.aggregates += of_type.aggregates;
        }
        Ok(totals)
    }

    fn of_type(&self, aggregate_type: &str) -> Result<&dyn Operations, ViewsError> {
        let views = &self.cqrs.views.operations;
        let view = views.iter().find(|view| view.key().aggregate_type() == aggregate_type);
        view.map(|view| &**view)
            .ok_or_else(|| ViewsError::UnknownAggregateType(aggregate_type.to_owned()))
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
    use crate::engine::{settings::Settings, test_support::*};

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
        let (cqrs, _) = engine().await;
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
        let (cqrs, _) = engine().await;
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
        let config = Settings::default().processor();
        let hour = crate::cqrs::projection::processor::config::ProcessorConfig::try_new(
            Duration::from_secs(3600),
            config.page_size(),
            config.lease(),
            config.delivery_limits(),
        )
        .unwrap();
        let defaults = Settings::default();
        let settings = Settings::try_new(
            defaults.snapshot_every(),
            defaults.lock_limits(),
            hour,
            defaults.task_runtime().clone(),
        )
        .unwrap();
        let cqrs = Cqrs::with_settings(settings).module(shows).build().await.unwrap();
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

    #[tokio::test]
    async fn the_views_name_their_rows_count_their_journal_and_its_size() {
        let (cqrs, _) = engine().await;
        book_every(&cqrs, &[("s-1", "A1", "fox"), ("s-1", "A2", "panda"), ("s-2", "A1", "fox")]).await;
        let views = cqrs.views();
        let keys: Vec<(String, &str)> = views
            .keys()
            .iter()
            .map(|key| (key.projection().to_owned(), key.aggregate_type()))
            .collect();
        assert_eq!(keys, [("halls".to_owned(), "show"), ("tickets".to_owned(), "wallet")]);
        assert_eq!(views.count_after("show", Position::START, 10).await.unwrap(), 3);
        assert_eq!(
            views.count_after("show", Position::START, 2).await.unwrap(),
            2,
            "up to the cap"
        );
        assert_eq!(views.count_after("wallet", Position::START, 10).await.unwrap(), 0);
        assert_eq!(
            views.count_after("seat", Position::START, 10).await.unwrap_err().to_string(),
            "no view of the aggregate type `seat` is registered"
        );
        assert_eq!(
            views.totals().await.unwrap(),
            JournalTotals {
                events: 3,
                aggregates: 2
            },
            "the shows, the wallets have no event yet"
        );
        cqrs.run_tasks().await;
        assert_eq!(
            views.totals().await.unwrap(),
            JournalTotals {
                events: 5,
                aggregates: 4
            },
            "every aggregate type of a view once: fox's second A1 is recorded already"
        );
    }
}
