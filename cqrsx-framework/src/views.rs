//! The views of a framework: the queries of one view ([`ViewQuery`]) and the operations over all of them ([`Views`]).
//! The rebuild of one document, the verification, the counts of the journal and the rebuild requests need the feature
//! `operation-utils`.
use crate::Cqrsx;
#[cfg(feature = "operation-utils")]
use crate::operation_utils::{Maintained, Maintenance, Totals};
use cqrsx::{
    domain::aggregate::Aggregate,
    journal::{PersistenceError, feed::Position, repository::EventRepository},
    projection::checkpoint::{CheckpointError, CheckpointKey, CheckpointStatus, CheckpointStore},
    view::{
        View,
        repository::{StoredView, ViewRepository},
    },
};
use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    fmt::{self, Display},
    future::Future,
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;

pub(crate) type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, PersistenceError>> + Send + 'a>>;
pub(crate) type CheckpointAnswer<'a, T> = Pin<Box<dyn Future<Output = Result<T, CheckpointError>> + Send + 'a>>;

/// The size of the journal of the views' aggregate types: their events and the aggregates that hold them
/// ([`Views::totals`]).
#[cfg(feature = "operation-utils")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JournalTotals {
    pub events: u64,
    pub aggregates: u64,
}

/// What a journal counts of one aggregate type: the events above a position (how far a view is behind) and its size.
#[cfg_attr(
    not(any(feature = "test-utils", feature = "projections-postgres", feature = "projections-mongodb")),
    expect(dead_code, reason = "only a view store counts a journal for a view")
)]
pub(crate) trait Counts: Send + Sync {
    fn count_after<A: Aggregate>(&self, after: Position, cap: u64) -> impl Future<Output = Result<u64, PersistenceError>> + Send;
}

/// The journal of whole histories a view is verified against and its size counted on: with the feature
/// `operation-utils` it also counts its size ([`Totals`]).
#[cfg(feature = "operation-utils")]
pub(crate) trait History: EventRepository + Totals {}
#[cfg(feature = "operation-utils")]
impl<H: EventRepository + Totals> History for H {}
/// The journal of whole histories a view is verified against: only the maintenance of a view reads it.
#[cfg(not(feature = "operation-utils"))]
pub(crate) trait History: EventRepository {}
#[cfg(not(feature = "operation-utils"))]
impl<H: EventRepository> History for H {}

/// What the operations over the views ask of the checkpoints, the store erased.
pub(crate) trait CheckpointOperations: Send + Sync {
    fn status<'a>(&'a self, projection: Option<&'a str>) -> CheckpointAnswer<'a, Vec<CheckpointStatus>>;
}

impl<C: CheckpointStore + 'static> CheckpointOperations for C {
    fn status<'a>(&'a self, projection: Option<&'a str>) -> CheckpointAnswer<'a, Vec<CheckpointStatus>> {
        Box::pin(CheckpointStore::status(self, projection))
    }
}

/// The queries of a view, its aggregate type erased.
pub(crate) trait Queries<V>: Send + Sync {
    fn get<'a>(&'a self, id: &'a str) -> Answer<'a, Option<StoredView<V>>>;
    fn list(&self) -> Answer<'_, Vec<StoredView<V>>>;
    fn range<'a>(&'a self, lower: &'a str, upper: &'a str) -> Answer<'a, Vec<StoredView<V>>>;
}

/// What the operations need of a view, its types erased. Its name and aggregate type are constants: finding a view by
/// either allocates nothing; the key of its checkpoint is built only for a request.
pub(crate) trait Operations: Send + Sync {
    fn name(&self) -> &'static str;
    #[cfg(feature = "operation-utils")]
    fn aggregate_type(&self) -> &'static str;
    fn key(&self) -> CheckpointKey;
    /// The checkpoints of the view's processor, the store erased: what the operations read and request.
    fn checkpoints(&self) -> &dyn CheckpointOperations;
    fn count_after(&self, after: Position, cap: u64) -> Answer<'_, u64>;
}

/// A view: its documents, the checkpoints of its processor and the journal its lag is counted on (`counter`).
#[cfg_attr(
    not(any(feature = "test-utils", feature = "projections-postgres", feature = "projections-mongodb")),
    expect(dead_code, reason = "only a view store registers a view")
)]
struct Registered<V, A, D, K> {
    documents: Arc<D>,
    checkpoints: Arc<dyn CheckpointOperations>,
    counter: K,
    types: std::marker::PhantomData<fn() -> (V, A)>,
}

impl<V, A, D, K> Queries<V> for Registered<V, A, D, K>
where
    V: View<A> + 'static,
    A: Aggregate + 'static,
    D: ViewRepository<V, A> + 'static,
    K: Send + Sync,
{
    fn get<'a>(&'a self, id: &'a str) -> Answer<'a, Option<StoredView<V>>> {
        Box::pin(async move { self.documents.load_stored(id).await })
    }

    fn list(&self) -> Answer<'_, Vec<StoredView<V>>> {
        Box::pin(self.documents.list())
    }

    fn range<'a>(&'a self, lower: &'a str, upper: &'a str) -> Answer<'a, Vec<StoredView<V>>> {
        Box::pin(self.documents.range(lower, upper))
    }
}

impl<V, A, D, K> Operations for Registered<V, A, D, K>
where
    V: View<A> + PartialEq + 'static,
    A: Aggregate + 'static,
    D: ViewRepository<V, A> + 'static,
    K: Counts + 'static,
{
    fn name(&self) -> &'static str {
        V::NAME
    }

    #[cfg(feature = "operation-utils")]
    fn aggregate_type(&self) -> &'static str {
        A::TYPE
    }

    fn key(&self) -> CheckpointKey {
        CheckpointKey::of::<V, A>()
    }

    fn checkpoints(&self) -> &dyn CheckpointOperations {
        &*self.checkpoints
    }

    fn count_after(&self, after: Position, cap: u64) -> Answer<'_, u64> {
        Box::pin(self.counter.count_after::<A>(after, cap))
    }
}

/// The views of a framework: the queries by the view's type, the operations by its name.
#[derive(Default)]
pub(crate) struct Registry {
    queries: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    pub(crate) operations: Vec<Arc<dyn Operations>>,
    /// The maintenance of each view, by the view's type.
    #[cfg(feature = "operation-utils")]
    pub(crate) maintenance: Vec<(TypeId, Arc<dyn Maintenance>)>,
}

impl Registry {
    #[cfg_attr(
        not(any(feature = "test-utils", feature = "projections-postgres", feature = "projections-mongodb")),
        expect(dead_code, reason = "only a view store registers a view")
    )]
    #[cfg_attr(
        not(feature = "operation-utils"),
        expect(
            unused_variables,
            clippy::needless_pass_by_value,
            reason = "only the maintenance of a view reads its history"
        )
    )]
    pub(crate) fn register<V, A, D, C, H, K>(&mut self, documents: Arc<D>, checkpoints: Arc<C>, history: H, counter: K)
    where
        V: View<A> + PartialEq + 'static,
        A: Aggregate + 'static,
        D: ViewRepository<V, A> + 'static,
        C: CheckpointStore + 'static,
        H: History + 'static,
        K: Counts + 'static,
    {
        #[cfg(feature = "operation-utils")]
        self.maintenance.push((
            TypeId::of::<V>(),
            Arc::new(Maintained::<V, A, D, C, H>::new(
                documents.clone(),
                checkpoints.clone(),
                history,
            )),
        ));
        let view = Arc::new(Registered::<V, A, D, K> {
            documents,
            checkpoints,
            counter,
            types: std::marker::PhantomData,
        });
        let queries: Arc<dyn Queries<V>> = view.clone();
        self.queries.insert(TypeId::of::<V>(), Box::new(queries));
        self.operations.push(view);
    }

    /// The queries of the view `V`, if it is registered.
    fn queries_of<V: 'static>(&self) -> Option<&dyn Queries<V>> {
        let queries = self.queries.get(&TypeId::of::<V>())?;
        queries.downcast_ref::<Arc<dyn Queries<V>>>().map(|queries| &**queries)
    }

    pub(crate) fn named(&self, name: &str) -> Result<&dyn Operations, ViewsError> {
        let view = self.operations.iter().find(|view| view.name() == name);
        view.map(|view| &**view).ok_or_else(|| ViewsError::Unknown(name.to_owned()))
    }
}

/// The queries of the view `V` ([`Cqrsx::view`]): a document by id, every document and a range of ids, ordered by the
/// bytes of the ids; with the feature `operation-utils` also the rebuild of one document. A view the framework does not
/// have answers with an error.
pub struct ViewQuery<'a, V> {
    pub(crate) registry: &'a Registry,
    types: std::marker::PhantomData<fn() -> V>,
}

// By hand: the queries are a trait object; what matters is whether the framework has the view.
impl<V: 'static> fmt::Debug for ViewQuery<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewQuery")
            .field("registered", &self.registry.queries_of::<V>().is_some())
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
        self.registry
            .queries_of::<V>()
            .ok_or_else(|| PersistenceError::Unexpected(unknown()))
    }
}

/// An operation over the views that could not run.
#[derive(Debug, Error)]
pub enum ViewsError {
    #[error("the view `{0}` is not registered")]
    Unknown(String),
    /// Only the operations of the feature `operation-utils` that count a journal by its aggregate type produce it.
    #[error("no view of the aggregate type `{0}` is registered")]
    UnknownAggregateType(String),
    /// The view's processor has not registered its checkpoint yet (it is starting, or the framework's processors are
    /// not running): a rebuild request (feature `operation-utils`) has no row to wait on. Repeat it later.
    #[error("the view's processor has not started: its checkpoint is not registered yet")]
    NotStarted,
    /// The checkpoint store failed or refused the request; never `NotRegistered`, which is `NotStarted`.
    #[error(transparent)]
    Checkpoint(CheckpointError),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

impl From<CheckpointError> for ViewsError {
    fn from(error: CheckpointError) -> Self {
        match error {
            CheckpointError::NotRegistered => Self::NotStarted,
            CheckpointError::Storage(_) | CheckpointError::Corrupt(_) | CheckpointError::Invalid(_) => Self::Checkpoint(error),
        }
    }
}

/// The operations over the views of a framework ([`Cqrsx::views`]), by a view's name: its checkpoint and how far it is
/// behind; with the feature `operation-utils` also the requests of a rebuild its processor takes, the verification of
/// its documents and the size of the journal.
pub struct Views<'a> {
    pub(crate) cqrsx: &'a Cqrsx,
}

// By hand: the framework is not printed with its views, only their keys.
impl fmt::Debug for Views<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Views").field("keys", &self.keys()).finish_non_exhaustive()
    }
}

impl Views<'_> {
    /// The rows of the views' checkpoints, in the order the modules registered the views: the projection (the view's
    /// name) and the aggregate type of each.
    pub fn keys(&self) -> Vec<CheckpointKey> {
        self.cqrsx.views.operations.iter().map(|view| view.key()).collect()
    }

    /// The checkpoint rows of the framework's views, in the order the modules registered them: positions, owners, leases,
    /// failures and rebuilds.
    pub async fn status(&self) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        let mut rows = Vec::new();
        for view in &self.cqrsx.views.operations {
            rows.extend(view.checkpoints().status(Some(view.name())).await?);
        }
        Ok(rows)
    }

    /// The events of its aggregate type above the checkpoint of the view `name`, counted up to the lag cap of the
    /// settings.
    pub async fn lag(&self, name: &str) -> Result<u64, ViewsError> {
        let view = self.cqrsx.views.named(name)?;
        let rows = view.checkpoints().status(Some(name)).await?;
        let after = rows.first().map_or(Position::START, |row| row.position);
        Ok(view.count_after(after, self.cqrsx.settings.lag_cap()).await?)
    }
}

impl Cqrsx {
    /// The queries of the view `V`.
    pub fn view<V: 'static>(&self) -> ViewQuery<'_, V> {
        ViewQuery {
            registry: &self.views,
            types: std::marker::PhantomData,
        }
    }

    pub fn views(&self) -> Views<'_> {
        Views { cqrsx: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory::InMemory, settings::Settings, test_support::*};
    use std::time::Duration;

    #[tokio::test]
    async fn a_view_answers_get_list_and_range_once_caught_up() {
        let (cqrsx, _) = framework().await;
        book_every(&cqrsx, &[("s-2", "A1", "fox"), ("s-1", "B2", "panda"), ("s-3", "C3", "fox")]).await;
        cqrsx.catch_up().await;
        cqrsx.catch_up().await;
        let halls = cqrsx.view::<Hall>();
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
        let unknown = cqrsx.view::<String>();
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
    async fn views_report_their_status_and_lag() {
        let (cqrsx, _) = framework().await;
        book_every(&cqrsx, &[("s-1", "A1", "fox"), ("s-1", "A2", "panda"), ("s-2", "A1", "fox")]).await;
        let views = cqrsx.views();
        assert_eq!(
            views.lag("halls").await.unwrap(),
            3,
            "nothing applied before the processors run"
        );
        assert!(views.status().await.unwrap().is_empty());
        cqrsx.catch_up().await;
        assert_eq!(views.lag("halls").await.unwrap(), 0);
        let rows = views.status().await.unwrap();
        let named: Vec<(&str, &str, bool)> = rows
            .iter()
            .map(|row| {
                (
                    row.projection.as_str(),
                    row.aggregate_type.as_str(),
                    row.owner.as_deref() == Some(&cqrsx.owner),
                )
            })
            .collect();
        assert_eq!(named, [("halls", "show", true), ("tickets", "wallet", true)]);
        assert_eq!(
            views.lag("seats").await.unwrap_err().to_string(),
            "the view `seats` is not registered"
        );
        let error: ViewsError = CheckpointError::Invalid("pace").into();
        assert_eq!(error.to_string(), "invalid checkpoint request: pace");
        assert!(matches!(error, ViewsError::Checkpoint(_)));
        let error: ViewsError = CheckpointError::NotRegistered.into();
        assert!(matches!(error, ViewsError::NotStarted), "{error:?}");
        let error: ViewsError = PersistenceError::Conflict.into();
        assert!(!error.to_string().is_empty());
    }

    /// G-09: a commit wakes the processors of its aggregate type, which read the journal before their poll.
    #[tokio::test(start_paused = true)]
    async fn a_commit_wakes_the_processors_of_its_type_before_their_poll() {
        let config = Settings::default().processor();
        let hour = cqrsx::projection::processor::config::ProcessorConfig::builder()
            .poll_interval(Duration::from_secs(3600))
            .page_size(config.page_size())
            .lease(config.lease())
            .delivery_limits(config.delivery_limits())
            .build()
            .unwrap();
        let defaults = Settings::default();
        let settings = Settings::try_new(
            defaults.snapshot_every(),
            defaults.lock_limits(),
            hour,
            defaults.task_runtime().clone(),
        )
        .unwrap();
        let cqrsx = Cqrsx::with_settings(settings)
            .event_store(InMemory)
            .projections(InMemory)
            .module(shows)
            .build()
            .await
            .unwrap();
        cqrsx.catch_up().await;
        let start = tokio::time::Instant::now();
        book_every(&cqrsx, &[("s-1", "A1", "fox")]).await;
        while cqrsx.view::<Hall>().get(&"s-1").await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "woken, not polled: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn the_views_name_their_rows() {
        let (cqrsx, _) = framework().await;
        book_every(&cqrsx, &[("s-1", "A1", "fox"), ("s-1", "A2", "panda"), ("s-2", "A1", "fox")]).await;
        let views = cqrsx.views();
        let keys: Vec<(String, &str)> = views
            .keys()
            .iter()
            .map(|key| (key.projection().to_owned(), key.aggregate_type()))
            .collect();
        assert_eq!(keys, [("halls".to_owned(), "show"), ("tickets".to_owned(), "wallet")]);
    }
}
