//! Materialized read models, their persisted event projection and the processor that feeds projections from
//! the journal.
pub mod checkpoint;
pub mod processor;
pub mod repository;
pub mod runtime;
pub mod verify;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{
    aggregate::Aggregate,
    event::EventEnvelope,
    event_handler::EventHandler,
    projection::repository::{StoredView, ViewContext, ViewRepository},
    store::{PersistenceError, PersistenceErrorObserver, checked_counter_add},
};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::{cmp::Ordering, fmt::Debug, marker::PhantomData, num::NonZeroUsize, sync::Arc};

/// A `View` represents a materialized view, generally serialized for persistence, that is updated by a projection.
/// This is the data of a CQRS read model.
///
/// `update` must be deterministic and free of side effects: a stored document is the fold of `update` over the
/// events `1..=last_sequence` of its aggregate, and two writers of one document (two processes, a rebuild next to
/// live delivery) meet at the same document only because the fold is the same.
pub trait View<A: Aggregate>: Debug + Default + Serialize + DeserializeOwned + Send + Sync {
    /// The name of the view: its collection or table, the name of its projection and of the projection's checkpoint
    /// row ([`CheckpointKey::of`](checkpoint::CheckpointKey::of)). One view per name; the name is stored, so it never
    /// changes.
    const NAME: &'static str;

    /// Each implemented view is responsible for updating its state based on events passed via
    /// this method.
    fn update(&mut self, event: &EventEnvelope<A>);
}

/// Builds and persists a materialized read model from committed events.
/// How many times a projection tries to write a document, the first attempt included, unless configured otherwise.
const DEFAULT_MAX_ATTEMPTS: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// Also provides read access to the stored view.
///
/// A document takes only the next event of its aggregate: events it has applied are skipped, a gap fails the
/// batch before anything is written (`PersistenceError::SequenceGap`). Delivery may therefore repeat events and
/// come from several processes without damaging the document. The projection is named after its view
/// ([`View::NAME`]).
pub struct ViewProjection<R, V, A>
where
    R: ViewRepository<V, A>,
    V: View<A>,
    A: Aggregate,
{
    view_repository: Arc<R>,
    error_observer: Option<Box<PersistenceErrorObserver>>,
    max_attempts: NonZeroUsize,
    phantom: PhantomData<(V, A)>,
}

impl<R, V, A> ViewProjection<R, V, A>
where
    R: ViewRepository<V, A>,
    V: View<A>,
    A: Aggregate,
{
    /// Creates a new `ViewProjection` using the provided `ViewRepository`.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::event::DomainEvent;
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// # use cqrsx::cqrs::projection::repository::StoredView;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl DomainEvent for MyEvents {
    /// #     fn event_type(&self) -> String {
    /// #         format!("{:?}", &self)
    /// #     }
    /// #     fn event_version(&self) -> String {
    /// #         "0.1.0".to_string()
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # pub enum MyCommands {
    /// #     DoSomething,
    /// #     BadCommand,
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyAggregate;
    /// #
    /// # impl Aggregate for MyAggregate {
    /// #     const TYPE: &'static str = "MyAggregate";
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct MyUserError(pub String);
    /// #
    /// # impl From<&str> for MyUserError {
    /// #     fn from(msg: &str) -> Self {
    /// #         Self(msg.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Clone, Default)]
    /// # pub struct MyService;
    /// #
    /// # #[derive(Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyView;
    /// #
    /// # impl View<MyAggregate> for MyView {
    /// #     const NAME: &'static str = "my_views";
    /// #     fn update(&mut self, _event: &EventEnvelope<MyAggregate>) {
    /// #         todo!()
    /// #     }
    /// # }
    /// #
    /// # pub struct MyDatabaseConnection;
    /// #
    /// # pub struct MyViewRepository;
    /// #
    /// # impl MyViewRepository {
    /// #     pub fn new(_db: MyDatabaseConnection) -> Self {
    /// #         Self
    /// #     }
    /// # }
    /// #
    /// # impl ViewRepository<MyView, MyAggregate> for MyViewRepository {
    /// #     async fn load(&self, _view_id: &str) -> Result<Option<MyView>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn load_with_context(
    /// #         &self,
    /// #         _view_id: &str,
    /// #     ) -> Result<Option<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn update_view(
    /// #         &self,
    /// #         _view: MyView,
    /// #         _context: ViewContext,
    /// #     ) -> Result<(), PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn list(&self) -> Result<Vec<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn range(&self, _lower: &str, _upper: &str) -> Result<Vec<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyDatabaseConnection;
    /// # use example::MyView;
    /// # use example::MyViewRepository;
    /// # use std::sync::Arc;
    /// # use cqrsx::cqrs::projection::ViewProjection;
    /// # fn config(my_db_connection: MyDatabaseConnection) {
    /// let repo = Arc::new(MyViewRepository::new(my_db_connection));
    /// let projection = ViewProjection::<MyViewRepository, MyView, MyAggregate>::new(repo);
    /// # }
    /// ```
    pub fn new(view_repository: Arc<R>) -> Self {
        Self {
            view_repository,
            error_observer: None,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            phantom: PhantomData,
        }
    }
    /// Sets an observer of the projection's failures: it sees every error of [`load`](Self::load), of
    /// [`handle`](EventHandler::handle) (a read or a write of the view, a gap in the events, a version conflict once the
    /// retries are spent) and of [`rebuild`](RebuildableProjection::rebuild), before the error is returned. The
    /// observer only looks: the error is still the caller's `Err`, nothing is retried, suppressed or replaced. Without an
    /// observer the errors are returned all the same. A second call replaces the observer.
    ///
    /// _Example: an observer that logs every error._
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::event::DomainEvent;
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// # use cqrsx::cqrs::projection::repository::StoredView;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl DomainEvent for MyEvents {
    /// #     fn event_type(&self) -> String {
    /// #         format!("{:?}", &self)
    /// #     }
    /// #     fn event_version(&self) -> String {
    /// #         "0.1.0".to_string()
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # pub enum MyCommands {
    /// #     DoSomething,
    /// #     BadCommand,
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyAggregate;
    /// #
    /// # impl Aggregate for MyAggregate {
    /// #     const TYPE: &'static str = "MyAggregate";
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct MyUserError(pub String);
    /// #
    /// # impl From<&str> for MyUserError {
    /// #     fn from(msg: &str) -> Self {
    /// #         Self(msg.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Clone, Default)]
    /// # pub struct MyService;
    /// #
    /// # #[derive(Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyView;
    /// #
    /// # impl View<MyAggregate> for MyView {
    /// #     const NAME: &'static str = "my_views";
    /// #     fn update(&mut self, _event: &EventEnvelope<MyAggregate>) {
    /// #         todo!()
    /// #     }
    /// # }
    /// #
    /// # pub struct MyDatabaseConnection;
    /// #
    /// # pub struct MyViewRepository;
    /// #
    /// # impl MyViewRepository {
    /// #     pub fn new(_db: MyDatabaseConnection) -> Self {
    /// #         Self
    /// #     }
    /// # }
    /// #
    /// # impl ViewRepository<MyView, MyAggregate> for MyViewRepository {
    /// #     async fn load(&self, _view_id: &str) -> Result<Option<MyView>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn load_with_context(
    /// #         &self,
    /// #         _view_id: &str,
    /// #     ) -> Result<Option<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn update_view(
    /// #         &self,
    /// #         _view: MyView,
    /// #         _context: ViewContext,
    /// #     ) -> Result<(), PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn list(&self) -> Result<Vec<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn range(&self, _lower: &str, _upper: &str) -> Result<Vec<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyViewRepository;
    /// # use example::MyView;
    /// # use cqrsx::cqrs::projection::ViewProjection;
    /// # fn config(mut projection: ViewProjection<MyViewRepository,MyView,MyAggregate>) {
    /// projection.set_error_observer(Box::new(|error| tracing::error!(%error, "view projection failed")));
    /// # }
    /// ```
    pub fn set_error_observer(&mut self, error_observer: Box<PersistenceErrorObserver>) {
        self.error_observer = Some(error_observer);
    }

    /// Loads and deserializes a view based on the provided view id.
    /// Use this method to load a materialized view when requested by a user.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::event::DomainEvent;
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// # use cqrsx::cqrs::projection::repository::StoredView;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl DomainEvent for MyEvents {
    /// #     fn event_type(&self) -> String {
    /// #         format!("{:?}", &self)
    /// #     }
    /// #     fn event_version(&self) -> String {
    /// #         "0.1.0".to_string()
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # pub enum MyCommands {
    /// #     DoSomething,
    /// #     BadCommand,
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyAggregate;
    /// #
    /// # impl Aggregate for MyAggregate {
    /// #     const TYPE: &'static str = "MyAggregate";
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct MyUserError(pub String);
    /// #
    /// # impl From<&str> for MyUserError {
    /// #     fn from(msg: &str) -> Self {
    /// #         Self(msg.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Clone, Default)]
    /// # pub struct MyService;
    /// #
    /// # #[derive(Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyView;
    /// #
    /// # impl View<MyAggregate> for MyView {
    /// #     const NAME: &'static str = "my_views";
    /// #     fn update(&mut self, _event: &EventEnvelope<MyAggregate>) {
    /// #         todo!()
    /// #     }
    /// # }
    /// #
    /// # pub struct MyDatabaseConnection;
    /// #
    /// # pub struct MyViewRepository;
    /// #
    /// # impl MyViewRepository {
    /// #     pub fn new(_db: MyDatabaseConnection) -> Self {
    /// #         Self
    /// #     }
    /// # }
    /// #
    /// # impl ViewRepository<MyView, MyAggregate> for MyViewRepository {
    /// #     async fn load(&self, _view_id: &str) -> Result<Option<MyView>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn load_with_context(
    /// #         &self,
    /// #         _view_id: &str,
    /// #     ) -> Result<Option<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn update_view(
    /// #         &self,
    /// #         _view: MyView,
    /// #         _context: ViewContext,
    /// #     ) -> Result<(), PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn list(&self) -> Result<Vec<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn range(&self, _lower: &str, _upper: &str) -> Result<Vec<StoredView<MyView>>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyViewRepository;
    /// # use example::MyView;
    /// # use cqrsx::cqrs::projection::ViewProjection;
    /// # async fn config(mut projection: ViewProjection<MyViewRepository,MyView,MyAggregate>) {
    /// let view = projection.load("customer-B24DA0").await.unwrap();
    /// # }
    /// ```
    pub async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        self.view_repository
            .load_with_context(view_id)
            .await
            .map(|option| option.map(|stored| stored.view))
            .inspect_err(|error| self.handle_error(error))
    }

    /// Limit optimistic view retries, including the first attempt (default: 16).
    pub fn with_max_attempts(mut self, attempts: NonZeroUsize) -> Self {
        self.max_attempts = attempts;
        self
    }

    async fn load_or_default(&self, view_id: String) -> Result<(V, ViewContext), PersistenceError> {
        Ok(match self.view_repository.load_with_context(&view_id).await? {
            Some(StoredView {
                id,
                version,
                last_sequence,
                view,
            }) => (view, ViewContext::new(id, version, last_sequence)),
            None => (V::default(), ViewContext::new(view_id, 0, 0)),
        })
    }

    /// Applies the batch to the stored document under the contiguity rule and writes the document when
    /// something was applied. A version conflict reloads the document and runs the rule again against what
    /// the other writer left, up to `max_attempts` times.
    pub(crate) async fn apply_events(&self, view_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        let mut attempt = 1;
        loop {
            let (mut view, mut context) = self.load_or_default(view_id.to_string()).await?;
            if !apply_contiguous(&mut view, &mut context, events)? {
                return Ok(());
            }
            match self.view_repository.update_view(view, context).await {
                Err(PersistenceError::OptimisticLockError) if attempt < self.max_attempts.get() => tokio::task::yield_now().await,
                result => return result,
            }
            attempt += 1;
        }
    }

    /// Replaces the stored document with the fold of `history[..prefix]`, written with the version of the
    /// document as loaded; a conflict reloads and runs again, up to `max_attempts` times. A document that is
    /// ahead of the history is newer than the read and is refused (see [`RebuildableProjection::rebuild`]).
    pub(crate) async fn replace(
        &self,
        view_id: &str,
        history: &[EventEnvelope<A>],
        prefix: usize,
    ) -> Result<(), PersistenceError> {
        let Some(last) = history.last() else {
            return Ok(());
        };
        let Some(events) = history.get(..prefix).filter(|events| !events.is_empty()) else {
            return Err(PersistenceError::UnknownError(
                "rebuild prefix must lie within the history".into(),
            ));
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let (_, stored) = self.load_or_default(view_id.to_string()).await?;
            if stored.last_sequence > last.sequence {
                return Err(PersistenceError::OptimisticLockError);
            }
            let mut view = V::default();
            let mut context = ViewContext::new(view_id.to_string(), stored.version, 0);
            apply_contiguous(&mut view, &mut context, events)?;
            match self.view_repository.update_view(view, context).await {
                Err(PersistenceError::OptimisticLockError) if attempt < self.max_attempts.get() => tokio::task::yield_now().await,
                result => return result,
            }
        }
    }

    fn handle_error(&self, error: &PersistenceError) {
        if let Some(handler) = &self.error_observer {
            (handler)(error);
        }
    }
}

/// The contiguity rule: a document takes only the next event of its aggregate.
///
/// An event at or below `context.last_sequence` was applied before and is skipped; the event
/// `last_sequence + 1` is applied and advances `last_sequence`; an event above it is a gap, and nothing of
/// the batch may be written. Returns whether any event was applied.
fn apply_contiguous<V, A>(view: &mut V, context: &mut ViewContext, events: &[EventEnvelope<A>]) -> Result<bool, PersistenceError>
where
    V: View<A>,
    A: Aggregate,
{
    let mut applied = false;
    for event in events {
        let expected = checked_counter_add(context.last_sequence, 1)?;
        match event.sequence.cmp(&expected) {
            Ordering::Less => {}
            Ordering::Equal => {
                view.update(event);
                context.last_sequence = event.sequence;
                applied = true;
            }
            Ordering::Greater => {
                return Err(PersistenceError::SequenceGap {
                    expected,
                    found: event.sequence,
                });
            }
        }
    }
    Ok(applied)
}

#[async_trait]
impl<R, V, A> EventHandler<A> for ViewProjection<R, V, A>
where
    R: ViewRepository<V, A>,
    V: View<A>,
    A: Aggregate,
{
    /// The name of the view, [`View::NAME`]: what the processor's failure reports name.
    fn name(&self) -> String {
        V::NAME.to_owned()
    }

    async fn handle(&self, view_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        self.apply_events(view_id, events)
            .await
            .inspect_err(|error| self.handle_error(error))
    }
}

/// A projection that can replace a document with the fold of its aggregate's whole history: what a
/// projection processor runs for one key on a rebuild of the journal, on a request for that key, and for a
/// document that refused a live event as a gap.
#[async_trait]
pub trait RebuildableProjection<A: Aggregate>: EventHandler<A> {
    /// Replaces the document of `aggregate_id` with the fold of `history[..prefix]`. `history` is the
    /// aggregate's complete history from its first event, as it was read; `0 < prefix <= history.len()`:
    /// the whole history for a rebuild in one write, `1` for a paced rebuild that then applies the rest
    /// event by event through `handle`. A document that is ahead of `history` (its `last_sequence` is above
    /// the last event) is newer than the read and is refused with `OptimisticLockError`, so that a reader
    /// whose history is stale never rolls a document back: the caller re-reads the history. An empty
    /// history writes nothing.
    async fn rebuild(&self, aggregate_id: &str, history: &[EventEnvelope<A>], prefix: usize) -> Result<(), PersistenceError>;
}

/// A shared projection rebuilds like the one instance, as it handles like it.
#[async_trait]
impl<A, P> RebuildableProjection<A> for Arc<P>
where
    A: Aggregate,
    P: RebuildableProjection<A> + ?Sized,
{
    async fn rebuild(&self, aggregate_id: &str, history: &[EventEnvelope<A>], prefix: usize) -> Result<(), PersistenceError> {
        (**self).rebuild(aggregate_id, history, prefix).await
    }
}

#[async_trait]
impl<R, V, A> RebuildableProjection<A> for ViewProjection<R, V, A>
where
    R: ViewRepository<V, A>,
    V: View<A>,
    A: Aggregate,
{
    async fn rebuild(&self, view_id: &str, history: &[EventEnvelope<A>], prefix: usize) -> Result<(), PersistenceError> {
        self.replace(view_id, history, prefix)
            .await
            .inspect_err(|error| self.handle_error(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::{
        projection::repository::StoredView,
        store::persisted::tests::{TestAggregate, TestEvents},
    };
    use serde::{Deserialize, Serialize};
    use std::{ops::RangeInclusive, sync::Mutex};

    /// The sequences applied, in order: equal to `1..=n` exactly when the document is a contiguous prefix.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    struct Projection(Vec<usize>);
    impl View<TestAggregate> for Projection {
        const NAME: &'static str = "halls";
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            self.0.push(event.sequence);
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Failure {
        /// Every load fails.
        Load,
        /// Every write conflicts.
        Update,
        /// The first write conflicts because another writer saved the prefix `1..=winner` meanwhile.
        ConflictOnce { winner: usize },
        /// The write lands, then the reply is lost.
        ReplyLost,
    }

    /// A repository with the version rules of the real ones, a call log and one scripted failure.
    #[derive(Default)]
    struct Repository {
        saved: Mutex<Option<(Projection, ViewContext)>>,
        calls: Mutex<Vec<String>>,
        failure: Mutex<Option<Failure>>,
    }
    impl Repository {
        fn seeded(id: &str, view: Projection, version: i64, last_sequence: usize) -> Arc<Self> {
            let repository = Self::default();
            *repository.saved.lock().unwrap() = Some((view, ViewContext::new(id.into(), version, last_sequence)));
            Arc::new(repository)
        }
        fn fail(self: &Arc<Self>, failure: Failure) -> Arc<Self> {
            *self.failure.lock().unwrap() = Some(failure);
            self.clone()
        }
        fn saved(&self) -> (Projection, ViewContext) {
            self.saved.lock().unwrap().clone().expect("a saved document")
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl ViewRepository<Projection, TestAggregate> for Repository {
        async fn load(&self, _: &str) -> Result<Option<Projection>, PersistenceError> {
            panic!("ViewProjection must retain the version with load_with_context")
        }
        async fn load_with_context(&self, id: &str) -> Result<Option<StoredView<Projection>>, PersistenceError> {
            self.calls.lock().unwrap().push(format!("load:{id}"));
            if *self.failure.lock().unwrap() == Some(Failure::Load) {
                return Err(PersistenceError::ConnectionError("load failed".into()));
            }
            Ok(self.saved.lock().unwrap().clone().map(|(view, context)| StoredView {
                id: context.view_instance_id,
                version: context.version,
                last_sequence: context.last_sequence,
                view,
            }))
        }
        async fn update_view(&self, view: Projection, context: ViewContext) -> Result<(), PersistenceError> {
            self.calls.lock().unwrap().push(format!(
                "update:{}:{}:{}",
                context.view_instance_id, context.version, context.last_sequence
            ));
            let failure = *self.failure.lock().unwrap();
            match failure {
                Some(Failure::ConflictOnce { winner }) => {
                    *self.failure.lock().unwrap() = None;
                    *self.saved.lock().unwrap() = Some((
                        Projection((1..=winner).collect()),
                        ViewContext::new(context.view_instance_id, context.version + 1, winner),
                    ));
                    Err(PersistenceError::OptimisticLockError)
                }
                Some(Failure::Update) => Err(PersistenceError::OptimisticLockError),
                _ => {
                    *self.saved.lock().unwrap() = Some((
                        view,
                        ViewContext::new(context.view_instance_id, context.version + 1, context.last_sequence),
                    ));
                    match failure {
                        Some(Failure::ReplyLost) => Err(PersistenceError::ConnectionError("reply lost after view write".into())),
                        _ => Ok(()),
                    }
                }
            }
        }
        async fn list(&self) -> Result<Vec<StoredView<Projection>>, PersistenceError> {
            panic!("ViewProjection never lists the documents")
        }
        async fn range(&self, _: &str, _: &str) -> Result<Vec<StoredView<Projection>>, PersistenceError> {
            panic!("ViewProjection never lists the documents")
        }
    }
    fn event(sequence: usize) -> EventEnvelope<TestAggregate> {
        EventEnvelope {
            aggregate_id: "source".into(),
            sequence,
            payload: TestEvents::SomethingWasDone,
            metadata: Default::default(),
        }
    }
    fn events(range: RangeInclusive<usize>) -> Vec<EventEnvelope<TestAggregate>> {
        range.map(event).collect()
    }
    fn prefix(range: RangeInclusive<usize>) -> Projection {
        Projection(range.collect())
    }
    fn context(version: i64, last_sequence: usize) -> ViewContext {
        ViewContext::new("view".into(), version, last_sequence)
    }

    #[test]
    fn a_projection_is_named_after_its_view() {
        let projection = ViewProjection::<_, Projection, TestAggregate>::new(Arc::new(Repository::default()));
        assert_eq!(projection.name(), "halls");
    }

    #[tokio::test]
    async fn a_new_document_starts_at_sequence_one_and_grows_in_batch_order() {
        let repo = Arc::new(Repository::default());
        let projection = ViewProjection::new(repo.clone());
        assert_eq!(projection.load("view").await.unwrap(), None);
        projection.handle("view", &events(1..=3)).await.unwrap();
        assert_eq!(projection.load("view").await.unwrap(), Some(prefix(1..=3)));
        projection.handle("view", &events(4..=4)).await.unwrap();
        assert_eq!(repo.saved(), (prefix(1..=4), context(2, 4)));
        assert_eq!(
            repo.calls(),
            [
                "load:view",
                "load:view",
                "update:view:0:3",
                "load:view",
                "load:view",
                "update:view:1:4"
            ]
        );
    }

    #[tokio::test]
    async fn a_new_document_refuses_a_batch_that_does_not_start_at_one() {
        let repo = Arc::new(Repository::default());
        let projection = ViewProjection::new(repo.clone());
        assert!(matches!(
            projection.handle("view", &events(2..=3)).await,
            Err(PersistenceError::SequenceGap { expected: 1, found: 2 })
        ));
        assert!(repo.saved.lock().unwrap().is_none());
        assert_eq!(repo.calls(), ["load:view"]);
    }

    #[tokio::test]
    async fn an_already_applied_batch_writes_nothing() {
        let repo = Repository::seeded("view", prefix(1..=5), 3, 5);
        let projection = ViewProjection::new(repo.clone());
        for batch in [events(1..=5), events(3..=5), events(5..=5), Vec::new()] {
            projection.handle("view", &batch).await.unwrap();
        }
        assert_eq!(repo.saved(), (prefix(1..=5), context(3, 5)));
        assert_eq!(repo.calls(), ["load:view"; 4]);
    }

    #[tokio::test]
    async fn a_batch_overlapping_the_document_applies_only_the_new_tail() {
        let repo = Repository::seeded("view", prefix(1..=5), 3, 5);
        let projection = ViewProjection::new(repo.clone());
        projection.handle("view", &events(3..=7)).await.unwrap();
        assert_eq!(repo.saved(), (prefix(1..=7), context(4, 7)));
        assert_eq!(repo.calls(), ["load:view", "update:view:3:7"]);
    }

    #[tokio::test]
    async fn a_gap_fails_without_writing() {
        let repo = Repository::seeded("view", prefix(1..=5), 3, 5);
        let mut projection = ViewProjection::new(repo.clone());
        let errors = Arc::new(Mutex::new(Vec::new()));
        projection.set_error_observer(Box::new({
            let errors = errors.clone();
            move |e| errors.lock().unwrap().push(e.to_string())
        }));
        assert!(matches!(
            projection.handle("view", &events(7..=8)).await,
            Err(PersistenceError::SequenceGap { expected: 6, found: 7 })
        ));
        assert_eq!(repo.saved(), (prefix(1..=5), context(3, 5)));
        assert_eq!(repo.calls(), ["load:view"]);
        assert_eq!(*errors.lock().unwrap(), ["sequence gap: expected event 6, found 7"]);
    }

    #[tokio::test]
    async fn a_gap_inside_the_batch_writes_nothing() {
        let repo = Repository::seeded("view", prefix(1..=5), 3, 5);
        let projection = ViewProjection::new(repo.clone());
        assert!(matches!(
            projection.handle("view", &[event(6), event(8), event(9)]).await,
            Err(PersistenceError::SequenceGap { expected: 7, found: 8 })
        ));
        assert_eq!(repo.saved(), (prefix(1..=5), context(3, 5)));
        assert_eq!(repo.calls(), ["load:view"]);
    }

    #[tokio::test]
    async fn a_version_conflict_rereads_and_skips_what_the_winner_applied() {
        // The winner applied part of the batch: the retry writes the tail.
        let repo = Repository::seeded("view", prefix(1..=7), 1, 7).fail(Failure::ConflictOnce { winner: 8 });
        ViewProjection::new(repo.clone())
            .handle("view", &events(8..=9))
            .await
            .unwrap();
        assert_eq!(repo.saved(), (prefix(1..=9), context(3, 9)));
        assert_eq!(repo.calls(), ["load:view", "update:view:1:9", "load:view", "update:view:2:9"]);

        // The winner applied the whole batch: the retry writes nothing.
        let repo = Repository::seeded("view", prefix(1..=7), 1, 7).fail(Failure::ConflictOnce { winner: 9 });
        ViewProjection::new(repo.clone())
            .handle("view", &events(8..=9))
            .await
            .unwrap();
        assert_eq!(repo.saved(), (prefix(1..=9), context(2, 9)));
        assert_eq!(repo.calls(), ["load:view", "update:view:1:9", "load:view"]);
    }

    #[tokio::test]
    async fn a_document_at_the_sequence_limit_cannot_grow() {
        let repo = Repository::seeded("view", Projection(vec![]), 1, usize::MAX);
        assert!(matches!(
            ViewProjection::new(repo.clone()).handle("view", &events(1..=1)).await,
            Err(PersistenceError::UnknownError(_))
        ));
        assert_eq!(repo.calls(), ["load:view"]);
    }

    #[tokio::test]
    async fn failures_are_reported_once_and_never_save_partial_views() {
        for handler in [false, true] {
            for failure in [Failure::Load, Failure::Update] {
                let repo = Repository::seeded("stored-context", prefix(1..=7), 9, 7).fail(failure);
                let mut projection = ViewProjection::new(repo.clone()).with_max_attempts(NonZeroUsize::new(3).unwrap());
                let errors = Arc::new(Mutex::new(Vec::new()));
                if handler {
                    let errors = errors.clone();
                    projection.set_error_observer(Box::new(move |e| errors.lock().unwrap().push(e.to_string())));
                }
                assert!(projection.handle("requested-view", &events(8..=9)).await.is_err());
                assert_eq!(repo.saved(), (prefix(1..=7), ViewContext::new("stored-context".into(), 9, 7)));
                assert_eq!(errors.lock().unwrap().len(), usize::from(handler));
                assert_eq!(
                    repo.calls(),
                    if failure == Failure::Load {
                        vec!["load:requested-view"]
                    } else {
                        ["load:requested-view", "update:stored-context:9:9"].repeat(3)
                    }
                );
            }
        }
    }

    #[tokio::test]
    async fn load_failure_returns_error_with_or_without_handler() {
        for handler in [false, true] {
            let repo = Arc::new(Repository::default()).fail(Failure::Load);
            let mut projection = ViewProjection::<_, Projection, TestAggregate>::new(repo);
            let errors = Arc::new(Mutex::new(Vec::new()));
            if handler {
                let errors = errors.clone();
                projection.set_error_observer(Box::new(move |e| errors.lock().unwrap().push(e.to_string())));
            }
            assert!(matches!(
                projection.load("missing").await,
                Err(PersistenceError::ConnectionError(_))
            ));
            assert_eq!(*errors.lock().unwrap(), if handler { vec!["load failed"] } else { vec![] });
        }
    }

    #[tokio::test]
    async fn default_limit_is_sixteen_including_first_attempt() {
        let repo = Arc::new(Repository::default()).fail(Failure::Update);
        assert!(matches!(
            ViewProjection::new(repo.clone()).handle("view", &events(1..=1)).await,
            Err(PersistenceError::OptimisticLockError)
        ));
        assert_eq!(repo.calls(), ["load:view", "update:view:0:1"].repeat(16));
        assert!(repo.saved.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_technical_error_after_a_real_view_write_is_never_retried() {
        let repo = Arc::new(Repository::default()).fail(Failure::ReplyLost);
        assert!(matches!(
            ViewProjection::new(repo.clone()).handle("view", &events(1..=2)).await,
            Err(PersistenceError::ConnectionError(_))
        ));
        assert_eq!(repo.saved(), (prefix(1..=2), context(1, 2)));
        assert_eq!(repo.calls(), ["load:view", "update:view:0:2"]);
    }

    mod rebuild {
        use super::*;

        #[tokio::test]
        async fn a_rebuild_replaces_a_document_of_any_version_with_the_fold_of_the_history() {
            let repo = Repository::seeded("view", prefix(1..=3), 7, 3);
            ViewProjection::new(repo.clone())
                .rebuild("view", &events(1..=5), 5)
                .await
                .unwrap();
            assert_eq!(repo.saved(), (prefix(1..=5), context(8, 5)));
            assert_eq!(repo.calls(), ["load:view", "update:view:7:5"]);
            // A missing document is created from the history.
            let repo = Arc::new(Repository::default());
            ViewProjection::new(repo.clone())
                .rebuild("view", &events(1..=2), 2)
                .await
                .unwrap();
            assert_eq!(repo.saved(), (prefix(1..=2), context(1, 2)));
        }

        #[tokio::test]
        async fn a_paced_rebuild_starts_the_document_at_the_first_event() {
            let repo = Repository::seeded("view", prefix(1..=3), 7, 3);
            ViewProjection::new(repo.clone())
                .rebuild("view", &events(1..=5), 1)
                .await
                .unwrap();
            assert_eq!(repo.saved(), (prefix(1..=1), context(8, 1)));
        }

        #[tokio::test]
        async fn an_empty_history_rebuilds_nothing_and_a_prefix_outside_the_history_is_refused() {
            let repo = Repository::seeded("view", prefix(1..=3), 7, 3);
            let projection = ViewProjection::new(repo.clone());
            projection.rebuild("view", &[], 1).await.unwrap();
            for bound in [0, 6] {
                assert!(matches!(
                    projection.rebuild("view", &events(1..=5), bound).await,
                    Err(PersistenceError::UnknownError(_))
                ));
            }
            assert!(repo.calls().is_empty());
            assert_eq!(repo.saved(), (prefix(1..=3), context(7, 3)));
        }

        #[tokio::test]
        async fn a_document_ahead_of_the_history_is_refused_without_a_write() {
            let repo = Repository::seeded("view", prefix(1..=7), 2, 7);
            assert!(matches!(
                ViewProjection::new(repo.clone()).rebuild("view", &events(1..=5), 5).await,
                Err(PersistenceError::OptimisticLockError)
            ));
            assert_eq!(repo.calls(), ["load:view"]);
            assert_eq!(repo.saved(), (prefix(1..=7), context(2, 7)));
            // The document moved ahead between the first attempt and the retry of its conflict.
            let repo = Repository::seeded("view", prefix(1..=3), 1, 3).fail(Failure::ConflictOnce { winner: 6 });
            assert!(matches!(
                ViewProjection::new(repo.clone()).rebuild("view", &events(1..=5), 5).await,
                Err(PersistenceError::OptimisticLockError)
            ));
            assert_eq!(repo.calls(), ["load:view", "update:view:1:5", "load:view"]);
            assert_eq!(repo.saved(), (prefix(1..=6), context(2, 6)), "the winner's document stays");
        }

        #[tokio::test]
        async fn a_conflict_with_an_older_document_is_retried_and_a_gap_in_the_history_writes_nothing() {
            // The winner wrote a shorter prefix: the retry replaces it.
            let repo = Repository::seeded("view", prefix(1..=3), 1, 3).fail(Failure::ConflictOnce { winner: 4 });
            ViewProjection::new(repo.clone())
                .rebuild("view", &events(1..=5), 5)
                .await
                .unwrap();
            assert_eq!(repo.saved(), (prefix(1..=5), context(3, 5)));
            assert_eq!(repo.calls(), ["load:view", "update:view:1:5", "load:view", "update:view:2:5"]);
            let repo = Repository::seeded("view", prefix(1..=3), 1, 3).fail(Failure::Update);
            let mut projection = ViewProjection::new(repo.clone()).with_max_attempts(NonZeroUsize::new(2).unwrap());
            let errors = Arc::new(Mutex::new(Vec::new()));
            projection.set_error_observer(Box::new({
                let errors = errors.clone();
                move |e| errors.lock().unwrap().push(e.to_string())
            }));
            assert!(matches!(
                projection.rebuild("view", &events(1..=5), 5).await,
                Err(PersistenceError::OptimisticLockError)
            ));
            assert_eq!(repo.calls(), ["load:view", "update:view:1:5"].repeat(2));
            assert!(matches!(
                projection.rebuild("view", &[event(1), event(2), event(4)], 3).await,
                Err(PersistenceError::SequenceGap { expected: 3, found: 4 })
            ));
            assert_eq!(
                *errors.lock().unwrap(),
                ["optimistic lock error", "sequence gap: expected event 3, found 4"]
            );
            assert_eq!(repo.saved(), (prefix(1..=3), context(1, 3)));
        }

        #[tokio::test]
        async fn a_shared_projection_rebuilds_like_the_one_instance() {
            let repo = Repository::seeded("view", prefix(1..=3), 7, 3);
            let shared: Arc<dyn RebuildableProjection<TestAggregate>> = Arc::new(ViewProjection::new(repo.clone()));
            let boxed: Box<dyn RebuildableProjection<TestAggregate>> = Box::new(shared.clone());
            assert_eq!(boxed.name(), "halls");
            boxed.rebuild("view", &events(1..=4), 4).await.unwrap();
            assert_eq!(repo.saved(), (prefix(1..=4), context(8, 4)));
        }
    }

    mod model {
        use super::*;
        use crate::cqrs::store::serialized_event::tests::property_config;
        use crate::cqrs::testing::mem_view_repository::MemViewRepository;
        use proptest::prelude::*;

        /// One delivery: `len` consecutive sequences starting `offset` away from the next expected event,
        /// with the event at index `hole` left out when there is one.
        #[derive(Clone, Debug)]
        struct Batch {
            offset: i64,
            len: usize,
            hole: Option<usize>,
        }

        fn batch() -> impl Strategy<Value = Batch> {
            (-3i64..=3, 1usize..=4, proptest::option::weighted(0.2, 0usize..4)).prop_map(|(offset, len, hole)| Batch {
                offset,
                len,
                hole,
            })
        }

        /// Replays the batches against the model "the applied sequences are `1..=applied`" and checks the
        /// stored document after every delivery: same prefix, same `last_sequence`, one version per write.
        async fn check(batches: Vec<Batch>) -> Result<(), TestCaseError> {
            let repository = MemViewRepository::<Projection, TestAggregate>::default();
            let projection = ViewProjection::new(Arc::new(repository.clone()));
            let (mut applied, mut version) = (0usize, 0i64);
            for batch in batches {
                let start = usize::try_from((i64::try_from(applied).unwrap() + 1 + batch.offset).max(1)).unwrap();
                let mut sequences: Vec<usize> = (start..start + batch.len).collect();
                if let Some(hole) = batch.hole
                    && sequences.len() > 1
                    && hole < sequences.len()
                {
                    sequences.remove(hole);
                }
                let (mut next, mut gap) = (applied, None);
                for &sequence in &sequences {
                    if sequence <= next {
                        continue;
                    }
                    if sequence == next + 1 {
                        next += 1;
                    } else {
                        gap = Some((next + 1, sequence));
                        break;
                    }
                }
                let delivered: Vec<_> = sequences.iter().copied().map(event).collect();
                let result = projection.handle("view", &delivered).await;
                match gap {
                    Some((expected, found)) => prop_assert!(
                        matches!(result, Err(PersistenceError::SequenceGap { expected: e, found: f }) if e == expected && f == found),
                        "{sequences:?} after {applied}: {result:?}"
                    ),
                    None => {
                        prop_assert!(result.is_ok(), "{sequences:?} after {applied}: {result:?}");
                        if next > applied {
                            version += 1;
                            applied = next;
                        }
                    }
                }
                match repository.load_with_context("view").await.unwrap() {
                    None => prop_assert_eq!((applied, version), (0, 0)),
                    Some(stored) => {
                        prop_assert_eq!(stored.view.0, (1..=applied).collect::<Vec<_>>());
                        prop_assert_eq!((stored.version, stored.last_sequence), (version, applied));
                    }
                }
            }
            Ok(())
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config { cases: 256, ..property_config() })]
            #[test]
            fn property_view_documents_stay_a_contiguous_prefix(batches in proptest::collection::vec(batch(), 1..40)) {
                let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
                runtime.block_on(check(batches))?;
            }
        }
    }
}
