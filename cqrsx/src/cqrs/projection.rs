//! Materialized read models and their persisted event projection.
pub mod repository;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{
    aggregate::Aggregate,
    event::EventEnvelope,
    event_handler::EventHandler,
    projection::repository::{ViewContext, ViewRepository},
    store::{PersistenceError, PersistenceErrorObserver},
};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Debug, marker::PhantomData, num::NonZeroUsize, sync::Arc};

/// A `View` represents a materialized view, generally serialized for persistence, that is updated by a projection.
/// This is the data of a CQRS read model.
///
pub trait View<A: Aggregate>: Debug + Default + Serialize + DeserializeOwned + Send + Sync {
    /// Each implemented view is responsible for updating its state based on events passed via
    /// this method.
    fn update(&mut self, event: &EventEnvelope<A>);
}

/// Builds and persists a materialized read model from committed events.
/// Also provides read access to the stored view.
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
    /// # use cqrsx::cqrs::event_sink::EventSink;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::stream::ReplayStream;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
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
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Services = MyService;
    /// #
    /// #     async fn handle(
    /// #         &mut self,
    /// #         command: Self::Command,
    /// #         _service: &Self::Services,
    /// #         sink: &EventSink<Self>,
    /// #     ) -> Result<(), Self::Error> {
    /// #         match command {
    /// #             MyCommands::DoSomething => {
    /// #                 sink.write(MyEvents::SomethingWasDone, self).await;
    /// #                 Ok(())
    /// #             }
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
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
    /// #     ) -> Result<Option<(MyView, ViewContext)>, PersistenceError> {
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
            max_attempts: NonZeroUsize::new(16).unwrap(),
            phantom: PhantomData,
        }
    }
    /// Allows the user to apply a custom error handler to the projection.
    /// Queries are infallible and _should_ never cause errors,
    /// but programming errors or other technical problems
    /// might. Adding an error handler allows the user to choose whether to
    /// panic the application, log the error or otherwise register the issue.
    ///
    /// Use of an error handler is *strongly* recommended since without one any error encountered
    /// by the projection repository will simply be ignored.
    ///
    /// _Example: An error handler that panics on any error._
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
    /// # use cqrsx::cqrs::event_sink::EventSink;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::stream::ReplayStream;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
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
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Services = MyService;
    /// #
    /// #     async fn handle(
    /// #         &mut self,
    /// #         command: Self::Command,
    /// #         _service: &Self::Services,
    /// #         sink: &EventSink<Self>,
    /// #     ) -> Result<(), Self::Error> {
    /// #         match command {
    /// #             MyCommands::DoSomething => {
    /// #                 sink.write(MyEvents::SomethingWasDone, self).await;
    /// #                 Ok(())
    /// #             }
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
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
    /// #     ) -> Result<Option<(MyView, ViewContext)>, PersistenceError> {
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
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyViewRepository;
    /// # use example::MyView;
    /// # use cqrsx::cqrs::projection::ViewProjection;
    /// # fn config(mut projection: ViewProjection<MyViewRepository,MyView,MyAggregate>) {
    /// projection.set_error_observer(Box::new(|e|panic!("{}",e)));
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
    /// # use cqrsx::cqrs::event_sink::EventSink;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::stream::ReplayStream;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
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
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Services = MyService;
    /// #
    /// #     async fn handle(
    /// #         &mut self,
    /// #         command: Self::Command,
    /// #         _service: &Self::Services,
    /// #         sink: &EventSink<Self>,
    /// #     ) -> Result<(), Self::Error> {
    /// #         match command {
    /// #             MyCommands::DoSomething => {
    /// #                 sink.write(MyEvents::SomethingWasDone, self).await;
    /// #                 Ok(())
    /// #             }
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
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
    /// #     ) -> Result<Option<(MyView, ViewContext)>, PersistenceError> {
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
            .map(|option| option.map(|(view, _)| view))
            .inspect_err(|error| self.handle_error(error))
    }

    /// Limit optimistic view retries, including the first attempt (default: 16).
    pub fn with_max_attempts(mut self, attempts: NonZeroUsize) -> Self {
        self.max_attempts = attempts;
        self
    }

    async fn load_or_default(&self, view_id: String) -> Result<(V, ViewContext), PersistenceError> {
        Ok(self
            .view_repository
            .load_with_context(&view_id)
            .await?
            .unwrap_or_else(|| (Default::default(), ViewContext::new(view_id, 0))))
    }

    pub(crate) async fn apply_events(&self, view_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        for attempt in 1..=self.max_attempts.get() {
            let (mut view, context) = self.load_or_default(view_id.to_string()).await?;
            for event in events {
                view.update(event);
            }
            match self.view_repository.update_view(view, context).await {
                Err(PersistenceError::OptimisticLockError) if attempt < self.max_attempts.get() => tokio::task::yield_now().await,
                result => return result,
            }
        }
        unreachable!("a nonzero attempt limit always returns from an attempt")
    }

    fn handle_error(&self, error: &PersistenceError) {
        if let Some(handler) = &self.error_observer {
            (handler)(error);
        }
    }
}

#[async_trait]
impl<R, V, A> EventHandler<A> for ViewProjection<R, V, A>
where
    R: ViewRepository<V, A>,
    V: View<A>,
    A: Aggregate,
{
    async fn handle(&self, view_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        self.apply_events(view_id, events)
            .await
            .inspect_err(|error| self.handle_error(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::store::persisted::tests::{TestAggregate, TestEvents};
    use serde::{Deserialize, Serialize};
    use std::sync::Mutex;

    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    struct Projection(Vec<usize>);
    impl View<TestAggregate> for Projection {
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            self.0.push(event.sequence);
        }
    }

    #[derive(Default)]
    struct Repository {
        saved: Mutex<Option<(Projection, ViewContext)>>,
        calls: Mutex<Vec<String>>,
        failure: Mutex<Option<&'static str>>,
    }
    impl ViewRepository<Projection, TestAggregate> for Repository {
        async fn load(&self, _: &str) -> Result<Option<Projection>, PersistenceError> {
            panic!("ViewProjection must retain the version with load_with_context")
        }
        async fn load_with_context(&self, id: &str) -> Result<Option<(Projection, ViewContext)>, PersistenceError> {
            self.calls.lock().unwrap().push(format!("load:{id}"));
            if *self.failure.lock().unwrap() == Some("load") {
                return Err(PersistenceError::ConnectionError("load failed".into()));
            }
            Ok(self
                .saved
                .lock()
                .unwrap()
                .as_ref()
                .map(|(v, c)| (v.clone(), ViewContext::new(c.view_instance_id.clone(), c.version))))
        }
        async fn update_view(&self, view: Projection, context: ViewContext) -> Result<(), PersistenceError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("update:{}:{}", context.view_instance_id, context.version));
            let failure = *self.failure.lock().unwrap();
            if failure == Some("once") {
                *self.failure.lock().unwrap() = None;
                *self.saved.lock().unwrap() = Some((Projection(vec![7]), ViewContext::new(context.view_instance_id, 1)));
                return Err(PersistenceError::OptimisticLockError);
            }
            if failure == Some("update") {
                return Err(PersistenceError::OptimisticLockError);
            }
            *self.saved.lock().unwrap() = Some((view, ViewContext::new(context.view_instance_id, context.version + 1)));
            if failure == Some("reply") {
                return Err(PersistenceError::ConnectionError("reply lost after view write".into()));
            }
            Ok(())
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

    #[tokio::test]
    async fn creates_then_updates_in_batch_order_using_loaded_context() {
        let repo = Arc::new(Repository::default());
        let projection = ViewProjection::new(repo.clone());
        assert_eq!(projection.load("view").await.unwrap(), None);
        projection.handle("view", &[event(3), event(1), event(2)]).await.unwrap();
        assert_eq!(projection.load("view").await.unwrap(), Some(Projection(vec![3, 1, 2])));
        projection.handle("view", &[event(4)]).await.unwrap();
        let saved = repo.saved.lock().unwrap();
        let (view, context) = saved.as_ref().unwrap();
        assert_eq!(view, &Projection(vec![3, 1, 2, 4]));
        assert_eq!(context.view_instance_id, "view");
        assert_eq!(context.version, 2);
        assert_eq!(
            *repo.calls.lock().unwrap(),
            [
                "load:view",
                "load:view",
                "update:view:0",
                "load:view",
                "load:view",
                "update:view:1"
            ]
        );
    }

    #[tokio::test]
    async fn failures_are_reported_once_and_never_save_partial_views() {
        for handler in [false, true] {
            for failure in ["load", "update"] {
                let repo = Arc::new(Repository::default());
                *repo.saved.lock().unwrap() = Some((Projection(vec![7]), ViewContext::new("stored-context".into(), 9)));
                let mut projection = ViewProjection::new(repo.clone()).with_max_attempts(NonZeroUsize::new(3).unwrap());
                let errors = Arc::new(Mutex::new(Vec::new()));
                if handler {
                    let errors = errors.clone();
                    projection.set_error_observer(Box::new(move |e| errors.lock().unwrap().push(e.to_string())));
                }
                *repo.failure.lock().unwrap() = Some(failure);
                assert!(projection.handle("requested-view", &[event(8), event(9)]).await.is_err());
                let saved = repo.saved.lock().unwrap();
                let (view, context) = saved.as_ref().unwrap();
                assert_eq!(view, &Projection(vec![7]));
                assert_eq!(context.version, 9);
                assert_eq!(context.view_instance_id, "stored-context");
                assert_eq!(errors.lock().unwrap().len(), usize::from(handler));
                assert_eq!(
                    *repo.calls.lock().unwrap(),
                    if failure == "load" {
                        vec!["load:requested-view"]
                    } else {
                        ["load:requested-view", "update:stored-context:9"].repeat(3)
                    }
                );
            }
        }
    }

    #[tokio::test]
    async fn load_failure_returns_error_with_or_without_handler() {
        for handler in [false, true] {
            let repo = Arc::new(Repository::default());
            *repo.failure.lock().unwrap() = Some("load");
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
    async fn conflict_reloads_fresh_state_and_preserves_batch_order() {
        let repo = Arc::new(Repository::default());
        *repo.failure.lock().unwrap() = Some("once");
        let projection = ViewProjection::new(repo.clone());
        projection.handle("view", &[event(8), event(9)]).await.unwrap();
        let saved = repo.saved.lock().unwrap();
        let (view, context) = saved.as_ref().unwrap();
        assert_eq!(view.0, [7, 8, 9]);
        assert_eq!(context.version, 2);
        assert_eq!(
            *repo.calls.lock().unwrap(),
            ["load:view", "update:view:0", "load:view", "update:view:1"]
        );
    }
    #[tokio::test]
    async fn default_limit_is_sixteen_including_first_attempt() {
        let repo = Arc::new(Repository::default());
        *repo.failure.lock().unwrap() = Some("update");
        let projection = ViewProjection::new(repo.clone());
        assert!(matches!(
            projection.handle("view", &[event(1)]).await,
            Err(PersistenceError::OptimisticLockError)
        ));
        assert_eq!(*repo.calls.lock().unwrap(), ["load:view", "update:view:0"].repeat(16));
        assert!(repo.saved.lock().unwrap().is_none());
    }
    #[tokio::test]
    async fn a_technical_error_after_a_real_view_write_is_never_retried() {
        let repo = Arc::new(Repository::default());
        *repo.failure.lock().unwrap() = Some("reply");
        let projection = ViewProjection::new(repo.clone());
        assert!(matches!(
            projection.handle("view", &[event(1), event(2)]).await,
            Err(PersistenceError::ConnectionError(_))
        ));
        let saved = repo.saved.lock().unwrap();
        let (view, context) = saved.as_ref().unwrap();
        assert_eq!(view.0, [1, 2]);
        assert_eq!(context.version, 1);
        assert_eq!(*repo.calls.lock().unwrap(), ["load:view", "update:view:0"]);
    }
}
