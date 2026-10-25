//! CQRS composition: load an aggregate, decide a command, commit events. Event handlers belong to
//! the command executors ([`command`]), which hand them the committed events.
//!
//! [`command`] adds command gateways and conflict retry, [`store`] implements event
//! persistence, [`event_handler`] owns committed-event reactions and replay, and
//! [`projection`] owns materialized read models. [`testing`] is the public
//! downstream testing toolkit.
pub mod aggregate;
pub mod command;
pub mod event;
pub mod event_handler;
pub mod event_sink;
pub mod projection;
pub mod store;
pub mod testing;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{
    aggregate::Aggregate,
    event::EventEnvelope,
    event_sink::EventSink,
    store::{AggregateContext, EventStore},
};
use std::{collections::HashMap, error};

/// This is the base framework for applying commands to produce events.
///
/// In [Domain Driven Design](https://en.wikipedia.org/wiki/Domain-driven_design) we require that
/// changes are made only after loading the entire `Aggregate` in order to ensure that the full
/// context is understood.
/// With event-sourcing this means:
/// 1. Loading all previous events for the aggregate instance.
/// 1. Applying these events, in order, to a new `Aggregate` in order to reach the correct state.
/// 1. Using the recreated `Aggregate` to handle an inbound `Command` producing events or an error
///    (see `handle` method in this trait).
/// 1. Persisting any generated events or roll back in the event of an error.
///
/// To manage these tasks we use a `CqrsFramework`. It returns the committed events and hands them to
/// nobody: event handlers are registered with a command executor
/// ([`CommandExecutorCore`](crate::cqrs::command::executor::CommandExecutorCore)), which builds a
/// framework per command through its factory and delivers the events itself.
///
pub struct CqrsFramework<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    store: ES,
    service: A::Services,
}

impl<A, ES> CqrsFramework<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    /// Creates new framework for dispatching commands using the provided elements.
    /// Takes an implementation of an `EventStore` and a set of services to be used within the
    /// command handler.
    ///
    /// For a simple in-memory `EventStore` suitable for experimentation or testing see
    /// [MemStore](crate::cqrs::testing::mem_store::MemStore).
    ///
    /// ```rust
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
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyService;
    /// use cqrsx::cqrs::CqrsFramework;
    /// use cqrsx::cqrs::testing::mem_store::MemStore;
    ///
    /// let store = MemStore::<MyAggregate>::default();
    /// let service = MyService::default();
    ///
    /// let cqrs = CqrsFramework::new(store, service);
    /// ```
    /// For production uses a
    /// [persistent event store](crate::cqrs::store::persisted::PersistedEventStore)
    /// backed by the local PostgreSQL adapter is required.
    ///
    pub fn new(store: ES, service: A::Services) -> Self
    where
        A: Aggregate,
        ES: EventStore<A>,
    {
        Self { store, service }
    }
    /// This applies a command to an aggregate. Executing a command
    /// in this way is the only way to make changes to
    /// the state of an aggregate in CQRS.
    ///
    /// An error while processing will result in no events committed and
    /// an [`AggregateError`]
    /// being returned.
    ///
    /// If successful the events produced are persisted in the backing `EventStore` and returned
    /// in commit order; no event handler sees them here.
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
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyCommands;
    /// # use example::MyUserError;
    /// # use cqrsx::cqrs::AggregateError;
    /// # use cqrsx::cqrs::CqrsFramework;
    /// # use cqrsx::cqrs::testing::mem_store::MemStore;
    /// # use std::collections::HashMap;
    /// # use chrono;
    /// type MyFramework = CqrsFramework<MyAggregate,MemStore<MyAggregate>>;
    ///
    /// async fn do_something(cqrs: MyFramework) -> Result<(),AggregateError<MyUserError>> {
    ///     let command = MyCommands::DoSomething;
    ///
    ///     let committed = cqrs.execute("agg-id-F39A0C", command).await?;
    ///     assert_eq!(committed.len(), 1);
    ///     Ok(())
    /// }
    /// ```
    /// `Clone` must provide independent mutable aggregate state. Command handling
    /// uses that working copy while commit retains the last committed context.
    pub async fn execute(
        &self,
        aggregate_id: &str,
        command: A::Command,
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>>
    where
        A: Clone,
    {
        self.execute_with_metadata(aggregate_id, command, HashMap::new()).await
    }

    /// This applies a command to an aggregate. Executing a command
    /// in this way is the only way to make changes to
    /// the state of an aggregate in CQRS.
    ///
    /// A `Hashmap<String,String>` is supplied with any contextual information that should be
    /// associated with this change. This metadata will be attached to any produced events and is
    /// meant to assist in debugging and auditing. Common information might include:
    /// - time of commit
    /// - user making the change
    /// - application version
    ///
    /// An error while processing will result in no events committed and
    /// an [`AggregateError`]
    /// being returned.
    ///
    /// If successful the events produced are persisted in the backing `EventStore` and returned
    /// in commit order; no event handler sees them here.
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
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyCommands;
    /// # use example::MyUserError;
    /// # use cqrsx::cqrs::AggregateError;
    /// # use cqrsx::cqrs::CqrsFramework;
    /// # use cqrsx::cqrs::testing::mem_store::MemStore;
    /// # use std::collections::HashMap;
    /// # use chrono;
    /// type MyFramework = CqrsFramework<MyAggregate,MemStore<MyAggregate>>;
    ///
    /// async fn do_something(cqrs: MyFramework) -> Result<(),AggregateError<MyUserError>>  {
    ///     let command = MyCommands::DoSomething;
    ///     let metadata = HashMap::from([("time".to_string(), chrono::Utc::now().to_rfc3339())]);
    ///
    ///     let committed = cqrs.execute_with_metadata("agg-id-F39A0C", command, metadata).await?;
    ///     assert_eq!(committed.len(), 1);
    ///     Ok(())
    /// }
    /// ```
    pub async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>>
    where
        A: Clone,
    {
        let mut aggregate_context = self.store.load_aggregate(aggregate_id).await?;
        let mut aggregate = aggregate_context.aggregate().clone();
        let sink: EventSink<A> = Default::default();
        aggregate
            .handle(command, &self.service, &sink)
            .await
            .map_err(AggregateError::Domain)?;
        let resultant_events = sink.collect().await;
        self.store.commit(resultant_events, aggregate_context, metadata).await
    }
}

/// The base error for the framework.
#[derive(Debug, thiserror::Error)]
pub enum AggregateError<T: error::Error> {
    /// A domain rule rejected the command; the payload is the aggregate domain error.
    #[error("{0}")]
    Domain(T),
    /// A command has been rejected due to a conflict with another command on the same aggregate
    /// instance. This is handled by optimistic locking in systems backed by an RDBMS.
    #[error("aggregate conflict")]
    AggregateConflict,
    /// A error occurred while attempting to read or write from a database.
    #[error("{0}")]
    DatabaseConnectionError(#[source] Box<dyn error::Error + Send + Sync + 'static>),
    /// A deserialization error occurred due to invalid JSON.
    #[error("{0}")]
    DeserializationError(#[source] Box<dyn error::Error + Send + Sync + 'static>),
    /// A technical error was encountered that prevented the command from being applied to the
    /// aggregate. In general the accompanying message should be logged for investigation rather
    /// than returned to the user.
    #[error("{0}")]
    UnexpectedError(#[source] Box<dyn error::Error + Send + Sync + 'static>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::{
        store::persisted::tests::{TestAggregate, TestCommands, TestService},
        testing::mem_store::MemStore,
    };

    #[tokio::test]
    async fn execute_commits_and_returns_the_events_in_commit_order() {
        let store = MemStore::<TestAggregate>::default();
        let framework = CqrsFramework::new(store.clone(), TestService);
        let metadata = HashMap::from([("trace".to_owned(), "t-1".to_owned())]);
        let committed = framework
            .execute_with_metadata("id", TestCommands::DoSomething, metadata.clone())
            .await
            .unwrap();
        assert_eq!(
            committed
                .iter()
                .map(|e| (e.aggregate_id.as_str(), e.sequence, &e.metadata))
                .collect::<Vec<_>>(),
            [("id", 1, &metadata)]
        );
        let next = framework.execute("id", TestCommands::DoSomething).await.unwrap();
        assert_eq!(next.iter().map(|e| e.sequence).collect::<Vec<_>>(), [2]);
        assert_eq!(store.load_events("id").await.unwrap().len(), 2);
    }
}
