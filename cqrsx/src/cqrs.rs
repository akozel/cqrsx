//! CQRS composition: load an aggregate, decide a command, commit events, notify event handlers.
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
    event_handler::EventHandler,
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
/// To manage these tasks we use a `CqrsFramework`.
///
pub struct CqrsFramework<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    store: ES,
    event_handlers: Vec<Box<dyn EventHandler<A>>>,
    service: A::Services,
}

impl<A, ES> CqrsFramework<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    /// Creates new framework for dispatching commands using the provided elements.
    /// Takes an implementation of an `EventStore`, a vector of event handlers and a set of services
    /// to be used within the command handler.
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
    /// let event_handlers = vec![];
    /// let service = MyService::default();
    ///
    /// let cqrs = CqrsFramework::new(store, event_handlers, service);
    /// ```
    /// For production uses a
    /// [persistent event store](crate::cqrs::store::persisted::PersistedEventStore)
    /// backed by the local PostgreSQL adapter is required.
    ///
    pub fn new(store: ES, event_handlers: Vec<Box<dyn EventHandler<A>>>, service: A::Services) -> Self
    where
        A: Aggregate,
        ES: EventStore<A>,
    {
        Self {
            store,
            event_handlers,
            service,
        }
    }
    /// Appends an additional handler to the framework.
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
    /// #
    /// # #[derive(Debug, Default)]
    /// # pub struct MyEventHandler;
    /// #
    /// # #[async_trait]
    /// # impl EventHandler<MyAggregate> for MyEventHandler {
    /// #     async fn handle(&self, _aggregate_id: &str, _events: &[EventEnvelope<MyAggregate>]) -> Result<(), cqrsx::cqrs::store::PersistenceError> { Ok(()) }
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyEventHandler;
    /// # use example::MyService;
    /// use cqrsx::cqrs::CqrsFramework;
    /// use cqrsx::cqrs::testing::mem_store::MemStore;
    ///
    /// let store = MemStore::<MyAggregate>::default();
    /// let event_handlers = vec![];
    /// let service = MyService::default();
    ///
    /// let cqrs = CqrsFramework::new(store, event_handlers, service)
    ///     .append_event_handler(Box::new(MyEventHandler::default()));
    /// ```
    pub fn append_event_handler(self, handler: Box<dyn EventHandler<A>>) -> Self
    where
        A: Aggregate,
        ES: EventStore<A>,
    {
        let mut event_handlers = self.event_handlers;
        event_handlers.push(handler);
        Self {
            store: self.store,
            event_handlers,
            service: self.service,
        }
    }
    /// This applies a command to an aggregate. Executing a command
    /// in this way is the only way to make changes to
    /// the state of an aggregate in CQRS.
    ///
    /// An error while processing will result in no events committed and
    /// an [`AggregateError`]
    /// being returned.
    ///
    /// If successful the events produced will be persisted in the backing `EventStore`
    /// before being applied to any configured `EventHandler`s.
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
    ///     cqrs.execute("agg-id-F39A0C", command).await
    /// }
    /// ```
    /// `Clone` must provide independent mutable aggregate state. Command handling
    /// uses that working copy while commit retains the last committed context.
    pub async fn execute(&self, aggregate_id: &str, command: A::Command) -> Result<(), AggregateError<A::Error>>
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
    /// If successful the events produced will be persisted in the backing `EventStore`
    /// before being applied to any configured `EventHandler`s.
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
    ///     cqrs.execute_with_metadata("agg-id-F39A0C", command, metadata).await
    /// }
    /// ```
    pub async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<(), AggregateError<A::Error>>
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
        let committed_events = self.store.commit(resultant_events, aggregate_context, metadata).await?;
        let Some(last) = committed_events.last() else {
            return Ok(());
        };
        let last_sequence = last.sequence;
        let mut errors = Vec::new();
        for (handler_index, handler) in self.event_handlers.iter().enumerate() {
            if let Err(error) = handler.handle(aggregate_id, &committed_events).await {
                errors.push(crate::cqrs::EventHandlerFailure { handler_index, error });
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(AggregateError::CommittedWithEventHandlerFailure(
                crate::cqrs::CommittedWithEventHandlerFailure { last_sequence, errors },
            ))
        }
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
    /// Events committed successfully; event handlers failed after commit. Never repeat the command.
    #[error(transparent)]
    CommittedWithEventHandlerFailure(CommittedWithEventHandlerFailure),
}

/// A committed command whose event handlers did not all succeed.
#[derive(Debug, thiserror::Error)]
#[error("events committed through sequence {last_sequence}; event handling failed")]
pub struct CommittedWithEventHandlerFailure {
    pub last_sequence: usize,
    pub errors: Vec<EventHandlerFailure>,
}

#[derive(Debug)]
pub struct EventHandlerFailure {
    pub handler_index: usize,
    pub error: crate::cqrs::store::PersistenceError,
}
