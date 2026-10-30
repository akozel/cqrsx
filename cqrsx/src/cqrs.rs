//! CQRS composition: load an aggregate, decide a command, commit events. The read side follows the
//! journal: projection processors ([`projection`]) read the committed events themselves.
//!
//! [`command`] adds command gateways and conflict retry, [`store`] implements event
//! persistence, [`event_handler`] holds the contract of a committed-event reaction, and
//! [`projection`] owns materialized read models and the processors that feed them. [`reactions`] say what follows
//! each event of an aggregate, as tasks. [`rejection`] is how a business refusal is reported (a code and its
//! params). [`testing`] is the public downstream testing toolkit.
pub mod aggregate;
pub mod command;
pub mod event;
pub mod event_handler;
pub mod projection;
pub mod reactions;
pub mod rejection;
pub mod store;
pub mod testing;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::{
    cqrs::{
        aggregate::Aggregate,
        command::{
            decide::Decide,
            planner::{Planning, TaskPlanError, TaskPlanner},
        },
        event::EventEnvelope,
        store::{AggregateContext, EventStore},
    },
    task::NewTask,
};
use std::{collections::HashMap, error, fmt};

/// This is the base framework for applying commands to produce events.
///
/// In [Domain Driven Design](https://en.wikipedia.org/wiki/Domain-driven_design) we require that
/// changes are made only after loading the entire `Aggregate` in order to ensure that the full
/// context is understood.
/// With event-sourcing this means:
/// 1. Loading all previous events for the aggregate instance.
/// 1. Applying these events, in order, to a new `Aggregate` in order to reach the correct state.
/// 1. Deciding an inbound `Command` on the recreated `Aggregate`: one event or an error
///    (see [`Decide`]).
/// 1. Persisting the event or, on an error, nothing.
///
/// To manage these tasks we use a `CqrsFramework`. It returns what it committed, the events and the tasks
/// its [`TaskPlanner`] planned from them, and hands them to nobody: a command executor
/// ([`CommandExecutorCore`](crate::cqrs::command::executor::CommandExecutorCore)) owns one framework for its
/// life, the projection processors read the events from the journal and the task runtime claims the tasks
/// from the outbox.
///
pub struct CqrsFramework<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    store: ES,
    rules: A::Rules,
    planning: Option<Planning<A>>,
}

impl<A, ES> CqrsFramework<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    /// Creates new framework for dispatching commands using the provided elements.
    /// Takes an implementation of an `EventStore` and the rules the commands decide with
    /// ([`Aggregate::Rules`]).
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
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
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
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyService;
    /// use cqrsx::cqrs::CqrsFramework;
    /// use cqrsx::cqrs::testing::mem_store::MemStore;
    ///
    /// let store = MemStore::<MyAggregate>::default();
    /// let rules = MyService::default();
    ///
    /// let cqrs = CqrsFramework::new(store, rules);
    /// ```
    /// For production uses a
    /// [persistent event store](crate::cqrs::store::persisted::PersistedEventStore)
    /// backed by the local PostgreSQL adapter is required.
    ///
    pub fn new(store: ES, rules: A::Rules) -> Self
    where
        A: Aggregate,
        ES: EventStore<A>,
    {
        Self {
            store,
            rules,
            planning: None,
        }
    }

    /// Plans the follow-up tasks of every decision through `planner`, after `decide` on every
    /// attempt and before the commit; the tasks commit atomically with the attempt's events. One planner per
    /// framework: a second call replaces the first, and several planners compose as a tuple.
    pub fn with_task_planner(mut self, planner: impl TaskPlanner<A> + 'static) -> Self
    where
        A::Command: Clone,
    {
        self.set_task_planner(Planning::new(planner));
        self
    }

    pub(crate) fn set_task_planner(&mut self, planning: Planning<A>) {
        self.planning = Some(planning);
    }

    /// The store the commands run against.
    pub(crate) fn store(&self) -> &ES {
        &self.store
    }
    /// This applies a command to an aggregate. Executing a command
    /// in this way is the only way to make changes to
    /// the state of an aggregate in CQRS.
    ///
    /// An error while processing will result in no events committed and
    /// an [`AggregateError`]
    /// being returned.
    ///
    /// If successful the event decided, and the tasks the planner planned from it, are persisted in
    /// the backing `EventStore` and returned; no event handler sees them here.
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
    ///     let committed = cqrs.execute(&"agg-id-F39A0C".into(), command).await?;
    ///     assert_eq!(committed.events.len(), 1);
    ///     Ok(())
    /// }
    /// ```
    /// `Clone` must provide independent aggregate state: with a planner the event is applied to a copy of
    /// the committed state (the decision's `after`), while commit retains the last committed context.
    pub async fn execute(&self, id: &A::Id, command: A::Command) -> Result<CommitReport<A>, AggregateError<A::Error>>
    where
        A: Clone,
    {
        self.execute_with_metadata(id, command, HashMap::new()).await
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
    /// If successful the event decided, and the tasks the planner planned from it, are persisted in
    /// the backing `EventStore` and returned; no event handler sees them here.
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
    ///     let committed = cqrs.execute_with_metadata(&"agg-id-F39A0C".into(), command, metadata).await?;
    ///     assert_eq!(committed.events.len(), 1);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// The command decides on the committed state ([`Decide::decide`]), synchronously: one event or a
    /// rejection ([`AggregateError::Domain`]), with nothing written.
    ///
    /// With a [task planner](Self::with_task_planner), the decision is planned before the commit: the
    /// planner sees the committed state, a copy of it with the event applied (made for the planner only),
    /// the command, the event and the metadata; the core stamps the causation of `metadata` on every task
    /// and derives its id. A planner error refuses the command ([`AggregateError::TaskPlan`]) with nothing
    /// written.
    ///
    /// The store and the task ids know the aggregate by the text of `id` (its `Display`), formatted here, once per
    /// call; a command executor formats it once per command, for all of its attempts.
    pub async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, AggregateError<A::Error>>
    where
        A: Clone,
    {
        self.attempt(id, &id.to_string(), command, metadata).await
    }

    /// One attempt of a command on `id`, whose text (its `Display`) is `key`: the command executor formats it
    /// once per command, for the lock and every attempt.
    pub(crate) async fn attempt(
        &self,
        id: &A::Id,
        key: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, AggregateError<A::Error>>
    where
        A: Clone,
    {
        let aggregate_context = self.store.load_aggregate(key).await?;
        // The planner's decision needs the command after `decide` consumed it: one owned clone, kept for the
        // planner, never a borrow across an await, so a command that is `Send` but not `Sync` keeps the future
        // `Send`.
        let (command, kept) = match &self.planning {
            Some(planning) => (planning.clone_command(&command), Some(command)),
            None => (command, None),
        };
        let event = command
            .decide(aggregate_context.aggregate(), &self.rules)
            .map_err(AggregateError::Domain)?;
        let tasks = match (&self.planning, &kept) {
            (Some(planning), Some(command)) => {
                // The state the commit leaves, for the planner only: one copy of the state and of the event.
                let before = aggregate_context.aggregate();
                let mut after = before.clone();
                after.apply(event.clone());
                planning
                    .plan(id, key, before, &after, command, std::slice::from_ref(&event), &metadata)
                    .map_err(AggregateError::TaskPlan)?
            }
            _ => Vec::new(),
        };
        let events = self.store.commit(vec![event], aggregate_context, metadata, &tasks).await?;
        Ok(CommitReport { events, tasks })
    }
}

/// What one command committed in one transaction: its events in commit order (the one event it decided, as
/// the store reports it) and the tasks planned from them, published atomically with them.
///
/// `Default` is derived: its bound `A: Default` holds for every aggregate.
#[derive(Default)]
pub struct CommitReport<A: Aggregate> {
    pub events: Vec<EventEnvelope<A>>,
    pub tasks: Vec<NewTask>,
}

// By hand: a derive would require `A` itself to be `Clone`, `Debug` and `PartialEq`, not only its events.
impl<A: Aggregate> Clone for CommitReport<A> {
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
            tasks: self.tasks.clone(),
        }
    }
}

impl<A: Aggregate> fmt::Debug for CommitReport<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitReport")
            .field("events", &self.events)
            .field("tasks", &self.tasks)
            .finish()
    }
}

impl<A: Aggregate> PartialEq for CommitReport<A> {
    fn eq(&self, other: &Self) -> bool {
        self.events == other.events && self.tasks == other.tasks
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
    /// The aggregate decided, but the decision's tasks could not be planned; nothing was written.
    #[error("{0}")]
    TaskPlan(#[source] TaskPlanError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cqrs::{
            command::{
                causation::Causation,
                planner::{Decision, TaskPlanError},
            },
            event::DomainEvent,
            store::persisted::tests::{TestAggregate, TestCommands, TestService},
            testing::mem_store::MemStore,
        },
        task::format::{TaskFormat, TaskPayload},
    };
    use serde::{Deserialize, Serialize};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn execute_commits_and_returns_the_events_in_commit_order() {
        let store = MemStore::<TestAggregate>::default();
        let framework = CqrsFramework::new(store.clone(), TestService);
        let metadata = HashMap::from([("trace".to_owned(), "t-1".to_owned())]);
        let committed = framework
            .execute_with_metadata(&"id".into(), TestCommands::DoSomething, metadata.clone())
            .await
            .unwrap();
        assert_eq!(
            committed
                .events
                .iter()
                .map(|e| (e.aggregate_id.as_str(), e.sequence, &e.metadata))
                .collect::<Vec<_>>(),
            [("id", 1, &metadata)]
        );
        assert!(committed.tasks.is_empty(), "no planner, no tasks");
        let next = framework.execute(&"id".into(), TestCommands::DoSomething).await.unwrap();
        assert_eq!(next.events.iter().map(|e| e.sequence).collect::<Vec<_>>(), [2]);
        assert_eq!(store.load_events("id").await.unwrap().len(), 2);
    }

    /// A command that counts its clones: the decision keeps one per attempt when a planner needs it.
    #[derive(Debug)]
    struct Probe {
        value: u32,
        clones: Arc<AtomicUsize>,
    }

    impl Clone for Probe {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::SeqCst);
            Self {
                value: self.value,
                clones: self.clones.clone(),
            }
        }
    }

    fn probe(value: u32) -> Probe {
        Probe {
            value,
            clones: Arc::default(),
        }
    }

    /// Adds the probe's value; 13 is rejected.
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Echo(u32);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Echoed(u32);

    impl DomainEvent for Echoed {
        fn event_type(&self) -> String {
            "echoed".into()
        }
        fn event_version(&self) -> String {
            "1".into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("unlucky")]
    struct Unlucky;

    impl Aggregate for Echo {
        const TYPE: &'static str = "echo";
        type Id = String;
        type Command = Probe;
        type Event = Echoed;
        type Error = Unlucky;
        type Rules = ();

        fn apply(&mut self, event: Echoed) {
            self.0 += event.0;
        }
    }

    impl Decide<Echo> for Probe {
        fn decide(self, _: &Echo, _: &()) -> Result<Echoed, Unlucky> {
            match self.value {
                13 => Err(Unlucky),
                value => Ok(Echoed(value)),
            }
        }
    }

    #[derive(Serialize)]
    struct Shout {
        total: u32,
    }

    impl TaskPayload for Shout {
        const TASK_TYPE: &'static str = "shout";
        const TASK_VERSION: i32 = 1;
    }

    type Seen = (u32, u32, u32, Vec<Echoed>, HashMap<String, String>, Option<Causation>);

    /// Records what it saw and plans one shout of the new total; refuses when told to.
    #[derive(Clone, Default)]
    struct Recorder {
        seen: Arc<Mutex<Vec<Seen>>>,
        refuse: bool,
    }

    impl TaskPlanner<Echo> for Recorder {
        fn plan(&self, d: &Decision<'_, Echo>) -> Result<Vec<NewTask>, TaskPlanError> {
            self.seen.lock().unwrap().push((
                d.before.0,
                d.after.0,
                d.command.value,
                d.events.to_vec(),
                d.metadata.clone(),
                d.causation.cloned(),
            ));
            if self.refuse {
                return Err(TaskPlanError::application("refused"));
            }
            Ok(vec![NewTask::new(Shout { total: d.after.0 })?])
        }
    }

    #[tokio::test]
    async fn the_planner_sees_the_decision_and_its_tasks_commit_with_the_events() {
        let recorder = Recorder::default();
        let framework = CqrsFramework::new(MemStore::<Echo>::default(), ()).with_task_planner(recorder.clone());
        framework.execute(&"e-1".into(), probe(5)).await.unwrap();
        let causation = Causation::root("ui");
        let metadata = causation.event_metadata();
        let report = framework
            .execute_with_metadata(&"e-1".into(), probe(7), metadata.clone())
            .await
            .unwrap();
        let seen = recorder.seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            [
                (0, 5, 5, vec![Echoed(5)], HashMap::new(), None),
                (5, 12, 7, vec![Echoed(7)], metadata, Some(causation.clone())),
            ],
            "before is the committed state, after has the events applied"
        );
        assert_eq!(report.events.iter().map(|e| e.sequence).collect::<Vec<_>>(), [2]);
        assert_eq!(report.tasks.len(), 1);
        let task = &report.tasks[0];
        assert_eq!(task.payload()["total"], 12);
        assert_eq!(
            task.id(),
            causation.task_id("echo", "e-1", &TaskFormat::of::<Shout>().unwrap(), 0)
        );
        assert_eq!(task.metadata()["correlation_id"], causation.correlation_id.to_string());
    }

    #[tokio::test]
    async fn a_rejection_plans_nothing() {
        let recorder = Recorder::default();
        let framework = CqrsFramework::new(MemStore::<Echo>::default(), ()).with_task_planner(recorder.clone());
        assert!(matches!(
            framework.execute(&"e-1".into(), probe(13)).await,
            Err(AggregateError::Domain(Unlucky))
        ));
        assert!(recorder.seen.lock().unwrap().is_empty(), "the planner was never called");
    }

    /// The events applied, in order: the state shows how often each was applied.
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Tape(Vec<u32>);

    /// Every state a decision was taken on, written by the decision through its rules.
    type Decided = Arc<Mutex<Vec<Vec<u32>>>>;

    impl Aggregate for Tape {
        const TYPE: &'static str = "tape";
        type Id = String;
        type Command = u32;
        type Event = Echoed;
        type Error = Unlucky;
        type Rules = Decided;

        fn apply(&mut self, event: Echoed) {
            self.0.push(event.0);
        }
    }

    impl Decide<Tape> for u32 {
        fn decide(self, tape: &Tape, decided: &Decided) -> Result<Echoed, Unlucky> {
            decided.lock().unwrap().push(tape.0.clone());
            if self == 13 { Err(Unlucky) } else { Ok(Echoed(self)) }
        }
    }

    /// A state before and after a decision.
    type BeforeAndAfter = (Vec<u32>, Vec<u32>);

    /// The states before and after every decision the planner saw.
    #[derive(Clone, Default)]
    struct Before(Arc<Mutex<Vec<BeforeAndAfter>>>);

    impl TaskPlanner<Tape> for Before {
        fn plan(&self, d: &Decision<'_, Tape>) -> Result<Vec<NewTask>, TaskPlanError> {
            self.0.lock().unwrap().push((d.before.0.clone(), d.after.0.clone()));
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn decide_reads_the_committed_state_and_the_planner_sees_its_event_applied_once() {
        let (store, decided, planner) = (MemStore::<Tape>::default(), Decided::default(), Before::default());
        let framework = CqrsFramework::new(store.clone(), decided.clone()).with_task_planner(planner.clone());
        framework.execute(&"t-1".into(), 1).await.unwrap();
        framework.execute(&"t-1".into(), 2).await.unwrap();
        assert!(matches!(
            framework.execute(&"t-1".into(), 13).await,
            Err(AggregateError::Domain(Unlucky))
        ));
        assert_eq!(
            *decided.lock().unwrap(),
            [vec![], vec![1], vec![1, 2]],
            "every command decided on the committed state"
        );
        assert_eq!(
            *planner.0.lock().unwrap(),
            [(vec![], vec![1]), (vec![1], vec![1, 2])],
            "the planner's copy has the event applied once; a rejection is not planned"
        );
        assert_eq!(
            store.load_events("t-1").await.unwrap().len(),
            2,
            "the rejection wrote nothing"
        );
    }

    #[tokio::test]
    async fn a_planning_error_refuses_the_command_and_commits_nothing() {
        let store = MemStore::<Echo>::default();
        let framework = CqrsFramework::new(store.clone(), ()).with_task_planner(Recorder {
            refuse: true,
            ..Recorder::default()
        });
        let Err(AggregateError::TaskPlan(error)) = framework.execute(&"e-1".into(), probe(5)).await else {
            panic!("a planning error refuses the command");
        };
        assert_eq!(error.to_string(), "refused");
        assert_eq!(AggregateError::<Unlucky>::TaskPlan(error).to_string(), "refused");
        assert!(store.load_events("e-1").await.unwrap().is_empty(), "nothing was committed");
    }

    #[tokio::test]
    async fn without_a_planner_the_command_is_moved_and_with_one_it_is_cloned_once() {
        let command = probe(5);
        let clones = command.clones.clone();
        CqrsFramework::new(MemStore::<Echo>::default(), ())
            .execute(&"e-1".into(), command)
            .await
            .unwrap();
        assert_eq!(clones.load(Ordering::SeqCst), 0);
        let command = probe(5);
        let clones = command.clones.clone();
        CqrsFramework::new(MemStore::<Echo>::default(), ())
            .with_task_planner(Recorder::default())
            .execute(&"e-1".into(), command)
            .await
            .unwrap();
        assert_eq!(clones.load(Ordering::SeqCst), 1, "one clone kept for the decision");
    }

    #[derive(Serialize)]
    struct Whisper {}

    impl TaskPayload for Whisper {
        const TASK_TYPE: &'static str = "whisper";
        const TASK_VERSION: i32 = 1;
    }

    struct Whisperer;

    impl TaskPlanner<Echo> for Whisperer {
        fn plan(&self, _: &Decision<'_, Echo>) -> Result<Vec<NewTask>, TaskPlanError> {
            Ok(vec![NewTask::new(Whisper {})?])
        }
    }

    #[tokio::test]
    async fn a_second_planner_replaces_the_first() {
        let recorder = Recorder::default();
        let framework = CqrsFramework::new(MemStore::<Echo>::default(), ())
            .with_task_planner(recorder.clone())
            .with_task_planner(Whisperer);
        let report = framework.execute(&"e-1".into(), probe(5)).await.unwrap();
        assert_eq!(report.tasks[0].format().to_string(), "whisper v1");
        assert!(recorder.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_commit_report_compares_its_parts_and_shows_them() {
        let report = CqrsFramework::new(MemStore::<Echo>::default(), ())
            .with_task_planner(Recorder::default())
            .execute(&"e-1".into(), probe(5))
            .await
            .unwrap();
        assert_eq!(report.clone(), report);
        assert_ne!(report, CommitReport::default());
        let shown = format!("{report:?}");
        for part in ["CommitReport", "EventEnvelope", "Echoed(5)", "shout"] {
            assert!(shown.contains(part), "{shown}");
        }
        let mut other = report.clone();
        other.events[0].sequence += 1;
        assert_ne!(other, report, "envelopes compare by every field");
    }
}
