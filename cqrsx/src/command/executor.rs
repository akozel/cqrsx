use super::{
    CommandExecutor, CommandGatewayError,
    locks::{AggregateLockRegistry, LockLimits, LockRefused},
    observer::{CommandObserver, CommandReport, Outcome},
    planner::TaskPlanner,
    retry::retry_on_aggregate_conflict,
};
use crate::{
    command::{AggregateError, CommitReport, attempt::CommandAttempt, retry::AggregateConflictRetryPolicy},
    domain::aggregate::{Aggregate, AggregateKey},
    journal::{EventStore, wakeup::CommitWakeup},
};
use std::{collections::HashMap, error::Error, fmt, sync::Arc};

/// The executor of the commands of one aggregate type over an [`EventStore`]: the attempt it owns
/// for its life holds the store, the rules and the task planner. The engine, the tests and the test executor
/// build it over a store of their own; there is no preset per database. One long-lived executor per aggregate type
/// and process, shared (e.g. in an `Arc`) by everything that issues its commands: a second executor of the same
/// type has locks of its own.
///
/// A command first waits for its aggregate's lock, at most the lock's `max_wait`, so the executor's commands
/// to one aggregate run one at a time, in arrival order; past the wait the command is
/// [`CommandGatewayError::Unavailable`] and nothing is loaded, planned or written. Two executors of one
/// aggregate type are like two processes, which meet only in the store's optimistic check. Holding the lock
/// the command runs through an attempt with bounded conflict retry: every attempt reloads the aggregate,
/// repeats clones of the command and of its metadata and plans its tasks again from its own decision
/// ([`with_task_planner`](Self::with_task_planner)). A commit with events wakes the aggregate type's projection
/// processors ([`with_commit_wakeup`](Self::with_commit_wakeup)), which read the journal themselves; then the
/// command releases the lock, tells the executor's observer what became of it
/// ([`with_observer`](Self::with_observer)) and answers with what it committed.
#[must_use]
pub struct StoreCommandExecutor<A: Aggregate, S: EventStore<A>> {
    attempt: CommandAttempt<A, S>,
    retry_policy: AggregateConflictRetryPolicy,
    locks: AggregateLockRegistry,
    wakeup: Option<CommitWakeup>,
    observer: Option<Arc<dyn CommandObserver<A>>>,
}

impl<A: Aggregate, S: EventStore<A>> fmt::Debug for StoreCommandExecutor<A, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreCommandExecutor")
            .field("attempt", &self.attempt)
            .field("retry_policy", &self.retry_policy)
            .field("lock_limits", &self.locks.limits())
            .field("commit_wakeup", &self.wakeup.is_some())
            .field("observer", &self.observer.is_some())
            .finish_non_exhaustive()
    }
}

impl<A: Aggregate + 'static, S: EventStore<A>> StoreCommandExecutor<A, S> {
    /// An executor over `store` with the rules the commands decide with ([`Aggregate::Rules`]) and a lock
    /// registry of its own, bounded by `lock_limits`, that wakes nobody, tells nobody and plans nothing.
    ///
    /// For experimentation or testing use the [persistent event store](crate::journal::persisted::PersistedEventStore)
    /// over the journal in memory ([`MemEventRepository`](crate::memory::event_repository::MemEventRepository)); for
    /// production, the same store over a database repository.
    ///
    /// ```rust
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewContext;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
    /// #     }
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
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
    /// use cqrsx::command::executor::StoreCommandExecutor;
    /// use cqrsx::command::locks::LockLimits;
    /// use cqrsx::command::retry::AggregateConflictRetryPolicy;
    /// use cqrsx::journal::persisted::PersistedEventStore;
    /// use cqrsx::memory::event_repository::MemEventRepository;
    ///
    /// let store = PersistedEventStore::<_, MyAggregate>::new(MemEventRepository::default());
    /// let rules = MyService::default();
    ///
    /// let executor = StoreCommandExecutor::new(store, rules, AggregateConflictRetryPolicy::default(), LockLimits::default());
    /// ```
    pub fn new(store: S, rules: A::Rules, retry_policy: AggregateConflictRetryPolicy, lock_limits: LockLimits) -> Self {
        Self {
            attempt: CommandAttempt::new(store, rules),
            retry_policy,
            locks: AggregateLockRegistry::new(lock_limits),
            wakeup: None,
            observer: None,
        }
    }

    /// Wakes `wakeup` after every commit with events, before the client gets its answer: the projection
    /// processors of this aggregate type then read the journal at once instead of at their next poll.
    pub fn with_commit_wakeup(mut self, wakeup: CommitWakeup) -> Self {
        self.wakeup = Some(wakeup);
        self
    }

    /// Plans every decision's follow-up tasks through `planner`, on every attempt, under the aggregate's lock, after
    /// `decide` and before the commit; the tasks commit atomically with the attempt's events. One planner per
    /// executor: a second call replaces the first, and several planners compose as a tuple.
    pub fn with_task_planner(mut self, planner: impl TaskPlanner<A> + 'static) -> Self
    where
        A::Command: Clone,
    {
        self.attempt = self.attempt.with_task_planner(planner);
        self
    }

    /// Tells `observer` about every command: once, after its attempts, once the aggregate's lock is released and the
    /// wake-up sent, before the answer (see [`CommandObserver`]). One observer per executor: a second replaces the
    /// first.
    pub fn with_observer(mut self, observer: Arc<dyn CommandObserver<A>>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// The store the commands run against.
    pub fn store(&self) -> &S {
        self.attempt.store()
    }
}

impl<A, S> StoreCommandExecutor<A, S>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone,
    S: EventStore<A> + 'static,
    S::Context: Send,
{
    /// Executes without event metadata; see [`execute_with_metadata`](Self::execute_with_metadata).
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewContext;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
    /// #     }
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
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
    /// # use cqrsx::command::{CommandGatewayError, executor::StoreCommandExecutor};
    /// # use cqrsx::{journal::persisted::PersistedEventStore, memory::event_repository::MemEventRepository};
    /// type MyExecutor = StoreCommandExecutor<MyAggregate, PersistedEventStore<MemEventRepository, MyAggregate>>;
    ///
    /// async fn do_something(executor: MyExecutor) -> Result<(), CommandGatewayError<MyUserError>> {
    ///     let committed = executor.execute(&"agg-id-F39A0C".into(), MyCommands::DoSomething).await?;
    ///     assert_eq!(committed.events.len(), 1);
    ///     Ok(())
    /// }
    /// ```
    pub async fn execute(&self, id: &A::Id, command: A::Command) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        self.execute_with_metadata(id, command, HashMap::new()).await
    }

    /// Waits for the aggregate's lock, then runs the command with bounded conflict retry. The id's text (its
    /// `Display`) is formatted once, for the lock's key, and every attempt loads and plans with that text. Each
    /// attempt receives a clone of `metadata`, so a retried command commits exactly the metadata of its first
    /// attempt, and plans its tasks from its own decision. Once the lock is released the observer, if any, hears the
    /// command and its outcome; then the caller gets the answer.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewContext;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
    /// #     }
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
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
    /// # use cqrsx::command::{CommandGatewayError, executor::StoreCommandExecutor};
    /// # use cqrsx::{journal::persisted::PersistedEventStore, memory::event_repository::MemEventRepository};
    /// # use std::collections::HashMap;
    /// # use chrono;
    /// type MyExecutor = StoreCommandExecutor<MyAggregate, PersistedEventStore<MemEventRepository, MyAggregate>>;
    ///
    /// async fn do_something(executor: MyExecutor) -> Result<(), CommandGatewayError<MyUserError>> {
    ///     let metadata = HashMap::from([("time".to_string(), chrono::Utc::now().to_rfc3339())]);
    ///     let committed = executor
    ///         .execute_with_metadata(&"agg-id-F39A0C".into(), MyCommands::DoSomething, metadata)
    ///         .await?;
    ///     assert_eq!(committed.events.len(), 1);
    ///     Ok(())
    /// }
    /// ```
    pub async fn execute_with_metadata(
        &self,
        id: &A::Id,
        mut command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        let answer = self.run(id, &mut command, &metadata).await;
        if let Some(observer) = &self.observer {
            observer.command(&CommandReport {
                aggregate_id: id,
                command: &command,
                metadata: &metadata,
                outcome: Outcome::of(&answer),
            });
        }
        answer
    }

    /// The command under its aggregate's lock: every attempt and the wake-up; the lock is released when it returns.
    /// Every attempt decides a clone of `command`; the command is borrowed `&mut`, which is `Send` for a command that
    /// is `Send`, where a shared borrow held across an await would need it `Sync`.
    async fn run(
        &self,
        id: &A::Id,
        command: &mut A::Command,
        metadata: &HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        // Held through every attempt and the wake-up.
        let lock = self
            .locks
            .acquire(AggregateKey::of::<A>(id))
            .await
            .map_err(lock_refused::<A::Error>(A::TYPE))?;
        let key = lock.key().aggregate_id(); // the id's text, formatted once: every attempt borrows it
        let attempt = &self.attempt;
        let report = retry_on_aggregate_conflict(self.retry_policy, move || {
            attempt.run(id, key, command.clone(), metadata.clone())
        })
        .await
        .inspect_err(|error| log_aggregate_error(A::TYPE, error))
        .map_err(map_aggregate_error)?;
        if !report.events.is_empty()
            && let Some(wakeup) = &self.wakeup
        {
            wakeup.wake(); // the journal has the events: the processors may read them now
        }
        Ok(report)
    }
}

fn lock_refused<D>(aggregate_type: &'static str) -> impl FnOnce(LockRefused) -> CommandGatewayError<D> {
    move |refused| {
        crate::diagnostics::log_lock_refused(aggregate_type, refused);
        CommandGatewayError::Unavailable
    }
}

/// Logs the technical failures of a load, a plan or a commit; domain rejections and conflicts are outcomes.
fn log_aggregate_error<D: Error>(aggregate_type: &str, error: &AggregateError<D>) {
    let (category, source): (&'static str, &(dyn Error + 'static)) = match error {
        AggregateError::DatabaseConnectionError(source) => ("connection", source.as_ref()),
        AggregateError::DeserializationError(source) => ("deserialization", source.as_ref()),
        AggregateError::UnexpectedError(source) => ("unexpected", source.as_ref()),
        AggregateError::TaskPlan(source) => ("task_plan", source),
        AggregateError::Domain(_) | AggregateError::AggregateConflict => return,
    };
    crate::diagnostics::log_command_failure(aggregate_type, category, source);
}

impl<A, S> CommandExecutor<A> for StoreCommandExecutor<A, S>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + 'static,
    A::Error: Send + 'static,
    S: EventStore<A> + 'static,
    S::Context: Send,
{
    async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        StoreCommandExecutor::execute_with_metadata(self, id, command, metadata).await
    }
}

pub(super) fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::Domain(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_)
        | AggregateError::TaskPlan(_) => CommandGatewayError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::domain::command::Decide;
    use crate::domain::event::EventEnvelope;
    use crate::domain::event::Events;
    use crate::journal::AggregateContext;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::command::AggregateError;
    use crate::command::{
        causation::Causation,
        locks::LockLimits,
        planner::{Decision, TaskPlanError},
    };
    use crate::domain::aggregate::Aggregate;
    use crate::journal::{EventStore, PersistenceError};
    use crate::task::{NewTask, Task};
    use crate::{journal::persisted::PersistedEventStore, memory::event_repository::MemEventRepository};
    use std::time::Duration;
    use thiserror::Error;

    #[derive(Clone, Debug, Error, Eq, PartialEq)]
    #[error("rejected")]
    struct DummyError;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum DummyCommand {
        Record(String),
        Reject,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    enum DummyEvent {
        Recorded(String),
    }

    impl Events for DummyEvent {
        fn event_type(&self) -> &'static str {
            "recorded"
        }

        fn event_version(&self) -> &'static str {
            "1"
        }
    }

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct DummyAggregate;

    impl Aggregate for DummyAggregate {
        const TYPE: &'static str = "dummy";
        type Id = String;
        type Command = DummyCommand;
        type Event = DummyEvent;
        type Error = DummyError;
        type Rules = Arc<Mutex<Vec<DummyCommand>>>;

        fn apply(&mut self, _: Self::Event) {}
    }

    impl Decide<DummyAggregate> for DummyCommand {
        fn decide(self, _: &DummyAggregate, seen: &Arc<Mutex<Vec<DummyCommand>>>) -> Result<DummyEvent, DummyError> {
            seen.lock().unwrap().push(self.clone());
            match self {
                DummyCommand::Record(value) => Ok(DummyEvent::Recorded(value)),
                DummyCommand::Reject => Err(DummyError),
            }
        }
    }

    struct DummyContext(DummyAggregate);

    impl AggregateContext<DummyAggregate> for DummyContext {
        fn aggregate(&self) -> &DummyAggregate {
            &self.0
        }
    }

    /// Conflicts on its first `conflicts` commits and records the tasks every commit carried.
    #[derive(Clone)]
    struct DummyStore {
        commits: Arc<AtomicUsize>,
        conflicts: usize,
        load_error: Option<fn() -> PersistenceError>,
        tasks: Arc<Mutex<Vec<Vec<NewTask>>>>,
    }

    impl DummyStore {
        fn new(conflicts: usize) -> Self {
            Self {
                commits: Arc::default(),
                conflicts,
                load_error: None,
                tasks: Arc::default(),
            }
        }

        fn failing(load_error: fn() -> PersistenceError) -> Self {
            Self {
                load_error: Some(load_error),
                ..Self::new(0)
            }
        }
    }

    impl EventStore<DummyAggregate> for DummyStore {
        type Context = DummyContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, PersistenceError> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::Context, PersistenceError> {
            if let Some(error) = self.load_error {
                return Err(error());
            }
            Ok(DummyContext(DummyAggregate))
        }

        async fn commit(
            &self,
            _: Vec<DummyEvent>,
            _: Self::Context,
            _: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, PersistenceError> {
            self.tasks.lock().unwrap().push(tasks.to_vec());
            if self.commits.fetch_add(1, Ordering::SeqCst) < self.conflicts {
                Err(PersistenceError::OptimisticLockError)
            } else {
                Ok(Vec::new())
            }
        }
    }

    #[derive(Serialize)]
    struct DummyTask {
        value: String,
    }

    impl Task for DummyTask {
        const TASK_TYPE: &'static str = "dummy";
        const TASK_VERSION: i32 = 1;
    }

    /// One task per recorded value; refuses `refuse` and panics on `panic`; counts its calls and writes
    /// `plan <value>` into `log` when it has one.
    #[derive(Clone, Default)]
    struct ValuePlanner {
        calls: Arc<AtomicUsize>,
        log: Option<Arc<Mutex<Vec<String>>>>,
    }

    impl TaskPlanner<DummyAggregate> for ValuePlanner {
        fn plan(&self, decision: &Decision<'_, DummyAggregate>) -> Result<Vec<NewTask>, TaskPlanError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut tasks = Vec::new();
            for DummyEvent::Recorded(value) in decision.events {
                match value.as_str() {
                    "refuse" => return Err(TaskPlanError::application("private_refusal")),
                    "panic" => panic!("the planner panics"),
                    _ => {}
                }
                if let Some(log) = &self.log {
                    log.lock().unwrap().push(format!("plan {value}"));
                }
                tasks.push(NewTask::try_new(DummyTask { value: value.clone() })?);
            }
            Ok(tasks)
        }
    }

    fn no_delay_policy(max_attempts: usize) -> AggregateConflictRetryPolicy {
        AggregateConflictRetryPolicy::try_new(max_attempts, Duration::ZERO, Duration::ZERO, 0.0).unwrap()
    }

    fn dummy_executor(store: DummyStore, attempts: usize) -> StoreCommandExecutor<DummyAggregate, DummyStore> {
        StoreCommandExecutor::new(store, Arc::default(), no_delay_policy(attempts), LockLimits::default())
    }

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct SendOnlyAggregate;

    impl Aggregate for SendOnlyAggregate {
        const TYPE: &'static str = "send_only";
        type Id = String;
        type Command = std::cell::Cell<u32>;
        type Event = DummyEvent;
        type Error = DummyError;
        type Rules = ();

        fn apply(&mut self, _: DummyEvent) {}
    }

    impl Decide<SendOnlyAggregate> for std::cell::Cell<u32> {
        fn decide(self, _: &SendOnlyAggregate, _: &()) -> Result<DummyEvent, DummyError> {
            Ok(DummyEvent::Recorded(self.get().to_string()))
        }
    }

    /// Plans one task from the command, read through the shared reference the decision holds.
    struct CommandPlanner;

    impl TaskPlanner<SendOnlyAggregate> for CommandPlanner {
        fn plan(&self, decision: &Decision<'_, SendOnlyAggregate>) -> Result<Vec<NewTask>, TaskPlanError> {
            Ok(vec![NewTask::try_new(DummyTask {
                value: decision.command.get().to_string(),
            })?])
        }
    }

    #[tokio::test]
    async fn executor_accepts_send_commands_without_requiring_sync() {
        let executor = Arc::new(StoreCommandExecutor::new(
            PersistedEventStore::<_, SendOnlyAggregate>::new(MemEventRepository::default()),
            (),
            no_delay_policy(1),
            LockLimits::default(),
        ));
        let observed = Arc::clone(&executor);
        let report = tokio::spawn(async move {
            CommandExecutor::execute(executor.as_ref(), &"send-only".into(), std::cell::Cell::new(7)).await
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.events.len(), 1);
        assert!(report.tasks.is_empty(), "no planner, no tasks");
        let stored = observed.store().load_events("send-only").await.unwrap();
        assert_eq!(stored.len(), 1, "the store the commands ran against holds the event");
    }

    #[tokio::test]
    async fn a_planner_reads_a_send_only_command_and_the_future_stays_send() {
        let executor = Arc::new(
            StoreCommandExecutor::new(
                PersistedEventStore::<_, SendOnlyAggregate>::new(MemEventRepository::default()),
                (),
                no_delay_policy(1),
                LockLimits::default(),
            )
            .with_task_planner(CommandPlanner),
        );
        let report = tokio::spawn(async move {
            CommandExecutor::execute(executor.as_ref(), &"send-only".into(), std::cell::Cell::new(7)).await
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.tasks.len(), 1);
        assert_eq!(report.tasks[0].payload()["value"], "7");
    }

    struct BorrowingExecutor;

    impl CommandExecutor<SendOnlyAggregate> for BorrowingExecutor {
        async fn execute_with_metadata(
            &self,
            aggregate_id: &String,
            command: std::cell::Cell<u32>,
            metadata: HashMap<String, String>,
        ) -> Result<CommitReport<SendOnlyAggregate>, CommandGatewayError<DummyError>> {
            let address = aggregate_id.as_ptr() as usize;
            command.set(command.get() + 1);
            tokio::task::yield_now().await;
            assert_eq!(aggregate_id, "borrowed-id");
            assert_eq!(aggregate_id.as_ptr() as usize, address);
            assert_eq!(command.get(), 8);
            assert!(metadata.is_empty(), "the default execute must pass an empty map");
            Ok(CommitReport::default())
        }
    }

    #[tokio::test]
    async fn native_command_contract_keeps_a_borrowed_id_and_send_only_command_across_await() {
        fn require_send<F: std::future::Future + Send>(_: &F) {}
        let executor = BorrowingExecutor;
        let aggregate_id = String::from("borrowed-id");
        let future = CommandExecutor::execute(&executor, &aggregate_id, std::cell::Cell::new(7));
        require_send(&future);
        assert_eq!(future.await, Ok(CommitReport::default()));
        assert_eq!(aggregate_id, "borrowed-id");
    }

    /// Records the metadata of every commit and conflicts a configured number of times first.
    #[derive(Clone)]
    struct MetadataStore {
        conflicts: usize,
        commits: Arc<Mutex<Vec<HashMap<String, String>>>>,
    }

    impl EventStore<DummyAggregate> for MetadataStore {
        type Context = DummyContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, PersistenceError> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::Context, PersistenceError> {
            Ok(DummyContext(DummyAggregate))
        }

        async fn commit(
            &self,
            _: Vec<DummyEvent>,
            _: Self::Context,
            metadata: HashMap<String, String>,
            _: &[NewTask],
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, PersistenceError> {
            let mut commits = self.commits.lock().unwrap();
            commits.push(metadata);
            if commits.len() <= self.conflicts {
                Err(PersistenceError::OptimisticLockError)
            } else {
                Ok(Vec::new())
            }
        }
    }

    fn metadata_executor(
        conflicts: usize,
        commits: &Arc<Mutex<Vec<HashMap<String, String>>>>,
    ) -> impl CommandExecutor<DummyAggregate> {
        StoreCommandExecutor::new(
            MetadataStore {
                conflicts,
                commits: commits.clone(),
            },
            Arc::default(),
            no_delay_policy(3),
            LockLimits::default(),
        )
    }

    #[tokio::test]
    async fn metadata_reaches_the_commit_and_stays_identical_across_conflict_retries() {
        let commits = Arc::new(Mutex::new(Vec::new()));
        let executor = metadata_executor(2, &commits);
        let metadata = HashMap::from([
            ("command_id".to_owned(), "cmd-1".to_owned()),
            ("source".to_owned(), "ui".to_owned()),
        ]);
        executor
            .execute_with_metadata(
                &"aggregate".into(),
                DummyCommand::Record("value".to_owned()),
                metadata.clone(),
            )
            .await
            .unwrap();
        assert_eq!(*commits.lock().unwrap(), vec![metadata.clone(), metadata.clone(), metadata]);
    }

    #[tokio::test]
    async fn execute_without_metadata_commits_an_empty_map_through_both_entry_points() {
        let commits = Arc::new(Mutex::new(Vec::new()));
        let executor = metadata_executor(0, &commits);
        CommandExecutor::execute(&executor, &"aggregate".into(), DummyCommand::Record("trait".to_owned()))
            .await
            .unwrap();
        let commits_inherent = Arc::new(Mutex::new(Vec::new()));
        let inherent = StoreCommandExecutor::new(
            MetadataStore {
                conflicts: 0,
                commits: commits_inherent.clone(),
            },
            Arc::default(),
            no_delay_policy(1),
            LockLimits::default(),
        );
        inherent
            .execute(&"aggregate".into(), DummyCommand::Record("inherent".to_owned()))
            .await
            .unwrap();
        assert_eq!(*commits.lock().unwrap(), vec![HashMap::new()]);
        assert_eq!(*commits_inherent.lock().unwrap(), vec![HashMap::new()]);
    }

    #[tokio::test]
    async fn a_retried_command_repeats_the_unchanged_command_and_plans_every_attempt_with_the_same_ids() {
        let store = DummyStore::new(2);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let planner = ValuePlanner::default();
        let executor = StoreCommandExecutor::new(store.clone(), commands.clone(), no_delay_policy(3), LockLimits::default())
            .with_task_planner(planner.clone());
        let metadata = Causation::root("test").event_metadata();

        let report = CommandExecutor::execute_with_metadata(
            &executor,
            &"aggregate".into(),
            DummyCommand::Record("same".to_owned()),
            metadata,
        )
        .await
        .unwrap();

        assert_eq!(store.commits.load(Ordering::SeqCst), 3);
        assert_eq!(
            planner.calls.load(Ordering::SeqCst),
            3,
            "every attempt plans its own decision"
        );
        assert_eq!(*commands.lock().unwrap(), vec![DummyCommand::Record("same".to_owned()); 3]);
        let attempts = store.tasks.lock().unwrap().clone();
        assert_eq!(attempts.len(), 3);
        assert!(
            attempts.iter().all(|tasks| tasks == &attempts[0]),
            "the same decision plans the same tasks, ids included"
        );
        assert_eq!(
            report.tasks, attempts[2],
            "the report holds what the committing attempt published"
        );
    }

    #[tokio::test]
    async fn executor_maps_domain_conflict_and_technical_errors_without_wrong_retries() {
        let domain_executor = dummy_executor(DummyStore::new(0), 3);
        assert_eq!(
            CommandExecutor::execute(&domain_executor, &"aggregate".into(), DummyCommand::Reject).await,
            Err(CommandGatewayError::Domain(DummyError))
        );

        let store = DummyStore::new(usize::MAX);
        let conflict_executor = dummy_executor(store.clone(), 3);
        assert_eq!(
            CommandExecutor::execute(
                &conflict_executor,
                &"aggregate".into(),
                DummyCommand::Record("value".to_owned())
            )
            .await,
            Err(CommandGatewayError::Conflict)
        );
        assert_eq!(store.commits.load(Ordering::SeqCst), 3);

        for error in [
            || PersistenceError::ConnectionError(Box::new(io::Error::other("connection"))),
            || PersistenceError::DeserializationError(Box::new(io::Error::other("json"))),
            || PersistenceError::UnknownError(Box::new(io::Error::other("unexpected"))),
        ] {
            let executor = dummy_executor(DummyStore::failing(error), 3);
            assert_eq!(
                CommandExecutor::execute(&executor, &"aggregate".into(), DummyCommand::Record("value".to_owned())).await,
                Err(CommandGatewayError::Unavailable)
            );
        }
    }

    #[tokio::test]
    async fn exhausted_conflicts_publish_nothing_and_report_the_conflict() {
        let store = DummyStore::new(usize::MAX);
        let planner = ValuePlanner::default();
        let executor = dummy_executor(store.clone(), 3).with_task_planner(planner.clone());
        assert_eq!(
            executor
                .execute(&"aggregate".into(), DummyCommand::Record("value".to_owned()))
                .await,
            Err(CommandGatewayError::Conflict)
        );
        assert_eq!(planner.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            store.commits.load(Ordering::SeqCst),
            3,
            "every planned attempt reached the store"
        );
    }

    #[tokio::test]
    async fn a_planning_failure_refuses_the_command_before_its_commit_and_is_logged_once_without_details() {
        let (_, log) = crate::diagnostics::test_support::capture(async {
            let store = DummyStore::new(0);
            let planner = ValuePlanner::default();
            let executor = dummy_executor(store.clone(), 3).with_task_planner(planner.clone());
            assert_eq!(
                executor
                    .execute(&"private_id".into(), DummyCommand::Record("refuse".to_owned()))
                    .await,
                Err(CommandGatewayError::Unavailable)
            );
            assert_eq!(planner.calls.load(Ordering::SeqCst), 1, "a planning failure is never retried");
            assert_eq!(store.commits.load(Ordering::SeqCst), 0, "nothing reached the store");
        })
        .await;
        assert_eq!(log.matches("command execution failed").count(), 1, "{log}");
        assert!(log.contains("category=\"task_plan\""), "{log}");
        assert!(!log.contains("private_"), "{log}");
    }

    #[tokio::test]
    async fn a_planning_failure_after_a_conflict_is_unavailable_and_writes_nothing_more() {
        let store = TallyStore::new(1);
        let executor = StoreCommandExecutor::new(store.clone(), (), no_delay_policy(3), LockLimits::default())
            .with_task_planner(TallyPlanner { refuse_above: Some(0) });
        assert_eq!(
            executor.execute(&"tally".into(), 5).await,
            Err(CommandGatewayError::Unavailable)
        );
        assert_eq!(
            store.attempts.lock().unwrap().len(),
            1,
            "the first attempt conflicted; the second one's plan was refused before its commit"
        );
    }

    #[tokio::test]
    async fn executor_logs_technical_failure_once_and_suppresses_domain_and_conflict_failures() {
        let (_, log) = crate::diagnostics::test_support::capture(async {
            for error in [
                || PersistenceError::ConnectionError(Box::new(io::Error::other("private_connection"))),
                || PersistenceError::DeserializationError(Box::new(io::Error::other("private_json"))),
                || PersistenceError::UnknownError(Box::new(io::Error::other("private_unexpected"))),
            ] {
                let executor = dummy_executor(DummyStore::failing(error), 3);
                assert_eq!(
                    executor
                        .execute(
                            &"private_id@example.com".into(),
                            DummyCommand::Record("private_command".into())
                        )
                        .await,
                    Err(CommandGatewayError::Unavailable)
                );
            }
            let store = DummyStore::new(usize::MAX);
            let executor = dummy_executor(store.clone(), 3);
            assert_eq!(
                executor.execute(&"private_id".into(), DummyCommand::Reject).await,
                Err(CommandGatewayError::Domain(DummyError))
            );
            assert_eq!(
                executor
                    .execute(&"private_id".into(), DummyCommand::Record("private_command".into()))
                    .await,
                Err(CommandGatewayError::Conflict)
            );
            assert_eq!(store.commits.load(Ordering::SeqCst), 3);
        })
        .await;
        assert_eq!(log.matches("command execution failed").count(), 3);
        assert!(log.contains("aggregate_type=\"dummy\""));
        for category in ["connection", "deserialization", "unexpected"] {
            assert!(log.contains(category));
        }
        assert!(!log.contains("private_"));
    }

    /// A journal with an optimistic check that yields between load and commit, so concurrent commands
    /// really interleave; `external_conflicts` first commits fail as if another node had written.
    #[derive(Clone, Default)]
    struct VersionedStore {
        journal: Arc<Mutex<Vec<String>>>,
        calls: Arc<Mutex<Vec<String>>>,
        external_conflicts: Arc<AtomicUsize>,
        /// When set, every load takes a permit first: a command holds its lock while its load waits.
        load_gate: Option<Arc<tokio::sync::Semaphore>>,
        /// Loads begun, counted before the gate.
        loads: Arc<AtomicUsize>,
        /// The tasks of every successful commit, with the sequence of its event.
        published: Arc<Mutex<Vec<(usize, NewTask)>>>,
    }

    struct VersionedContext(DummyAggregate, usize);

    impl AggregateContext<DummyAggregate> for VersionedContext {
        fn aggregate(&self) -> &DummyAggregate {
            &self.0
        }
    }

    impl EventStore<DummyAggregate> for VersionedStore {
        type Context = VersionedContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, PersistenceError> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::Context, PersistenceError> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.load_gate {
                gate.acquire().await.unwrap().forget();
            }
            self.calls.lock().unwrap().push("load".to_owned());
            let sequence = self.journal.lock().unwrap().len();
            tokio::task::yield_now().await;
            Ok(VersionedContext(DummyAggregate, sequence))
        }

        async fn commit(
            &self,
            events: Vec<DummyEvent>,
            context: Self::Context,
            _: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, PersistenceError> {
            let [DummyEvent::Recorded(value)] = &events[..] else {
                panic!("one event per command");
            };
            let mut journal = self.journal.lock().unwrap();
            let external = self
                .external_conflicts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if external || journal.len() != context.1 {
                self.calls.lock().unwrap().push(format!("conflict {value}"));
                return Err(PersistenceError::OptimisticLockError);
            }
            journal.push(value.clone());
            self.calls.lock().unwrap().push(format!("commit {value}"));
            let sequence = journal.len();
            self.published
                .lock()
                .unwrap()
                .extend(tasks.iter().map(|task| (sequence, task.clone())));
            Ok(vec![EventEnvelope {
                aggregate_id: "aggregate".to_owned(),
                sequence,
                payload: events[0].clone(),
                metadata: HashMap::new(),
            }])
        }
    }

    fn versioned_executor(store: &VersionedStore, attempts: usize) -> StoreCommandExecutor<DummyAggregate, VersionedStore> {
        versioned_executor_with(store, attempts, LockLimits::default())
    }

    fn versioned_executor_with(
        store: &VersionedStore,
        attempts: usize,
        limits: LockLimits,
    ) -> StoreCommandExecutor<DummyAggregate, VersionedStore> {
        StoreCommandExecutor::new(store.clone(), Arc::default(), no_delay_policy(attempts), limits)
    }

    fn record(value: &str) -> DummyCommand {
        DummyCommand::Record(value.to_owned())
    }

    #[tokio::test]
    async fn locks_accept_twenty_parallel_commands_with_a_single_attempt_where_separate_executors_conflict() {
        for shared in [true, false] {
            let store = VersionedStore::default();
            let executor = Arc::new(versioned_executor(&store, 1));
            let mut commands = tokio::task::JoinSet::new();
            for index in 0..20 {
                // An executor of its own per command is like a process per command: nothing orders them.
                let executor = if shared {
                    executor.clone()
                } else {
                    Arc::new(versioned_executor(&store, 1))
                };
                commands.spawn(async move { executor.execute(&"aggregate".into(), record(&index.to_string())).await });
            }
            let mut conflicts = 0;
            while let Some(result) = commands.join_next().await {
                match result.unwrap() {
                    Ok(_) => {}
                    Err(CommandGatewayError::Conflict) => conflicts += 1,
                    other => panic!("unexpected result {other:?}"),
                }
            }
            let accepted = store.journal.lock().unwrap().len();
            if shared {
                assert_eq!(
                    (accepted, conflicts),
                    (20, 0),
                    "one at a time, every load sees the last commit"
                );
            } else {
                assert_eq!((accepted, conflicts), (1, 19), "the control: all load the same version");
            }
        }
    }

    #[tokio::test]
    async fn a_conflict_retries_inside_the_lock_before_the_next_command_starts() {
        for shared in [true, false] {
            let store = VersionedStore::default();
            store.external_conflicts.store(1, Ordering::SeqCst);
            let executor = Arc::new(versioned_executor(&store, 3));
            let second_executor = if shared {
                executor.clone()
            } else {
                Arc::new(versioned_executor(&store, 3))
            };
            let first = tokio::spawn({
                let executor = executor.clone();
                async move { executor.execute(&"aggregate".into(), record("a")).await }
            });
            let second = tokio::spawn(async move { second_executor.execute(&"aggregate".into(), record("b")).await });
            assert!(first.await.unwrap().is_ok() && second.await.unwrap().is_ok());
            let calls = store.calls.lock().unwrap().clone();
            if shared {
                assert_eq!(calls, ["load", "conflict a", "load", "commit a", "load", "commit b"]);
            } else {
                assert_eq!(
                    calls[..2],
                    ["load", "load"],
                    "the control: with an executor of its own `b` loads while `a` is still deciding"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_planner_runs_inside_the_aggregate_lock_between_the_load_and_the_commit() {
        let store = VersionedStore::default();
        let planner = ValuePlanner {
            log: Some(store.calls.clone()),
            ..ValuePlanner::default()
        };
        let executor = Arc::new(versioned_executor(&store, 1).with_task_planner(planner));
        let mut commands = tokio::task::JoinSet::new();
        for index in 0..20 {
            let executor = executor.clone();
            commands.spawn(async move { executor.execute(&"aggregate".into(), record(&index.to_string())).await });
        }
        while let Some(result) = commands.join_next().await {
            assert_eq!(result.unwrap().unwrap().tasks.len(), 1);
        }
        let calls = store.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 60);
        for step in calls.chunks(3) {
            let value = step[1].strip_prefix("plan ").expect("a plan follows every load");
            assert_eq!(
                step,
                ["load".to_owned(), format!("plan {value}"), format!("commit {value}")],
                "no other command of the key loads between a load and its commit: {calls:?}"
            );
        }
        let published = store.published.lock().unwrap().clone();
        let journal = store.journal.lock().unwrap().clone();
        assert_eq!(published.len(), 20);
        for (sequence, task) in published {
            assert_eq!(
                task.payload()["value"],
                journal[sequence - 1],
                "every task goes with its own event"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_executors_publish_only_the_tasks_of_attempts_that_committed() {
        let store = VersionedStore::default();
        let planner = ValuePlanner::default();
        let nodes = [
            Arc::new(versioned_executor(&store, 3).with_task_planner(planner.clone())),
            Arc::new(versioned_executor(&store, 3).with_task_planner(planner.clone())),
        ];
        let mut commands = tokio::task::JoinSet::new();
        for index in 0..20 {
            let node = nodes[index % 2].clone();
            commands.spawn(async move { (index, node.execute(&"aggregate".into(), record(&index.to_string())).await) });
        }
        let mut reports = Vec::new();
        while let Some(result) = commands.join_next().await {
            match result.unwrap() {
                (index, Ok(report)) => reports.push((index, report)),
                (_, Err(CommandGatewayError::Conflict)) => {}
                (_, other) => panic!("unexpected result {other:?}"),
            }
        }
        let journal = store.journal.lock().unwrap().clone();
        let published = store.published.lock().unwrap().clone();
        assert_eq!(
            published.len(),
            journal.len(),
            "one task per committed event, none of a lost attempt"
        );
        for (sequence, task) in &published {
            assert_eq!(task.payload()["value"], journal[sequence - 1]);
        }
        assert_eq!(reports.len(), journal.len());
        for (index, report) in reports {
            assert_eq!(report.tasks.len(), 1);
            assert_eq!(report.tasks[0].payload()["value"], index.to_string());
            assert_eq!(report.events[0].payload, DummyEvent::Recorded(index.to_string()));
        }
        assert!(
            planner.calls.load(Ordering::SeqCst) >= journal.len(),
            "lost attempts planned too, and published nothing"
        );
    }

    #[tokio::test]
    async fn a_panicking_planner_unwinds_and_releases_the_aggregate_lock() {
        let store = VersionedStore::default();
        let executor = Arc::new(
            versioned_executor_with(&store, 1, LockLimits::try_new(Duration::from_millis(500), 4).unwrap())
                .with_task_planner(ValuePlanner::default()),
        );
        let panicking = tokio::spawn({
            let executor = executor.clone();
            async move { executor.execute(&"aggregate".into(), record("panic")).await }
        });
        assert!(panicking.await.unwrap_err().is_panic());
        let report = executor.execute(&"aggregate".into(), record("after")).await.unwrap();
        assert_eq!(report.tasks.len(), 1, "the lock was released and the next command ran");
        assert_eq!(
            *store.journal.lock().unwrap(),
            ["after"],
            "the panicking command wrote nothing"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_lock_is_unavailable_logged_without_id_and_loads_or_plans_nothing() {
        let load_gate = closed_gate();
        let store = VersionedStore {
            load_gate: Some(load_gate.clone()),
            ..VersionedStore::default()
        };
        let planner = ValuePlanner::default();
        let executor = Arc::new(
            versioned_executor_with(&store, 1, LockLimits::try_new(Duration::from_millis(100), 1).unwrap())
                .with_task_planner(planner.clone()),
        );
        // The holder keeps the key's lock while its load waits at the gate.
        let holder = tokio::spawn({
            let executor = executor.clone();
            async move { executor.execute(&"private_id".into(), record("private_holder")).await }
        });
        until(|| store.loads.load(Ordering::SeqCst) == 1).await;
        let (_, log) = crate::diagnostics::test_support::capture_at(tracing::Level::WARN, async {
            let private_id = String::from("private_id");
            let mut waiting = Box::pin(executor.execute(&private_id, record("private_waiting")));
            assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending())).await);
            assert_eq!(
                executor.execute(&"private_id".into(), record("private_refused")).await,
                Err(CommandGatewayError::Unavailable),
                "the line is full"
            );
            // The clock is paused: the idle runtime advances it to the waiter's deadline.
            assert_eq!(
                waiting.await,
                Err(CommandGatewayError::Unavailable),
                "the lock was not acquired in time"
            );
        })
        .await;
        load_gate.add_permits(1);
        assert_eq!(holder.await.unwrap().unwrap().tasks.len(), 1);
        assert_eq!(
            (store.loads.load(Ordering::SeqCst), planner.calls.load(Ordering::SeqCst)),
            (1, 1),
            "only the holder loaded and planned: a refused command loads, plans and writes nothing"
        );
        assert_eq!(log.matches("command refused before it ran").count(), 2, "{log}");
        assert!(
            log.contains("reason=\"too_many_waiters\"") && log.contains("reason=\"timed_out\""),
            "{log}"
        );
        assert!(log.contains("aggregate_type=\"dummy\""), "{log}");
        assert!(!log.contains("private_"), "{log}");
    }

    #[test]
    fn the_command_future_holding_a_lock_is_send() {
        fn require_send<F: std::future::Future + Send>(_: F) {}
        let executor = versioned_executor(&VersionedStore::default(), 1).with_task_planner(ValuePlanner::default());
        require_send(executor.execute(&"aggregate".into(), record("a")));
    }

    fn closed_gate() -> Arc<tokio::sync::Semaphore> {
        Arc::new(tokio::sync::Semaphore::new(0))
    }

    async fn until(mut done: impl FnMut() -> bool) {
        for _ in 0..1000 {
            if done() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the condition never came true");
    }

    #[tokio::test]
    async fn the_commit_wakeup_fires_after_a_commit_with_events_and_not_otherwise() {
        use std::{future::Future, pin::pin, task::Context, task::Waker};
        let wakeup = CommitWakeup::new();
        let store = VersionedStore::default();
        let executor = versioned_executor(&store, 1).with_commit_wakeup(wakeup.clone());
        let mut woken = pin!(wakeup.wait());
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        executor.execute(&"aggregate".into(), record("a")).await.unwrap();
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        // Neither a rejected command nor one without events wakes anybody.
        let quiet = dummy_executor(DummyStore::new(0), 1).with_commit_wakeup(wakeup.clone());
        let mut silent = pin!(wakeup.wait());
        assert!(silent.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert_eq!(
            quiet.execute(&"aggregate".into(), DummyCommand::Reject).await,
            Err(CommandGatewayError::Domain(DummyError))
        );
        assert_eq!(
            quiet.execute(&"aggregate".into(), record("a")).await,
            Ok(CommitReport::default())
        );
        assert!(silent.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }

    #[test]
    fn maps_every_framework_error_category() {
        assert_eq!(
            map_aggregate_error(AggregateError::Domain(DummyError)),
            CommandGatewayError::Domain(DummyError)
        );
        assert_eq!(
            map_aggregate_error::<DummyError>(AggregateError::AggregateConflict),
            CommandGatewayError::Conflict
        );
        for error in [
            AggregateError::DatabaseConnectionError(Box::new(io::Error::other("db"))),
            AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
            AggregateError::TaskPlan(TaskPlanError::application("refused")),
        ] {
            assert_eq!(map_aggregate_error::<DummyError>(error), CommandGatewayError::Unavailable);
        }
    }

    /// A running total whose state the planner can see: every load returns what the previous attempts
    /// left, so a retried attempt decides on a fresh state.
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Tally(u32);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Added(u32);

    impl Events for Added {
        fn event_type(&self) -> &'static str {
            "added"
        }
        fn event_version(&self) -> &'static str {
            "1"
        }
    }

    impl Aggregate for Tally {
        const TYPE: &'static str = "tally";
        type Id = String;
        type Command = u32;
        type Event = Added;
        type Error = DummyError;
        type Rules = ();

        fn apply(&mut self, event: Added) {
            self.0 += event.0;
        }
    }

    impl Decide<Tally> for u32 {
        fn decide(self, _: &Tally, _: &()) -> Result<Added, DummyError> {
            Ok(Added(self))
        }
    }

    struct TallyContext(Tally);

    impl AggregateContext<Tally> for TallyContext {
        fn aggregate(&self) -> &Tally {
            &self.0
        }
    }

    /// Every load starts from 10 more than the last, as if another node committed in between; the first
    /// `conflicts` commits conflict. Records the tasks of every commit attempt.
    #[derive(Clone)]
    struct TallyStore {
        loads: Arc<AtomicUsize>,
        conflicts: usize,
        attempts: Arc<Mutex<Vec<Vec<NewTask>>>>,
    }

    impl TallyStore {
        fn new(conflicts: usize) -> Self {
            Self {
                loads: Arc::default(),
                conflicts,
                attempts: Arc::default(),
            }
        }
    }

    impl EventStore<Tally> for TallyStore {
        type Context = TallyContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<Tally>>, PersistenceError> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::Context, PersistenceError> {
            let loads = self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(TallyContext(Tally(10 * loads as u32)))
        }

        async fn commit(
            &self,
            events: Vec<Added>,
            _: Self::Context,
            metadata: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<EventEnvelope<Tally>>, PersistenceError> {
            let mut attempts = self.attempts.lock().unwrap();
            attempts.push(tasks.to_vec());
            if attempts.len() <= self.conflicts {
                return Err(PersistenceError::OptimisticLockError);
            }
            Ok(events
                .into_iter()
                .map(|payload| EventEnvelope {
                    aggregate_id: "tally".to_owned(),
                    sequence: attempts.len(),
                    payload,
                    metadata: metadata.clone(),
                })
                .collect())
        }
    }

    #[derive(Serialize)]
    struct Total {
        before: u32,
        after: u32,
    }

    impl Task for Total {
        const TASK_TYPE: &'static str = "total";
        const TASK_VERSION: i32 = 1;
    }

    /// Plans the state before and after the decision; refuses a decision on a state above `refuse_above`.
    struct TallyPlanner {
        refuse_above: Option<u32>,
    }

    impl TaskPlanner<Tally> for TallyPlanner {
        fn plan(&self, decision: &Decision<'_, Tally>) -> Result<Vec<NewTask>, TaskPlanError> {
            if self.refuse_above.is_some_and(|limit| decision.before.0 > limit) {
                return Err(TaskPlanError::application("too much"));
            }
            Ok(vec![NewTask::try_new(Total {
                before: decision.before.0,
                after: decision.after.0,
            })?])
        }
    }

    #[tokio::test]
    async fn a_conflict_replans_from_the_fresh_state_and_keeps_the_task_ids() {
        let store = TallyStore::new(2);
        let executor = StoreCommandExecutor::new(store.clone(), (), no_delay_policy(3), LockLimits::default())
            .with_task_planner(TallyPlanner { refuse_above: None });
        let report = executor
            .execute_with_metadata(&"tally".into(), 5, Causation::root("test").event_metadata())
            .await
            .unwrap();
        let attempts = store.attempts.lock().unwrap().clone();
        let totals: Vec<_> = attempts
            .iter()
            .map(|tasks| (tasks[0].payload()["before"].clone(), tasks[0].payload()["after"].clone()))
            .collect();
        assert_eq!(
            totals,
            [
                (serde_json::json!(0), serde_json::json!(5)),
                (serde_json::json!(10), serde_json::json!(15)),
                (serde_json::json!(20), serde_json::json!(25)),
            ],
            "every attempt planned from the state it loaded"
        );
        assert!(
            attempts.iter().all(|tasks| tasks[0].id() == attempts[0][0].id()),
            "the id is the command's, whatever the attempt decided"
        );
        assert_eq!(report.tasks, attempts[2]);
        assert_eq!(report.events.len(), 1);
    }

    /// How often a [`RoomId`] was formatted; only the test below formats one.
    static ROOM_IDS_FORMATTED: AtomicUsize = AtomicUsize::new(0);

    /// A room's id whose text is not its fields: `RoomId { floor: 3, number: 7 }` is `3-07`. Every formatting is
    /// counted.
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct RoomId {
        floor: u8,
        number: u8,
    }

    impl std::fmt::Display for RoomId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            ROOM_IDS_FORMATTED.fetch_add(1, Ordering::SeqCst);
            write!(f, "{}-{:02}", self.floor, self.number)
        }
    }

    impl std::str::FromStr for RoomId {
        type Err = String;

        fn from_str(text: &str) -> Result<Self, String> {
            let (floor, number) = text.split_once('-').ok_or_else(|| format!("no room id: {text}"))?;
            match (floor.parse(), number.parse()) {
                (Ok(floor), Ok(number)) => Ok(Self { floor, number }),
                _ => Err(format!("no room id: {text}")),
            }
        }
    }

    /// A hotel room, known by a typed id.
    #[derive(Clone, Default, Serialize, Deserialize)]
    struct Room;

    impl Aggregate for Room {
        const TYPE: &'static str = "room";
        type Id = RoomId;
        type Command = ();
        type Event = DummyEvent;
        type Error = DummyError;
        type Rules = ();

        fn apply(&mut self, _: DummyEvent) {}
    }

    impl Decide<Room> for () {
        fn decide(self, _: &Room, _: &()) -> Result<DummyEvent, DummyError> {
            Ok(DummyEvent::Recorded("checked in".to_owned()))
        }
    }

    struct RoomContext(Room, String);

    impl AggregateContext<Room> for RoomContext {
        fn aggregate(&self) -> &Room {
            &self.0
        }
    }

    /// Records the key of every load and lets a load in only with a permit of its gate; the first commit
    /// conflicts, and a committed event carries the key it was loaded with.
    #[derive(Clone)]
    struct RoomStore {
        loads: Arc<Mutex<Vec<String>>>,
        gate: Arc<tokio::sync::Semaphore>,
        commits: Arc<AtomicUsize>,
    }

    impl EventStore<Room> for RoomStore {
        type Context = RoomContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<Room>>, PersistenceError> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, key: &str) -> Result<Self::Context, PersistenceError> {
            self.loads.lock().unwrap().push(key.to_owned());
            self.gate.acquire().await.unwrap().forget();
            Ok(RoomContext(Room, key.to_owned()))
        }

        async fn commit(
            &self,
            events: Vec<DummyEvent>,
            context: Self::Context,
            metadata: HashMap<String, String>,
            _: &[NewTask],
        ) -> Result<Vec<EventEnvelope<Room>>, PersistenceError> {
            if self.commits.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(PersistenceError::OptimisticLockError);
            }
            Ok(events
                .into_iter()
                .map(|payload| EventEnvelope {
                    aggregate_id: context.1.clone(),
                    sequence: 1,
                    payload,
                    metadata: metadata.clone(),
                })
                .collect())
        }
    }

    /// Plans one task and writes down the address of the id every decision holds.
    #[derive(Clone, Default)]
    struct RoomPlanner {
        ids: Arc<Mutex<Vec<usize>>>,
    }

    impl TaskPlanner<Room> for RoomPlanner {
        fn plan(&self, decision: &Decision<'_, Room>) -> Result<Vec<NewTask>, TaskPlanError> {
            self.ids
                .lock()
                .unwrap()
                .push(std::ptr::from_ref(decision.aggregate_id) as usize);
            Ok(vec![NewTask::try_new(DummyTask {
                value: "room".to_owned(),
            })?])
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_id_is_formatted_once_per_command_and_its_text_keys_the_lock_the_store_and_the_task_ids() {
        let store = RoomStore {
            loads: Arc::default(),
            gate: closed_gate(),
            commits: Arc::default(),
        };
        let planner = RoomPlanner::default();
        let executor = StoreCommandExecutor::new(
            store.clone(),
            (),
            no_delay_policy(2),
            LockLimits::try_new(Duration::from_millis(100), 1).unwrap(),
        )
        .with_task_planner(planner.clone());
        let id = RoomId { floor: 3, number: 7 };
        let causation = Causation::root("test");
        let mut command = std::pin::pin!(executor.execute_with_metadata(&id, (), causation.event_metadata()));
        // One poll: the command takes its lock and waits at the gate of its first load.
        assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(command.as_mut().poll(cx).is_pending())).await);
        assert_eq!(
            executor
                .locks
                .acquire(AggregateKey::new(Room::TYPE, "3-07".to_owned()))
                .await
                .err(),
            Some(LockRefused::TimedOut),
            "the command holds the lock of the id's text"
        );
        drop(
            executor
                .locks
                .acquire(AggregateKey::new(Room::TYPE, "3-08".to_owned()))
                .await
                .unwrap(),
        );
        store.gate.add_permits(2);
        let report = command.await.unwrap();
        assert_eq!(
            *store.loads.lock().unwrap(),
            ["3-07", "3-07"],
            "both attempts load the id's text"
        );
        assert_eq!(
            ROOM_IDS_FORMATTED.load(Ordering::SeqCst),
            1,
            "one formatting for the lock and both attempts"
        );
        assert_eq!(report.events[0].aggregate_id, "3-07");
        assert_eq!(report.events[0].aggregate_id.parse::<RoomId>(), Ok(id.clone()));
        assert_eq!(
            report.tasks[0].id(),
            causation.task_id("room", "3-07", report.tasks[0].format(), 0)
        );
        let address = std::ptr::from_ref(&id) as usize;
        assert_eq!(
            *planner.ids.lock().unwrap(),
            [address, address],
            "every decision holds the caller's id, not a copy"
        );
        // The framework on its own runs one attempt and formats the id once for it.
        store.gate.add_permits(1);
        executor.attempt.execute_with_metadata(&id, (), HashMap::new()).await.unwrap();
        assert_eq!(ROOM_IDS_FORMATTED.load(Ordering::SeqCst), 2);
        assert_eq!(store.loads.lock().unwrap().last().map(String::as_str), Some("3-07"));
        assert_eq!("3".parse::<RoomId>(), Err("no room id: 3".to_owned()));
        assert_eq!("3-x".parse::<RoomId>(), Err("no room id: 3-x".to_owned()));
    }

    /// What a report says: the id, the command, the `source` of the metadata and the outcome (the values of an accepted
    /// command's tasks).
    type Report = (String, DummyCommand, Option<String>, String);

    /// Notes every report.
    #[derive(Default)]
    struct Heard(Mutex<Vec<Report>>);

    impl Heard {
        fn taken(&self) -> Vec<Report> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }
    }

    impl CommandObserver<DummyAggregate> for Heard {
        fn command(&self, report: &CommandReport<'_, DummyAggregate>) {
            let outcome = match report.outcome {
                Outcome::Accepted { tasks } => format!(
                    "accepted {:?}",
                    tasks
                        .iter()
                        .map(|task| task.payload()["value"].as_str().unwrap_or_default().to_owned())
                        .collect::<Vec<_>>()
                ),
                Outcome::Rejected(error) => format!("rejected: {error}"),
                Outcome::Conflict => "conflict".to_owned(),
                Outcome::Unavailable => "unavailable".to_owned(),
            };
            self.0.lock().unwrap().push((
                report.aggregate_id.clone(),
                report.command.clone(),
                report.metadata.get("source").cloned(),
                outcome,
            ));
        }
    }

    fn heard(id: &str, command: DummyCommand, source: Option<&str>, outcome: &str) -> Report {
        (id.to_owned(), command, source.map(str::to_owned), outcome.to_owned())
    }

    #[tokio::test]
    async fn the_observer_hears_every_outcome_once_with_the_id_the_command_and_its_metadata() {
        let observer = Arc::new(Heard::default());
        let accepting = dummy_executor(DummyStore::new(0), 1)
            .with_task_planner(ValuePlanner::default())
            .with_observer(observer.clone());
        let source = HashMap::from([("source".to_owned(), "test".to_owned())]);
        let report = accepting
            .execute_with_metadata(&"a-1".into(), record("x"), source.clone())
            .await
            .unwrap();
        assert_eq!(report.tasks.len(), 1);
        assert_eq!(
            accepting.execute(&"a-2".into(), DummyCommand::Reject).await,
            Err(CommandGatewayError::Domain(DummyError))
        );
        let conflicting = dummy_executor(DummyStore::new(5), 2).with_observer(observer.clone());
        assert_eq!(
            conflicting.execute(&"a-3".into(), record("y")).await,
            Err(CommandGatewayError::Conflict)
        );
        let failing = dummy_executor(
            DummyStore::failing(|| PersistenceError::ConnectionError(Box::new(io::Error::other("down")))),
            1,
        )
        .with_observer(observer.clone());
        assert_eq!(
            failing.execute_with_metadata(&"a-4".into(), record("z"), source).await,
            Err(CommandGatewayError::Unavailable)
        );
        assert_eq!(
            observer.taken(),
            [
                heard("a-1", record("x"), Some("test"), r#"accepted ["x"]"#),
                heard("a-2", DummyCommand::Reject, None, "rejected: rejected"),
                heard("a-3", record("y"), None, "conflict"),
                heard("a-4", record("z"), Some("test"), "unavailable"),
            ],
            "one report per command, after its last attempt"
        );
    }

    /// Notes the state before every decision whose tasks a report names.
    #[derive(Default)]
    struct Totals(Mutex<Vec<Vec<serde_json::Value>>>);

    impl CommandObserver<Tally> for Totals {
        fn command(&self, report: &CommandReport<'_, Tally>) {
            let Outcome::Accepted { tasks } = report.outcome else {
                panic!("the tally accepts every command");
            };
            let before = tasks.iter().map(|task| task.payload()["before"].clone()).collect();
            self.0.lock().unwrap().push(before);
        }
    }

    #[tokio::test]
    async fn the_observer_hears_the_tasks_of_the_attempt_that_committed() {
        let store = TallyStore::new(2);
        let (replaced, observer) = (Arc::new(Totals::default()), Arc::new(Totals::default()));
        let executor = StoreCommandExecutor::new(store.clone(), (), no_delay_policy(3), LockLimits::default())
            .with_task_planner(TallyPlanner { refuse_above: None })
            .with_observer(replaced.clone())
            .with_observer(observer.clone());
        let report = executor.execute(&"tally".into(), 5).await.unwrap();
        let attempts = store.attempts.lock().unwrap().clone();
        assert_eq!(attempts.len(), 3, "two conflicts, then the commit");
        assert_eq!(report.tasks, attempts[2]);
        assert_eq!(
            *observer.0.lock().unwrap(),
            [vec![serde_json::json!(20)]],
            "the tasks planned from the state the committing attempt loaded"
        );
        assert!(replaced.0.lock().unwrap().is_empty(), "a second observer replaces the first");
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_lock_is_reported_unavailable_and_loads_nothing() {
        let load_gate = closed_gate();
        let store = VersionedStore {
            load_gate: Some(load_gate.clone()),
            ..VersionedStore::default()
        };
        let observer = Arc::new(Heard::default());
        let executor = Arc::new(
            versioned_executor_with(&store, 1, LockLimits::try_new(Duration::from_millis(100), 1).unwrap())
                .with_observer(observer.clone()),
        );
        // The holder keeps the key's lock while its load waits at the gate.
        let holder = tokio::spawn({
            let executor = executor.clone();
            async move { executor.execute(&"aggregate".into(), record("holder")).await }
        });
        until(|| store.loads.load(Ordering::SeqCst) == 1).await;
        // The clock is paused: the idle runtime advances it to the waiter's deadline.
        assert_eq!(
            executor.execute(&"aggregate".into(), record("refused")).await,
            Err(CommandGatewayError::Unavailable)
        );
        assert_eq!(
            observer.taken(),
            [heard("aggregate", record("refused"), None, "unavailable")],
            "the refusal is reported while the holder still waits"
        );
        load_gate.add_permits(1);
        assert!(holder.await.unwrap().is_ok());
        assert_eq!(observer.taken(), [heard("aggregate", record("holder"), None, "accepted []")]);
        assert_eq!(store.loads.load(Ordering::SeqCst), 1, "the refused command loaded nothing");
    }

    /// Keeps the report of `first` until `second` has reported (at most five seconds), and notes whether the commit
    /// wake-up had come by then.
    struct Holding {
        entered: Mutex<std::sync::mpsc::Sender<()>>,
        second: Mutex<std::sync::mpsc::Sender<()>>,
        second_reported: Mutex<std::sync::mpsc::Receiver<()>>,
        woken: Mutex<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
        log: Mutex<Vec<String>>,
    }

    impl CommandObserver<DummyAggregate> for Holding {
        fn command(&self, report: &CommandReport<'_, DummyAggregate>) {
            if *report.command != record("first") {
                self.log.lock().unwrap().push("second".to_owned());
                self.second.lock().unwrap().send(()).unwrap();
                return;
            }
            let woken = self
                .woken
                .lock()
                .unwrap()
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_ready();
            self.entered.lock().unwrap().send(()).unwrap();
            let second = self
                .second_reported
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .is_ok();
            self.log
                .lock()
                .unwrap()
                .push(format!("first: woken {woken}, the second reported meanwhile {second}"));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_observer_runs_once_the_lock_is_released_and_the_wakeup_sent() {
        use std::future::Future;
        let wakeup = CommitWakeup::new();
        let mut woken: std::pin::Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin({
            let wakeup = wakeup.clone();
            async move { wakeup.wait().await }
        });
        let waiting = woken
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
        assert!(waiting.is_pending(), "the wait is registered before the command");
        let (entered, first_entered) = std::sync::mpsc::channel();
        let (second, second_reported) = std::sync::mpsc::channel();
        let observer = Arc::new(Holding {
            entered: Mutex::new(entered),
            second: Mutex::new(second),
            second_reported: Mutex::new(second_reported),
            woken: Mutex::new(woken),
            log: Mutex::default(),
        });
        let store = VersionedStore::default();
        let executor = Arc::new(
            versioned_executor(&store, 1)
                .with_commit_wakeup(wakeup)
                .with_observer(observer.clone()),
        );
        let first = tokio::spawn({
            let executor = executor.clone();
            async move { executor.execute(&"aggregate".into(), record("first")).await }
        });
        // A blocking wait: the observer blocks a worker, and a worker blocked outside the driver stalls the timers.
        assert!(
            first_entered.recv_timeout(Duration::from_secs(5)).is_ok(),
            "the first command's observer is running"
        );
        // The default lock limits: a lock still held would refuse the second command after a second.
        assert!(
            executor.execute(&"aggregate".into(), record("second")).await.is_ok(),
            "the next command of the key does not wait for the observer"
        );
        assert!(first.await.unwrap().is_ok());
        assert_eq!(
            *observer.log.lock().unwrap(),
            ["second", "first: woken true, the second reported meanwhile true"]
        );
        assert_eq!(*store.journal.lock().unwrap(), ["first", "second"]);
    }

    #[tokio::test]
    async fn the_reports_of_one_key_come_in_the_order_of_its_commits() {
        let store = VersionedStore::default();
        let observer = Arc::new(Heard::default());
        let executor = Arc::new(versioned_executor(&store, 1).with_observer(observer.clone()));
        let mut commands = tokio::task::JoinSet::new();
        for index in 0..12 {
            let executor = executor.clone();
            commands.spawn(async move { executor.execute(&"aggregate".into(), record(&index.to_string())).await });
        }
        while let Some(result) = commands.join_next().await {
            assert!(result.unwrap().is_ok());
        }
        let reported: Vec<String> = observer
            .taken()
            .into_iter()
            .map(|(_, command, _, _)| match command {
                DummyCommand::Record(value) => value,
                DummyCommand::Reject => panic!("no rejection was sent"),
            })
            .collect();
        assert_eq!(reported, *store.journal.lock().unwrap());
    }

    /// Panics on the report of `boom`.
    struct Panicking;

    impl CommandObserver<DummyAggregate> for Panicking {
        fn command(&self, report: &CommandReport<'_, DummyAggregate>) {
            if *report.command == record("boom") {
                panic!("the observer panics");
            }
        }
    }

    #[tokio::test]
    async fn a_panicking_observer_panics_its_caller_after_the_commit_and_the_lock_stays_free() {
        let store = VersionedStore::default();
        let executor = Arc::new(versioned_executor(&store, 1).with_observer(Arc::new(Panicking)));
        let boom = tokio::spawn({
            let executor = executor.clone();
            async move { executor.execute(&"aggregate".into(), record("boom")).await }
        });
        assert!(boom.await.unwrap_err().is_panic());
        assert_eq!(*store.journal.lock().unwrap(), ["boom"], "the commit stays");
        assert!(
            executor.execute(&"aggregate".into(), record("next")).await.is_ok(),
            "the lock was released before the observer"
        );
        assert_eq!(*store.journal.lock().unwrap(), ["boom", "next"]);
    }
}
