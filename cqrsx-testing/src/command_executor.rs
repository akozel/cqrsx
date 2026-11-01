//! A command executor over a test event store or an in-memory journal, for use-case tests.
use crate::memory_store::MemoryStore;
use cqrsx::{
    command::{
        CommandError, CommandExecutor, CommitReport, executor::StoreCommandExecutor, locks::LockLimits,
        retry::AggregateConflictRetryPolicy,
    },
    domain::aggregate::Aggregate,
    journal::EventStore,
    mutex::lock,
    task::NewTask,
};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

/// The production command path behind [`CommandExecutor`] over a test store: one [`StoreCommandExecutor`]
/// with aggregate locks, the default conflict retry, an optional task planner, commit wake-up and observer, plus a
/// record of the tasks the committed calls published.
///
/// *Published* tasks are those of the calls that committed (`Ok`): what their commits report, i.e. what a
/// transactional outbox would hold. Clones share the store, the locks, the planner, the wake-up, the observer and
/// the record, so a use case can own one clone while the test inspects another. The planner, the wake-up and the
/// observer are registered on the executor handed to [`configured`](Self::configured), before it is shared.
///
/// Over a [`MemoryStore`] ([`new`](Self::new)) the commands only change the aggregates. Over a
/// [`JournalEventStore`](cqrsx::journal::store::JournalEventStore) on a
/// [`MemoryEventRepository`](cqrsx::memory::event_repository::MemoryEventRepository) ([`configured`](Self::configured))
/// they append to a journal that projection processors read, and the journal's outbox
/// ([`MemoryEventRepository::outbox`](cqrsx::memory::event_repository::MemoryEventRepository::outbox)) holds the published tasks: give
/// the processors the same journal and the executor their [`CommitWakeup`](cqrsx::journal::wakeup::CommitWakeup).
///
/// ```
/// use cqrsx::{
///     command::{
///         CommandExecutor, CommandError,
///         causation::Causation,
///         planner::{Decision, TaskPlanError, TaskPlanner},
///     },
///     domain::{aggregate::Aggregate, command::Decide, event::Events},
/// };
/// use cqrsx::task::{NewTask, Task};
/// use cqrsx_testing::{command_executor::TestCommandExecutor, memory_store::MemoryStore};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Default, Serialize, Deserialize)]
/// struct Counter(u32);
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// struct Incremented;
/// impl Events for Incremented {
///     fn event_type(&self) -> &'static str { "incremented" }
///     fn event_version(&self) -> &'static str { "1" }
/// }
/// #[derive(Debug, PartialEq, thiserror::Error)]
/// #[error("limit reached")]
/// struct LimitReached;
/// impl Aggregate for Counter {
///     const TYPE: &'static str = "counter";
///     type Id = String;
///     type Command = ();
///     type Event = Incremented;
///     type Rejection = LimitReached;
///     type Rules = u32;
///     fn apply(&mut self, _: Incremented) { self.0 += 1; }
/// }
/// impl Decide<Counter> for () {
///     fn decide(self, counter: &Counter, limit: &u32) -> Result<Incremented, LimitReached> {
///         if counter.0 >= *limit {
///             return Err(LimitReached);
///         }
///         Ok(Incremented)
///     }
/// }
/// #[derive(Serialize)]
/// struct Notify {}
/// impl Task for Notify {
///     const TYPE: &'static str = "notify";
///     const VERSION: i32 = 1;
/// }
/// /// Every increment notifies.
/// struct Notifier;
/// impl TaskPlanner<Counter> for Notifier {
///     fn plan(&self, decision: &Decision<'_, Counter>) -> Result<Vec<NewTask>, TaskPlanError> {
///         Ok(vec![NewTask::try_new(Notify {})?])
///     }
/// }
///
/// # tokio_test(async {
/// let executor = TestCommandExecutor::configured(MemoryStore::<Counter>::default(), 1, |executor| {
///     executor.with_task_planner(Notifier)
/// });
/// let use_case = executor.clone(); // what the application under test receives
/// let causation = Causation::root("test");
/// let report = use_case.execute_with_metadata(&"c-1".into(), (), causation.event_metadata()).await.unwrap();
/// assert_eq!(report.tasks[0].metadata()["correlation_id"], causation.correlation_id.to_string());
/// assert_eq!(
///     use_case.execute(&"c-1".into(), ()).await,
///     Err(CommandError::Rejected(LimitReached))
/// );
/// assert_eq!(executor.take_published_tasks().len(), 1, "the second call was rejected");
/// # });
/// # fn tokio_test(future: impl std::future::Future<Output = ()>) {
/// #     tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(future)
/// # }
/// ```
#[must_use]
pub struct TestCommandExecutor<A: Aggregate, S: EventStore<A> = MemoryStore<A>> {
    executor: Arc<StoreCommandExecutor<A, S>>,
    published: Arc<Mutex<Vec<NewTask>>>,
}

// By hand: the published tasks are not printed, their payloads are the application's data.
impl<A: Aggregate, S: EventStore<A>> fmt::Debug for TestCommandExecutor<A, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestCommandExecutor")
            .field("executor", &self.executor)
            .finish_non_exhaustive()
    }
}

impl<A: Aggregate + 'static, S: EventStore<A>> TestCommandExecutor<A, S> {
    /// Execute against `store` with `rules`, the default conflict retry and the default lock limits.
    pub fn new(store: S, rules: A::Rules) -> Self {
        Self::configured(store, rules, |executor| executor)
    }

    /// Execute against `store` with `rules` like [`new`](Self::new), on the executor `configure` returns: register the
    /// task planner (`with_task_planner`), the commit wake-up of the projection processors under test
    /// (`with_commit_wakeup`) and the observer of every command (`with_observer`) on it. The executor is shared only
    /// afterwards, so every clone has them.
    pub fn configured(
        store: S,
        rules: A::Rules,
        configure: impl FnOnce(StoreCommandExecutor<A, S>) -> StoreCommandExecutor<A, S>,
    ) -> Self {
        let executor = StoreCommandExecutor::new(store, rules, AggregateConflictRetryPolicy::default(), LockLimits::default());
        Self {
            executor: Arc::new(configure(executor)),
            published: Arc::default(),
        }
    }

    /// The store the commands run against.
    pub fn store(&self) -> &S {
        self.executor.store()
    }

    /// The tasks of the calls that committed since the last call, in call order; clears the record.
    pub fn take_published_tasks(&self) -> Vec<NewTask> {
        std::mem::take(&mut *lock(&self.published, "test executor published tasks"))
    }
}

impl<A: Aggregate, S: EventStore<A>> Clone for TestCommandExecutor<A, S> {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            published: self.published.clone(),
        }
    }
}

impl<A, S> CommandExecutor<A> for TestCommandExecutor<A, S>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + 'static,
    A::Rejection: Send + 'static,
    S: EventStore<A> + 'static,
    S::Context: Send,
{
    async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandError<A::Rejection>> {
        let result = self.executor.execute_with_metadata(id, command, metadata).await;
        if let Ok(report) = &result {
            lock(&self.published, "test executor published tasks").extend(report.tasks.iter().cloned());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unavailable_store::UnavailableStore;
    use cqrsx::{
        command::{
            causation::Causation,
            observer::CommandObserver,
            planner::{Decision, TaskPlanError, TaskPlanner},
        },
        domain::{command::Decide, event::Events},
        journal::{repository::EventRepository, store::JournalEventStore, wakeup::CommitWakeup},
        memory::event_repository::MemoryEventRepository,
        task::Task,
    };
    use serde::{Deserialize, Serialize};

    /// Accepts values up to the cap in its rules.
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Stack(Vec<u8>);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Pushed(u8);

    impl Events for Pushed {
        fn event_type(&self) -> &'static str {
            "pushed"
        }
        fn event_version(&self) -> &'static str {
            "1"
        }
    }

    #[derive(Debug, PartialEq, thiserror::Error)]
    #[error("full")]
    struct Full;

    impl Aggregate for Stack {
        const TYPE: &'static str = "stack";
        type Id = String;
        type Command = u8;
        type Event = Pushed;
        type Rejection = Full;
        type Rules = usize;

        fn apply(&mut self, event: Pushed) {
            self.0.push(event.0);
        }
    }

    impl Decide<Stack> for u8 {
        fn decide(self, stack: &Stack, cap: &usize) -> Result<Pushed, Full> {
            if stack.0.len() >= *cap {
                return Err(Full);
            }
            Ok(Pushed(self))
        }
    }

    #[derive(Serialize)]
    struct Notify {
        value: u8,
    }

    impl Task for Notify {
        const TYPE: &'static str = "notify";
        const VERSION: i32 = 1;
    }

    /// One notification per pushed value.
    struct Notifier;

    impl TaskPlanner<Stack> for Notifier {
        fn plan(&self, decision: &Decision<'_, Stack>) -> Result<Vec<NewTask>, TaskPlanError> {
            let Pushed(value) = decision.event;
            Ok(vec![NewTask::try_new(Notify { value: *value })?])
        }
    }

    /// A test executor over `store` that plans one notification per pushed value.
    fn notifying<S: EventStore<Stack> + 'static>(store: S, cap: usize) -> TestCommandExecutor<Stack, S> {
        TestCommandExecutor::configured(store, cap, |executor| executor.with_task_planner(Notifier))
    }

    #[test]
    fn an_executor_prints_its_attempt_and_settings_but_not_its_tasks() {
        let executor = notifying(MemoryStore::<Stack>::default(), 1);
        assert_eq!(
            format!("{executor:?}"),
            format!(
                "TestCommandExecutor {{ executor: StoreCommandExecutor {{ attempt: CommandAttempt {{ aggregate_type: \"stack\", \
                 task_planner: true, .. }}, retry_policy: {:?}, lock_limits: {:?}, commit_wakeup: false, observer: false, .. }}, .. }}",
                AggregateConflictRetryPolicy::default(),
                LockLimits::default()
            )
        );
    }

    #[tokio::test]
    async fn published_tasks_are_those_of_the_committed_calls() {
        let executor = notifying(MemoryStore::<Stack>::default(), 1);
        let use_case = executor.clone();
        let metadata = Causation::root("test").event_metadata();
        let report = use_case.execute_with_metadata(&"s".into(), 1, metadata).await.unwrap();
        assert_eq!(use_case.execute(&"s".into(), 2).await, Err(CommandError::Rejected(Full)));
        let published = executor.take_published_tasks();
        assert_eq!(published, report.tasks, "the record is the committed report's tasks");
        assert_eq!(published[0].payload()["value"], 1);
        assert!(executor.take_published_tasks().is_empty(), "taking clears the record");
        let events = executor.store().load_events("s").await.unwrap();
        assert_eq!((events.len(), events[0].metadata["source"].as_str()), (1, "test"));
    }

    #[tokio::test]
    async fn an_unavailable_store_is_reported_as_unavailable_and_publishes_nothing() {
        let executor = notifying(UnavailableStore, 1);
        assert_eq!(executor.execute(&"s".into(), 1).await, Err(CommandError::Unavailable));
        assert!(executor.take_published_tasks().is_empty());
        let _: &UnavailableStore = executor.store();
    }

    #[tokio::test]
    async fn a_journal_executor_publishes_into_the_journal_outbox_and_wakes_its_processors() {
        use std::{future::Future, pin::pin, task::Context, task::Waker};
        let (journal, wakeup) = (MemoryEventRepository::default(), CommitWakeup::new());
        let executor = TestCommandExecutor::<Stack, _>::configured(JournalEventStore::new(journal.clone()), 2, |executor| {
            executor.with_task_planner(Notifier).with_commit_wakeup(wakeup.clone())
        });
        let mut woken = pin!(wakeup.wait());
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        for value in 1..=2 {
            assert_eq!(executor.execute(&"s".into(), value).await.unwrap().tasks.len(), 1);
        }
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        assert_eq!(
            executor.execute(&"s".into(), 3).await,
            Err(CommandError::Rejected(Full)),
            "the second load came from the journal"
        );
        let journaled = journal.events::<Stack>("s").await.unwrap();
        assert_eq!(journaled.iter().map(|event| event.sequence).collect::<Vec<_>>(), [1, 2]);
        let outbox = journal.outbox::<Stack>();
        assert_eq!(
            outbox
                .iter()
                .map(|entry| (entry.aggregate_id.as_str(), entry.aggregate_sequence))
                .collect::<Vec<_>>(),
            [("s", 1), ("s", 2)],
            "every task went to the outbox with its event"
        );
        assert_eq!(
            executor.take_published_tasks(),
            outbox.into_iter().map(|entry| entry.task).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn a_commit_wakes_the_registered_wakeup_of_every_clone() {
        use std::{future::Future, pin::pin, task::Context, task::Waker};
        let wakeup = CommitWakeup::new();
        let executor = TestCommandExecutor::configured(MemoryStore::<Stack>::default(), 5, |executor| {
            executor.with_commit_wakeup(wakeup.clone())
        });
        let use_case = executor.clone();
        let mut woken = pin!(wakeup.wait());
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert!(
            use_case.execute(&"s".into(), 1).await.unwrap().tasks.is_empty(),
            "no planner, no tasks"
        );
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
    }

    /// Notes `id value: outcome` for every command.
    #[derive(Default)]
    struct Outcomes(Mutex<Vec<String>>);

    impl CommandObserver<Stack> for Outcomes {
        fn observe(&self, report: &cqrsx::command::observer::CommandReport<'_, Stack>) {
            use cqrsx::command::observer::Outcome;
            let outcome = match report.outcome {
                Outcome::Accepted { tasks } => format!("accepted with {} tasks", tasks.len()),
                Outcome::Rejected(error) => format!("rejected: {error}"),
                Outcome::Conflict => "conflict".to_owned(),
                Outcome::Unavailable => "unavailable".to_owned(),
            };
            self.0
                .lock()
                .unwrap()
                .push(format!("{} {}: {outcome}", report.aggregate_id, report.command));
        }
    }

    #[tokio::test]
    async fn the_observer_hears_the_commands_of_every_clone() {
        let outcomes = Arc::new(Outcomes::default());
        let executor = TestCommandExecutor::configured(MemoryStore::<Stack>::default(), 1, |executor| {
            executor.with_task_planner(Notifier).with_observer(outcomes.clone())
        });
        let use_case = executor.clone();
        assert!(use_case.execute(&"s".into(), 1).await.is_ok());
        assert_eq!(executor.execute(&"s".into(), 2).await, Err(CommandError::Rejected(Full)));
        assert_eq!(
            *outcomes.0.lock().unwrap(),
            ["s 1: accepted with 1 tasks", "s 2: rejected: full"]
        );
    }
}
