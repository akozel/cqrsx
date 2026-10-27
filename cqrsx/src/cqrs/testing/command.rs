//! A command executor over a test event store or an in-memory journal, for use-case tests.
use crate::{
    cqrs::{
        CommitReport, CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError,
            executor::CommandExecutorCore,
            locks::LockLimits,
            observer::CommandObserver,
            planner::{Planning, TaskPlanner},
            retry::AggregateConflictRetryPolicy,
            wakeup::CommitWakeup,
        },
        store::{EventStore, persisted::PersistedEventStore},
        testing::{mem_event_repository::MemEventRepository, mem_store::MemStore},
    },
    task::NewTask,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};

/// The production command path behind [`CommandExecutor`] over a test store: one [`CommandExecutorCore`]
/// with aggregate locks, the default conflict retry, an optional task planner, commit wake-up and observer, plus a
/// record of the tasks the committed calls published.
///
/// *Published* tasks are those of the calls that committed (`Ok`): what their commits report, i.e. what a
/// transactional outbox would hold. Clones share the store, the locks, the planner, the wake-up, the observer and
/// the record, so a use case can own one clone while the test inspects another; register the planner, the
/// wake-up and the observer before cloning.
///
/// Over a [`MemStore`] ([`new`](Self::new)) the commands only change the aggregates. Over a
/// [`MemEventRepository`] ([`over_journal`](Self::over_journal)) they append to a journal that projection
/// processors read, and the journal's outbox ([`MemEventRepository::outbox`]) holds the published tasks: give
/// the processors the same journal and the executor their [`CommitWakeup`].
///
/// ```
/// use cqrsx::cqrs::{
///     aggregate::Aggregate,
///     command::{
///         CommandExecutor, CommandGatewayError, causation::Causation,
///         decide::Decide,
///         planner::{Decision, TaskPlanError, TaskPlanner},
///     },
///     event::DomainEvent,
///     testing::{command::TestCommandExecutor, mem_store::MemStore},
/// };
/// use cqrsx::task::{NewTask, format::TaskPayload};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Default, Serialize, Deserialize)]
/// struct Counter(u32);
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// struct Incremented;
/// impl DomainEvent for Incremented {
///     fn event_type(&self) -> String { "incremented".into() }
///     fn event_version(&self) -> String { "1".into() }
/// }
/// #[derive(Debug, PartialEq, thiserror::Error)]
/// #[error("limit reached")]
/// struct LimitReached;
/// impl Aggregate for Counter {
///     const TYPE: &'static str = "counter";
///     type Id = String;
///     type Command = ();
///     type Event = Incremented;
///     type Error = LimitReached;
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
/// impl TaskPayload for Notify {
///     const TASK_TYPE: &'static str = "notify";
///     const TASK_VERSION: i32 = 1;
/// }
/// /// Every increment notifies.
/// struct Notifier;
/// impl TaskPlanner<Counter> for Notifier {
///     fn plan(&self, decision: &Decision<'_, Counter>) -> Result<Vec<NewTask>, TaskPlanError> {
///         decision.events.iter().map(|_| Ok(NewTask::new(Notify {})?)).collect()
///     }
/// }
///
/// # tokio_test(async {
/// let executor = TestCommandExecutor::new(MemStore::<Counter>::default(), 1).with_task_planner(Notifier);
/// let use_case = executor.clone(); // what the application under test receives
/// let causation = Causation::root("test");
/// let report = use_case.execute_with_metadata(&"c-1".into(), (), causation.event_metadata()).await.unwrap();
/// assert_eq!(report.tasks[0].metadata()["correlation_id"], causation.correlation_id.to_string());
/// assert_eq!(
///     use_case.execute(&"c-1".into(), ()).await,
///     Err(CommandGatewayError::Domain(LimitReached))
/// );
/// assert_eq!(executor.take_published_tasks().len(), 1, "the second call was rejected");
/// # });
/// # fn tokio_test(future: impl std::future::Future<Output = ()>) {
/// #     tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(future)
/// # }
/// ```
pub struct TestCommandExecutor<A: Aggregate, S: EventStore<A> = MemStore<A>> {
    inner: Arc<CommandExecutorCore<A, S>>,
    published: Arc<Mutex<Vec<NewTask>>>,
}

impl<A: Aggregate + 'static, S: EventStore<A>> TestCommandExecutor<A, S> {
    /// Execute against `store` with `rules`, the default conflict retry and the default lock limits.
    pub fn new(store: S, rules: A::Rules) -> Self {
        Self {
            inner: Arc::new(CommandExecutorCore::new(
                CqrsFramework::new(store, rules),
                AggregateConflictRetryPolicy::default(),
                LockLimits::default(),
            )),
            published: Arc::default(),
        }
    }

    /// Plans every decision's follow-up tasks through `planner`, as the production executor does.
    ///
    /// # Panics
    ///
    /// When a clone of the executor exists.
    pub fn with_task_planner(mut self, planner: impl TaskPlanner<A> + 'static) -> Self
    where
        A::Command: Clone,
    {
        Arc::get_mut(&mut self.inner)
            .expect("register the task planner before cloning the executor")
            .set_task_planner(Planning::new(planner));
        self
    }

    /// Wakes `wakeup` after every commit with events, for the projection processors under test.
    ///
    /// # Panics
    ///
    /// When a clone of the executor exists.
    pub fn with_commit_wakeup(mut self, wakeup: CommitWakeup) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("register the commit wake-up before cloning the executor")
            .set_commit_wakeup(wakeup);
        self
    }

    /// Tells `observer` about every command of the executor and its clones, as the production executor does.
    ///
    /// # Panics
    ///
    /// When a clone of the executor exists.
    pub fn with_observer(mut self, observer: Arc<dyn CommandObserver<A>>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("register the observer before cloning the executor")
            .set_observer(observer);
        self
    }

    /// The store the commands run against.
    pub fn store(&self) -> &S {
        self.inner.framework().store()
    }

    /// The tasks of the calls that committed since the last call, in call order; clears the record.
    pub fn take_published_tasks(&self) -> Vec<NewTask> {
        std::mem::take(&mut *self.published.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl<A: Aggregate + 'static> TestCommandExecutor<A, PersistedEventStore<MemEventRepository, A>> {
    /// Execute against `journal` with `rules`: every command loads its aggregate from the journal and
    /// appends its events, and its tasks to the journal's outbox, where projection processors and tests
    /// over the same journal read them.
    pub fn over_journal(journal: MemEventRepository, rules: A::Rules) -> Self {
        Self::new(PersistedEventStore::new_event_store(journal), rules)
    }
}

impl<A: Aggregate, S: EventStore<A>> Clone for TestCommandExecutor<A, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            published: self.published.clone(),
        }
    }
}

impl<A, S> CommandExecutor<A> for TestCommandExecutor<A, S>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + 'static,
    A::Error: Send + 'static,
    S: EventStore<A> + 'static,
    S::AC: Send,
{
    async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        let result = self.inner.execute_with_metadata(id, command, metadata).await;
        if let Ok(report) = &result {
            self.published
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(report.tasks.iter().cloned());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cqrs::{
            command::{
                causation::Causation,
                decide::Decide,
                planner::{Decision, TaskPlanError},
            },
            event::DomainEvent,
            store::repository::PersistedEventRepository,
            testing::unavailable_store::UnavailableStore,
        },
        task::format::TaskPayload,
    };
    use serde::{Deserialize, Serialize};

    /// Accepts values up to the cap in its rules.
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Stack(Vec<u8>);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Pushed(u8);

    impl DomainEvent for Pushed {
        fn event_type(&self) -> String {
            "pushed".into()
        }
        fn event_version(&self) -> String {
            "1".into()
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
        type Error = Full;
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

    impl TaskPayload for Notify {
        const TASK_TYPE: &'static str = "notify";
        const TASK_VERSION: i32 = 1;
    }

    /// One notification per pushed value.
    struct Notifier;

    impl TaskPlanner<Stack> for Notifier {
        fn plan(&self, decision: &Decision<'_, Stack>) -> Result<Vec<NewTask>, TaskPlanError> {
            decision
                .events
                .iter()
                .map(|Pushed(value)| Ok(NewTask::new(Notify { value: *value })?))
                .collect()
        }
    }

    #[tokio::test]
    async fn published_tasks_are_those_of_the_committed_calls() {
        let executor = TestCommandExecutor::new(MemStore::<Stack>::default(), 1).with_task_planner(Notifier);
        let use_case = executor.clone();
        let metadata = Causation::root("test").event_metadata();
        let report = use_case.execute_with_metadata(&"s".into(), 1, metadata).await.unwrap();
        assert_eq!(use_case.execute(&"s".into(), 2).await, Err(CommandGatewayError::Domain(Full)));
        let published = executor.take_published_tasks();
        assert_eq!(published, report.tasks, "the record is the committed report's tasks");
        assert_eq!(published[0].payload()["value"], 1);
        assert!(executor.take_published_tasks().is_empty(), "taking clears the record");
        let events = executor.store().load_events("s").await.unwrap();
        assert_eq!((events.len(), events[0].metadata["source"].as_str()), (1, "test"));
    }

    #[tokio::test]
    async fn an_unavailable_store_is_reported_as_unavailable_and_publishes_nothing() {
        let executor = TestCommandExecutor::<Stack, _>::new(UnavailableStore, 1).with_task_planner(Notifier);
        assert_eq!(executor.execute(&"s".into(), 1).await, Err(CommandGatewayError::Unavailable));
        assert!(executor.take_published_tasks().is_empty());
        let _: &UnavailableStore = executor.store();
    }

    #[tokio::test]
    async fn a_journal_executor_publishes_into_the_journal_outbox_and_wakes_its_processors() {
        use std::{future::Future, pin::pin, task::Context, task::Waker};
        let (journal, wakeup) = (MemEventRepository::default(), CommitWakeup::new());
        let executor = TestCommandExecutor::<Stack, _>::over_journal(journal.clone(), 2)
            .with_task_planner(Notifier)
            .with_commit_wakeup(wakeup.clone());
        let mut woken = pin!(wakeup.wait());
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        for value in 1..=2 {
            assert_eq!(executor.execute(&"s".into(), value).await.unwrap().tasks.len(), 1);
        }
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        assert_eq!(
            executor.execute(&"s".into(), 3).await,
            Err(CommandGatewayError::Domain(Full)),
            "the second load came from the journal"
        );
        let journaled = journal.get_events::<Stack>("s").await.unwrap();
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
    async fn a_commit_wakes_the_registered_wakeup_and_a_late_registration_is_refused() {
        use std::{future::Future, pin::pin, task::Context, task::Waker};
        let wakeup = CommitWakeup::new();
        let executor = TestCommandExecutor::new(MemStore::<Stack>::default(), 5).with_commit_wakeup(wakeup.clone());
        let use_case = executor.clone();
        let mut woken = pin!(wakeup.wait());
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert!(
            use_case.execute(&"s".into(), 1).await.unwrap().tasks.is_empty(),
            "no planner, no tasks"
        );
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        let late_wakeup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            TestCommandExecutor::<Stack>::new(MemStore::<Stack>::default(), 1)
                .clone()
                .with_commit_wakeup(CommitWakeup::new())
        }));
        assert!(late_wakeup.is_err(), "a wake-up registered after a clone is refused");
        let late_planner = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            TestCommandExecutor::<Stack>::new(MemStore::<Stack>::default(), 1)
                .clone()
                .with_task_planner(Notifier)
        }));
        assert!(late_planner.is_err(), "a planner registered after a clone is refused");
    }

    /// Notes `id value: outcome` for every command.
    #[derive(Default)]
    struct Outcomes(Mutex<Vec<String>>);

    impl CommandObserver<Stack> for Outcomes {
        fn command(&self, report: &crate::cqrs::command::observer::CommandReport<'_, Stack>) {
            use crate::cqrs::command::observer::Outcome;
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
    async fn the_observer_hears_the_commands_of_every_clone_and_a_late_registration_is_refused() {
        let outcomes = Arc::new(Outcomes::default());
        let executor = TestCommandExecutor::new(MemStore::<Stack>::default(), 1)
            .with_task_planner(Notifier)
            .with_observer(outcomes.clone());
        let use_case = executor.clone();
        assert!(use_case.execute(&"s".into(), 1).await.is_ok());
        assert_eq!(executor.execute(&"s".into(), 2).await, Err(CommandGatewayError::Domain(Full)));
        assert_eq!(
            *outcomes.0.lock().unwrap(),
            ["s 1: accepted with 1 tasks", "s 2: rejected: full"]
        );
        let late = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            TestCommandExecutor::<Stack>::new(MemStore::<Stack>::default(), 1)
                .clone()
                .with_observer(outcomes.clone())
        }));
        assert!(late.is_err(), "an observer registered after a clone is refused");
    }
}
