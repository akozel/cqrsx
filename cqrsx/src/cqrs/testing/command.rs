//! A command executor over a test event store or an in-memory journal, for use-case tests.
use crate::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError,
            executor::{CommandExecutorCore, FrameworkFactory},
            locks::LockLimits,
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

/// Builds every command's framework over the one test store; the tasks go to the executor's records, as
/// the store does not model the outbox.
struct TestFrameworkFactory<A: Aggregate, S> {
    store: S,
    services: A::Services,
}

impl<A, S> FrameworkFactory<A> for TestFrameworkFactory<A, S>
where
    A: Aggregate,
    A::Services: Clone,
    S: EventStore<A> + Clone,
{
    type Store = S;

    fn framework(&self, _: Vec<NewTask>) -> CqrsFramework<A, S> {
        CqrsFramework::new(self.store.clone(), self.services.clone())
    }
}

/// The production command path behind [`CommandExecutor`] over a test store: one [`CommandExecutorCore`]
/// with aggregate locks, the default conflict retry and an optional commit wake-up, plus a record of the
/// tasks every call carries.
///
/// *Bound* tasks are everything handed to the executor, whatever the outcome. *Published* tasks
/// are only those of calls that committed (`Ok`), i.e. what a transactional outbox would hold. Clones share
/// the store, the locks, the wake-up and both records, so a use case can own one clone while the test
/// inspects another; register the wake-up before cloning.
///
/// Over a [`MemStore`] ([`new`](Self::new)) the commands only change the aggregates. Over a
/// [`MemEventRepository`] ([`over_journal`](Self::over_journal)) they append to a journal that projection
/// processors read, so a use-case test can follow the commands to its read models: give the processors the
/// same journal and the executor their [`CommitWakeup`].
///
/// The store does not model the outbox: a call that commits no event still counts as
/// published, while the PostgreSQL outbox rejects tasks without an event.
///
/// ```
/// use cqrsx::cqrs::{
///     aggregate::Aggregate, command::{CommandExecutor, CommandGatewayError}, event::DomainEvent,
///     event_sink::EventSink, testing::command::TestCommandExecutor, testing::mem_store::MemStore,
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
///     type Command = ();
///     type Event = Incremented;
///     type Error = LimitReached;
///     type Services = u32;
///     async fn handle(&mut self, _: (), limit: &u32, sink: &EventSink<Self>) -> Result<(), LimitReached> {
///         if self.0 >= *limit {
///             return Err(LimitReached);
///         }
///         sink.write(Incremented, self).await;
///         Ok(())
///     }
///     fn apply(&mut self, _: Incremented) { self.0 += 1; }
/// }
/// #[derive(Serialize)]
/// struct Notify {}
/// impl TaskPayload for Notify {
///     const TASK_TYPE: &'static str = "notify";
///     const TASK_VERSION: i32 = 1;
/// }
///
/// # tokio_test(async {
/// let executor = TestCommandExecutor::new(MemStore::<Counter>::default(), 1);
/// let use_case = executor.clone(); // what the application under test receives
/// for _ in 0..2 {
///     let _ = use_case.execute("c-1", (), vec![NewTask::new(Notify {}).unwrap()]).await;
/// }
/// assert_eq!(executor.take_bound_tasks().len(), 2);
/// assert_eq!(executor.take_published_tasks().len(), 1, "the second call was rejected");
/// assert_eq!(
///     use_case.execute("c-1", (), vec![]).await,
///     Err(CommandGatewayError::Domain(LimitReached))
/// );
/// # });
/// # fn tokio_test(future: impl std::future::Future<Output = ()>) {
/// #     tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(future)
/// # }
/// ```
pub struct TestCommandExecutor<A: Aggregate, S = MemStore<A>> {
    inner: Arc<CommandExecutorCore<A, TestFrameworkFactory<A, S>>>,
    bound: Arc<Mutex<Vec<NewTask>>>,
    published: Arc<Mutex<Vec<NewTask>>>,
}

impl<A: Aggregate + 'static, S> TestCommandExecutor<A, S> {
    /// Execute against `store` with `services`, the default conflict retry and the default lock limits.
    pub fn new(store: S, services: A::Services) -> Self {
        let factory = TestFrameworkFactory { store, services };
        Self {
            inner: Arc::new(CommandExecutorCore::new(
                factory,
                AggregateConflictRetryPolicy::default(),
                LockLimits::default(),
            )),
            bound: Arc::default(),
            published: Arc::default(),
        }
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

    /// The store the commands run against.
    pub fn store(&self) -> &S {
        &self.inner.factory().store
    }

    /// Every task handed to the executor since the last call, in call order and whatever the
    /// outcome; clears the record.
    pub fn take_bound_tasks(&self) -> Vec<NewTask> {
        take(&self.bound)
    }

    /// The tasks of the calls that committed since the last call, in call order; clears the record.
    pub fn take_published_tasks(&self) -> Vec<NewTask> {
        take(&self.published)
    }
}

impl<A: Aggregate + 'static> TestCommandExecutor<A, PersistedEventStore<MemEventRepository, A>> {
    /// Execute against `journal` with `services`: every command loads its aggregate from the journal and
    /// appends its events there, where projection processors over the same journal read them.
    pub fn over_journal(journal: MemEventRepository, services: A::Services) -> Self {
        Self::new(PersistedEventStore::new_event_store(journal), services)
    }
}

fn take(tasks: &Mutex<Vec<NewTask>>) -> Vec<NewTask> {
    std::mem::take(&mut *tasks.lock().unwrap_or_else(PoisonError::into_inner))
}

impl<A: Aggregate, S> Clone for TestCommandExecutor<A, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            bound: self.bound.clone(),
            published: self.published.clone(),
        }
    }
}

impl<A, S> CommandExecutor<A> for TestCommandExecutor<A, S>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
    A::Services: Clone,
    S: EventStore<A> + Clone + 'static,
    S::AC: Send,
{
    async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        self.bound
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(tasks.iter().cloned());
        let candidates = tasks.clone();
        let result = self.inner.execute_with_metadata(aggregate_id, command, metadata, tasks).await;
        if result.is_ok() {
            self.published
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(candidates);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cqrs::{
            event::DomainEvent, event_sink::EventSink, store::repository::PersistedEventRepository,
            testing::unavailable_store::UnavailableStore,
        },
        task::format::TaskPayload,
    };
    use serde::{Deserialize, Serialize};

    /// Accepts values up to the cap in its services; `Conflict` in the store forces retries.
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
        type Command = u8;
        type Event = Pushed;
        type Error = Full;
        type Services = usize;

        async fn handle(&mut self, value: u8, cap: &usize, sink: &EventSink<Self>) -> Result<(), Full> {
            if self.0.len() >= *cap {
                return Err(Full);
            }
            sink.write(Pushed(value), self).await;
            Ok(())
        }

        fn apply(&mut self, event: Pushed) {
            self.0.push(event.0);
        }
    }

    #[derive(Serialize)]
    struct Notify(u8);

    impl TaskPayload for Notify {
        const TASK_TYPE: &'static str = "notify";
        const TASK_VERSION: i32 = 1;
    }

    fn task(value: u8) -> NewTask {
        NewTask::from_payload(
            crate::task::format::TaskFormat::of::<Notify>().unwrap(),
            serde_json::json!({ "value": value }),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn bound_and_published_tasks_differ_for_a_rejected_command() {
        let executor = TestCommandExecutor::new(MemStore::<Stack>::default(), 1);
        let use_case = executor.clone();
        let metadata = HashMap::from([("source".to_owned(), "test".to_owned())]);
        assert_eq!(use_case.execute_with_metadata("s", 1, metadata, vec![task(1)]).await, Ok(()));
        assert_eq!(
            use_case.execute("s", 2, vec![task(2)]).await,
            Err(CommandGatewayError::Domain(Full))
        );
        assert_eq!(executor.take_bound_tasks().len(), 2);
        let published = executor.take_published_tasks();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].payload()["value"], 1);
        assert!(executor.take_bound_tasks().is_empty(), "taking clears the record");
        assert!(executor.take_published_tasks().is_empty());
        let events = executor.store().load_events("s").await.unwrap();
        assert_eq!((events.len(), events[0].metadata["source"].as_str()), (1, "test"));
    }

    #[tokio::test]
    async fn an_unavailable_store_is_reported_as_unavailable_and_publishes_nothing() {
        let executor = TestCommandExecutor::<Stack, _>::new(UnavailableStore, 1);
        assert_eq!(
            executor.execute("s", 1, vec![task(1)]).await,
            Err(CommandGatewayError::Unavailable)
        );
        assert_eq!(executor.take_bound_tasks().len(), 1);
        assert!(executor.take_published_tasks().is_empty());
        let _: &UnavailableStore = executor.store();
    }

    #[tokio::test]
    async fn a_journal_executor_appends_to_the_journal_and_wakes_its_processors() {
        use std::{future::Future, pin::pin, task::Context, task::Waker};
        let (journal, wakeup) = (MemEventRepository::default(), CommitWakeup::new());
        let executor = TestCommandExecutor::<Stack, _>::over_journal(journal.clone(), 2).with_commit_wakeup(wakeup.clone());
        let mut woken = pin!(wakeup.wait());
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        for value in 1..=2 {
            assert_eq!(executor.execute("s", value, vec![task(value)]).await, Ok(()));
        }
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        assert_eq!(
            executor.execute("s", 3, vec![task(3)]).await,
            Err(CommandGatewayError::Domain(Full)),
            "the second load came from the journal"
        );
        let journaled = journal.get_events::<Stack>("s").await.unwrap();
        assert_eq!(journaled.iter().map(|event| event.sequence).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(executor.take_published_tasks().len(), 2);
    }

    #[tokio::test]
    async fn a_commit_wakes_the_registered_wakeup_and_a_late_registration_is_refused() {
        use std::{future::Future, pin::pin, task::Context, task::Waker};
        let wakeup = CommitWakeup::new();
        let executor = TestCommandExecutor::new(MemStore::<Stack>::default(), 5).with_commit_wakeup(wakeup.clone());
        let use_case = executor.clone();
        let mut woken = pin!(wakeup.wait());
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        use_case.execute("s", 1, vec![]).await.unwrap();
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        let late = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            TestCommandExecutor::<Stack>::new(MemStore::<Stack>::default(), 1)
                .clone()
                .with_commit_wakeup(CommitWakeup::new())
        }));
        assert!(late.is_err(), "a wake-up registered after a clone is refused");
    }
}
