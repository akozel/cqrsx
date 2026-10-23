//! A command executor over a test event store, for use-case tests.
use crate::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError,
            executor::{CqrsCommandExecutor, FrameworkFactory},
            locks::LockLimits,
            retry::AggregateConflictRetryPolicy,
        },
        event_handler::{
            EventHandler,
            delivery::{DeliveryBacklog, DeliveryConfig},
        },
        store::EventStore,
        testing::mem_store::MemStore,
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

/// The production command path behind [`CommandExecutor`] over a test store: one [`CqrsCommandExecutor`]
/// with aggregate locks, the default conflict retry and post-commit event handlers, plus a record of the
/// tasks every call carries.
///
/// *Bound* tasks are everything handed to the executor, whatever the outcome. *Published* tasks
/// are only those of calls that committed (`Ok` or `CommittedWithEventHandlerFailure`), i.e.
/// what a transactional outbox would hold. Clones share the store, the handlers, the locks and
/// both records, so a use case can own one clone while the test inspects another; register the
/// handlers before cloning.
///
/// Handlers get the committed events the way production handlers do: through an ordered buffer per
/// aggregate key, in a worker task of their own, so the test needs a Tokio runtime. A command returns
/// only after its handlers' receipts; a failing or panicking handler shows as
/// `CommittedWithEventHandlerFailure`.
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
    inner: Arc<CqrsCommandExecutor<A, TestFrameworkFactory<A, S>>>,
    bound: Arc<Mutex<Vec<NewTask>>>,
    published: Arc<Mutex<Vec<NewTask>>>,
}

impl<A: Aggregate + 'static, S> TestCommandExecutor<A, S> {
    /// Execute against `store` with `services`, no event handlers, the default conflict retry and
    /// the default lock limits.
    pub fn new(store: S, services: A::Services) -> Self {
        let factory = TestFrameworkFactory { store, services };
        Self {
            inner: Arc::new(CqrsCommandExecutor::new(
                factory,
                AggregateConflictRetryPolicy::default(),
                LockLimits::default(),
            )),
            bound: Arc::default(),
            published: Arc::default(),
        }
    }

    /// Register a handler after the handlers registered before it; it gets the committed events through
    /// a buffer per aggregate key bounded by `config`.
    ///
    /// # Panics
    ///
    /// When a clone of the executor exists: the handler would serve one clone and not the other.
    pub fn with_event_handler(mut self, handler: Arc<dyn EventHandler<A>>, config: DeliveryConfig) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("register event handlers before cloning the executor")
            .add_event_handler(handler, config);
        self
    }

    /// The hand-overs not handled yet, shared by every clone.
    pub fn backlog(&self) -> DeliveryBacklog {
        self.inner.backlog()
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
        if matches!(
            result,
            Ok(()) | Err(CommandGatewayError::CommittedWithEventHandlerFailure { .. })
        ) {
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
            command::FailedHandler,
            event::{DomainEvent, EventEnvelope},
            event_sink::EventSink,
            store::PersistenceError,
            testing::unavailable_store::UnavailableStore,
        },
        task::format::TaskPayload,
    };
    use async_trait::async_trait;
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

    /// Fails every call after recording the sequences it saw.
    #[derive(Default)]
    struct Broken(Mutex<Vec<usize>>);

    #[async_trait]
    impl EventHandler<Stack> for Broken {
        fn name(&self) -> String {
            "broken".into()
        }

        async fn handle(&self, _: &str, events: &[EventEnvelope<Stack>]) -> Result<(), PersistenceError> {
            self.0.lock().unwrap().extend(events.iter().map(|e| e.sequence));
            Err(PersistenceError::OptimisticLockError)
        }
    }

    #[tokio::test]
    async fn bound_and_published_tasks_differ_for_a_rejected_command() {
        let executor = TestCommandExecutor::new(MemStore::<Stack>::default(), 1);
        let use_case = executor.clone();
        assert_eq!(use_case.execute("s", 1, vec![task(1)]).await, Ok(()));
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
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn committed_handler_failures_publish_and_report_the_handler() {
        let broken = Arc::new(Broken::default());
        let executor = TestCommandExecutor::new(MemStore::<Stack>::default(), 5)
            .with_event_handler(broken.clone(), DeliveryConfig::default());
        let metadata = HashMap::from([("source".to_owned(), "test".to_owned())]);
        assert_eq!(
            executor.execute_with_metadata("s", 7, metadata, vec![task(7)]).await,
            Err(CommandGatewayError::CommittedWithEventHandlerFailure {
                last_sequence: 1,
                handlers: vec![FailedHandler {
                    index: 0,
                    name: "broken".into()
                }]
            })
        );
        assert_eq!(*broken.0.lock().unwrap(), [1]);
        assert!(
            executor.clone().backlog().is_empty(),
            "the command returned after its receipt"
        );
        assert_eq!(executor.take_published_tasks().len(), 1);
        let events = executor.store().load_events("s").await.unwrap();
        assert_eq!(events[0].metadata["source"], "test");
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

    #[test]
    #[should_panic(expected = "register event handlers before cloning the executor")]
    fn a_handler_registered_after_a_clone_is_refused() {
        let executor = TestCommandExecutor::new(MemStore::<Stack>::default(), 1);
        let _use_case = executor.clone();
        let _ = executor.with_event_handler(Arc::new(Broken::default()), DeliveryConfig::default());
    }
}
