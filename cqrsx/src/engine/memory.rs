//! The stores of the engine in memory, and what a test does with an engine: run the tasks, let the views catch up.
#[cfg(feature = "postgres")]
use crate::postgres::task_queue::PostgresTaskQueue;
use crate::{
    cqrs::{
        aggregate::Aggregate,
        projection::View,
        testing::{
            mem_checkpoint_store::MemCheckpointStore, mem_event_repository::MemEventRepository,
            mem_view_repository::MemViewRepository,
        },
    },
    engine::{Cqrs, Journal},
    locking::lock,
    task::{
        adapter::boxed::BoxedTaskAdapter,
        dispatcher::TaskDispatcher,
        executor::{TaskExecutor, default_executor::DefaultTaskExecutor},
        queue::TaskQueue,
        testing::mem_task_queue::MemTaskQueue,
    },
};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

/// The stores of the engine in memory: the journal with its outbox, the task queue over that outbox, the checkpoints
/// of the processors and the repository of every view. An engine over other stores keeps them empty.
pub(crate) struct MemoryBackend {
    pub(crate) journal: MemEventRepository,
    pub(crate) queue: Arc<MemTaskQueue>,
    pub(crate) checkpoints: Arc<MemCheckpointStore>,
    /// The repository of every view in memory, by the view's type: an `Arc<MemViewRepository<V, A>>`.
    documents: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl MemoryBackend {
    pub(crate) fn new() -> Self {
        let journal = MemEventRepository::default();
        Self {
            queue: Arc::new(MemTaskQueue::new(journal.clone())),
            journal,
            checkpoints: Arc::default(),
            documents: HashMap::new(),
        }
    }

    /// The repository of the view `V` in memory, made on the first call.
    pub(crate) fn documents<V: View<A> + 'static, A: Aggregate + 'static>(&mut self) -> Arc<MemViewRepository<V, A>> {
        let documents = self
            .documents
            .entry(TypeId::of::<V>())
            .or_insert_with(|| Arc::new(MemViewRepository::<V, A>::default()));
        documents.clone().downcast().unwrap_or_default()
    }
}

impl Cqrs {
    /// The journal in memory: the events, the snapshots and the outbox of every aggregate type.
    pub fn journal(&self) -> &MemEventRepository {
        &self.memory.journal
    }

    /// The task queue over the journal's outbox in memory: every task with its delivery.
    pub fn outbox(&self) -> &MemTaskQueue {
        &self.memory.queue
    }

    /// The checkpoints of the processors in memory, e.g. for a test that plays an owner or a store that does not
    /// answer.
    pub fn checkpoints(&self) -> &MemCheckpointStore {
        &self.memory.checkpoints
    }

    /// The documents of the view `V` of the aggregate `A` in memory, e.g. for a test that writes them by hand; `None`
    /// for a view the engine keeps elsewhere or does not have.
    pub fn documents<V: View<A> + 'static, A: Aggregate + 'static>(&self) -> Option<&MemViewRepository<V, A>> {
        let documents = self.memory.documents.get(&TypeId::of::<V>())?;
        documents.downcast_ref::<MemViewRepository<V, A>>()
    }

    /// Runs the tasks until none is due: every task due now, those they publish included, one at a time, by type in the
    /// order of their registration, then by when they became due, each attempt as the task runtime runs it (the
    /// handler's timeout, the pause of a retry by the policy of its type), on the engine's queue: in memory or in
    /// PostgreSQL. Expired leases return first; a delayed task and a retry wait for their time (in memory, the Tokio
    /// clock). The answer is the number of attempts.
    pub async fn run_tasks(&self) -> usize {
        let Some(dispatcher) = &self.dispatcher else {
            return 0;
        };
        match &self.journal {
            Journal::Memory => self.run_due(&self.memory.queue, dispatcher).await,
            #[cfg(feature = "postgres")]
            Journal::Postgres(pools) => {
                let queue = Arc::new(PostgresTaskQueue::new(pools.main.clone()));
                self.run_due(&queue, dispatcher).await
            }
        }
    }

    async fn run_due<Q: TaskQueue + 'static>(&self, queue: &Arc<Q>, dispatcher: &TaskDispatcher<BoxedTaskAdapter>) -> usize {
        let settings = &self.settings;
        let executor =
            DefaultTaskExecutor::new(queue.clone(), settings.task_storage().clone()).with_policies(self.policies.clone());
        let mut attempts = 0;
        loop {
            let _ = queue.recover_expired(isize::MAX as usize).await;
            let before = attempts;
            for format in dispatcher.formats() {
                let claim = || queue.claim(format, &self.owner, 1, settings.task_lease());
                while let Some(task) = claim().await.ok().and_then(|mut tasks| tasks.pop()) {
                    let _ = executor
                        .execute_and_settle(dispatcher, task, settings.handler_timeout())
                        .await;
                    attempts += 1;
                }
            }
            if attempts == before {
                return attempts;
            }
        }
    }

    /// Starts the projection processors on the first call, then waits until every view has applied the journal of its
    /// aggregate type to its head.
    pub async fn catch_up(&self) {
        let processors = std::mem::take(&mut *lock(&self.processors, "engine processors"));
        if !processors.is_empty() {
            tokio::spawn(processors.run(self.shutdown.clone()));
        }
        for name in self.views.names() {
            while self.views().lag(&name).await.map_or(true, |lag| lag > 0) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cqrs::{
            command::causation::{CAUSATION_ID, CORRELATION_ID, SOURCE},
            projection::{checkpoint::CheckpointStore, repository::ViewRepository},
            store::repository::PersistedEventRepository,
        },
        engine::{Cqrs, test_support::*},
        task::testing::mem_task_queue::TaskStatus,
    };
    use std::{sync::atomic::Ordering, time::Duration};

    fn statuses(cqrs: &Cqrs) -> Vec<(String, TaskStatus, i32)> {
        let tasks = cqrs.outbox().tasks();
        tasks
            .into_iter()
            .map(|task| (task.entry.task.format().to_string(), task.status, task.attempts))
            .collect()
    }

    /// G-14, G-15, G-19: the chain runs to its end across both aggregates, its step continues the causation of the
    /// booking, and a retry waits the pause of its policy.
    #[tokio::test(start_paused = true)]
    async fn run_tasks_runs_a_chain_across_two_aggregates_to_its_end_and_leaves_a_paused_retry_for_the_clock() {
        let (cqrs, mailbox) = engine().await;
        mailbox.refusals.store(1, Ordering::SeqCst);
        let booked = cqrs
            .from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        let _ = cqrs.from("ui").execute::<Show>(&"s-1".into(), book("A1", "panda")).await;
        assert_eq!(cqrs.run_tasks().await, 2, "the step and the first attempt of its e-mail");
        assert_eq!(
            statuses(&cqrs),
            [
                ("record_ticket v1".to_owned(), TaskStatus::Completed, 1),
                ("mail_ticket v1".to_owned(), TaskStatus::Pending, 1)
            ],
            "only the booking that committed published a task"
        );
        assert!(mailbox.sent().is_empty());
        assert_eq!(cqrs.run_tasks().await, 0, "the retry waits a second (MAIL)");
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(cqrs.run_tasks().await, 1);
        assert_eq!(mailbox.sent(), ["fox:A1"]);
        let step = &cqrs.outbox().tasks()[0].entry.task;
        let recorded = cqrs.journal().events::<Wallet>("fox").await.unwrap();
        let metadata = &recorded[0].metadata;
        let correlation = booked.tasks[0].metadata()[CORRELATION_ID].clone();
        assert_eq!(
            (&metadata[CORRELATION_ID], &metadata[CAUSATION_ID], &metadata[SOURCE]),
            (
                &correlation,
                &serde_json::json!(step.id()),
                &serde_json::json!("task:record_ticket")
            )
        );
    }

    /// A handler that panics on every attempt is retried as if it answered retry: after the pauses of its policy (`MAIL`:
    /// 1, 2, 4, 8, 8 s), until the policy's budget of six attempts is spent and the task fails. The runner lives on.
    #[tokio::test(start_paused = true)]
    async fn a_handler_that_always_panics_spends_the_budget_of_its_policy_and_its_task_fails() {
        let (cqrs, mailbox) = engine().await;
        mailbox.crashes.store(usize::MAX, Ordering::SeqCst);
        cqrs.from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        assert_eq!(cqrs.run_tasks().await, 2, "the step and the first attempt of its e-mail");
        for pause in [1, 2, 4, 8, 8] {
            assert_eq!(cqrs.run_tasks().await, 0, "the retry waits {pause} s");
            tokio::time::advance(Duration::from_secs(pause)).await;
            assert_eq!(cqrs.run_tasks().await, 1);
        }
        assert_eq!(
            statuses(&cqrs),
            [
                ("record_ticket v1".to_owned(), TaskStatus::Completed, 1),
                ("mail_ticket v1".to_owned(), TaskStatus::Failed, 6)
            ]
        );
        assert!(mailbox.sent().is_empty());
    }

    /// G-16: the process stops after the effects and before the acknowledgements; both tasks come back once their
    /// leases run out. The step is done by R9 and publishes nothing twice; the e-mail goes out again.
    #[tokio::test(start_paused = true)]
    async fn a_step_redelivered_after_a_lost_acknowledgement_is_done_and_publishes_nothing_twice() {
        let (cqrs, mailbox) = engine().await;
        cqrs.from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        cqrs.outbox().lose_settlements(true);
        assert_eq!(cqrs.run_tasks().await, 2);
        cqrs.outbox().lose_settlements(false);
        tokio::time::advance(cqrs.settings().task_lease()).await;
        assert_eq!(cqrs.run_tasks().await, 0, "returned, they wait the redelivery backoff");
        tokio::time::advance(Duration::from_secs(6)).await;
        assert_eq!(cqrs.run_tasks().await, 2, "both again");
        assert_eq!(
            statuses(&cqrs),
            [
                ("record_ticket v1".to_owned(), TaskStatus::Completed, 2),
                ("mail_ticket v1".to_owned(), TaskStatus::Completed, 2)
            ],
            "one e-mail task: one ticket"
        );
        assert_eq!(mailbox.sent(), ["fox:A1", "fox:A1"], "at least once");
        cqrs.catch_up().await;
        let tickets = cqrs.view::<Tickets>().get(&"fox").await.unwrap().unwrap();
        assert_eq!(tickets.view.seats, ["A1"]);
    }

    #[tokio::test]
    async fn an_engine_without_task_types_runs_none() {
        let cqrs = Cqrs::in_memory().module(shows).build().await.unwrap();
        cqrs.from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        assert_eq!(cqrs.run_tasks().await, 0);
        assert_eq!(statuses(&cqrs), [("record_ticket v1".to_owned(), TaskStatus::Pending, 0)]);
    }

    #[tokio::test]
    async fn a_test_reaches_the_checkpoints_and_the_documents_in_memory() {
        let (cqrs, _) = engine().await;
        cqrs.from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        cqrs.catch_up().await;
        let halls = cqrs.documents::<Hall, Show>().unwrap();
        assert!(halls.load("s-1").await.unwrap().is_some(), "the processor wrote it");
        assert!(cqrs.documents::<Tickets, Wallet>().unwrap().list().await.unwrap().is_empty());
        let rows = cqrs.checkpoints().status(None).await.unwrap();
        assert_eq!(rows.len(), 2, "both processors registered their rows");
        let wallets_only = Cqrs::in_memory().module(wallets).build().await.unwrap();
        assert!(wallets_only.documents::<Hall, Show>().is_none(), "no such view");
    }
}
