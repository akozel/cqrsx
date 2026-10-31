//! One attempt of a command on one aggregate: load, decide, plan the tasks, commit.

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx/licenses/.
use crate::{
    command::{
        AggregateError, CommitReport,
        planner::{Planning, TaskPlanner},
    },
    domain::{aggregate::Aggregate, command::Decide},
    journal::{AggregateContext, EventStore},
};
use std::{collections::HashMap, fmt};

/// One attempt of a command, the part of an executor that touches the store.
///
/// Changes are made only after loading the entire aggregate, so that the full context is understood:
/// 1. Load the aggregate: its snapshot and the events after it, applied in order to a new `Aggregate`.
/// 1. Decide an inbound command on the loaded state: one event or an error (see [`Decide`]).
/// 1. Plan the follow-up tasks of the decision, when a [`TaskPlanner`] is set.
/// 1. Persist the event with its tasks or, on an error, nothing.
///
/// An attempt returns what it committed and hands it to nobody: the executor
/// ([`StoreCommandExecutor`](crate::command::executor::StoreCommandExecutor)) owns one attempt for its life,
/// the projection processors read the events from the journal and the task runtime claims the tasks from the
/// outbox. This is where an error of the journal ([`PersistenceError`](crate::journal::PersistenceError))
/// becomes the error of a command ([`AggregateError`]).
#[must_use]
pub(crate) struct CommandAttempt<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    store: ES,
    rules: A::Rules,
    planning: Option<Planning<A>>,
}

// By hand: the store and the rules need not be `Debug`.
impl<A, ES> fmt::Debug for CommandAttempt<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandAttempt")
            .field("aggregate_type", &A::TYPE)
            .field("task_planner", &self.planning.is_some())
            .finish_non_exhaustive()
    }
}

impl<A, ES> CommandAttempt<A, ES>
where
    A: Aggregate,
    ES: EventStore<A>,
{
    /// An attempt on `store` with the rules the commands decide with ([`Aggregate::Rules`]) and no planner.
    pub(crate) fn new(store: ES, rules: A::Rules) -> Self {
        Self {
            store,
            rules,
            planning: None,
        }
    }

    /// Plans the follow-up tasks of every decision through `planner`, after `decide` and before the commit; the
    /// tasks commit atomically with the attempt's events. One planner per attempt: a second call replaces the first.
    pub(crate) fn with_task_planner(mut self, planner: impl TaskPlanner<A> + 'static) -> Self
    where
        A::Command: Clone,
    {
        self.planning = Some(Planning::new(planner));
        self
    }

    /// The store the commands run against.
    pub(crate) fn store(&self) -> &ES {
        &self.store
    }

    /// Executes without event metadata, formatting the id's text once; a test shortcut of [`run`](Self::run).
    #[cfg(test)]
    pub(crate) async fn execute(&self, id: &A::Id, command: A::Command) -> Result<CommitReport<A>, AggregateError<A::Error>>
    where
        A: Clone,
    {
        self.execute_with_metadata(id, command, HashMap::new()).await
    }

    /// Executes with `metadata`, formatting the id's text once; a test shortcut of [`run`](Self::run).
    #[cfg(test)]
    pub(crate) async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, AggregateError<A::Error>>
    where
        A: Clone,
    {
        self.run(id, &id.to_string(), command, metadata).await
    }

    /// One attempt of a command on `id`, whose text (its `Display`) is `key`: the executor formats it once per
    /// command, for the lock and every attempt.
    ///
    /// The command decides on the committed state ([`Decide::decide`]), synchronously: one event or a rejection
    /// ([`AggregateError::Domain`]), with nothing written. With a planner, the decision is planned before the
    /// commit: the planner sees the committed state, a copy of it with the event applied (made for the planner only),
    /// the command, the event and the metadata; the executor stamps the causation of `metadata` on every task and
    /// derives its id. A planner error refuses the command ([`AggregateError::TaskPlan`]) with nothing written.
    /// `Clone` of the aggregate must provide independent state.
    pub(crate) async fn run(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::{
            causation::Causation,
            planner::{Decision, TaskPlanError},
        },
        domain::event::Events,
        journal::PersistenceError,
        journal::persisted::PersistedEventStore,
        journal::persisted::tests::{TestAggregate, TestCommands, TestService},
        memory::event_repository::MemEventRepository,
        task::{NewTask, Task, format::TaskFormat},
    };
    use serde::{Deserialize, Serialize};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    type Mem<A> = PersistedEventStore<MemEventRepository, A>;

    /// A journal in memory under the store, as the engine in memory has it.
    fn mem_store<A: Aggregate>() -> Mem<A> {
        PersistedEventStore::new(MemEventRepository::default())
    }

    #[tokio::test]
    async fn execute_commits_and_returns_the_events_in_commit_order() {
        let store = mem_store::<TestAggregate>();
        let framework = CommandAttempt::new(store.clone(), TestService);
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

    impl Events for Echoed {
        fn event_type(&self) -> &'static str {
            "echoed"
        }
        fn event_version(&self) -> &'static str {
            "1"
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

    impl Task for Shout {
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
            Ok(vec![NewTask::try_new(Shout { total: d.after.0 })?])
        }
    }

    #[tokio::test]
    async fn the_planner_sees_the_decision_and_its_tasks_commit_with_the_events() {
        let recorder = Recorder::default();
        let framework = CommandAttempt::new(mem_store::<Echo>(), ()).with_task_planner(recorder.clone());
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
        assert_eq!(task.id(), causation.task_id("echo", "e-1", &TaskFormat::of::<Shout>(), 0));
        assert_eq!(task.metadata()["correlation_id"], causation.correlation_id.to_string());
    }

    #[tokio::test]
    async fn a_rejection_plans_nothing() {
        let recorder = Recorder::default();
        let framework = CommandAttempt::new(mem_store::<Echo>(), ()).with_task_planner(recorder.clone());
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
        let (store, decided, planner) = (mem_store::<Tape>(), Decided::default(), Before::default());
        let framework = CommandAttempt::new(store.clone(), decided.clone()).with_task_planner(planner.clone());
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
        let store = mem_store::<Echo>();
        let framework = CommandAttempt::new(store.clone(), ()).with_task_planner(Recorder {
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
        CommandAttempt::new(mem_store::<Echo>(), ())
            .execute(&"e-1".into(), command)
            .await
            .unwrap();
        assert_eq!(clones.load(Ordering::SeqCst), 0);
        let command = probe(5);
        let clones = command.clones.clone();
        CommandAttempt::new(mem_store::<Echo>(), ())
            .with_task_planner(Recorder::default())
            .execute(&"e-1".into(), command)
            .await
            .unwrap();
        assert_eq!(clones.load(Ordering::SeqCst), 1, "one clone kept for the decision");
    }

    #[derive(Serialize)]
    struct Whisper {}

    impl Task for Whisper {
        const TASK_TYPE: &'static str = "whisper";
        const TASK_VERSION: i32 = 1;
    }

    struct Whisperer;

    impl TaskPlanner<Echo> for Whisperer {
        fn plan(&self, _: &Decision<'_, Echo>) -> Result<Vec<NewTask>, TaskPlanError> {
            Ok(vec![NewTask::try_new(Whisper {})?])
        }
    }

    #[tokio::test]
    async fn a_second_planner_replaces_the_first() {
        let recorder = Recorder::default();
        let framework = CommandAttempt::new(mem_store::<Echo>(), ())
            .with_task_planner(recorder.clone())
            .with_task_planner(Whisperer);
        let report = framework.execute(&"e-1".into(), probe(5)).await.unwrap();
        assert_eq!(report.tasks[0].format().to_string(), "whisper v1");
        assert!(recorder.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_commit_report_compares_its_parts_and_shows_them() {
        let report = CommandAttempt::new(mem_store::<Echo>(), ())
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

    /// A store whose calls fail with the journal error `fail()` returns, on load or on commit.
    #[derive(Clone)]
    struct FailingStore {
        inner: Mem<Echo>,
        fail: fn() -> PersistenceError,
        on_commit: bool,
    }

    impl EventStore<Echo> for FailingStore {
        type Context = <Mem<Echo> as EventStore<Echo>>::Context;

        async fn load_events(&self, id: &str) -> Result<Vec<crate::domain::event::EventEnvelope<Echo>>, PersistenceError> {
            self.inner.load_events(id).await
        }

        async fn load_aggregate(&self, id: &str) -> Result<Self::Context, PersistenceError> {
            if self.on_commit {
                return self.inner.load_aggregate(id).await;
            }
            Err((self.fail)())
        }

        async fn commit(
            &self,
            events: Vec<Echoed>,
            context: Self::Context,
            metadata: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<crate::domain::event::EventEnvelope<Echo>>, PersistenceError> {
            if self.on_commit {
                return Err((self.fail)());
            }
            self.inner.commit(events, context, metadata, tasks).await
        }
    }

    /// The attempt is where the journal's error becomes the command's: each category of the port, from the load and
    /// from the commit, arrives as the `AggregateError` its `From` names, with the cause kept.
    #[tokio::test]
    async fn a_journal_failure_of_a_load_or_a_commit_becomes_the_matching_command_error() {
        type Case = (fn() -> PersistenceError, &'static str);
        let cases: [Case; 5] = [
            (|| PersistenceError::OptimisticLockError, "conflict"),
            (|| PersistenceError::ConnectionError("down".into()), "connection"),
            (|| PersistenceError::DeserializationError("json".into()), "deserialization"),
            (|| PersistenceError::UnknownError("odd".into()), "unexpected"),
            (|| PersistenceError::SequenceGap { expected: 2, found: 5 }, "unexpected"),
        ];
        for on_commit in [false, true] {
            for (fail, expected) in cases {
                let store = FailingStore {
                    inner: mem_store(),
                    fail,
                    on_commit,
                };
                let attempt = CommandAttempt::new(store, ());
                let error = attempt.execute(&"e-1".into(), probe(5)).await.unwrap_err();
                let category = match &error {
                    AggregateError::AggregateConflict => "conflict",
                    AggregateError::DatabaseConnectionError(_) => "connection",
                    AggregateError::DeserializationError(_) => "deserialization",
                    AggregateError::UnexpectedError(_) => "unexpected",
                    AggregateError::Domain(_) | AggregateError::TaskPlan(_) => "other",
                };
                assert_eq!(category, expected, "on_commit: {on_commit}");
                if expected != "conflict" {
                    assert_eq!(error.to_string(), fail().to_string(), "the cause is kept");
                }
            }
        }
    }
}
