//! Reactions: what follows each event of an aggregate.
//!
//! An aggregate's reactions are one `match` over its events: every branch says what follows its event, as tasks
//! ([`Then::task`], [`Then::task_after`]); a new event without a branch does not compile. A reaction is pure and
//! synchronous: it runs under the aggregate's lock on every attempt of a command, before the commit, so the same
//! decision always plans the same tasks; data from outside comes in the command. What it sees is [`Decided`]: the
//! aggregate's id, the state after the event, the command, the event and the command's metadata, read-only.
//!
//! The executor runs the reactions through [`ReactionsPlanner`], a [`TaskPlanner`]: the core stamps every task with
//! the command's causation and derives its id from the command, the task's format and its place among the tasks of
//! that format, in the order of the calls; the tasks commit with the event or not at all. A reaction never names a
//! policy: the attempt budget of a task comes from the policy of its type ([`Policies`]).
//!
//! ```
//! use cqrsx::{
//!     Task,
//!     cqrs::{
//!         aggregate::Aggregate,
//!         command::{CommandExecutor, decide::Decide},
//!         event::DomainEvent,
//!         reactions::{Decided, Reactions, ReactionsPlanner, Then},
//!         testing::{command::TestCommandExecutor, mem_store::MemStore},
//!     },
//!     task::policy::Policies,
//! };
//! use serde::{Deserialize, Serialize};
//! use std::{collections::HashMap, time::Duration};
//!
//! #[derive(Clone, Debug, Default, Serialize, Deserialize)]
//! struct Order {
//!     reserved: bool,
//! }
//! #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
//! enum OrderEvent {
//!     Reserved,
//!     Paid { total: u32 },
//! }
//! impl DomainEvent for OrderEvent {
//!     fn event_type(&self) -> String {
//!         match self {
//!             OrderEvent::Reserved => "reserved".into(),
//!             OrderEvent::Paid { .. } => "paid".into(),
//!         }
//!     }
//!     fn event_version(&self) -> String { "1".into() }
//! }
//! #[derive(Debug, thiserror::Error)]
//! #[error("not reserved")]
//! struct NotReserved;
//! impl Aggregate for Order {
//!     const TYPE: &'static str = "order";
//!     type Id = String;
//!     type Command = Option<u32>; // `None` reserves, `Some(total)` pays
//!     type Event = OrderEvent;
//!     type Error = NotReserved;
//!     type Rules = ();
//!     fn apply(&mut self, event: OrderEvent) { self.reserved = matches!(event, OrderEvent::Reserved); }
//! }
//! impl Decide<Order> for Option<u32> {
//!     fn decide(self, order: &Order, _: &()) -> Result<OrderEvent, NotReserved> {
//!         match self {
//!             None => Ok(OrderEvent::Reserved),
//!             Some(total) if order.reserved => Ok(OrderEvent::Paid { total }),
//!             Some(_) => Err(NotReserved),
//!         }
//!     }
//! }
//!
//! #[derive(Serialize, Task)]
//! #[task(name = "release_unpaid", version = 1)]
//! struct ReleaseUnpaid { order_id: String }
//! #[derive(Serialize, Task)]
//! #[task(name = "ship", version = 1)]
//! struct Ship { order_id: String, total: u32 }
//!
//! /// What follows each fact of an order.
//! impl Reactions for Order {
//!     fn react(decided: &Decided<'_, Self>, then: &mut Then) {
//!         match decided.event {
//!             OrderEvent::Reserved => {
//!                 then.task_after(Duration::from_secs(15 * 60), ReleaseUnpaid { order_id: decided.id.clone() })
//!             }
//!             OrderEvent::Paid { total } => then.task(Ship { order_id: decided.id.clone(), total: *total }),
//!         }
//!     }
//! }
//!
//! // A reaction is a function of what was decided: a test builds the decision by hand.
//! let (id, state, metadata) = ("o-1".to_owned(), Order::default(), HashMap::new());
//! let mut then = Then::default();
//! Order::react(&Decided::new(&id, &state, &Some(30), &OrderEvent::Paid { total: 30 }, &metadata), &mut then);
//! let tasks = then.into_tasks().unwrap();
//! assert_eq!((tasks[0].format().to_string(), &tasks[0].payload()["total"]), ("ship v1".to_owned(), &30.into()));
//!
//! // The executor runs them after every decision and commits the tasks with the event.
//! let orders = TestCommandExecutor::new(MemStore::<Order>::default(), ())
//!     .with_task_planner(ReactionsPlanner::<Order>::new(Policies::default()));
//! let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
//! let reserved = runtime.block_on(orders.execute(&id, None)).unwrap();
//! assert_eq!(reserved.tasks[0].format().to_string(), "release_unpaid v1");
//! ```
//!
//! A new event without a branch does not compile:
//!
//! ```compile_fail
//! use cqrsx::cqrs::{
//!     aggregate::Aggregate,
//!     command::decide::Decide,
//!     event::DomainEvent,
//!     reactions::{Decided, Reactions, Then},
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, Default, Serialize, Deserialize)]
//! struct Order;
//! #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
//! enum OrderEvent {
//!     Reserved,
//!     Paid,
//! }
//! impl DomainEvent for OrderEvent {
//!     fn event_type(&self) -> String { "order".into() }
//!     fn event_version(&self) -> String { "1".into() }
//! }
//! impl Aggregate for Order {
//!     const TYPE: &'static str = "order";
//!     type Id = String;
//!     type Command = bool;
//!     type Event = OrderEvent;
//!     type Error = std::convert::Infallible;
//!     type Rules = ();
//!     fn apply(&mut self, _: OrderEvent) {}
//! }
//! impl Decide<Order> for bool {
//!     fn decide(self, _: &Order, _: &()) -> Result<OrderEvent, std::convert::Infallible> { Ok(OrderEvent::Paid) }
//! }
//!
//! impl Reactions for Order {
//!     fn react(decided: &Decided<'_, Self>, _: &mut Then) {
//!         match decided.event {
//!             OrderEvent::Reserved => {}
//!         }
//!     }
//! }
//! ```
use crate::{
    cqrs::{
        aggregate::Aggregate,
        command::planner::{Decision, TaskPlanError, TaskPlanner},
    },
    task::{NewTask, TaskSchedule, TaskValidationError, format::TaskPayload, policy::Policies},
};
use std::{collections::HashMap, marker::PhantomData, time::Duration};

/// What follows each event of the aggregate: the tasks of one decision, planned through `then` in order. A pure,
/// synchronous function of `decided` (see the [module](self)).
pub trait Reactions: Aggregate {
    fn react(decided: &Decided<'_, Self>, then: &mut Then);
}

/// What one attempt of a command decided, as its reactions see it.
pub struct Decided<'a, A: Aggregate> {
    /// The aggregate the command ran on.
    pub id: &'a A::Id,
    /// The state after the event: the copy the executor holds under the aggregate's lock. What the event removed is
    /// in the event, not here.
    pub state: &'a A,
    /// The command that was decided.
    pub command: &'a A::Command,
    /// The one event the command decided.
    pub event: &'a A::Event,
    metadata: &'a HashMap<String, String>,
}

impl<'a, A: Aggregate> Decided<'a, A> {
    /// A decision built by hand: the executor builds it from every attempt, a test of a reaction builds its own.
    pub fn new(
        id: &'a A::Id,
        state: &'a A,
        command: &'a A::Command,
        event: &'a A::Event,
        metadata: &'a HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            state,
            command,
            event,
            metadata,
        }
    }

    /// The command's metadata, stored with its event: its causation, its time and the like. The engine carries it
    /// on to the tasks itself; a reaction only reads it.
    pub fn metadata(&self) -> &'a HashMap<String, String> {
        self.metadata
    }
}

/// The tasks a reaction plans, in the order of the calls. The first task that cannot be built (its payload does not
/// serialize, its delay is not representable) refuses the whole plan, and nothing after it is built.
#[derive(Debug, Default)]
pub struct Then {
    tasks: Vec<NewTask>,
    refused: Option<TaskValidationError>,
}

impl Then {
    /// A task of `payload`, eligible at once.
    pub fn task(&mut self, payload: impl TaskPayload) {
        if self.refused.is_none() {
            self.keep(NewTask::new(payload));
        }
    }

    /// A task of `payload`, eligible `delay` after the commit; the delay is relative, a reaction has no clock.
    pub fn task_after(&mut self, delay: Duration, payload: impl TaskPayload) {
        if self.refused.is_none() {
            self.keep(NewTask::new(payload).and_then(|task| task.scheduled(TaskSchedule::After(delay))));
        }
    }

    /// The planned tasks in the order of the calls, or why the first refused one could not be built. Without the
    /// budgets of the policies: the planner sets them.
    pub fn into_tasks(self) -> Result<Vec<NewTask>, TaskValidationError> {
        match self.refused {
            Some(error) => Err(error),
            None => Ok(self.tasks),
        }
    }

    fn keep(&mut self, task: Result<NewTask, TaskValidationError>) {
        match task {
            Ok(task) => self.tasks.push(task),
            Err(error) => self.refused = Some(error),
        }
    }
}

/// The task planner of an aggregate's reactions: builds the [`Decided`] of every decision, lets
/// [`Reactions::react`] plan, and gives every task the attempt budget of its type's policy (a type bound to none
/// keeps [`DEFAULT_MAX_ATTEMPTS`](crate::task::DEFAULT_MAX_ATTEMPTS)). A task that cannot be built refuses the
/// command (`TaskPlanError::Task`): it is unavailable and nothing is written.
pub struct ReactionsPlanner<A> {
    policies: Policies,
    aggregate: PhantomData<fn() -> A>,
}

impl<A: Reactions> ReactionsPlanner<A> {
    pub fn new(policies: Policies) -> Self {
        Self {
            policies,
            aggregate: PhantomData,
        }
    }
}

impl<A: Reactions> TaskPlanner<A> for ReactionsPlanner<A> {
    /// A decision holds one event (D-02); one built by hand with another number is refused.
    fn plan(&self, decision: &Decision<'_, A>) -> Result<Vec<NewTask>, TaskPlanError> {
        let [event] = decision.events else {
            return Err(TaskPlanError::application(format!(
                "a reaction sees one event, the decision has {}",
                decision.events.len()
            )));
        };
        let decided = Decided::new(
            decision.aggregate_id,
            decision.after,
            decision.command,
            event,
            decision.metadata,
        );
        let mut then = Then::default();
        A::react(&decided, &mut then);
        let tasks = then
            .into_tasks()?
            .into_iter()
            .map(|task| match self.policies.of(task.format()) {
                Some(policy) => task.with_max_attempts(policy.max_attempts()),
                None => Ok(task),
            });
        Ok(tasks.collect::<Result<_, _>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cqrs::{
            command::{CommandExecutor, CommandGatewayError, causation::Causation, decide::Decide, planner::Planning},
            event::DomainEvent,
            store::repository::PersistedEventRepository,
            testing::{command::TestCommandExecutor, mem_event_repository::MemEventRepository},
        },
        task::{
            DEFAULT_MAX_ATTEMPTS,
            format::TaskFormat,
            policy::{PolicyName, Retry, TaskPolicy},
        },
    };
    use serde::{Deserialize, Serialize, Serializer};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A till: what it rang up so far.
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Till {
        total: u32,
    }

    /// A sale is its own event: every variant makes the reaction plan something else.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum Sale {
        /// A receipt of what the reaction saw.
        Rung(u32),
        /// `ping 1`, `pong`, `ping 2`.
        Burst,
        /// A pong now, a ping after the delay in seconds.
        Later(u64),
        /// A ping, a task that does not serialize, a counted task.
        Broken,
        /// A ping after a delay no schedule can hold.
        Overdue,
    }

    impl DomainEvent for Sale {
        fn event_type(&self) -> String {
            "sale".into()
        }
        fn event_version(&self) -> String {
            "1".into()
        }
    }

    impl Aggregate for Till {
        const TYPE: &'static str = "till";
        type Id = String;
        type Command = Sale;
        type Event = Sale;
        type Error = std::convert::Infallible;
        type Rules = ();

        fn apply(&mut self, event: Sale) {
            if let Sale::Rung(amount) = event {
                self.total += amount;
            }
        }
    }

    impl Decide<Till> for Sale {
        fn decide(self, _: &Till, _: &()) -> Result<Sale, std::convert::Infallible> {
            Ok(self)
        }
    }

    #[derive(Debug, Serialize)]
    struct Receipt {
        till: String,
        total: u32,
        command: String,
        source: Option<String>,
    }

    impl TaskPayload for Receipt {
        const TASK_TYPE: &'static str = "receipt";
        const TASK_VERSION: i32 = 1;
    }

    #[derive(Serialize)]
    struct Ping {
        value: u32,
    }

    impl TaskPayload for Ping {
        const TASK_TYPE: &'static str = "ping";
        const TASK_VERSION: i32 = 1;
    }

    #[derive(Serialize)]
    struct Pong {}

    impl TaskPayload for Pong {
        const TASK_TYPE: &'static str = "pong";
        const TASK_VERSION: i32 = 1;
    }

    /// Never serializes; what it would say is private.
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("private_reason"))
        }
    }

    impl TaskPayload for Unserializable {
        const TASK_TYPE: &'static str = "unserializable";
        const TASK_VERSION: i32 = 1;
    }

    /// How many times a [`Counted`] was serialized; only one test plans it.
    static COUNTED: AtomicUsize = AtomicUsize::new(0);

    struct Counted;

    impl Serialize for Counted {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            COUNTED.fetch_add(1, Ordering::SeqCst);
            serializer.serialize_unit_struct("Counted")
        }
    }

    impl TaskPayload for Counted {
        const TASK_TYPE: &'static str = "counted";
        const TASK_VERSION: i32 = 1;
    }

    impl Reactions for Till {
        fn react(decided: &Decided<'_, Self>, then: &mut Then) {
            match decided.event {
                Sale::Rung(_) => then.task(Receipt {
                    till: decided.id.clone(),
                    total: decided.state.total,
                    command: format!("{:?}", decided.command),
                    source: decided.metadata().get("source").cloned(),
                }),
                Sale::Burst => {
                    then.task(Ping { value: 1 });
                    then.task(Pong {});
                    then.task(Ping { value: 2 });
                }
                Sale::Later(seconds) => {
                    then.task(Pong {});
                    then.task_after(Duration::from_secs(*seconds), Ping { value: 3 });
                }
                Sale::Broken => {
                    then.task(Ping { value: 1 });
                    then.task(Unserializable);
                    then.task(Counted);
                    then.task_after(Duration::from_secs(1), Counted);
                }
                Sale::Overdue => then.task_after(Duration::MAX, Ping { value: 4 }),
            }
        }
    }

    const STEP: PolicyName = PolicyName::new("step");

    /// Pings are steps of six attempts; pongs and receipts have no policy.
    fn policies() -> Policies {
        Policies::default()
            .with_policy(STEP, TaskPolicy::new(Retry::fixed(Duration::from_secs(1)), 6).unwrap())
            .unwrap()
            .with_task::<Ping>(STEP)
            .unwrap()
    }

    fn planner() -> ReactionsPlanner<Till> {
        ReactionsPlanner::new(policies())
    }

    /// What the planner plans for `events` decided by `sale` on till `t-1`, which had rung up 5 before.
    fn plan(sale: Sale, events: &[Sale], metadata: &HashMap<String, String>) -> Result<Vec<NewTask>, TaskPlanError> {
        let before = Till { total: 5 };
        let mut after = before.clone();
        for event in events {
            after.apply(event.clone());
        }
        planner().plan(&Decision {
            aggregate_id: &"t-1".into(),
            before: &before,
            after: &after,
            command: &sale,
            events,
            metadata,
            causation: None,
        })
    }

    fn formats(tasks: &[NewTask]) -> Vec<String> {
        tasks.iter().map(|task| task.format().to_string()).collect()
    }

    #[test]
    fn a_reaction_sees_the_id_the_state_after_the_event_the_command_the_event_and_the_metadata() {
        let metadata = HashMap::from([("source".to_owned(), "ui".to_owned())]);
        let tasks = plan(Sale::Rung(3), &[Sale::Rung(3)], &metadata).unwrap();
        let [receipt] = &tasks[..] else {
            panic!("one receipt: {tasks:?}");
        };
        assert_eq!(
            receipt.payload(),
            &json!({ "till": "t-1", "total": 8, "command": "Rung(3)", "source": "ui" }),
            "the state after the event (5 + 3), not before it"
        );
        let (id, state, metadata) = ("t-2".to_owned(), Till { total: 1 }, HashMap::new());
        let decided = Decided::<Till>::new(&id, &state, &Sale::Burst, &Sale::Rung(1), &metadata);
        assert_eq!(
            (
                decided.id,
                decided.state.total,
                decided.command,
                decided.event,
                decided.metadata()
            ),
            (&id, 1, &Sale::Burst, &Sale::Rung(1), &metadata)
        );
    }

    #[test]
    fn a_decision_without_exactly_one_event_is_refused() {
        let none = plan(Sale::Burst, &[], &HashMap::new()).unwrap_err();
        assert!(matches!(&none, TaskPlanError::Application(_)));
        assert_eq!(none.to_string(), "a reaction sees one event, the decision has 0");
        let two = plan(Sale::Burst, &[Sale::Burst, Sale::Burst], &HashMap::new()).unwrap_err();
        assert_eq!(two.to_string(), "a reaction sees one event, the decision has 2");
    }

    #[test]
    fn then_plans_in_call_order_and_the_core_numbers_the_tasks_of_each_format() {
        let causation = Causation::root("ui");
        let planning = Planning::new(planner());
        let decide = || {
            planning.plan(
                &"t-1".into(),
                "t-1",
                &Till::default(),
                &Till::default(),
                &Sale::Burst,
                &[Sale::Burst],
                &causation.event_metadata(),
            )
        };
        let tasks = decide().unwrap();
        let (ping, pong) = (TaskFormat::of::<Ping>().unwrap(), TaskFormat::of::<Pong>().unwrap());
        assert_eq!(formats(&tasks), ["ping v1", "pong v1", "ping v1"]);
        assert_eq!(
            tasks.iter().map(|task| task.payload().clone()).collect::<Vec<_>>(),
            [json!({ "value": 1 }), json!({}), json!({ "value": 2 })]
        );
        assert_eq!(
            tasks.iter().map(NewTask::id).collect::<Vec<_>>(),
            [
                causation.task_id("till", "t-1", &ping, 0),
                causation.task_id("till", "t-1", &pong, 0),
                causation.task_id("till", "t-1", &ping, 1),
            ],
            "the ordinal counts the tasks of a format in the order of the calls"
        );
        assert_eq!(
            decide().unwrap().iter().map(NewTask::id).collect::<Vec<_>>(),
            tasks.iter().map(NewTask::id).collect::<Vec<_>>(),
            "every attempt plans the same ids"
        );
    }

    #[test]
    fn task_after_schedules_after_its_delay_and_task_plans_now() {
        let tasks = plan(Sale::Later(90), &[Sale::Later(90)], &HashMap::new()).unwrap();
        assert_eq!(formats(&tasks), ["pong v1", "ping v1"]);
        assert_eq!(
            (tasks[0].schedule(), tasks[1].schedule()),
            (&TaskSchedule::Now, &TaskSchedule::After(Duration::from_secs(90)))
        );
        assert_eq!(tasks[1].payload(), &json!({ "value": 3 }));
    }

    #[test]
    fn the_policy_of_a_task_type_sets_its_budget_and_a_type_without_one_keeps_the_default() {
        let tasks = plan(Sale::Burst, &[Sale::Burst], &HashMap::new()).unwrap();
        assert_eq!(
            tasks.iter().map(NewTask::max_attempts).collect::<Vec<_>>(),
            [6, DEFAULT_MAX_ATTEMPTS, 6]
        );
        let unbound = ReactionsPlanner::<Till>::new(Policies::default());
        let tasks = unbound
            .plan(&Decision {
                aggregate_id: &"t-1".into(),
                before: &Till::default(),
                after: &Till::default(),
                command: &Sale::Burst,
                events: &[Sale::Burst],
                metadata: &HashMap::new(),
                causation: None,
            })
            .unwrap();
        assert_eq!(
            tasks.iter().map(NewTask::max_attempts).collect::<Vec<_>>(),
            [DEFAULT_MAX_ATTEMPTS; 3]
        );
    }

    #[test]
    fn the_first_task_that_cannot_be_built_refuses_the_plan() {
        let mut counting = Then::default();
        counting.task(Counted);
        assert_eq!(COUNTED.load(Ordering::SeqCst), 1, "building a task serializes it");
        let broken = plan(Sale::Broken, &[Sale::Broken], &HashMap::new()).unwrap_err();
        assert!(matches!(broken, TaskPlanError::Task(TaskValidationError::Serialization)));
        assert_eq!(COUNTED.load(Ordering::SeqCst), 1, "nothing after the refused task is built");
        let overdue = plan(Sale::Overdue, &[Sale::Overdue], &HashMap::new()).unwrap_err();
        assert!(matches!(overdue, TaskPlanError::Task(TaskValidationError::InvalidSchedule)));
        let mut then = Then::default();
        then.task(Unserializable);
        then.task_after(Duration::ZERO, Ping { value: 1 });
        assert_eq!(then.into_tasks(), Err(TaskValidationError::Serialization));
    }

    #[tokio::test]
    async fn a_task_that_does_not_serialize_makes_the_command_unavailable_writes_nothing_and_is_logged_once() {
        let journal = MemEventRepository::default();
        let tills = TestCommandExecutor::over_journal(journal.clone(), ()).with_task_planner(planner());
        let (_, log) = crate::diagnostics::test_support::capture(async {
            assert_eq!(
                tills.execute(&"private_till".into(), Sale::Broken).await,
                Err(CommandGatewayError::Unavailable)
            );
            assert!(tills.take_published_tasks().is_empty());
        })
        .await;
        assert!(
            journal.get_events::<Till>("private_till").await.unwrap().is_empty() && journal.outbox::<Till>().is_empty(),
            "nothing was committed"
        );
        assert_eq!(log.matches("command execution failed").count(), 1, "{log}");
        assert!(log.contains("category=\"task_plan\""), "{log}");
        assert!(!log.contains("private_"), "{log}");
        // The same executor commits a sale whose receipt builds: the event and its task, together.
        tills.execute(&"private_till".into(), Sale::Rung(2)).await.unwrap();
        assert_eq!(
            (
                journal.get_events::<Till>("private_till").await.unwrap().len(),
                journal.outbox::<Till>().len()
            ),
            (1, 1)
        );
    }
}
