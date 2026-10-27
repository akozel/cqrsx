//! Task planning: the follow-up tasks of a command, planned from what its aggregate decided.
//!
//! An executor runs its [`TaskPlanner`] after the command's `decide`, on every attempt, under the
//! aggregate's lock and before the commit; the tasks it plans commit atomically with that attempt's
//! events, and a conflict retry plans again from the fresh state. The aggregate decides only events: a
//! task is a delivery contract of the application, so the reaction to an event belongs to the planner.
//! The core then stamps every planned task with the command's causation and gives it an id derived from
//! the command ([`Causation::task_id`]).
//!
//! A business process reads as one type that plans for every aggregate type it reacts to; an
//! aggregate's executor registers one planner, a tuple of the processes that react to it. A planner is a
//! plain function of a [`Decision`], so a test builds one by hand:
//!
//! ```
//! use cqrsx::cqrs::{
//!     aggregate::Aggregate,
//!     command::{
//!         decide::Decide,
//!         planner::{Decision, TaskPlanError, TaskPlanner},
//!     },
//!     event::DomainEvent,
//! };
//! use cqrsx::task::{NewTask, format::TaskPayload};
//! use serde::{Deserialize, Serialize};
//! use std::collections::HashMap;
//!
//! #[derive(Clone, Default, Serialize, Deserialize)]
//! struct Order {
//!     placed: bool,
//! }
//! #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
//! enum OrderEvent {
//!     Placed { total: u32 },
//! }
//! impl DomainEvent for OrderEvent {
//!     fn event_type(&self) -> String { "placed".into() }
//!     fn event_version(&self) -> String { "1".into() }
//! }
//! #[derive(Debug, thiserror::Error)]
//! #[error("already placed")]
//! struct AlreadyPlaced;
//! impl Aggregate for Order {
//!     const TYPE: &'static str = "order";
//!     type Id = String;
//!     type Command = u32;
//!     type Event = OrderEvent;
//!     type Error = AlreadyPlaced;
//!     type Rules = ();
//!     fn apply(&mut self, _: OrderEvent) { self.placed = true; }
//! }
//! impl Decide<Order> for u32 {
//!     fn decide(self, order: &Order, _: &()) -> Result<OrderEvent, AlreadyPlaced> {
//!         if order.placed {
//!             return Err(AlreadyPlaced);
//!         }
//!         Ok(OrderEvent::Placed { total: self })
//!     }
//! }
//!
//! #[derive(Serialize)]
//! struct Ship { order_id: String }
//! impl TaskPayload for Ship {
//!     const TASK_TYPE: &'static str = "ship";
//!     const TASK_VERSION: i32 = 1;
//! }
//! #[derive(Serialize)]
//! struct Invoice { order_id: String, total: u32 }
//! impl TaskPayload for Invoice {
//!     const TASK_TYPE: &'static str = "invoice";
//!     const TASK_VERSION: i32 = 1;
//! }
//!
//! /// Shipping: a placed order is shipped.
//! struct Shipping;
//! impl TaskPlanner<Order> for Shipping {
//!     fn plan(&self, decision: &Decision<'_, Order>) -> Result<Vec<NewTask>, TaskPlanError> {
//!         let mut tasks = Vec::new();
//!         for event in decision.events {
//!             let OrderEvent::Placed { .. } = event;
//!             tasks.push(NewTask::new(Ship { order_id: decision.aggregate_id.into() })?);
//!         }
//!         Ok(tasks)
//!     }
//! }
//! /// Billing: a placed order is invoiced at its total.
//! struct Billing;
//! impl TaskPlanner<Order> for Billing {
//!     fn plan(&self, decision: &Decision<'_, Order>) -> Result<Vec<NewTask>, TaskPlanError> {
//!         let mut tasks = Vec::new();
//!         for event in decision.events {
//!             let OrderEvent::Placed { total } = event;
//!             tasks.push(NewTask::new(Invoice { order_id: decision.aggregate_id.into(), total: *total })?);
//!         }
//!         Ok(tasks)
//!     }
//! }
//!
//! // The one planner of the `Order` executor: every process that reacts to an order, in order.
//! let planner = (Shipping, Billing);
//! let events = [OrderEvent::Placed { total: 30 }];
//! let (before, mut after) = (Order::default(), Order::default());
//! after.apply(events[0].clone());
//! let decision = Decision {
//!     aggregate_id: &"o-1".into(),
//!     before: &before,
//!     after: &after,
//!     command: &30,
//!     events: &events,
//!     metadata: &HashMap::new(),
//!     causation: None,
//! };
//! let tasks = planner.plan(&decision).unwrap();
//! let formats: Vec<String> = tasks.iter().map(|task| task.format().to_string()).collect();
//! assert_eq!(formats, ["ship v1", "invoice v1"]);
//! assert_eq!(tasks[1].payload()["total"], 30);
//! ```
use crate::{
    cqrs::{
        aggregate::Aggregate,
        command::causation::{Causation, CausationError},
    },
    task::{NewTask, TaskValidationError, format::TaskFormat},
};
use std::{collections::HashMap, error::Error};

/// What one attempt of a command decided: everything its follow-up tasks may be planned from.
pub struct Decision<'a, A: Aggregate> {
    /// The instance the command ran on: the id the command was executed with (its text, the `Display`, keys the
    /// store and the task ids).
    pub aggregate_id: &'a A::Id,
    /// The committed state the command was decided on (`A::default()` for a new aggregate).
    pub before: &'a A,
    /// `before` with `events` applied: the state the commit leaves (a copy made for the planner).
    pub after: &'a A,
    pub command: &'a A::Command,
    /// What the command decided, in commit order: its one event.
    pub events: &'a [A::Event],
    /// The command's metadata, stored with every event.
    pub metadata: &'a HashMap<String, String>,
    /// The causation in `metadata` ([`Causation::from_event_metadata`]); the core stamps it on every task.
    pub causation: Option<&'a Causation>,
}

/// The follow-up tasks of one aggregate type, planned from every attempt's [`Decision`] under the
/// aggregate's lock, before the commit; they commit atomically with that attempt's events.
///
/// A planner is pure and synchronous: no I/O (it runs under the lock, and the signature rules out an
/// `await`), no clock and no randomness its tasks depend on. Data from outside comes in the command;
/// schedule with `TaskSchedule::After`, not `At(now + …)`. It is called for every decision (an event; a
/// rejection is not planned), once per attempt. An error refuses the command with nothing written.
///
/// Several planners compose as a tuple of two to eight: `(A, B)` plans `A`'s tasks, then `B`'s, and the
/// first error wins. The order is part of the derived task ids when two planners plan tasks of one
/// format.
pub trait TaskPlanner<A: Aggregate>: Send + Sync {
    fn plan(&self, decision: &Decision<'_, A>) -> Result<Vec<NewTask>, TaskPlanError>;
}

/// Why a decision's tasks could not be planned; the command is refused with nothing written.
#[derive(Debug, thiserror::Error)]
pub enum TaskPlanError {
    /// A planned task is invalid.
    #[error(transparent)]
    Task(#[from] TaskValidationError),
    /// The metadata names a command but does not carry its causation, so its tasks could be neither stamped
    /// nor identified.
    #[error(transparent)]
    Causation(#[from] CausationError),
    /// The planner refused the decision for a reason of its own.
    #[error("{0}")]
    Application(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl TaskPlanError {
    pub fn application(error: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        Self::Application(error.into())
    }
}

macro_rules! tuple_planner {
    ($($planner:ident $index:tt),+) => {
        impl<A: Aggregate, $($planner: TaskPlanner<A>),+> TaskPlanner<A> for ($($planner,)+) {
            fn plan(&self, decision: &Decision<'_, A>) -> Result<Vec<NewTask>, TaskPlanError> {
                let mut tasks = Vec::new();
                $(tasks.extend(self.$index.plan(decision)?);)+
                Ok(tasks)
            }
        }
    };
}

tuple_planner!(P0 0, P1 1);
tuple_planner!(P0 0, P1 1, P2 2);
tuple_planner!(P0 0, P1 1, P2 2, P3 3);
tuple_planner!(P0 0, P1 1, P2 2, P3 3, P4 4);
tuple_planner!(P0 0, P1 1, P2 2, P3 3, P4 4, P5 5);
tuple_planner!(P0 0, P1 1, P2 2, P3 3, P4 4, P5 5, P6 6);
tuple_planner!(P0 0, P1 1, P2 2, P3 3, P4 4, P5 5, P6 6, P7 7);

/// A framework's planner and the way it clones a command for it: the decision needs the command after
/// `decide` has consumed it. The clone is captured where `A::Command: Clone` is known, so a framework
/// without a planner needs no `Clone` command.
pub(crate) struct Planning<A: Aggregate> {
    planner: Box<dyn TaskPlanner<A>>,
    clone_command: fn(&A::Command) -> A::Command,
}

impl<A: Aggregate> Planning<A> {
    pub(crate) fn new(planner: impl TaskPlanner<A> + 'static) -> Self
    where
        A::Command: Clone,
    {
        Self {
            planner: Box::new(planner),
            clone_command: <A::Command as Clone>::clone,
        }
    }

    pub(crate) fn clone_command(&self, command: &A::Command) -> A::Command {
        (self.clone_command)(command)
    }

    /// The tasks of one attempt's decision: planned, then stamped with the causation
    /// of `metadata` and identified by `Causation::task_id` from `key`, the text of `id`, the ordinal counted
    /// per format over the whole plan. Without causation the tasks keep the planner's ids and metadata. A
    /// causation that does not parse refuses a non-empty plan only: an empty one has nothing to stamp.
    #[allow(clippy::too_many_arguments, reason = "one attempt's decision and its id's text")]
    pub(crate) fn plan(
        &self,
        id: &A::Id,
        key: &str,
        before: &A,
        after: &A,
        command: &A::Command,
        events: &[A::Event],
        metadata: &HashMap<String, String>,
    ) -> Result<Vec<NewTask>, TaskPlanError> {
        let causation = Causation::from_event_metadata(metadata);
        let decision = Decision {
            aggregate_id: id,
            before,
            after,
            command,
            events,
            metadata,
            causation: causation.as_ref().ok().and_then(Option::as_ref),
        };
        let tasks = self.planner.plan(&decision)?;
        if tasks.is_empty() {
            return Ok(tasks);
        }
        let Some(causation) = causation? else {
            return Ok(tasks);
        };
        let mut ordinals: HashMap<TaskFormat, usize> = HashMap::new();
        Ok(tasks
            .into_iter()
            .map(|task| {
                let ordinal = ordinals.entry(task.format().clone()).or_default();
                let task_id = causation.task_id(A::TYPE, key, task.format(), *ordinal);
                *ordinal += 1;
                causation.stamp_task(task).with_id(task_id)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cqrs::{command::decide::Decide, event::DomainEvent},
        task::{TaskSchedule, format::TaskPayload},
    };
    use proptest::prelude::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use uuid::Uuid;

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Counter(u32);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Counted(u32);

    impl DomainEvent for Counted {
        fn event_type(&self) -> String {
            "counted".into()
        }
        fn event_version(&self) -> String {
            "1".into()
        }
    }

    impl Aggregate for Counter {
        const TYPE: &'static str = "counter";
        type Id = String;
        type Command = u32;
        type Event = Counted;
        type Error = std::convert::Infallible;
        type Rules = ();

        fn apply(&mut self, event: Counted) {
            self.0 += event.0;
        }
    }

    impl Decide<Counter> for u32 {
        fn decide(self, _: &Counter, _: &()) -> Result<Counted, std::convert::Infallible> {
            Ok(Counted(self))
        }
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

    /// Plans `count` pings carrying `value`, or refuses with `fail`; counts its calls.
    struct Pinger {
        value: u32,
        count: usize,
        fail: bool,
        calls: Arc<AtomicUsize>,
    }

    impl Pinger {
        fn new(value: u32, count: usize) -> Self {
            Self {
                value,
                count,
                fail: false,
                calls: Arc::default(),
            }
        }

        fn failing() -> Self {
            Self {
                fail: true,
                ..Self::new(0, 0)
            }
        }
    }

    impl TaskPlanner<Counter> for Pinger {
        fn plan(&self, _: &Decision<'_, Counter>) -> Result<Vec<NewTask>, TaskPlanError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(TaskPlanError::application("refused"));
            }
            (0..self.count)
                .map(|_| Ok(NewTask::new(Ping { value: self.value })?))
                .collect()
        }
    }

    fn decide<P: TaskPlanner<Counter>>(planner: &P) -> Result<Vec<NewTask>, TaskPlanError> {
        let (before, after) = (Counter(0), Counter(1));
        planner.plan(&Decision {
            aggregate_id: &"c-1".into(),
            before: &before,
            after: &after,
            command: &1,
            events: &[Counted(1)],
            metadata: &HashMap::new(),
            causation: None,
        })
    }

    fn values(tasks: &[NewTask]) -> Vec<u64> {
        tasks.iter().map(|task| task.payload()["value"].as_u64().unwrap()).collect()
    }

    #[test]
    fn tuples_plan_in_order_and_the_first_error_stops_the_rest() {
        assert_eq!(values(&decide(&(Pinger::new(1, 1), Pinger::new(2, 2))).unwrap()), [1, 2, 2]);
        assert_eq!(
            values(&decide(&(Pinger::new(1, 1), Pinger::new(2, 0), Pinger::new(3, 1))).unwrap()),
            [1, 3]
        );
        let eight = (
            Pinger::new(0, 1),
            Pinger::new(1, 1),
            Pinger::new(2, 1),
            Pinger::new(3, 1),
            Pinger::new(4, 1),
            Pinger::new(5, 1),
            Pinger::new(6, 1),
            Pinger::new(7, 1),
        );
        assert_eq!(values(&decide(&eight).unwrap()), [0, 1, 2, 3, 4, 5, 6, 7]);
        let after_the_error = Pinger::new(3, 1);
        let calls = after_the_error.calls.clone();
        let failed = decide(&(Pinger::new(1, 1), Pinger::failing(), after_the_error));
        assert!(matches!(failed, Err(TaskPlanError::Application(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 0, "a planner after the error is not called");
    }

    #[test]
    fn a_plan_error_names_its_cause() {
        let application = TaskPlanError::application(std::io::Error::other("unknown aggregate id"));
        assert_eq!(application.to_string(), "unknown aggregate id");
        assert!(application.source().unwrap().downcast_ref::<std::io::Error>().is_some());
        let task = TaskPlanError::from(TaskValidationError::InvalidAttemptLimit);
        assert_eq!(task.to_string(), "task attempt limit must be positive");
        let causation = TaskPlanError::from(CausationError::Missing("causation_id"));
        assert_eq!(causation.to_string(), "causation key `causation_id` is missing");
        assert!(format!("{causation:?}").contains("Missing"));
    }

    /// Plans what it is given: a fixed list of tasks.
    struct Fixed(Vec<NewTask>);

    impl TaskPlanner<Counter> for Fixed {
        fn plan(&self, _: &Decision<'_, Counter>) -> Result<Vec<NewTask>, TaskPlanError> {
            Ok(self.0.clone())
        }
    }

    fn plan_with(tasks: Vec<NewTask>, metadata: &HashMap<String, String>) -> Result<Vec<NewTask>, TaskPlanError> {
        let (before, after) = (Counter(0), Counter(1));
        Planning::new(Fixed(tasks)).plan(&"c-1".into(), "c-1", &before, &after, &1, &[Counted(1)], metadata)
    }

    fn ping(value: u32) -> NewTask {
        NewTask::new(Ping { value }).unwrap()
    }

    #[test]
    fn planned_tasks_are_stamped_and_identified_by_format_and_ordinal() {
        let causation = Causation::root("ui");
        let scheduled = ping(1)
            .with_metadata(json!({ "tenant": "t-1", "command_id": "stale" }))
            .unwrap()
            .with_max_attempts(4)
            .unwrap()
            .scheduled(TaskSchedule::After(Duration::from_secs(3)))
            .unwrap();
        let pong = NewTask::new(Pong {}).unwrap();
        let tasks = plan_with(vec![scheduled.clone(), pong.clone(), ping(2)], &causation.event_metadata()).unwrap();
        let ping_format = TaskFormat::of::<Ping>().unwrap();
        assert_eq!(
            tasks.iter().map(NewTask::id).collect::<Vec<_>>(),
            [
                causation.task_id("counter", "c-1", &ping_format, 0),
                causation.task_id("counter", "c-1", pong.format(), 0),
                causation.task_id("counter", "c-1", &ping_format, 1),
            ],
            "the ordinal counts per format: the pong restarts at 0"
        );
        assert_eq!(
            tasks[0].metadata(),
            &json!({
                "tenant": "t-1",
                "command_id": causation.command_id.to_string(),
                "correlation_id": causation.correlation_id.to_string(),
                "causation_id": causation.causation_id.to_string(),
            }),
            "the application keys stay, a stale causation key is replaced"
        );
        assert_eq!(
            (tasks[0].payload(), tasks[0].schedule(), tasks[0].max_attempts()),
            (scheduled.payload(), scheduled.schedule(), 4)
        );
    }

    #[test]
    fn without_causation_the_tasks_keep_their_ids_and_metadata() {
        let planned = vec![ping(1).with_metadata(json!({ "tenant": "t-1" })).unwrap(), ping(2)];
        let metadata = HashMap::from([("source".to_owned(), "ui".to_owned())]);
        assert_eq!(plan_with(planned.clone(), &metadata).unwrap(), planned);
    }

    #[test]
    fn a_causation_that_does_not_parse_refuses_tasks_but_not_an_empty_plan() {
        let metadata = HashMap::from([("command_id".to_owned(), "cmd-1".to_owned())]);
        assert!(matches!(
            plan_with(vec![ping(1)], &metadata),
            Err(TaskPlanError::Causation(CausationError::NotUuid("command_id")))
        ));
        assert_eq!(plan_with(vec![], &metadata).unwrap(), vec![]);
    }

    /// The aggregate id, before, after, command, events and causation a planner saw.
    type View = (String, u32, u32, u32, Vec<Counted>, Option<Causation>);

    /// Hands the planner's view of the decision to the test.
    struct Seen(std::sync::Mutex<Option<View>>);

    impl TaskPlanner<Counter> for Seen {
        fn plan(&self, d: &Decision<'_, Counter>) -> Result<Vec<NewTask>, TaskPlanError> {
            *self.0.lock().unwrap() = Some((
                d.aggregate_id.to_owned(),
                d.before.0,
                d.after.0,
                *d.command,
                d.events.to_vec(),
                d.causation.cloned(),
            ));
            Ok(vec![])
        }
    }

    #[test]
    fn the_planner_gets_the_parsed_causation_and_none_for_a_malformed_one() {
        let seen = Arc::new(Seen(std::sync::Mutex::new(None)));
        struct Shared(Arc<Seen>);
        impl TaskPlanner<Counter> for Shared {
            fn plan(&self, d: &Decision<'_, Counter>) -> Result<Vec<NewTask>, TaskPlanError> {
                self.0.plan(d)
            }
        }
        let planning = Planning::new(Shared(seen.clone()));
        let causation = Causation::root("ui");
        planning
            .plan(
                &"c-1".into(),
                "c-1",
                &Counter(2),
                &Counter(5),
                &3,
                &[Counted(3)],
                &causation.event_metadata(),
            )
            .unwrap();
        assert_eq!(
            seen.0.lock().unwrap().take(),
            Some(("c-1".to_owned(), 2, 5, 3, vec![Counted(3)], Some(causation)))
        );
        let malformed = HashMap::from([("command_id".to_owned(), "cmd-1".to_owned())]);
        planning
            .plan(&"c-1".into(), "c-1", &Counter(0), &Counter(1), &1, &[Counted(1)], &malformed)
            .unwrap();
        assert_eq!(seen.0.lock().unwrap().take().unwrap().5, None);
        assert_eq!(planning.clone_command(&9), 9);
    }

    proptest! {
        #[test]
        fn task_ids_are_unique_and_stable_over_random_plans(formats in proptest::collection::vec(0..3_usize, 1..40)) {
            let tasks: Vec<NewTask> = formats
                .iter()
                .map(|format| match format {
                    0 => ping(1),
                    1 => NewTask::new(Pong {}).unwrap(),
                    _ => NewTask::from_payload(TaskFormat::new("ping", 2).unwrap(), json!({})).unwrap(),
                })
                .collect();
            let metadata = Causation::root("ui").event_metadata();
            let first = plan_with(tasks.clone(), &metadata).unwrap();
            let again = plan_with(tasks, &metadata).unwrap();
            let ids: std::collections::HashSet<Uuid> = first.iter().map(NewTask::id).collect();
            prop_assert_eq!(ids.len(), first.len(), "every planned task has an id of its own");
            prop_assert_eq!(
                first.iter().map(NewTask::id).collect::<Vec<_>>(),
                again.iter().map(NewTask::id).collect::<Vec<_>>(),
                "planning the same decision again gives the same ids"
            );
        }
    }
}
