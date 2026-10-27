//! Test tools the flows of several modules share: what a planner (the reactions' planner of an aggregate) plans for a
//! decision built by hand.
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        command::planner::{Decision, TaskPlanError, TaskPlanner},
    },
    task::NewTask,
};
use std::collections::HashMap;

/// What `planner` plans for `events` decided on `aggregate_id`, the decision built by hand (the reactions
/// read neither the state nor the command).
pub fn plan<A: Aggregate, P: TaskPlanner<A>>(
    planner: &P,
    aggregate_id: &A::Id,
    command: &A::Command,
    events: &[A::Event],
) -> Result<Vec<NewTask>, TaskPlanError> {
    let state = A::default();
    planner.plan(&Decision {
        aggregate_id,
        before: &state,
        after: &state,
        command,
        events,
        metadata: &HashMap::new(),
        causation: None,
    })
}

/// The formats of the planned tasks, in plan order.
pub fn formats(tasks: &[cqrsx::task::NewTask]) -> Vec<String> {
    tasks.iter().map(|task| task.format().to_string()).collect()
}
