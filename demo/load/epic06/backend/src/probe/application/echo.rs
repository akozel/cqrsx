//! Task `encore_probe_echo`: what a ping plans `ENCORE_LOAD_TASKS` times. It does nothing: the stand measures the way
//! of a task through the outbox, the queue and the worker, not its work.
use cqrsx::{Task, task::result::TaskResult, task_handler};
use serde::{Deserialize, Serialize};

/// One of the tasks of a ping: its place among them, and the probe as its label (kept in the task's metadata, as
/// what is said of a task besides its business data).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Task)]
#[task(name = "encore_probe_echo", version = 1)]
pub struct ProbeEchoV1 {
    #[label]
    pub probe_id: String,
    pub slot: u8,
}

/// Done at once.
#[task_handler]
pub async fn echo(_task: ProbeEchoV1) -> TaskResult {
    TaskResult::done()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::task::{NewTask, format::TaskPayload};
    use serde_json::json;

    #[tokio::test]
    async fn an_echo_keeps_the_probe_in_its_label_and_is_done_at_once() {
        let task = ProbeEchoV1 {
            probe_id: "p-1".into(),
            slot: 2,
        };
        assert_eq!(ProbeEchoV1::LABELS, ["probe_id"]);
        let new = NewTask::new(task.clone()).unwrap();
        assert_eq!(
            (new.payload(), new.metadata()),
            (&json!({ "slot": 2 }), &json!({ "probe_id": "p-1" }))
        );
        assert_eq!(echo.call(task).await, TaskResult::Done);
    }
}
