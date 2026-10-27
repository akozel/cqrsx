//! What a task handler answers for one attempt.
use crate::cqrs::{command::CommandGatewayError, rejection::Rejection};
use std::fmt;

/// What a task handler answers for one attempt: done, try again, or stop.
///
/// The handler does not choose the pause before the next attempt: the executor applies the policy of the task's type
/// ([`Policies`](super::policy::Policies)), and the handler can read that pause in its context
/// ([`TaskContext::retry_pause`](super::context::TaskContext::retry_pause)). A reason is persisted with the task
/// (`last_error`): it carries no payload, credentials or secrets.
///
/// ```
/// use cqrsx::task::result::TaskResult;
///
/// assert_eq!(TaskResult::done(), TaskResult::Done);
/// assert_eq!(TaskResult::retry("mailbox_busy").to_string(), "mailbox_busy");
/// assert_eq!(TaskResult::fail("bad address"), TaskResult::Fail { reason: "bad address".into() });
/// ```
#[must_use = "the result settles the task"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskResult {
    /// The effect happened, now or by an earlier attempt: the task is completed.
    Done,
    /// Not now: another attempt after the pause of the task type's policy, within its attempt budget.
    Retry { reason: String },
    /// Stop automatic delivery. This does not prove the effect never occurred.
    Fail { reason: String },
}

impl TaskResult {
    /// The effect happened.
    pub fn done() -> Self {
        Self::Done
    }

    /// Another attempt, for `reason`.
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry { reason: reason.into() }
    }

    /// No more attempts, for `reason`.
    pub fn fail(reason: impl Into<String>) -> Self {
        Self::Fail { reason: reason.into() }
    }
}

/// `done`, or the reason of a retry or a failure.
impl fmt::Display for TaskResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Done => f.write_str("done"),
            Self::Retry { reason } | Self::Fail { reason } => f.write_str(reason),
        }
    }
}

/// The answer of a command a task issued ([`TaskContext::execute`](super::context::TaskContext::execute)) as the
/// task's result: accepted is done; a conflict and an unavailable store are retried, with the reasons `conflict` and
/// `unavailable`; a rejection fails the task with the rejection's code. A handler matches the rejections that mean
/// "already done" or "too early" first and converts the rest.
impl<T, E: Rejection> From<Result<T, CommandGatewayError<E>>> for TaskResult {
    fn from(answer: Result<T, CommandGatewayError<E>>) -> Self {
        match answer {
            Ok(_) => Self::Done,
            Err(CommandGatewayError::Domain(rejection)) => Self::fail(rejection.code()),
            Err(CommandGatewayError::Conflict) => Self::retry("conflict"),
            Err(CommandGatewayError::Unavailable) => Self::retry("unavailable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[derive(Debug)]
    struct SoldOut;

    impl Rejection for SoldOut {
        fn code(&self) -> &'static str {
            "sold_out"
        }

        fn params(&self) -> Value {
            json!({})
        }
    }

    #[test]
    fn the_constructors_say_what_the_task_answers_and_display_their_reason() {
        assert_eq!(TaskResult::done(), TaskResult::Done);
        assert_eq!(
            TaskResult::retry("busy"),
            TaskResult::Retry {
                reason: "busy".to_owned()
            }
        );
        assert_eq!(
            TaskResult::fail(String::from("rejected")),
            TaskResult::Fail {
                reason: "rejected".to_owned()
            }
        );
        assert_eq!(
            [TaskResult::done(), TaskResult::retry("busy"), TaskResult::fail("rejected")].map(|result| result.to_string()),
            ["done", "busy", "rejected"]
        );
    }

    #[test]
    fn the_answer_of_a_command_becomes_done_a_retry_with_the_old_reasons_or_a_failure_with_its_code() {
        let answers: [Result<u8, CommandGatewayError<SoldOut>>; 4] = [
            Ok(7),
            Err(CommandGatewayError::Domain(SoldOut)),
            Err(CommandGatewayError::Conflict),
            Err(CommandGatewayError::Unavailable),
        ];
        assert_eq!(
            answers.map(TaskResult::from),
            [
                TaskResult::Done,
                TaskResult::fail("sold_out"),
                TaskResult::retry("conflict"),
                TaskResult::retry("unavailable"),
            ]
        );
    }
}
