//! The diagnostics contract of the crates of the framework: what a log field may carry, and what a storage adapter
//! tells the core's logs about a failed operation.
//!
//! Public, for the crates that write log lines of their own or wrap a storage's errors:
//! - [`safe_type`]: a name chosen by the application as a log field, never a free text;
//! - [`StorageFailure`]: the fixed category and the code of a failed storage operation. An adapter wraps the error of
//!   its driver in it; the core finds it in the chain of sources of a failed command or projection batch and writes
//!   `storage_category` and `sqlstate` fields, without knowing the driver.
//!
//! The log lines of the core are private: their fields are fixed, and no id, payload or error text reaches them.
use crate::projection::processor::{Operation, observer::FailureCategory};
use std::{error::Error, fmt};

/// An application-chosen name (an aggregate or task type) as a log field: itself when it is a plain identifier of
/// ASCII letters, digits and `_`, else `unknown`. The name is never trusted into a log as it is.
pub fn safe_type(value: &str) -> &str {
    if !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        value
    } else {
        "unknown"
    }
}

/// A failure of a storage operation with the two facts a log may carry: a fixed `category` word (`io`, `pool_closed`,
/// ...) and the storage's own `code` (for PostgreSQL, the SQLSTATE), never the message of the driver.
///
/// A storage adapter wraps the error of its driver in it where the error becomes a [`PersistenceError`] (or another
/// error of the core); the core then logs `storage_category` and `sqlstate` for a failed command or projection batch.
/// The wrapper is transparent: `Display`, `Debug` and [`source`](Error::source) are those of the cause, so the text of
/// every error that holds it is the one the driver's error gave. To reach the driver's error, find the `StorageFailure`
/// in the chain of sources (`downcast_ref::<StorageFailure>()`) and take [`cause`](Self::cause).
///
/// [`PersistenceError`]: crate::journal::PersistenceError
pub struct StorageFailure {
    category: &'static str,
    code: Option<String>,
    cause: Box<dyn Error + Send + Sync + 'static>,
}

impl StorageFailure {
    /// `category` is a fixed word the adapter chooses; `code` is the storage's code, when it gave a valid one.
    pub fn new(category: &'static str, code: Option<String>, cause: impl Error + Send + Sync + 'static) -> Self {
        Self {
            category,
            code,
            cause: Box::new(cause),
        }
    }

    /// The fixed category word.
    pub fn category(&self) -> &'static str {
        self.category
    }

    /// The storage's code, when there is one.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// The error of the driver.
    pub fn cause(&self) -> &(dyn Error + Send + Sync + 'static) {
        &*self.cause
    }
}

impl fmt::Debug for StorageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.cause, f)
    }
}

impl fmt::Display for StorageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.cause, f)
    }
}

impl Error for StorageFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause.source()
    }
}

/// The first [`StorageFailure`] in `source`'s chain: the only technical detail safe to log.
fn find_storage_failure<'e>(source: &'e (dyn Error + 'static)) -> Option<&'e StorageFailure> {
    let mut current = Some(source);
    while let Some(error) = current {
        if let Some(failure) = error.downcast_ref::<StorageFailure>() {
            return Some(failure);
        }
        current = error.source();
    }
    None
}

pub(crate) fn log_command_failure(aggregate_type: &str, category: &'static str, source: &(dyn std::error::Error + 'static)) {
    // These details are inspected before CommandError intentionally discards
    // technical sources. They never change the outward error or retry policy.
    if let Some(failure) = find_storage_failure(source) {
        tracing::error!(component = "command", operation = "execute", aggregate_type = safe_type(aggregate_type), category,
            storage_category = failure.category(), sqlstate = ?failure.code(), "command execution failed");
        return;
    }
    tracing::error!(
        component = "command",
        operation = "execute",
        aggregate_type = safe_type(aggregate_type),
        category,
        "command execution failed"
    );
}

/// A command refused before it ran: no lock for its aggregate. Expected under overload, hence `warn`.
pub(crate) fn log_lock_refused(aggregate_type: &str, refused: crate::command::locks::LockRefused) {
    tracing::warn!(
        component = "command",
        operation = "lock",
        aggregate_type = safe_type(aggregate_type),
        reason = refused.reason(),
        "command refused before it ran"
    );
}

/// A task handler ran a command of an aggregate type the assembly gave no executor: the command did not run, the task
/// is retried. A wiring fault, hence `error`; no id, no payload.
pub(crate) fn log_missing_executor(aggregate_type: &str) {
    tracing::error!(
        component = "task",
        operation = "execute",
        aggregate_type = safe_type(aggregate_type),
        "no command executor for the task's command"
    );
}

/// A projection failed for a batch of `jobs` of one key; logged once for the whole batch. `projection` is the
/// processor's name, chosen by the application, never a payload. `operation` is `deliver` for a batch of
/// live jobs and `rebuild` for the rebuild of a key.
pub(crate) fn log_projection_batch_failure(
    projection: &str,
    aggregate_type: &str,
    operation: &'static str,
    jobs: usize,
    source: &(dyn std::error::Error + 'static),
) {
    if let Some(failure) = find_storage_failure(source) {
        tracing::error!(component = "projection", operation, projection = safe_type(projection),
            aggregate_type = safe_type(aggregate_type), jobs, storage_category = failure.category(),
            sqlstate = ?failure.code(), "projection batch failed");
        return;
    }
    tracing::error!(
        component = "projection",
        operation,
        projection = safe_type(projection),
        aggregate_type = safe_type(aggregate_type),
        jobs,
        "projection batch failed"
    );
}

/// A projection panicked while it handled a batch of `jobs` of one key (or rebuilt a key).
pub(crate) fn log_projection_batch_panic(projection: &str, aggregate_type: &str, operation: &'static str, jobs: usize) {
    tracing::error!(
        component = "projection",
        operation,
        projection = safe_type(projection),
        aggregate_type = safe_type(aggregate_type),
        jobs,
        "projection batch panicked"
    );
}

/// A broken invariant of the crate, a bug: `invariant` says what should have held. The caller fails its operation, or
/// skips a step that has nothing to work on, and says which in its documentation.
pub(crate) fn log_broken_invariant(component: &'static str, invariant: &'static str) {
    tracing::error!(component, invariant, "an invariant of cqrsx is broken");
}

/// A thread panicked while it held the mutex `lock`; the crate clears the poison and goes on (see `mutex`). A bug, hence
/// `error`.
pub(crate) fn log_poisoned_lock(lock: &'static str) {
    tracing::error!(
        component = "lock",
        lock,
        "a thread panicked while holding the lock; the poison is cleared and the lock used on"
    );
}

/// A projection processor changed state: a lease acquired, lost or released, a rebuild started, finished
/// or interrupted, a key healed. `projection` is the projection's name, chosen by the application.
pub(crate) fn log_projection_change(projection: &str, aggregate_type: &str, change: &'static str) {
    tracing::info!(
        component = "projection",
        projection = safe_type(projection),
        aggregate_type = safe_type(aggregate_type),
        event = change,
        "projection processor event"
    );
}

/// An operation of a projection processor failed: `operation` names it (`page`, `rebuild`, `feed`,
/// `checkpoints`, `lease`, `head`), `category` the kind of error, `failures` the failed rounds in a row
/// (0 when the failure does not make the processor back off).
pub(crate) fn log_projection_failure(
    projection: &str,
    aggregate_type: &str,
    operation: Operation,
    category: FailureCategory,
    failures: u32,
) {
    tracing::warn!(
        component = "projection",
        projection = safe_type(projection),
        aggregate_type = safe_type(aggregate_type),
        operation = operation.as_str(),
        category = category.as_str(),
        failures,
        "projection processor operation failed"
    );
}

pub(crate) fn log_task_failure(task: &crate::worker::queue::settlement::TaskFailure) {
    let task_type = task.format.task_type();
    let safe_type = crate::diagnostics::safe_type(task_type);
    tracing::error!(
        task_id = %task.task_id,
        task_type = %safe_type,
        task_version = task.format.task_version(),
        attempt = task.attempt_number,
        reason = %task.cause.as_str(),
        "task failed"
    );
}

/// A task's handler panicked; the attempt is settled as a retry. Neither the panic's message nor the payload nor the
/// lease token reach the log: the message may hold the payload, the token fences the lease.
pub(crate) fn log_task_handler_panic(attempt: &crate::task::attempt::TaskAttempt) {
    tracing::error!(
        task_id = %attempt.task_id(),
        task_type = %safe_type(attempt.format().task_type()),
        task_version = attempt.format().task_version(),
        attempt = attempt.number(),
        "task handler panicked; the task is retried"
    );
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::{future::Future, io};

    /// The output of `work` and the `ERROR` lines it logged, one subscriber for this task alone.
    pub(crate) async fn capture<T>(work: impl Future<Output = T>) -> (T, String) {
        capture_at(tracing::Level::ERROR, work).await
    }

    /// The output of `work` and the lines of `level` and above it logged.
    pub(crate) async fn capture_at<T>(level: tracing::Level, work: impl Future<Output = T>) -> (T, String) {
        cqrsx_fixtures::logs::capture_at(level, prime, work).await
    }

    /// Touches every log line of the crate under the capturing subscriber, so that no callsite that parallel tests
    /// registered without a subscriber stays disabled.
    fn prime() {
        log_command_failure("prime", "unexpected", &io::Error::other("prime"));
        log_command_failure(
            "prime",
            "unexpected",
            &StorageFailure::new("prime", None, io::Error::other("prime")),
        );
        log_lock_refused("prime", crate::command::locks::LockRefused::TimedOut);
        log_missing_executor("prime");
        log_projection_batch_failure("prime", "prime", "prime", 1, &io::Error::other("prime"));
        log_projection_batch_failure(
            "prime",
            "prime",
            "prime",
            1,
            &StorageFailure::new("prime", None, io::Error::other("prime")),
        );
        log_projection_batch_panic("prime", "prime", "prime", 1);
        log_projection_change("prime", "prime", "prime");
        log_poisoned_lock("prime");
        log_broken_invariant("prime", "prime");
        log_projection_failure("prime", "prime", Operation::Page, FailureCategory::Unexpected, 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names_allow_ascii_identifiers_and_reject_unsafe_characters() {
        for name in [
            "Account",
            "account_1",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_",
            "_",
        ] {
            assert_eq!(safe_type(name), name);
        }
        for name in [
            "",
            " ",
            "Account Name",
            "account-name",
            "account.name",
            "a/b",
            "a\nb",
            "a\rb",
            "a\tb",
            "a\0b",
            "Счёт",
            "café",
            "Ａ",
        ] {
            assert_eq!(safe_type(name), "unknown", "unsafe type: {name:?}");
        }
    }

    #[tokio::test]
    async fn framework_diagnostics_keep_category_and_sanitize_type_without_source_text() {
        let (_, log) = test_support::capture(async {
            for category in ["connection", "deserialization", "unexpected"] {
                log_command_failure("Aggregate_1", category, &std::io::Error::other("private_source"));
            }
            log_command_failure(
                "private_type\nmail@example.com",
                "unexpected",
                &std::io::Error::other("private_source"),
            );
        })
        .await;
        assert_eq!(log.matches("command execution failed").count(), 4);
        for category in ["connection", "deserialization", "unexpected"] {
            assert!(log.contains(category));
        }
        assert!(log.contains("aggregate_type=\"Aggregate_1\""));
        assert!(log.contains("aggregate_type=\"unknown\""));
        assert!(log.contains("operation=\"execute\""));
        assert!(!log.contains("private_"));
        assert_eq!(safe_type(""), "unknown");
    }

    #[derive(Debug, thiserror::Error)]
    #[error("private_wrapper")]
    struct Wrapper(#[source] Box<dyn std::error::Error + Send + Sync>);

    #[derive(Debug, thiserror::Error)]
    #[error("private_cause")]
    struct Cause {
        #[source]
        inner: std::io::Error,
    }

    #[tokio::test]
    async fn command_diagnostics_find_a_nested_storage_failure_and_emit_one_safe_event() {
        let failure = StorageFailure::new("database", Some("23505".into()), std::io::Error::other("private_driver"));
        let source = Wrapper(Box::new(Wrapper(Box::new(failure))));
        let (_, log) = test_support::capture(async {
            log_command_failure("private_type\nmail@example.com", "deserialization", &source);
        })
        .await;
        assert_eq!(log.lines().count(), 1);
        for field in [
            "component=\"command\"",
            "operation=\"execute\"",
            "aggregate_type=\"unknown\"",
            "category=\"deserialization\"",
            "storage_category=\"database\"",
            "sqlstate=Some(\"23505\")",
        ] {
            assert!(log.contains(field), "missing {field}: {log}");
        }
        assert!(!log.contains("private_"));
        assert!(!log.contains("mail@example.com"));

        let source = Wrapper(Box::new(std::io::Error::other("private_source")));
        let (_, log) = test_support::capture(async { log_command_failure("Account", "unexpected", &source) }).await;
        assert_eq!(log.lines().count(), 1);
        assert!(log.contains("category=\"unexpected\""));
        assert!(!log.contains("storage_category"));
        assert!(!log.contains("sqlstate"));
        assert!(!log.contains("private_"));
    }

    #[tokio::test]
    async fn a_storage_failure_without_a_code_logs_its_category_and_no_code() {
        let failure = StorageFailure::new("pool_closed", None, std::io::Error::other("private_driver"));
        let (_, log) = test_support::capture(async { log_command_failure("Account", "connection", &failure) }).await;
        assert_eq!(log.lines().count(), 1);
        assert!(
            log.contains("storage_category=\"pool_closed\"") && log.contains("sqlstate=None"),
            "{log}"
        );
        assert!(!log.contains("private_"));
    }

    #[tokio::test]
    async fn a_projection_batch_failure_logs_the_code_of_the_storage_failure() {
        let source = crate::journal::PersistenceError::Connection(Box::new(StorageFailure::new(
            "database",
            Some("40001".into()),
            std::io::Error::other("private_driver"),
        )));
        let (_, log) =
            test_support::capture(async { log_projection_batch_failure("halls", "Concert", "deliver", 2, &source) }).await;
        assert_eq!(log.lines().count(), 1, "{log}");
        assert!(
            log.contains("storage_category=\"database\"") && log.contains("sqlstate=Some(\"40001\")"),
            "{log}"
        );
        assert!(!log.contains("private_"), "{log}");
    }

    #[test]
    fn a_storage_failure_is_transparent_and_hands_out_its_facts_and_its_cause() {
        use std::error::Error as _;
        let failure = StorageFailure::new(
            "io",
            Some("08006".into()),
            Cause {
                inner: std::io::Error::other("deeper"),
            },
        );
        assert_eq!((failure.category(), failure.code()), ("io", Some("08006")));
        let cause = Cause {
            inner: std::io::Error::other("deeper"),
        };
        assert_eq!(failure.to_string(), cause.to_string());
        assert_eq!(format!("{failure:?}"), format!("{cause:?}"));
        assert_eq!(failure.source().unwrap().to_string(), "deeper");
        assert!(failure.cause().is::<Cause>());
        assert_eq!(StorageFailure::new("other", None, cause).code(), None);
    }

    #[tokio::test]
    async fn projection_batch_diagnostics_log_one_safe_event_per_batch_with_the_storage_category() {
        let source = crate::journal::PersistenceError::Unexpected(Box::new(StorageFailure::new(
            "pool_timeout",
            None,
            std::io::Error::other("private_driver"),
        )));
        let (_, log) = test_support::capture(async {
            log_projection_batch_failure("concert_halls", "private_type\nmail@example.com", "deliver", 3, &source);
            log_projection_batch_failure(
                "private_projection\n",
                "Account",
                "rebuild",
                1,
                &std::io::Error::other("private_source"),
            );
            log_projection_batch_panic("fan_wallets", "Account", "rebuild", 1);
        })
        .await;
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 3, "{log}");
        for field in [
            "projection batch failed",
            "component=\"projection\"",
            "operation=\"deliver\"",
            "projection=\"concert_halls\"",
            "aggregate_type=\"unknown\"",
            "jobs=3",
            "storage_category=\"pool_timeout\"",
        ] {
            assert!(lines[0].contains(field), "missing {field}: {log}");
        }
        assert!(
            lines[1].contains("projection=\"unknown\"")
                && lines[1].contains("operation=\"rebuild\"")
                && lines[1].contains("jobs=1")
                && !lines[1].contains("storage_category"),
            "{log}"
        );
        assert!(
            lines[2].contains("projection batch panicked") && lines[2].contains("projection=\"fan_wallets\""),
            "{log}"
        );
        assert!(!log.contains("private_") && !log.contains("mail@example.com"), "{log}");
    }
}

#[cfg(test)]
mod projection_tests {
    use super::{FailureCategory, Operation, log_projection_change, log_projection_failure, test_support};

    #[tokio::test]
    async fn projection_diagnostics_name_the_projection_safely_and_carry_fixed_fields_only() {
        let (_, log) = test_support::capture_at(tracing::Level::INFO, async {
            log_projection_change("concert_halls", "Concert", "lease acquired");
            log_projection_change("private\nmail@example.com", "Concert", "rebuild finished");
            log_projection_failure("concert_halls", "Concert", Operation::Page, FailureCategory::Connection, 3);
        })
        .await;
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 3, "{log}");
        assert!(
            lines[0].contains("INFO")
                && lines[0].contains("projection processor event")
                && lines[0].contains("projection=\"concert_halls\"")
                && lines[0].contains("aggregate_type=\"Concert\"")
                && lines[0].contains("event=\"lease acquired\""),
            "{log}"
        );
        assert!(lines[1].contains("projection=\"unknown\""), "{log}");
        assert!(
            lines[2].contains("WARN")
                && lines[2].contains("projection processor operation failed")
                && lines[2].contains("operation=\"page\"")
                && lines[2].contains("category=\"connection\"")
                && lines[2].contains("failures=3"),
            "{log}"
        );
        assert!(!log.contains("private") && !log.contains("mail@example.com"), "{log}");
    }
}

#[cfg(test)]
mod task_failure_tests {
    use super::{log_task_failure, test_support};
    use crate::task::format::TaskFormat;
    use crate::worker::queue::settlement::{TaskFailure, TaskFailureCause};

    #[tokio::test]
    async fn confirmed_failure_events_contain_identity_and_safe_reason_only() {
        let (_, log) = test_support::capture(async {
            for cause in [
                TaskFailureCause::HandlerFailure,
                TaskFailureCause::AttemptLimitReached,
                TaskFailureCause::LeaseExpired,
            ] {
                let task = TaskFailure {
                    task_id: uuid::Uuid::from_u128(1),
                    format: TaskFormat::try_new("create_account", 1).unwrap(),
                    attempt_number: 2,
                    cause,
                };
                log_task_failure(&task);
            }
            let task = TaskFailure {
                task_id: uuid::Uuid::from_u128(2),
                format: TaskFormat::try_new("secret\nalice@example.com", 1).unwrap(),
                attempt_number: 1,
                cause: TaskFailureCause::LeaseExpired,
            };
            log_task_failure(&task);
        })
        .await;
        assert_eq!(log.lines().count(), 4);
        assert!(log.contains("task_id=00000000-0000-0000-0000-000000000001"));
        assert!(log.contains("task_type=create_account task_version=1 attempt=2"));
        for reason in ["permanent failure", "retry limit reached", "worker lease expired"] {
            assert!(log.contains(reason));
        }
        assert!(log.contains("task_type=unknown"));
        assert!(!log.contains("secret"));
        assert!(!log.contains("alice@example.com"));
        assert!(!log.contains("token"));
    }
}
