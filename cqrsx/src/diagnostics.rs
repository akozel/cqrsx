//! Fixed diagnostic context, independent of error Display/Debug and payloads.
use crate::projection::processor::{Operation, observer::FailureCategory};

pub(crate) fn safe_type(value: &str) -> &str {
    if !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        value
    } else {
        "unknown"
    }
}

#[cfg(feature = "postgres")]
pub(crate) fn sqlx_kind(error: &sqlx::Error) -> &'static str {
    match error {
        sqlx::Error::Io(_) => "io",
        sqlx::Error::Tls(_) => "tls",
        sqlx::Error::Database(_) => "database",
        sqlx::Error::PoolTimedOut => "pool_timeout",
        sqlx::Error::PoolClosed => "pool_closed",
        sqlx::Error::Decode(_) | sqlx::Error::ColumnDecode { .. } => "decode",
        sqlx::Error::Configuration(_) => "configuration",
        _ => "other",
    }
}

#[cfg(feature = "postgres")]
pub(crate) fn sqlstate(error: &sqlx::Error) -> Option<std::borrow::Cow<'_, str>> {
    let code = error.as_database_error()?.code()?;
    (code.len() == 5 && code.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())).then_some(code)
}

#[cfg(feature = "postgres")]
pub(crate) fn log_sqlx_failure(operation: &'static str, error: &sqlx::Error) {
    tracing::error!(component = "postgres", operation, category = sqlx_kind(error), sqlstate = ?sqlstate(error), "storage operation failed");
}

/// A fixed word for a MongoDB driver error.
#[cfg(feature = "mongodb")]
pub(crate) fn mongodb_kind(error: &::mongodb::error::Error) -> &'static str {
    use ::mongodb::error::ErrorKind;
    match error.kind.as_ref() {
        ErrorKind::Io(_) => "io",
        ErrorKind::ServerSelection { .. } => "server_selection",
        ErrorKind::Command(_) => "command",
        ErrorKind::Write(_) => "write",
        ErrorKind::BsonDeserialization(_) | ErrorKind::BsonSerialization(_) => "bson",
        _ => "other",
    }
}

#[cfg(feature = "mongodb")]
pub(crate) fn log_mongodb_failure(operation: &'static str, error: &::mongodb::error::Error) {
    tracing::error!(component = "mongodb", operation, category = mongodb_kind(error), code = ?crate::mongodb::server_code(error), "storage operation failed");
}

pub(crate) fn log_command_failure(aggregate_type: &str, category: &'static str, source: &(dyn std::error::Error + 'static)) {
    // These details are inspected before CommandGatewayError intentionally discards
    // technical sources. They never change the outward error or retry policy.
    #[cfg(feature = "postgres")]
    if let Some(error) = find_sqlx(source) {
        tracing::error!(component = "command", operation = "execute", aggregate_type = safe_type(aggregate_type), category,
            storage_category = sqlx_kind(error), sqlstate = ?sqlstate(error), "command execution failed");
        return;
    }
    #[cfg(not(feature = "postgres"))]
    let _ = source;
    tracing::error!(
        component = "command",
        operation = "execute",
        aggregate_type = safe_type(aggregate_type),
        category,
        "command execution failed"
    );
}

/// The first SQLx error in `source`'s chain: the only technical detail safe to log.
#[cfg(feature = "postgres")]
fn find_sqlx<'e>(source: &'e (dyn std::error::Error + 'static)) -> Option<&'e sqlx::Error> {
    let mut current = Some(source);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<sqlx::Error>() {
            return Some(error);
        }
        current = error.source();
    }
    None
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

/// A command for an aggregate type the engine does not know: a wiring fault, logged without the id.
pub(crate) fn log_unregistered_aggregate(aggregate_type: &str) {
    tracing::error!(
        component = "engine",
        operation = "execute",
        aggregate_type = safe_type(aggregate_type),
        "no module registers the aggregate type of the command"
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
    #[cfg(feature = "postgres")]
    if let Some(error) = find_sqlx(source) {
        tracing::error!(component = "projection", operation, projection = safe_type(projection),
            aggregate_type = safe_type(aggregate_type), jobs, storage_category = sqlx_kind(error),
            sqlstate = ?sqlstate(error), "projection batch failed");
        return;
    }
    #[cfg(not(feature = "postgres"))]
    let _ = source;
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

/// A thread panicked while it held the mutex `lock`; the crate clears the poison and goes on (see `locking`). A bug, hence
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
pub(crate) fn log_projection_event(projection: &str, aggregate_type: &str, event: &'static str) {
    tracing::info!(
        component = "projection",
        projection = safe_type(projection),
        aggregate_type = safe_type(aggregate_type),
        event,
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
    use std::{
        future::Future,
        io,
        sync::{Arc, Mutex},
    };
    use tracing::instrument::WithSubscriber;

    #[derive(Clone, Default)]
    struct Writer(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Writer {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    pub(crate) async fn capture<T>(work: impl Future<Output = T>) -> (T, String) {
        capture_at(tracing::Level::ERROR, work).await
    }

    pub(crate) async fn capture_at<T>(level: tracing::Level, work: impl Future<Output = T>) -> (T, String) {
        let writer = Writer::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_writer(writer.clone())
                .with_max_level(level)
                .with_ansi(false)
                .without_time()
                .finish(),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            // Prime shared callsites that parallel tests may register without a subscriber.
            super::log_command_failure("prime", "unexpected", &io::Error::other("prime"));
            super::log_lock_refused("prime", crate::command::locks::LockRefused::TimedOut);
            super::log_missing_executor("prime");
            super::log_projection_batch_failure("prime", "prime", "prime", 1, &io::Error::other("prime"));
            super::log_projection_batch_panic("prime", "prime", "prime", 1);
            super::log_projection_event("prime", "prime", "prime");
            super::log_poisoned_lock("prime");
            super::log_broken_invariant("prime", "prime");
            super::log_projection_failure("prime", "prime", super::Operation::Page, super::FailureCategory::Unknown, 1);
            #[cfg(feature = "postgres")]
            {
                super::log_command_failure("prime", "unexpected", &sqlx::Error::RowNotFound);
                super::log_projection_batch_failure("prime", "prime", "prime", 1, &sqlx::Error::RowNotFound);
                super::log_sqlx_failure("prime", &sqlx::Error::RowNotFound);
            }
            #[cfg(feature = "mongodb")]
            super::log_mongodb_failure("prime", &::mongodb::error::Error::custom("prime"));
            tracing::callsite::rebuild_interest_cache();
            writer.0.lock().unwrap().clear();
        });
        let result = work.with_subscriber(dispatch).await;
        let text = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        (result, text)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::Write;

        #[test]
        fn writer_clones_append_to_shared_buffer_and_flush_preserves_bytes() {
            let mut writer = Writer::default();
            let mut clone = writer.clone();
            writer.write_all(b"first event\n").unwrap();
            clone.write_all(b"second event\n").unwrap();
            clone.flush().unwrap();
            assert_eq!(*writer.0.lock().unwrap(), b"first event\nsecond event\n");
        }

        #[tokio::test]
        async fn capture_keeps_subscriber_across_await_and_filters_by_level() {
            let (result, log) = capture(async {
                tracing::info!("filtered info event");
                tokio::task::yield_now().await;
                tracing::error!("captured error event");
                42
            })
            .await;
            assert_eq!(result, 42);
            assert!(log.contains("captured error event"));
            assert!(!log.contains("filtered info event"));
            assert!(!log.contains("prime"));

            let (_, log) = capture_at(tracing::Level::INFO, async {
                tokio::task::yield_now().await;
                tracing::info!("captured info event");
            })
            .await;
            assert!(log.contains("captured info event"));
        }
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

    #[cfg(feature = "postgres")]
    #[derive(Debug, thiserror::Error)]
    #[error("private_database_message_payload_query_constraint")]
    struct DatabaseFault {
        code: Option<&'static str>,
    }

    #[cfg(feature = "postgres")]
    impl sqlx::error::DatabaseError for DatabaseFault {
        fn message(&self) -> &str {
            "private_database_message"
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            self.code.map(Into::into)
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn sqlstate_validation_rejects_malformed_codes_without_logging_them() {
        for code in ["42P01", "23505", "00000", "ABCDE", "12345"] {
            let error = sqlx::Error::Database(Box::new(DatabaseFault { code: Some(code) }));
            assert_eq!(sqlstate(&error).as_deref(), Some(code));
            let (_, log) = test_support::capture(async { log_sqlx_failure("connect", &error) }).await;
            assert!(log.contains(&format!("sqlstate=Some(\"{code}\")")));
            assert!(!log.contains("private_"));
        }

        for code in [
            None,
            Some(""),
            Some("1234"),
            Some("123456"),
            Some("42p01"),
            Some("42_01"),
            Some("42\n01"),
            Some("42\u{0}01"),
            Some("éABC"),
            Some("１２３４５"),
        ] {
            let error = sqlx::Error::Database(Box::new(DatabaseFault { code }));
            assert_eq!(sqlstate(&error), None);
            let (_, log) = test_support::capture(async {
                log_sqlx_failure("settle", &error);
                log_command_failure("Account", "connection", &error);
            })
            .await;
            assert_eq!(log.lines().count(), 2);
            for line in log.lines() {
                assert!(line.contains("sqlstate=None"));
                assert!(!line.contains("private_"));
            }
        }
        assert_eq!(sqlstate(&sqlx::Error::PoolClosed), None);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn command_diagnostics_find_nested_sqlx_source_and_emit_one_safe_event() {
        #[derive(Debug, thiserror::Error)]
        #[error("private_wrapper")]
        struct Wrapper(#[source] Box<dyn std::error::Error + Send + Sync>);

        let source = Wrapper(Box::new(Wrapper(Box::new(sqlx::Error::Database(Box::new(DatabaseFault {
            code: Some("23505"),
        }))))));
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

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn projection_batch_diagnostics_log_one_safe_event_per_batch_with_the_storage_category() {
        let source = crate::journal::PersistenceError::UnknownError(Box::new(sqlx::Error::PoolTimedOut));
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

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn logging_preserves_database_error_identity_and_downcasting() {
        let fault: Box<dyn sqlx::error::DatabaseError> = Box::new(DatabaseFault { code: Some("42P01") });
        let identity = std::ptr::from_ref(fault.downcast_ref::<DatabaseFault>());
        let error = sqlx::Error::Database(fault);
        let (_, log) = test_support::capture(async {
            log_sqlx_failure("claim", &error);
            log_command_failure("Account", "deserialization", &error);
        })
        .await;
        assert!(!log.contains("private_"));
        let sqlx::Error::Database(mut fault) = error else {
            panic!("database error must be preserved")
        };
        assert_eq!(fault.message(), "private_database_message");
        assert_eq!(fault.kind(), sqlx::error::ErrorKind::Other);
        assert_eq!(
            std::ptr::from_ref(fault.as_error_mut().downcast_mut::<DatabaseFault>().unwrap()),
            identity
        );
        let fault = fault.into_error().downcast::<DatabaseFault>().unwrap();
        assert_eq!(std::ptr::from_ref(fault.as_ref()), identity);
        assert_eq!(fault.code, Some("42P01"));
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn sqlx_classification_uses_fixed_categories_and_valid_sqlstate_only() {
        use std::io;
        let errors = [
            (sqlx::Error::Io(io::Error::other("private_io")), "io"),
            (sqlx::Error::Tls(Box::new(io::Error::other("private_tls"))), "tls"),
            (sqlx::Error::PoolTimedOut, "pool_timeout"),
            (sqlx::Error::PoolClosed, "pool_closed"),
            (sqlx::Error::Decode(Box::new(io::Error::other("private_decode"))), "decode"),
            (
                sqlx::Error::ColumnDecode {
                    index: "private_column".into(),
                    source: Box::new(io::Error::other("private_decode")),
                },
                "decode",
            ),
            (
                sqlx::Error::Configuration(Box::new(io::Error::other("private_url"))),
                "configuration",
            ),
            (sqlx::Error::Protocol("private_protocol".into()), "other"),
            (
                sqlx::Error::Database(Box::new(DatabaseFault { code: Some("42P01") })),
                "database",
            ),
        ];
        let (_, log) = test_support::capture(async {
            for (error, category) in &errors {
                assert_eq!(sqlx_kind(error), *category);
                log_sqlx_failure("claim", error);
                log_command_failure("Invoice", "unexpected", error);
            }
            for code in [None, Some("private_code"), Some("42p01"), Some("1234"), Some("123456")] {
                let error = sqlx::Error::Database(Box::new(DatabaseFault { code }));
                assert!(sqlstate(&error).is_none());
                log_sqlx_failure("settle", &error);
            }
            #[derive(Debug, thiserror::Error)]
            #[error("private_wrapper")]
            struct Wrapper(#[source] sqlx::Error);
            log_command_failure("Invoice", "unexpected", &Wrapper(sqlx::Error::PoolClosed));
        })
        .await;
        assert!(log.contains("42P01"));
        assert!(log.contains("storage_category=\"pool_closed\""));
        assert!(!log.contains("private_"));
        for (_, category) in errors {
            assert!(log.contains(category));
        }
    }
}

#[cfg(test)]
mod projection_tests {
    use super::{FailureCategory, Operation, log_projection_event, log_projection_failure, test_support};

    #[tokio::test]
    async fn projection_diagnostics_name_the_projection_safely_and_carry_fixed_fields_only() {
        let (_, log) = test_support::capture_at(tracing::Level::INFO, async {
            log_projection_event("concert_halls", "Concert", "lease acquired");
            log_projection_event("private\nmail@example.com", "Concert", "rebuild finished");
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
                TaskFailureCause::PermanentRejection,
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

#[cfg(all(test, feature = "mongodb"))]
mod mongodb_tests {
    use super::{log_mongodb_failure, mongodb_kind, test_support};
    use crate::mongodb::test_support::*;

    #[tokio::test]
    async fn mongodb_failures_log_a_fixed_category_and_the_server_code_without_the_message() {
        let errors = [io(), command(11000), write(121), write_concern(64), bson(), custom()];
        assert_eq!(
            errors.each_ref().map(mongodb_kind),
            ["io", "command", "write", "write", "bson", "other"]
        );
        let (_, log) = test_support::capture(async {
            for error in &errors {
                log_mongodb_failure("claim", error);
            }
        })
        .await;
        assert_eq!(log.matches("storage operation failed").count(), errors.len(), "{log}");
        assert_eq!(log.matches("component=\"mongodb\"").count(), errors.len(), "{log}");
        assert_eq!(log.matches("operation=\"claim\"").count(), errors.len(), "{log}");
        for (category, code) in [
            ("command", "Some(11000)"),
            ("write", "Some(121)"),
            ("write", "Some(64)"),
            ("io", "None"),
        ] {
            assert!(
                log.contains(&format!("category=\"{category}\" code={code}")),
                "{category} {code}: {log}"
            );
        }
        assert!(!log.contains("private_"), "{log}");
    }
}
