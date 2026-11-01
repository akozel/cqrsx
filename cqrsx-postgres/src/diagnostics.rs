//! Fixed diagnostic context for the failures of PostgreSQL, independent of the message of the driver and of payloads.
use cqrsx::diagnostics::StorageFailure;
use std::borrow::Cow;

/// A fixed word for a driver error.
fn sqlx_category(error: &sqlx::Error) -> &'static str {
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

/// The SQLSTATE of a database error, when it is five uppercase letters or digits: nothing else is logged.
fn sqlstate(error: &sqlx::Error) -> Option<Cow<'_, str>> {
    let code = error.as_database_error()?.code()?;
    (code.len() == 5 && code.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())).then_some(code)
}

/// Logs a failed storage operation of the adapter itself, before its error is erased.
pub(crate) fn log_sqlx_failure(operation: &'static str, error: &sqlx::Error) {
    tracing::error!(component = "postgres", operation, storage_category = sqlx_category(error), sqlstate = ?sqlstate(error), "storage operation failed");
}

/// The error of the driver with its category and SQLSTATE, for the logs of the core (`cqrsx::diagnostics`). Every place
/// where a driver error becomes an error of the core goes through it.
pub(crate) fn storage_failure(error: sqlx::Error) -> StorageFailure {
    StorageFailure::new(sqlx_category(&error), sqlstate(&error).map(Cow::into_owned), error)
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::future::Future;

    /// The output of `work` and the `ERROR` lines it logged.
    pub(crate) async fn capture<T>(work: impl Future<Output = T>) -> (T, String) {
        cqrsx_fixtures::logs::capture(|| super::log_sqlx_failure("prime", &sqlx::Error::RowNotFound), work).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[derive(Debug, thiserror::Error)]
    #[error("private_database_message_payload_query_constraint")]
    struct DatabaseFault {
        code: Option<&'static str>,
    }

    impl sqlx::error::DatabaseError for DatabaseFault {
        fn message(&self) -> &str {
            "private_database_message"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
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

    #[tokio::test]
    async fn sqlstate_validation_rejects_malformed_codes_without_logging_them() {
        for code in ["42P01", "23505", "00000", "ABCDE", "12345"] {
            let error = sqlx::Error::Database(Box::new(DatabaseFault { code: Some(code) }));
            assert_eq!(sqlstate(&error).as_deref(), Some(code));
            let (_, log) = test_support::capture(async { log_sqlx_failure("connect", &error) }).await;
            assert!(log.contains(&format!("sqlstate=Some(\"{code}\")")));
            assert!(!log.contains("private_"));
            assert_eq!(storage_failure(error).code(), Some(code));
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
            let (_, log) = test_support::capture(async { log_sqlx_failure("settle", &error) }).await;
            assert_eq!(log.lines().count(), 1);
            assert!(log.contains("sqlstate=None"));
            assert!(!log.contains("private_"));
            assert_eq!(storage_failure(error).code(), None);
        }
        assert_eq!(sqlstate(&sqlx::Error::PoolClosed), None);
    }

    #[tokio::test]
    async fn logging_preserves_database_error_identity_and_downcasting() {
        let fault: Box<dyn sqlx::error::DatabaseError> = Box::new(DatabaseFault { code: Some("42P01") });
        let identity = std::ptr::from_ref(fault.downcast_ref::<DatabaseFault>());
        let error = sqlx::Error::Database(fault);
        let (_, log) = test_support::capture(async { log_sqlx_failure("claim", &error) }).await;
        assert!(!log.contains("private_"));
        let failure = storage_failure(error);
        let Some(sqlx::Error::Database(fault)) = failure.cause().downcast_ref::<sqlx::Error>() else {
            panic!("the driver's database error must be preserved")
        };
        assert_eq!(fault.message(), "private_database_message");
        assert_eq!(fault.kind(), sqlx::error::ErrorKind::Other);
        assert_eq!(std::ptr::from_ref(fault.downcast_ref::<DatabaseFault>()), identity);
        assert_eq!(fault.code().as_deref(), Some("42P01"));
    }

    #[tokio::test]
    async fn sqlx_classification_uses_fixed_categories_and_valid_sqlstate_only() {
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
                assert_eq!(sqlx_category(error), *category);
                log_sqlx_failure("claim", error);
            }
            for code in [None, Some("private_code"), Some("42p01"), Some("1234"), Some("123456")] {
                let error = sqlx::Error::Database(Box::new(DatabaseFault { code }));
                assert!(sqlstate(&error).is_none());
                log_sqlx_failure("settle", &error);
            }
        })
        .await;
        assert!(log.contains("42P01"));
        assert!(!log.contains("private_"));
        for (_, category) in &errors {
            assert!(log.contains(category));
        }
    }

    #[test]
    fn a_driver_error_becomes_a_storage_failure_with_its_category_and_code_and_the_same_text() {
        for (error, category, code) in [
            (sqlx::Error::PoolClosed, "pool_closed", None),
            (sqlx::Error::PoolTimedOut, "pool_timeout", None),
            (sqlx::Error::Io(io::Error::other("private_io")), "io", None),
            (
                sqlx::Error::Database(Box::new(DatabaseFault { code: Some("23505") })),
                "database",
                Some("23505"),
            ),
        ] {
            let text = (error.to_string(), format!("{error:?}"));
            let failure = storage_failure(error);
            assert_eq!((failure.category(), failure.code()), (category, code));
            assert_eq!((failure.to_string(), format!("{failure:?}")), text);
            assert!(failure.cause().is::<sqlx::Error>());
        }
    }
}
