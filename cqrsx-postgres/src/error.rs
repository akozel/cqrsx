// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx-postgres/licenses/.
use crate::diagnostics::storage_failure;
use cqrsx::journal::PersistenceError;
use sqlx::Error;

#[derive(Debug, thiserror::Error)]
pub enum PostgresRepositoryError {
    #[error("optimistic lock error")]
    Conflict,
    #[error("{0}")]
    Connection(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    Deserialization(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    Unexpected(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<sqlx::Error> for PostgresRepositoryError {
    fn from(err: sqlx::Error) -> Self {
        match &err {
            Error::Io(_) | Error::Tls(_) => Self::Connection(Box::new(storage_failure(err))),
            _ => Self::Unexpected(Box::new(storage_failure(err))),
        }
    }
}

impl From<serde_json::Error> for PostgresRepositoryError {
    fn from(err: serde_json::Error) -> Self {
        match err.classify() {
            serde_json::error::Category::Data | serde_json::error::Category::Syntax => Self::Deserialization(Box::new(err)),
            serde_json::error::Category::Io | serde_json::error::Category::Eof => Self::Unexpected(Box::new(err)),
        }
    }
}

impl From<PostgresRepositoryError> for PersistenceError {
    fn from(err: PostgresRepositoryError) -> Self {
        match err {
            PostgresRepositoryError::Conflict => Self::Conflict,
            PostgresRepositoryError::Connection(error) => Self::Connection(error),
            PostgresRepositoryError::Deserialization(error) => Self::Deserialization(error),
            PostgresRepositoryError::Unexpected(error) => Self::Unexpected(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::command::AttemptError;
    use std::{convert::Infallible, io};
    #[test]
    fn postgres_categories_retain_causes_through_persistence_to_the_command_error() {
        for category in 0..4 {
            let make = || {
                let cause = Box::new(io::Error::other("original"));
                match category {
                    0 => PostgresRepositoryError::Conflict,
                    1 => PostgresRepositoryError::Connection(cause),
                    2 => PostgresRepositoryError::Deserialization(cause),
                    _ => PostgresRepositoryError::Unexpected(cause),
                }
            };
            let aggregate = AttemptError::<Infallible>::from(PersistenceError::from(make()));
            if category != 0 {
                use std::error::Error as _;
                assert_eq!(
                    aggregate.source().unwrap().downcast_ref::<io::Error>().unwrap().to_string(),
                    "original"
                );
            }
            match (category, aggregate) {
                (0, AttemptError::Conflict) => {}
                (1, AttemptError::Connection(e)) | (2, AttemptError::Deserialization(e)) | (3, AttemptError::Unexpected(e)) => {
                    assert_eq!(e.downcast_ref::<io::Error>().unwrap().to_string(), "original")
                }
                _ => panic!("incorrect aggregate mapping"),
            }
            let persistence = PersistenceError::from(make());
            if category != 0 {
                assert_eq!(
                    std::error::Error::source(&make())
                        .unwrap()
                        .downcast_ref::<io::Error>()
                        .unwrap()
                        .to_string(),
                    "original"
                );
                assert_eq!(
                    std::error::Error::source(&persistence)
                        .unwrap()
                        .downcast_ref::<io::Error>()
                        .unwrap()
                        .to_string(),
                    "original"
                );
            }
            match (category, persistence) {
                (0, PersistenceError::Conflict) => {}
                (1, PersistenceError::Connection(e))
                | (2, PersistenceError::Deserialization(e))
                | (3, PersistenceError::Unexpected(e)) => {
                    assert_eq!(e.downcast_ref::<io::Error>().unwrap().to_string(), "original")
                }
                _ => panic!("incorrect persistence mapping"),
            }
        }
    }
    /// The driver's own error inside the `StorageFailure` that wraps it.
    fn driver_error<'e>(error: &'e (dyn std::error::Error + Send + Sync + 'static)) -> Option<&'e Error> {
        error
            .downcast_ref::<cqrsx::diagnostics::StorageFailure>()?
            .cause()
            .downcast_ref::<Error>()
    }

    #[test]
    fn technical_sqlx_failures_are_not_conflicts() {
        for error in [
            Error::Io(io::Error::other("io")),
            Error::Tls("tls".into()),
            Error::PoolClosed,
            Error::PoolTimedOut,
            Error::RowNotFound,
            Error::Protocol("protocol".into()),
        ] {
            let connection = matches!(error, Error::Io(_) | Error::Tls(_));
            let mapped = PostgresRepositoryError::from(error);
            match mapped {
                PostgresRepositoryError::Connection(e) if connection => assert!(driver_error(&*e).is_some()),
                PostgresRepositoryError::Unexpected(e) if !connection => assert!(driver_error(&*e).is_some()),
                _ => panic!("technical error became conflict"),
            }
        }
        let io_error = serde_json::Error::io(io::Error::other("reader failed"));
        assert!(
            matches!(PostgresRepositoryError::from(io_error),PostgresRepositoryError::Unexpected(source) if source.is::<serde_json::Error>())
        );
        for text in ["false", "?", ""] {
            let error = serde_json::from_str::<u32>(text).unwrap_err();
            let eof = error.is_eof();
            let mapped = PostgresRepositoryError::from(error);
            assert!(
                matches!(mapped, PostgresRepositoryError::Unexpected(_) if eof)
                    || matches!(mapped, PostgresRepositoryError::Deserialization(_) if !eof)
            );
        }
    }
}
