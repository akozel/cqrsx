// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{AggregateError, store::PersistenceError};
use sqlx::Error;

#[derive(Debug, thiserror::Error)]
pub enum PostgresRepositoryError {
    #[error("optimistic lock error")]
    OptimisticLock,
    #[error("{0}")]
    ConnectionError(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    DeserializationError(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    UnknownError(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<sqlx::Error> for PostgresRepositoryError {
    fn from(err: sqlx::Error) -> Self {
        match &err {
            Error::Io(_) | Error::Tls(_) => Self::ConnectionError(Box::new(err)),
            _ => Self::UnknownError(Box::new(err)),
        }
    }
}

impl<T: std::error::Error> From<PostgresRepositoryError> for AggregateError<T> {
    fn from(err: PostgresRepositoryError) -> Self {
        match err {
            PostgresRepositoryError::OptimisticLock => Self::AggregateConflict,
            PostgresRepositoryError::ConnectionError(error) => Self::DatabaseConnectionError(error),
            PostgresRepositoryError::DeserializationError(error) => Self::DeserializationError(error),
            PostgresRepositoryError::UnknownError(error) => Self::UnexpectedError(error),
        }
    }
}

impl From<serde_json::Error> for PostgresRepositoryError {
    fn from(err: serde_json::Error) -> Self {
        match err.classify() {
            serde_json::error::Category::Data | serde_json::error::Category::Syntax => Self::DeserializationError(Box::new(err)),
            serde_json::error::Category::Io | serde_json::error::Category::Eof => Self::UnknownError(Box::new(err)),
        }
    }
}

impl From<PostgresRepositoryError> for PersistenceError {
    fn from(err: PostgresRepositoryError) -> Self {
        match err {
            PostgresRepositoryError::OptimisticLock => Self::OptimisticLockError,
            PostgresRepositoryError::ConnectionError(error) => Self::ConnectionError(error),
            PostgresRepositoryError::DeserializationError(error) => Self::DeserializationError(error),
            PostgresRepositoryError::UnknownError(error) => Self::UnknownError(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{convert::Infallible, io};
    #[test]
    fn postgres_categories_retain_causes_across_both_boundaries() {
        for category in 0..4 {
            let make = || {
                let cause = Box::new(io::Error::other("original"));
                match category {
                    0 => PostgresRepositoryError::OptimisticLock,
                    1 => PostgresRepositoryError::ConnectionError(cause),
                    2 => PostgresRepositoryError::DeserializationError(cause),
                    _ => PostgresRepositoryError::UnknownError(cause),
                }
            };
            for aggregate in [
                AggregateError::<Infallible>::from(make()),
                AggregateError::<Infallible>::from(PersistenceError::from(make())),
            ] {
                if category != 0 {
                    use std::error::Error as _;
                    assert_eq!(
                        aggregate.source().unwrap().downcast_ref::<io::Error>().unwrap().to_string(),
                        "original"
                    );
                }
                match (category, aggregate) {
                    (0, AggregateError::AggregateConflict) => {}
                    (1, AggregateError::DatabaseConnectionError(e))
                    | (2, AggregateError::DeserializationError(e))
                    | (3, AggregateError::UnexpectedError(e)) => {
                        assert_eq!(e.downcast_ref::<io::Error>().unwrap().to_string(), "original")
                    }
                    _ => panic!("incorrect aggregate mapping"),
                }
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
                (0, PersistenceError::OptimisticLockError) => {}
                (1, PersistenceError::ConnectionError(e))
                | (2, PersistenceError::DeserializationError(e))
                | (3, PersistenceError::UnknownError(e)) => {
                    assert_eq!(e.downcast_ref::<io::Error>().unwrap().to_string(), "original")
                }
                _ => panic!("incorrect persistence mapping"),
            }
        }
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
                PostgresRepositoryError::ConnectionError(e) if connection => assert!(e.is::<Error>()),
                PostgresRepositoryError::UnknownError(e) if !connection => assert!(e.is::<Error>()),
                _ => panic!("technical error became conflict"),
            }
        }
        let io_error = serde_json::Error::io(io::Error::other("reader failed"));
        assert!(
            matches!(PostgresRepositoryError::from(io_error),PostgresRepositoryError::UnknownError(source) if source.is::<serde_json::Error>())
        );
        for text in ["false", "?", ""] {
            let error = serde_json::from_str::<u32>(text).unwrap_err();
            let eof = error.is_eof();
            let mapped = PostgresRepositoryError::from(error);
            assert!(
                matches!(mapped, PostgresRepositoryError::UnknownError(_) if eof)
                    || matches!(mapped, PostgresRepositoryError::DeserializationError(_) if !eof)
            );
        }
    }
}
