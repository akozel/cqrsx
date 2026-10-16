//! PostgreSQL task ownership with transaction-local statement timeouts.
//! Each operation owns its complete transaction and preserves token/expiry fencing.
mod claim;
mod recovery;
mod settlement;

use crate::task::{
    format::TaskFormat,
    queue::{
        QueueRequestError, TaskProcessingError, TaskQueue,
        lease::LeasedTask,
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
    },
};
use sqlx::{PgPool, Postgres, Transaction};
use std::time::Duration;
use thiserror::Error;

pub struct PostgresTaskQueue {
    pool: PgPool,
    statement_timeout: Duration,
}

impl PostgresTaskQueue {
    pub fn new(pool: PgPool) -> Self {
        Self::with_config(pool, PostgresTaskQueueConfig::default())
    }

    pub fn with_config(pool: PgPool, config: PostgresTaskQueueConfig) -> Self {
        Self {
            pool,
            statement_timeout: config.statement_timeout(),
        }
    }

    async fn begin_queue_transaction(&self, operation: &'static str) -> Result<Transaction<'_, Postgres>, TaskProcessingError> {
        let mut tx = self.pool.begin().await.map_err(|error| storage_error(error, operation))?;
        // Transaction-local settings never change the shared pool's session defaults.
        sqlx::query("SELECT set_config('statement_timeout', $1, true)")
            .bind(self.statement_timeout.as_millis().to_string())
            .execute(&mut *tx)
            .await
            .map_err(|error| storage_error(error, operation))?;
        Ok(tx)
    }
}

fn storage_error(error: sqlx::Error, operation: &'static str) -> TaskProcessingError {
    crate::diagnostics::log_sqlx_failure(operation, &error);
    TaskProcessingError::Unavailable
}

fn milliseconds(duration: Duration, invalid: QueueRequestError) -> Result<f64, TaskProcessingError> {
    i64::try_from(duration.as_millis())
        .map(|n| n as f64)
        .map_err(|_| TaskProcessingError::from(invalid))
}

impl TaskQueue for PostgresTaskQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        claim::claim(self, format, owner, limit, lease_duration).await
    }
    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        settlement::settle(self, task, request).await
    }
    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        recovery::recover_expired(self, limit).await
    }
}

const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 4_000;
/// Transaction-local PostgreSQL statement cap, not an end-to-end I/O budget.
#[derive(Clone, Debug)]
pub struct PostgresTaskQueueConfig {
    statement_timeout: Duration,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PostgresTaskQueueConfigError {
    #[error("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS must be an unsigned integer")]
    InvalidEnvironmentValue,
    #[error("statement timeout must be 1..=2147483647 milliseconds")]
    InvalidStatementTimeout,
}
impl Default for PostgresTaskQueueConfig {
    fn default() -> Self {
        Self::new(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS)).expect("valid default statement timeout")
    }
}
impl PostgresTaskQueueConfig {
    pub fn new(statement_timeout: Duration) -> Result<Self, PostgresTaskQueueConfigError> {
        if statement_timeout.as_millis() == 0 || statement_timeout.as_millis() > i32::MAX as u128 {
            return Err(PostgresTaskQueueConfigError::InvalidStatementTimeout);
        }
        Ok(Self { statement_timeout })
    }
    pub fn statement_timeout(&self) -> Duration {
        self.statement_timeout
    }
    pub fn from_env() -> Result<Self, PostgresTaskQueueConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, PostgresTaskQueueConfigError> {
        let millis = crate::env::read_u64(
            &mut lookup,
            "OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS",
            DEFAULT_STATEMENT_TIMEOUT_MS,
        )
        .map_err(|_| PostgresTaskQueueConfigError::InvalidEnvironmentValue)?;
        Self::new(Duration::from_millis(millis))
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[tokio::test]
    async fn unavailable_queue_logs_operation_and_cause_before_erasing_source() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        pool.close().await;
        let queue = PostgresTaskQueue::new(pool);
        let task = crate::task::test_support::claimed();
        let (_, log) = crate::diagnostics::test_support::capture(async {
            assert_eq!(
                queue
                    .claim(task.attempt().format(), "private_owner", 1, Duration::from_secs(60))
                    .await
                    .unwrap_err(),
                TaskProcessingError::Unavailable
            );
            assert_eq!(
                queue.settle(&task, SettlementRequest::Completed).await.unwrap_err(),
                TaskProcessingError::Unavailable
            );
            assert_eq!(queue.recover_expired(1).await.unwrap_err(), TaskProcessingError::Unavailable);
        })
        .await;
        assert_eq!(log.matches("storage operation failed").count(), 3);
        for operation in ["claim", "settle", "recover_expired"] {
            assert!(log.contains(operation));
        }
        assert!(log.contains("pool_closed"));
        assert!(!log.contains("private_"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_validates_settings_in_an_isolated_process() {
        use crate::env::test_support;
        if let Some(scenario) = test_support::scenario() {
            match scenario.as_str() {
                "defaults" => assert_eq!(
                    PostgresTaskQueueConfig::from_env().unwrap().statement_timeout(),
                    Duration::from_secs(4)
                ),
                "override" => assert_eq!(
                    PostgresTaskQueueConfig::from_env().unwrap().statement_timeout(),
                    Duration::from_millis(125)
                ),
                "invalid" => assert_eq!(
                    PostgresTaskQueueConfig::from_env().unwrap_err(),
                    PostgresTaskQueueConfigError::InvalidEnvironmentValue
                ),
                "zero" | "overflow" => assert_eq!(
                    PostgresTaskQueueConfig::from_env().unwrap_err(),
                    PostgresTaskQueueConfigError::InvalidStatementTimeout
                ),
                _ => panic!("unknown test scenario"),
            }
            return;
        }
        for (scenario, value) in [
            ("defaults", None),
            ("override", Some("125")),
            ("invalid", Some("invalid")),
            ("zero", Some("0")),
            ("overflow", Some("2147483648")),
        ] {
            test_support::isolated(
                concat!(module_path!(), "::from_env_validates_settings_in_an_isolated_process"),
                scenario,
                |child| {
                    if let Some(value) = value {
                        child.env("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS", value);
                    }
                },
            );
        }
    }

    #[test]
    fn statement_timeout_config_validates_defaults_overrides_and_server_range() {
        assert_eq!(
            PostgresTaskQueueConfig::from_lookup(|_| None).unwrap().statement_timeout(),
            Duration::from_secs(4)
        );
        assert_eq!(
            PostgresTaskQueueConfig::from_lookup(|_| Some("125".into()))
                .unwrap()
                .statement_timeout(),
            Duration::from_millis(125)
        );
        assert!(PostgresTaskQueueConfig::from_lookup(|_| Some(i32::MAX.to_string())).is_ok());
        for value in ["0", "invalid", "-1", "2147483648", "18446744073709551616"] {
            assert!(PostgresTaskQueueConfig::from_lookup(|_| Some(value.into())).is_err());
        }
        for statement_timeout in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
            assert!(PostgresTaskQueueConfig::new(statement_timeout).is_err());
        }
    }
}
