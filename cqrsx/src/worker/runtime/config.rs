//! The task runtime's configuration: the storage budget and the worker limits, checked together.
use crate::worker::{
    queue::storage_config::{TaskStorageConfig, TaskStorageConfigError},
    runtime::worker_pool_config::{TaskWorkerPoolConfig, TaskWorkerPoolConfigError},
};
use thiserror::Error;

/// Checked storage and worker budgets, independent of the queue technology.
/// Defaults do not read env; PostgreSQL statement timeout remains adapter config.
#[derive(Clone, Debug)]
pub struct TaskRuntimeConfig {
    storage: TaskStorageConfig,
    worker_pool: TaskWorkerPoolConfig,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskRuntimeConfigError {
    #[error("{0}")]
    Storage(#[from] TaskStorageConfigError),
    #[error("{0}")]
    WorkerPool(#[from] TaskWorkerPoolConfigError),
}

impl TaskRuntimeConfig {
    /// `storage` and `worker` if they fit together: the lease covers a claim, the queued waves, the handler and the
    /// settlement ([`validate_for_storage_timeout`](TaskWorkerPoolConfig::validate_for_storage_timeout)).
    pub fn try_new(storage: TaskStorageConfig, worker: TaskWorkerPoolConfig) -> Result<Self, TaskRuntimeConfigError> {
        worker.validate_for_storage_timeout(storage.storage_timeout())?;
        Ok(Self {
            storage,
            worker_pool: worker,
        })
    }

    pub fn storage(&self) -> &TaskStorageConfig {
        &self.storage
    }

    pub fn worker_pool(&self) -> &TaskWorkerPoolConfig {
        &self.worker_pool
    }

    pub(super) fn into_parts(self) -> (TaskStorageConfig, TaskWorkerPoolConfig) {
        (self.storage, self.worker_pool)
    }

    /// [`from_lookup`](Self::from_lookup) over the process environment.
    pub fn from_env() -> Result<Self, TaskRuntimeConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// The storage budget ([`TaskStorageConfig::from_lookup`], `OUTBOX_STORAGE_TIMEOUT_MS`) and the worker limits
    /// ([`TaskWorkerPoolConfig::from_lookup`], the other `OUTBOX_*` variables), checked together as
    /// [`try_new`](Self::try_new) checks them.
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, TaskRuntimeConfigError> {
        Self::try_new(
            TaskStorageConfig::from_lookup(&mut lookup)?,
            TaskWorkerPoolConfig::from_lookup(&mut lookup)?,
        )
    }
}

impl Default for TaskRuntimeConfig {
    /// The default storage and worker budgets, which fit together (a test checks them as [`try_new`](Self::try_new) does).
    fn default() -> Self {
        Self {
            storage: TaskStorageConfig::default(),
            worker_pool: TaskWorkerPoolConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn from_env_checks_the_combined_lease_budget_in_an_isolated_process() {
        use crate::env::test_support;
        if let Some(scenario) = test_support::scenario() {
            match scenario.as_str() {
                "defaults" => {
                    let config = TaskRuntimeConfig::from_env().unwrap();
                    assert_eq!(config.storage().storage_timeout(), Duration::from_secs(6));
                    assert_eq!(config.worker_pool().lease(), Duration::from_secs(60));
                }
                "override" => {
                    let config = TaskRuntimeConfig::from_env().unwrap();
                    assert_eq!(config.storage().storage_timeout(), Duration::from_millis(500));
                    assert_eq!(config.worker_pool().concurrency(), 2);
                    assert_eq!(config.worker_pool().buffer_capacity(), 3);
                    assert_eq!(config.worker_pool().poll_interval(), Duration::from_millis(25));
                    assert_eq!(config.worker_pool().handler_timeout(), Duration::from_secs(2));
                    assert_eq!(config.worker_pool().lease(), Duration::from_secs(30));
                }
                "invalid" => assert_eq!(
                    TaskRuntimeConfig::from_env().unwrap_err(),
                    TaskRuntimeConfigError::Storage(TaskStorageConfigError::NotAnInteger)
                ),
                "lease_too_short" => assert_eq!(
                    TaskRuntimeConfig::from_env().unwrap_err(),
                    TaskRuntimeConfigError::WorkerPool(TaskWorkerPoolConfigError::LeaseTooShort)
                ),
                _ => panic!("unknown test scenario"),
            }
            return;
        }
        for scenario in ["defaults", "override", "invalid", "lease_too_short"] {
            test_support::isolated(
                concat!(
                    module_path!(),
                    "::from_env_checks_the_combined_lease_budget_in_an_isolated_process"
                ),
                scenario,
                |child| match scenario {
                    "override" => {
                        child
                            .env("OUTBOX_STORAGE_TIMEOUT_MS", "500")
                            .env("OUTBOX_WORKER_CONCURRENCY", "2")
                            .env("OUTBOX_QUEUE_CAPACITY", "3")
                            .env("OUTBOX_POLL_INTERVAL_MS", "25")
                            .env("OUTBOX_HANDLER_TIMEOUT_SECS", "2")
                            .env("OUTBOX_LEASE_SECS", "30");
                    }
                    "invalid" => {
                        child.env("OUTBOX_STORAGE_TIMEOUT_MS", "invalid");
                    }
                    "lease_too_short" => {
                        child.env("OUTBOX_LEASE_SECS", "53");
                    }
                    _ => {}
                },
            );
        }
    }

    fn lookup(settings: &[(&str, &str)]) -> Result<TaskRuntimeConfig, TaskRuntimeConfigError> {
        TaskRuntimeConfig::from_lookup(|name| {
            settings
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        })
    }

    #[test]
    fn combined_defaults_and_independent_overrides_use_existing_settings_once() {
        let default = TaskRuntimeConfig::default();
        assert!(
            TaskRuntimeConfig::try_new(*default.storage(), default.worker_pool().clone()).is_ok(),
            "the defaults fit together"
        );
        assert_eq!(default.worker_pool().concurrency(), 128);
        assert_eq!(default.worker_pool().buffer_capacity(), 128);
        assert_eq!(default.worker_pool().poll_interval(), Duration::from_secs(1));
        assert_eq!(default.worker_pool().handler_timeout(), Duration::from_secs(15));
        assert_eq!(default.worker_pool().lease(), Duration::from_secs(60));
        assert_eq!(default.storage().storage_timeout(), Duration::from_secs(6));
        let mut reads = Vec::new();
        TaskRuntimeConfig::from_lookup(|name| {
            reads.push(name.to_owned());
            None
        })
        .unwrap();
        assert_eq!(reads.len(), 7);
        reads.sort_unstable();
        reads.dedup();
        assert_eq!(reads.len(), 7);
        let c = lookup(&[("OUTBOX_WORKER_CONCURRENCY", "256")]).unwrap();
        assert_eq!(c.worker_pool().buffer_capacity(), 128);
        assert_eq!(c.worker_pool().concurrency(), 256);
        let q = lookup(&[("OUTBOX_QUEUE_CAPACITY", "256"), ("OUTBOX_LEASE_SECS", "75")]).unwrap();
        assert_eq!(q.worker_pool().concurrency(), 128);
        assert_eq!(q.worker_pool().buffer_capacity(), 256);
        let storage = lookup(&[("OUTBOX_STORAGE_TIMEOUT_MS", "5000")]).unwrap();
        assert_eq!(storage.storage().storage_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn parsing_errors_preserve_the_configuration_boundary_and_source() {
        let error = lookup(&[("OUTBOX_STORAGE_TIMEOUT_MS", "invalid")]).unwrap_err();
        assert_eq!(error, TaskRuntimeConfigError::Storage(TaskStorageConfigError::NotAnInteger));
        assert!(std::error::Error::source(&error).is_some());
        let error = lookup(&[("OUTBOX_WORKER_CONCURRENCY", "invalid")]).unwrap_err();
        assert_eq!(
            error,
            TaskRuntimeConfigError::WorkerPool(TaskWorkerPoolConfigError::NotAnInteger("OUTBOX_WORKER_CONCURRENCY"))
        );
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(
            lookup(&[("OUTBOX_STORAGE_TIMEOUT_MS", "0")]).unwrap_err(),
            TaskRuntimeConfigError::Storage(TaskStorageConfigError::ZeroStorageTimeout)
        );
    }

    #[test]
    fn full_budget_is_checked_at_load_time_with_strict_wave_boundaries() {
        for settings in [vec![("OUTBOX_LEASE_SECS", "53")], vec![("OUTBOX_QUEUE_CAPACITY", "129")]] {
            assert_eq!(
                lookup(&settings).unwrap_err(),
                TaskRuntimeConfigError::WorkerPool(TaskWorkerPoolConfigError::LeaseTooShort)
            );
        }
        lookup(&[("OUTBOX_LEASE_SECS", "54")]).unwrap();
        lookup(&[("OUTBOX_QUEUE_CAPACITY", "129"), ("OUTBOX_LEASE_SECS", "75")]).unwrap();
        #[cfg(target_pointer_width = "64")]
        {
            let capacity = (usize::MAX >> 3).to_string();
            assert_eq!(
                lookup(&[("OUTBOX_WORKER_CONCURRENCY", "1"), ("OUTBOX_QUEUE_CAPACITY", &capacity)]).unwrap_err(),
                TaskRuntimeConfigError::WorkerPool(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
            );
        }
    }
}
