//! The time budget of one call to the task queue's storage.
use std::time::Duration;
use thiserror::Error;
const DEFAULT_STORAGE_TIMEOUT: Duration = Duration::from_millis(6_000);
/// Validated end-to-end storage budget: the runtime gives up on a claim, settlement or recovery after it
/// ([`TimedOut`](crate::worker::queue::TaskProcessingError::TimedOut)). Defaults do not read the environment.
#[derive(Clone, Copy, Debug)]
pub struct TaskStorageConfig {
    storage_timeout: Duration,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskStorageConfigError {
    #[error("OUTBOX_STORAGE_TIMEOUT_MS must be an unsigned integer")]
    NotAnInteger,
    #[error("storage timeout must be positive")]
    ZeroStorageTimeout,
    #[error("storage timeout is too long to represent")]
    StorageTimeoutTooLong,
}
impl Default for TaskStorageConfig {
    /// A storage timeout of six seconds.
    fn default() -> Self {
        Self {
            storage_timeout: DEFAULT_STORAGE_TIMEOUT,
        }
    }
}
impl TaskStorageConfig {
    pub fn try_new(storage_timeout: Duration) -> Result<Self, TaskStorageConfigError> {
        if storage_timeout.is_zero() {
            return Err(TaskStorageConfigError::ZeroStorageTimeout);
        }
        if storage_timeout.as_millis() > i64::MAX as u128 {
            return Err(TaskStorageConfigError::StorageTimeoutTooLong);
        }
        Ok(Self { storage_timeout })
    }
    pub fn storage_timeout(&self) -> Duration {
        self.storage_timeout
    }
    /// [`from_lookup`](Self::from_lookup) over the process environment.
    pub fn from_env() -> Result<Self, TaskStorageConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    /// The budget from `OUTBOX_STORAGE_TIMEOUT_MS` (milliseconds, default 6 000): a variable that is missing (or not
    /// Unicode, for [`from_env`](Self::from_env)) takes the default, one that is not an unsigned integer is
    /// `NotAnInteger`, zero is `ZeroStorageTimeout`.
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, TaskStorageConfigError> {
        let millis = crate::env::read_u64(
            &mut lookup,
            "OUTBOX_STORAGE_TIMEOUT_MS",
            DEFAULT_STORAGE_TIMEOUT.as_millis() as u64,
        )
        .map_err(|_| TaskStorageConfigError::NotAnInteger)?;
        Self::try_new(Duration::from_millis(millis))
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
                "defaults" | "non_unicode" => assert_eq!(
                    TaskStorageConfig::from_env().unwrap().storage_timeout(),
                    Duration::from_secs(6)
                ),
                "override" => assert_eq!(
                    TaskStorageConfig::from_env().unwrap().storage_timeout(),
                    Duration::from_millis(750)
                ),
                "invalid" => assert_eq!(
                    TaskStorageConfig::from_env().unwrap_err(),
                    TaskStorageConfigError::NotAnInteger
                ),
                "zero" => assert_eq!(
                    TaskStorageConfig::from_env().unwrap_err(),
                    TaskStorageConfigError::ZeroStorageTimeout
                ),
                _ => panic!("unknown test scenario"),
            }
            return;
        }
        for (scenario, value) in [
            ("defaults", None),
            ("override", Some("750")),
            ("invalid", Some("invalid")),
            ("zero", Some("0")),
        ] {
            test_support::isolated(
                concat!(module_path!(), "::from_env_validates_settings_in_an_isolated_process"),
                scenario,
                |child| {
                    if let Some(value) = value {
                        child.env("OUTBOX_STORAGE_TIMEOUT_MS", value);
                    }
                },
            );
        }
        #[cfg(unix)]
        test_support::isolated(
            concat!(module_path!(), "::from_env_validates_settings_in_an_isolated_process"),
            "non_unicode",
            |child| {
                use std::os::unix::ffi::OsStringExt;
                child.env("OUTBOX_STORAGE_TIMEOUT_MS", std::ffi::OsString::from_vec(vec![0xff]));
            },
        );
    }

    #[test]
    fn storage_timeout_config_has_defaults_overrides_and_rejects_invalid_budgets() {
        assert_eq!(
            TaskStorageConfig::from_lookup(|_| None).unwrap().storage_timeout(),
            Duration::from_secs(6)
        );
        assert_eq!(TaskStorageConfig::default().storage_timeout(), Duration::from_secs(6));
        assert_eq!(
            TaskStorageConfig::from_lookup(|_| Some("750".into()))
                .unwrap()
                .storage_timeout(),
            Duration::from_millis(750)
        );
        for value in ["0", "invalid", "-1", "9223372036854775808", "18446744073709551616"] {
            assert!(TaskStorageConfig::from_lookup(|_| Some(value.into())).is_err());
        }
        assert_eq!(
            TaskStorageConfig::try_new(Duration::ZERO).unwrap_err(),
            TaskStorageConfigError::ZeroStorageTimeout
        );
        assert_eq!(
            TaskStorageConfig::try_new(Duration::MAX).unwrap_err(),
            TaskStorageConfigError::StorageTimeoutTooLong
        );
        assert_eq!(
            TaskStorageConfigError::ZeroStorageTimeout.to_string(),
            "storage timeout must be positive"
        );
        assert_eq!(
            TaskStorageConfigError::StorageTimeoutTooLong.to_string(),
            "storage timeout is too long to represent"
        );
    }
}
