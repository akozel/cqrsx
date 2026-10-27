//! One module per aggregate: its command executor with the rules its module registered, the snapshots, conflict
//! retry and lock limits of the engine's settings, the commit wake-up of its read model's processor (see
//! [`projections`](super::projections)) and the activity log's observer of its commands. A new aggregate gets a module
//! like these two, listed here.
pub mod concert;
pub mod fan;

use cqrsx::{
    engine::setup::Settings,
    postgres::command::{PostgresCommandConfig, PostgresCommandConfigError},
};

/// Every executor runs its commands the same way: a snapshot every `snapshot_every` events, the conflict retry and the
/// lock limits of the settings. An executor owns the locks of its aggregate, so the HTTP commands and the task
/// handlers' commands to one concert or ledger wait for each other.
pub fn command_config(settings: &Settings) -> Result<PostgresCommandConfig, PostgresCommandConfigError> {
    PostgresCommandConfig::new(settings.snapshot_every.get(), settings.conflict_retry, settings.lock_limits)
}
