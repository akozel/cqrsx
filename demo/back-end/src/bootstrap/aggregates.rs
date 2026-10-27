//! One module per aggregate: its command executor with the locks and snapshots of the configuration, the
//! commit wake-up of its read model's processor (see [`projections`](super::projections)) and the activity feed's
//! observer of its commands. A new aggregate gets a module like these two, listed here.
pub mod concert;
pub mod fan;

use super::WiringError;
use crate::config::Config;
use cqrsx::{cqrs::command::retry::AggregateConflictRetryPolicy, postgres::command::PostgresCommandConfig};

/// Every executor runs its commands the same way: a snapshot every `ENCORE_SNAPSHOT_EVERY` events, the
/// library's conflict retry and the lock limits of the configuration. An executor owns the locks of its
/// aggregate, so the HTTP commands and the task handlers' commands to one concert or ledger wait for each
/// other.
fn command_config(config: &Config) -> Result<PostgresCommandConfig, WiringError> {
    Ok(PostgresCommandConfig::new(
        config.snapshot_every,
        AggregateConflictRetryPolicy::default(),
        config.command_lock_limits,
    )?)
}
