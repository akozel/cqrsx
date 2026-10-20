use sqlx::{
    Connection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{num::NonZeroU32, time::Duration};

/// The pool size `connect_pool` uses: SQLx's own default.
pub const DEFAULT_MAX_CONNECTIONS: NonZeroU32 = NonZeroU32::new(10).expect("10 is not zero");

/// A threshold for `ConnectionPingPolicy::Idle` when the process has no better one: with 10
/// connections the pings stop above ~100 acquisitions per second.
pub const DEFAULT_IDLE_PING_AFTER: Duration = Duration::from_millis(100);

/// When the pool checks that a connection is alive (`Sync` → `ReadyForQuery`, one round trip)
/// before handing it out. Every pool this crate connects takes one explicitly. SQLx still pings
/// every connection when it returns to the pool, whatever the policy; that one is not configurable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionPingPolicy {
    /// SQLx's own behaviour: a ping on every hand-out (`test_before_acquire`), i.e. one more round
    /// trip per acquisition, three per command.
    Default,
    /// A ping only for a connection idle for longer than this. SQLx's idle queue is first in,
    /// first out, so a connection idles for about `max_connections / acquisitions per second`: the
    /// pings stop under load and stay where they cost nothing.
    ///
    /// Risk: a connection that died less than this ago (PostgreSQL restarted, network cut) is found
    /// on its first query instead of the ping. That query fails, which a command reports as
    /// `CommandGatewayError::Unavailable` (the write did not happen, but the contract leaves its
    /// outcome unknown; only conflicts are retried) and a queue operation as
    /// `TaskProcessingError::Unavailable` (the lease recovery returns the task); SQLx then drops the
    /// connection and the next acquisition opens a new one. A failed ping after a longer idle period
    /// drops the connection and hands out another, as with `Default`.
    Idle(Duration),
}

impl ConnectionPingPolicy {
    /// This policy on top of the caller's SQLx pool options, for callers that connect themselves.
    /// `Default` restores `test_before_acquire(true)`; `Idle` sets `test_before_acquire(false)` and
    /// the `before_acquire` hook, replacing one set before.
    pub fn apply(self, pool: PgPoolOptions) -> PgPoolOptions {
        match self {
            Self::Default => pool.test_before_acquire(true),
            Self::Idle(idle_ping_after) => pool.test_before_acquire(false).before_acquire(move |connection, meta| {
                Box::pin(async move {
                    if needs_ping(meta.idle_for, idle_ping_after) {
                        connection.ping().await?;
                    }
                    Ok(true)
                })
            }),
        }
    }
}

/// Connect using caller-supplied options, the given ping policy and SQLx's pool defaults
/// (`DEFAULT_MAX_CONNECTIONS`). See `connect_pool_with` for the rest of the contract.
pub async fn connect_pool(options: PgConnectOptions, ping: ConnectionPingPolicy) -> Result<PgPool, sqlx::Error> {
    connect_pool_with(options, PgPoolOptions::new(), ping).await
}

/// Connect using caller-supplied connection and pool options with the given ping policy on top.
/// One pool is usually shared by every PostgreSQL user of a process (command executors, the task
/// runtime's poller and workers), so its size (`PgPoolOptions::max_connections`) is the ceiling of
/// concurrent database work and belongs to the process configuration.
/// No dotenv loading, URL parsing, custom retries or logger installation occurs here.
/// The original SQLx error is returned; diagnostics omit its arbitrary text.
pub async fn connect_pool_with(
    options: PgConnectOptions,
    pool: PgPoolOptions,
    ping: ConnectionPingPolicy,
) -> Result<PgPool, sqlx::Error> {
    ping.apply(pool)
        .connect_with(options)
        .await
        .inspect_err(|error| crate::diagnostics::log_sqlx_failure("connect", error))
}

fn needs_ping(idle_for: Duration, idle_ping_after: Duration) -> bool {
    idle_for > idle_ping_after
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_connection_idle_for_longer_than_the_threshold_is_pinged() {
        let threshold = DEFAULT_IDLE_PING_AFTER;
        assert!(!needs_ping(Duration::ZERO, threshold));
        assert!(!needs_ping(threshold - Duration::from_millis(1), threshold));
        assert!(!needs_ping(threshold, threshold));
        assert!(needs_ping(threshold + Duration::from_millis(1), threshold));
    }

    #[test]
    fn policy_sets_the_hand_out_ping_and_keeps_the_other_pool_settings() {
        let idle = ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER).apply(PgPoolOptions::new().max_connections(3));
        assert_eq!(idle.get_max_connections(), 3);
        assert!(!idle.get_test_before_acquire());
        let default = ConnectionPingPolicy::Default.apply(PgPoolOptions::new().max_connections(4).test_before_acquire(false));
        assert_eq!(default.get_max_connections(), 4);
        assert!(default.get_test_before_acquire());
    }
}
