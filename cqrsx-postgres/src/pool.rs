use sqlx::{
    Connection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{fmt, num::NonZeroU32, time::Duration};

/// The pool size `connect_pool` uses: SQLx's own default.
pub const DEFAULT_MAX_CONNECTIONS: NonZeroU32 = NonZeroU32::new(10).expect("10 is not zero");

/// A threshold for `ConnectionPingPolicy::Idle` when the process has no better one: with 10
/// connections the pings stop above ~100 acquisitions per second.
pub const DEFAULT_IDLE_PING_AFTER: Duration = Duration::from_millis(100);

/// The server-side cap of `StatementTimeout::Default`.
pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(4);

/// How long PostgreSQL lets one statement of the pool run (`statement_timeout`, a startup parameter
/// of every session, so no statement needs a transaction to scope it). Every pool this crate connects
/// takes one explicitly.
///
/// The commands and the task queue issue short static statements over indexed tables: the latest
/// snapshot, the events after it, one write; claim, settle, recovery. A cap turns a stuck statement
/// (a lock wait, an overloaded server) into a fast failure, reported as `CommandError::Unavailable`
/// with an unknown outcome or as `TaskProcessingError::Unavailable`, instead of a connection held for as
/// long as it takes. Without snapshots, though, a command reads its aggregate's whole history: an
/// aggregate with a long history and no snapshots can reach the cap, so give such aggregates snapshots.
/// Long reads (counting a whole journal with `totals`, reading many whole histories at once) belong to a
/// pool of their own with `Unlimited`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementTimeout {
    /// `DEFAULT_STATEMENT_TIMEOUT`, 4 s.
    Default,
    /// From 1 ms to `i32::MAX` ms; anything else fails the connection with `sqlx::Error::Configuration`.
    Custom(Duration),
    /// No cap (`statement_timeout = 0`), whatever the server or role configures.
    Unlimited,
}

/// The cap as a setting's description writes it: whole seconds, else whole milliseconds, else nanoseconds (`4 s`,
/// `2500 ms`), or `unlimited`.
impl fmt::Display for StatementTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timeout = match self {
            Self::Default => DEFAULT_STATEMENT_TIMEOUT,
            Self::Custom(timeout) => *timeout,
            Self::Unlimited => return f.write_str("unlimited"),
        };
        if timeout.subsec_nanos() == 0 {
            write!(f, "{} s", timeout.as_secs())
        } else if timeout.subsec_nanos().is_multiple_of(1_000_000) {
            write!(f, "{} ms", timeout.as_millis())
        } else {
            write!(f, "{} ns", timeout.as_nanos())
        }
    }
}

impl StatementTimeout {
    /// The `statement_timeout` value in milliseconds; `0` means no cap.
    fn milliseconds(self) -> Result<u128, sqlx::Error> {
        match self {
            Self::Default => Ok(DEFAULT_STATEMENT_TIMEOUT.as_millis()),
            Self::Unlimited => Ok(0),
            Self::Custom(timeout) => match timeout.as_millis() {
                millis @ 1..=MAX_STATEMENT_TIMEOUT_MS => Ok(millis),
                _ => Err(sqlx::Error::Configuration(
                    format!("statement timeout must be 1..={MAX_STATEMENT_TIMEOUT_MS} ms").into(),
                )),
            },
        }
    }
}

const MAX_STATEMENT_TIMEOUT_MS: u128 = i32::MAX as u128;

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
    /// `CommandError::Unavailable` (the write did not happen, but the contract leaves its
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

/// Connect using caller-supplied options, the given ping policy and statement timeout, and SQLx's
/// pool defaults (`DEFAULT_MAX_CONNECTIONS`). See `connect_pool_with` for the rest of the contract.
pub async fn connect_pool(
    options: PgConnectOptions,
    ping: ConnectionPingPolicy,
    timeout: StatementTimeout,
) -> Result<PgPool, sqlx::Error> {
    connect_pool_with(options, PgPoolOptions::new(), ping, timeout).await
}

/// Connect using caller-supplied connection and pool options with the given ping policy and
/// statement timeout on top. One pool is usually shared by the command executors and the task
/// runtime's poller and workers, so its size (`PgPoolOptions::max_connections`) is the ceiling of
/// concurrent database work and belongs to the process configuration; long reads take a second
/// pool with `StatementTimeout::Unlimited`.
/// No dotenv loading, URL parsing, custom retries or logger installation occurs here.
/// The original SQLx error is returned; diagnostics omit its arbitrary text.
pub async fn connect_pool_with(
    options: PgConnectOptions,
    pool: PgPoolOptions,
    ping: ConnectionPingPolicy,
    timeout: StatementTimeout,
) -> Result<PgPool, sqlx::Error> {
    let options = options.options([("statement_timeout", timeout.milliseconds()?.to_string())]);
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

    /// The stores print their pool, never the password of its URL.
    #[tokio::test]
    async fn the_postgres_stores_print_their_pool_without_the_password() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody:s3cret@localhost/none")
            .unwrap();
        let stores = [
            format!("{:?}", crate::checkpoint_store::PostgresCheckpointStore::new(pool.clone())),
            format!("{:?}", crate::task_queue::PostgresTaskQueue::new(pool.clone())),
            format!("{:?}", crate::outbox::PostgresEventRepositoryWithOutbox::new(pool)),
        ];
        for printed in stores {
            assert!(printed.contains("pool: Pool {") && !printed.contains("s3cret"), "{printed}");
        }
    }

    #[test]
    fn only_a_connection_idle_for_longer_than_the_threshold_is_pinged() {
        let threshold = DEFAULT_IDLE_PING_AFTER;
        assert!(!needs_ping(Duration::ZERO, threshold));
        assert!(!needs_ping(threshold - Duration::from_millis(1), threshold));
        assert!(!needs_ping(threshold, threshold));
        assert!(needs_ping(threshold + Duration::from_millis(1), threshold));
    }

    #[test]
    fn a_statement_timeout_is_written_as_the_settings_write_durations() {
        assert_eq!(StatementTimeout::Default.to_string(), "4 s");
        assert_eq!(StatementTimeout::Custom(Duration::from_millis(2500)).to_string(), "2500 ms");
        assert_eq!(
            StatementTimeout::Custom(Duration::from_micros(1500)).to_string(),
            "1500000 ns"
        );
        assert_eq!(StatementTimeout::Unlimited.to_string(), "unlimited");
    }

    #[test]
    fn statement_timeout_maps_to_milliseconds_and_rejects_what_postgresql_cannot_hold() {
        assert_eq!(StatementTimeout::Default.milliseconds().unwrap(), 4000);
        assert_eq!(StatementTimeout::Unlimited.milliseconds().unwrap(), 0);
        assert_eq!(
            StatementTimeout::Custom(Duration::from_millis(125)).milliseconds().unwrap(),
            125
        );
        assert_eq!(
            StatementTimeout::Custom(Duration::from_millis(i32::MAX as u64))
                .milliseconds()
                .unwrap(),
            i32::MAX as u128
        );
        for invalid in [
            Duration::ZERO,
            Duration::from_micros(999),
            Duration::from_millis(i32::MAX as u64 + 1),
        ] {
            assert!(matches!(
                StatementTimeout::Custom(invalid).milliseconds(),
                Err(sqlx::Error::Configuration(_))
            ));
        }
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
