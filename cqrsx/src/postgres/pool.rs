use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::num::NonZeroU32;

/// The pool size `connect_pool` uses: SQLx's own default.
pub const DEFAULT_MAX_CONNECTIONS: NonZeroU32 = NonZeroU32::new(10).expect("10 is not zero");

/// Connect using caller-supplied options, `DEFAULT_MAX_CONNECTIONS` connections and other SQLx
/// defaults. See `connect_pool_with_size` for the rest of the contract.
pub async fn connect_pool(options: PgConnectOptions) -> Result<PgPool, sqlx::Error> {
    connect_pool_with_size(options, DEFAULT_MAX_CONNECTIONS).await
}

/// Connect using caller-supplied options and at most `max_connections` connections; other SQLx
/// defaults apply. One pool is usually shared by every PostgreSQL user of a process (command
/// executors, the task runtime's poller and workers), so its size is the ceiling of concurrent
/// database work and belongs to the process configuration.
/// No dotenv loading, URL parsing, custom retries or logger installation occurs here.
/// The original SQLx error is returned; diagnostics omit its arbitrary text.
pub async fn connect_pool_with_size(options: PgConnectOptions, max_connections: NonZeroU32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections.get())
        .connect_with(options)
        .await
        .inspect_err(|error| crate::diagnostics::log_sqlx_failure("connect", error))
}
