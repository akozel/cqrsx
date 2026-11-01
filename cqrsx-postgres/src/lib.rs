//! The PostgreSQL stores of cqrsx: the event repository with snapshots and the feed, atomic publication of events and
//! tasks through the outbox, the task queue, the checkpoints of the projection processors and the view repository,
//! with the canonical SQL of their tables.
//!
//! The core knows none of it: the stores implement its ports (`cqrsx::journal::repository::EventRepository`,
//! `cqrsx::worker::queue::TaskQueue`, ...), and the engine (`cqrsx-engine`, feature `postgres`) assembles them. Where a
//! driver error becomes an error of the core, it is wrapped in `cqrsx::diagnostics::StorageFailure`, so that the core's
//! logs carry its category and SQLSTATE; the message of the driver is never logged.
//!
//! Nothing here creates a table. The schemas are applied before the stores are used, in the order of their numbers:
//! `sql/001-event-store.sql` (the journal and the snapshots), `sql/002-transactional-outbox.sql` (the outbox and the
//! task queue) and `sql/003-projection-checkpoints.sql` (the checkpoints and the requested rebuild keys). Each is
//! idempotent (`CREATE … IF NOT EXISTS`), so a deployment runs them at every start with `psql -f`, as an init script
//! of the database container, or from code through the same texts in [`EVENT_STORE_SCHEMA`],
//! [`TRANSACTIONAL_OUTBOX_SCHEMA`] and [`PROJECTION_CHECKPOINTS_SCHEMA`] (`sqlx::raw_sql(EVENT_STORE_SCHEMA)`). A view
//! kept in PostgreSQL has a table of its own, created by the application in the shape
//! [`PostgresViewRepository::new`](view_repository::PostgresViewRepository::new) gives. `sql/requeue-failed-outbox.sql`
//! ([`REQUEUE_FAILED_OUTBOX_TASK`]) is an operator's statement, not a schema: it returns one failed task to the queue.
//!
//! Derived in part from `postgres-es` 0.5.0 (Apache-2.0), copyright 2023 Serverless Technology Consulting, LLC; the
//! text of the license is in `cqrsx-postgres/licenses/`, the notice in `doc/components.md#upstream-attribution`.
mod diagnostics;
pub mod outbox;
pub mod pool;
pub mod task_queue;

/// Canonical PostgreSQL event-store schema used by this runtime.
pub const EVENT_STORE_SCHEMA: &str = include_str!("../sql/001-event-store.sql");

/// Canonical PostgreSQL transactional-outbox schema used by this runtime.
pub const TRANSACTIONAL_OUTBOX_SCHEMA: &str = include_str!("../sql/002-transactional-outbox.sql");

/// Canonical PostgreSQL schema of the projection checkpoints and the requested rebuild keys.
pub const PROJECTION_CHECKPOINTS_SCHEMA: &str = include_str!("../sql/003-projection-checkpoints.sql");

/// Operational statement for explicitly requeuing one failed task.
pub const REQUEUE_FAILED_OUTBOX_TASK: &str = include_str!("../sql/requeue-failed-outbox.sql");

pub mod checkpoint_store;
pub mod event_repository;
pub mod view_repository;

mod error;
