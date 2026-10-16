pub mod command;
pub mod outbox;
pub mod pool;
pub mod task_queue;
#[cfg(feature = "worker")]
pub mod task_runtime;

/// Canonical PostgreSQL event-store schema used by this runtime.
pub const EVENT_STORE_SCHEMA: &str = include_str!("../sql/001-event-store.sql");

/// Canonical PostgreSQL transactional-outbox schema used by this runtime.
pub const TRANSACTIONAL_OUTBOX_SCHEMA: &str = include_str!("../sql/002-transactional-outbox.sql");

/// Operational statement for explicitly requeuing one failed task.
pub const REQUEUE_FAILED_OUTBOX_TASK: &str = include_str!("../sql/requeue-failed-outbox.sql");

pub mod event_repository;
pub mod view_repository;

mod error;
