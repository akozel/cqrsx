//! The backend in memory: the journal, the checkpoints and the views, and a task queue over the outbox of that
//! journal.
pub mod checkpoint_store;
pub mod event_repository;
pub mod task_queue;
pub mod view_repository;
