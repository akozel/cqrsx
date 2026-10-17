//! Technical adapters: PostgreSQL (event store, outbox), MongoDB (read models, the mail inbox), the
//! simulated mail service and the wiring of the cqrsx task runtime.
pub mod mail;
pub mod mongo;
pub mod postgres;
pub mod task_runtime;
