//! Technical adapters: PostgreSQL (event store, outbox), MongoDB (read models) and the HTTP client of
//! the external mail service.
pub mod mail;
pub mod mongo;
pub mod postgres;
