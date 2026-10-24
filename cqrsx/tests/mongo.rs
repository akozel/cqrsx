//! The MongoDB read side against a real server: the view repository, the checkpoint store and the
//! projection processor whose views and checkpoint live in MongoDB. Every test starts its own container.
#![cfg(feature = "mongodb")]

#[path = "mongo/checkpoints.rs"]
mod checkpoints;
#[cfg(feature = "postgres")]
#[path = "mongo/journal_in_postgres.rs"]
mod journal_in_postgres;
#[path = "support/mongo.rs"]
mod mongo_container;
#[path = "support/port_lease.rs"]
mod port_lease;
#[cfg(feature = "postgres")]
#[path = "support/postgres.rs"]
mod postgres_container;
#[path = "mongo/processor.rs"]
mod processor;
#[path = "mongo/support.rs"]
mod support;
#[path = "mongo/view_repository.rs"]
mod view_repository;
