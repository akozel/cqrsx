//! The engine: modules assembled in memory and run end to end; over PostgreSQL and MongoDB on containers.
#[path = "engine/modules.rs"]
mod modules;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "support/mongo.rs"]
mod mongo_container;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "support/port_lease.rs"]
mod port_lease;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "engine/postgres.rs"]
mod postgres;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "support/postgres.rs"]
mod postgres_container;
#[path = "engine/scenario.rs"]
mod scenario;
