//! Encore back-end: the box office on the cqrsx engine. The engine reads its settings from the `ENCORE_*` variables
//! (and the library's `OUTBOX_*`), keeps the journal in PostgreSQL and the read models in MongoDB, and serves the HTTP
//! API of the modules next to the task runtime and the projection processors until a signal.
use cqrsx_engine::{Cqrs, mongodb::MongoDb, postgres::Postgres};
use encore_backend::{DEMO_DEFAULTS, concert, fan, operations};

fn fail(message: impl std::fmt::Display) -> ! {
    tracing::error!("{message}");
    eprintln!("encore-backend: {message}");
    std::process::exit(2)
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let served = Cqrs::from_env("ENCORE")
        .defaults(DEMO_DEFAULTS)
        .event_store(Postgres)
        .projections(MongoDb)
        .module(concert::module)
        .module(fan::module)
        .module(operations::module)
        .serve()
        .await;
    if let Err(error) = served {
        fail(error)
    }
}
