//! The load stand's back-end of backlog epic 06: the demo's `main.rs` with one more module, the synthetic [`probe`]
//! (`ENCORE_LOAD_VIEWS`, `ENCORE_LOAD_TASKS`, `POST /api/probe/{id}`). Everything else is the demo's: the `ENCORE_*`
//! and `OUTBOX_*` variables, the journal in PostgreSQL, the read models in MongoDB, the box office's modules.
use cqrsx::engine::{Cqrs, mongodb::MongoDb, postgres::Postgres};
use encore_backend::{DEMO_DEFAULTS, concert, fan, operations};
use epic06_backend::probe;

fn fail(message: impl std::fmt::Display) -> ! {
    tracing::error!("{message}");
    eprintln!("epic06-backend: {message}");
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
        .module(probe::module)
        .serve()
        .await;
    if let Err(error) = served {
        fail(error)
    }
}
