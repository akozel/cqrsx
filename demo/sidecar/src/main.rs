//! Encore sidecar: everything around the box office that is not the box office.
//!
//! * A read-only window into PostgreSQL (events, snapshots, outbox) and MongoDB (read models, mail
//!   inbox) under `/inspect`, polled every `SIDECAR_POLL_MS` and streamed over SSE.
//! * The demo lifecycle: seed at start-up, 🧨 wipe of the read models, ⟲ reset of everything.
//! * The demo's external mail service (`POST /mail`): the moody provider the back-end's e-mail
//!   tasks call over HTTP, so that the back-end carries no demo-only code.
//!
//! Independent of `cqrsx`: it shows the databases "from the outside". What runs is assembled in
//! `bootstrap`; how it starts, serves and stops is `process`.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod bootstrap;
mod config;
mod demo;
mod http;
mod mail;
mod mongo;
mod postgres;
mod process;
mod state;

use process::Process;

fn fail(message: impl std::fmt::Display) -> ! {
    tracing::error!("{message}");
    eprintln!("encore-sidecar: {message}");
    std::process::exit(2)
}

#[tokio::main]
async fn main() {
    let process = Process::start().await.unwrap_or_else(|e| fail(e)); // .env, logging, Config, inspectors
    let app = bootstrap::wire(&process).unwrap_or_else(|e| fail(e)); // X-ray poller and routes, demo lifecycle, mail service
    process.serve(app).await.unwrap_or_else(|e| fail(e)); // HTTP until a signal, then stop the poller and the seeder
}
