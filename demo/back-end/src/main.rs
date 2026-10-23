//! Encore back-end: the process. What runs in it is assembled in `bootstrap`; how it starts, serves and
//! stops is `process`.
use encore_backend::{bootstrap, process::Process};

fn fail(message: impl std::fmt::Display) -> ! {
    tracing::error!("{message}");
    eprintln!("encore-backend: {message}");
    std::process::exit(2)
}

#[tokio::main]
async fn main() {
    let process = Process::start().await.unwrap_or_else(|e| fail(e)); // .env, logging, Config, PostgreSQL, MongoDB
    let app = bootstrap::wire(&process).unwrap_or_else(|e| fail(e)); // aggregates, projections, tasks, routes
    process.serve(app).await.unwrap_or_else(|e| fail(e)); // HTTP API + task runtime until a signal, drain, close
}
