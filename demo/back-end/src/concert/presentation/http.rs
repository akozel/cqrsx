//! `/api/concerts`: the box office — its one write endpoint, the read model, the rebuild of one hall and the time
//! machine. A handler lies in the directory of the kind of framework entry it uses; the URL table below has a section per
//! kind. The command endpoint is a directory (`commands/execute/{request,response}.rs`), because its input is converted
//! and checked.
use axum::{
    Router,
    routing::{get, post},
};
use cqrsx_framework::Cqrs;
use std::sync::Arc;

/// Change the state: `cqrs.source(..).execute`.
pub mod commands {
    pub mod execute;
}
/// Read a projection: `cqrs.view::<V>()`.
pub mod queries {
    pub mod get;
    pub mod list;
}
/// Replay the journal: `cqrs.history::<A>(..)`.
pub mod history {
    pub mod state_at;
    pub mod summary;
    pub mod timeline;
}
/// Serve the read side: `cqrs.views()`, `cqrs.view::<V>().rebuild(..)`.
pub mod maintenance {
    pub mod rebuild;
}

/// The URL table of the box office: a section per kind, one line per endpoint.
pub fn routes() -> Router<Arc<Cqrs>> {
    Router::new()
        // commands: change the state
        .route("/api/concerts/{concert_id}/commands", post(commands::execute::handle))
        // queries: read the projection
        .route("/api/concerts", get(queries::list::handle))
        .route("/api/concerts/{concert_id}", get(queries::get::handle))
        // history: replay the journal
        .route("/api/concerts/{concert_id}/timeline", get(history::timeline::handle))
        .route(
            "/api/concerts/{concert_id}/state-at/{sequence}",
            get(history::state_at::handle),
        )
        // maintenance: the read side
        .route("/api/concerts/{concert_id}/rebuild", post(maintenance::rebuild::handle))
}
