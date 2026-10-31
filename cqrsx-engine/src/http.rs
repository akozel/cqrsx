//! The HTTP API of an engine (feature `http`, axum 0.8): every module routes its requests under the engine as their
//! state ([`Module::routes`]); [`Cqrs::router`] is the routes of every module with a trace of every request, what
//! `serve` serves and a test sends its requests to.
use crate::{Cqrs, module::Module};
use axum::Router;
use std::sync::Arc;
use tower_http::trace::TraceLayer;

impl Module {
    /// The routes of the module: its handlers take the engine as their state (`State<Arc<Cqrs>>`) and reach the
    /// commands, the views and the module's dependencies through it. Two modules routing one path fail the build of the
    /// engine with axum's panic.
    pub fn routes(mut self, routes: Router<Arc<Cqrs>>) -> Self {
        self.routes = Some(match self.routes.take() {
            Some(before) => before.merge(routes),
            None => routes,
        });
        self
    }
}

impl Cqrs {
    /// The routes of every module under this engine, with a trace of every request (`TraceLayer::new_for_http`).
    pub fn router(self: &Arc<Self>) -> Router {
        self.routes.clone().layer(TraceLayer::new_for_http()).with_state(self.clone())
    }
}
