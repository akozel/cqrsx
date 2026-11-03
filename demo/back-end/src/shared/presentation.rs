//! What the HTTP API (axum) of every module shares. The handlers live in the `presentation` of each module, laid out by
//! transport, kind of framework entry and action (rule 10 of `.claude/rules/cqrsx-application.md`), routed by one URL table
//! per module (`presentation::http::routes`); their state is the framework (`State<Arc<Cqrs>>`).
pub mod http;
