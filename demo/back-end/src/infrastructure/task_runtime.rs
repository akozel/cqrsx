//! Wiring of the cqrsx task runtime: a dispatcher factory that builds fresh handlers for each
//! worker, every handler behind the library's `BoxedTaskAdapter`.
pub mod dispatcher;
