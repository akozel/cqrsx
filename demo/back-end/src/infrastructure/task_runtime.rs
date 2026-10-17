//! Wiring of the cqrsx task runtime: one adapter type over every task handler and a dispatcher
//! factory that builds fresh handlers for each worker.
pub mod adapter;
pub mod dispatcher;
