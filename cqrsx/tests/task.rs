//! Public task adaptation, execution and optional worker-runtime contracts.
#[path = "task/adapter.rs"]
mod adapter;
#[path = "task/executor.rs"]
mod executor;
#[cfg(feature = "worker")]
#[path = "task/runtime.rs"]
mod runtime;
