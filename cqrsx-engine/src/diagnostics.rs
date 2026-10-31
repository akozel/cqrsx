//! The engine's own log lines, fixed in their fields so that no id, payload or error text reaches them.
use cqrsx::diagnostics::safe_type;

/// A command for an aggregate type the engine does not know: a wiring fault, logged without the id.
pub(crate) fn log_unregistered_aggregate(aggregate_type: &str) {
    tracing::error!(
        component = "engine",
        operation = "execute",
        aggregate_type = safe_type(aggregate_type),
        "no module registers the aggregate type of the command"
    );
}
