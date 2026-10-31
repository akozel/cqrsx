//! The engine's own log lines, fixed in their fields so that no id, payload or error text reaches them.

/// A command for an aggregate type the engine does not know: a wiring fault, logged without the id.
pub(crate) fn log_unregistered_aggregate(aggregate_type: &str) {
    tracing::error!(
        component = "engine",
        operation = "execute",
        aggregate_type = safe_type(aggregate_type),
        "no module registers the aggregate type of the command"
    );
}

/// An aggregate type's name as a log field: itself when it is a plain identifier, else `unknown` (the name is chosen
/// by the application, never trusted into a log).
fn safe_type(value: &str) -> &str {
    if !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        value
    } else {
        "unknown"
    }
}
