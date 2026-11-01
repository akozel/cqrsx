//! Fixed diagnostic context for the failures of the MongoDB driver, independent of its messages and of payloads.

/// A fixed word for a MongoDB driver error.
fn mongodb_category(error: &::mongodb::error::Error) -> &'static str {
    use ::mongodb::error::ErrorKind;
    match error.kind.as_ref() {
        ErrorKind::Io(_) => "io",
        ErrorKind::ServerSelection { .. } => "server_selection",
        ErrorKind::Command(_) => "command",
        ErrorKind::Write(_) => "write",
        ErrorKind::BsonDeserialization(_) | ErrorKind::BsonSerialization(_) => "bson",
        _ => "other",
    }
}

/// Logs a failed storage operation before its error is erased.
pub(crate) fn log_mongodb_failure(operation: &'static str, error: &::mongodb::error::Error) {
    tracing::error!(component = "mongodb", operation, category = mongodb_category(error), code = ?crate::server_code(error), "storage operation failed");
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::future::Future;

    /// The output of `work` and the `ERROR` lines it logged.
    pub(crate) async fn capture<T>(work: impl Future<Output = T>) -> (T, String) {
        cqrsx_fixtures::logs::capture(
            || super::log_mongodb_failure("prime", &::mongodb::error::Error::custom("prime")),
            work,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    #[tokio::test]
    async fn mongodb_failures_log_a_fixed_category_and_the_server_code_without_the_message() {
        let errors = [io(), command(11000), write(121), write_concern(64), bson(), custom()];
        assert_eq!(
            errors.each_ref().map(mongodb_category),
            ["io", "command", "write", "write", "bson", "other"]
        );
        let (_, log) = test_support::capture(async {
            for error in &errors {
                log_mongodb_failure("claim", error);
            }
        })
        .await;
        assert_eq!(log.matches("storage operation failed").count(), errors.len(), "{log}");
        assert_eq!(log.matches("component=\"mongodb\"").count(), errors.len(), "{log}");
        assert_eq!(log.matches("operation=\"claim\"").count(), errors.len(), "{log}");
        for (category, code) in [
            ("command", "Some(11000)"),
            ("write", "Some(121)"),
            ("write", "Some(64)"),
            ("io", "None"),
        ] {
            assert!(
                log.contains(&format!("category=\"{category}\" code={code}")),
                "{category} {code}: {log}"
            );
        }
        assert!(!log.contains("private_"), "{log}");
    }
}
