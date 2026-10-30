#![cfg(feature = "logging")]
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]

// A separate test executable isolates irreversible process-wide installation.
#[test]
fn logger_installation_is_explicit_and_duplicate_installation_is_fallible() {
    use cqrsx::logging::LoggingConfig;
    use std::error::Error;

    assert!(!tracing::dispatcher::has_been_set());
    let expected = std::env::var("RUST_LOG")
        .ok()
        .unwrap_or_else(|| "info".into())
        .parse::<tracing::level_filters::LevelFilter>();
    match (LoggingConfig::from_env(), expected) {
        (Ok(actual), Ok(expected)) => assert_eq!(actual.level_filter(), expected),
        (Err(_), Err(_)) => {}
        _ => panic!("from_env must retain level parsing"),
    }
    let config = LoggingConfig::from_lookup(|_| None).unwrap();
    assert!(!tracing::dispatcher::has_been_set());
    config.try_init().unwrap();
    assert!(tracing::dispatcher::has_been_set());
    let error = config.try_init().unwrap_err();
    assert!(error.source().is_some());
    assert_eq!(error.to_string(), "unable to initialize process logger");
    assert_eq!(format!("{error:?}"), error.to_string());
}
