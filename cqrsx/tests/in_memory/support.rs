//! Fixtures owned by the public CQRS integration suite.
#[path = "support/ledger.rs"]
pub(crate) mod ledger;
#[path = "support/scenario.rs"]
pub(crate) mod scenario;

pub(crate) fn property_config() -> proptest::test_runner::Config {
    proptest::test_runner::Config {
        cases: 1024,
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            "../target/proptest-regressions.txt",
        ))),
        ..Default::default()
    }
}
