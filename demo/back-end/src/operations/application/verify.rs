//! 🔍 Verify (D-20): every read model against the history of its aggregate, by the framework (cqrsx
//! `view::verify`, through `cqrsx.views().verify`). Documents behind the journal are counted apart; only a wrong
//! document is a difference.
use crate::shared::application::StorageError;
use async_trait::async_trait;
use cqrsx::view::verify::{Difference, Verification};
use std::{collections::BTreeMap, sync::Arc};

/// What verify needs from every read model, whatever its view type: the framework's view in the process
/// (`VerifyService::of`), doubles in the tests.
#[async_trait]
pub trait VerifiedReadModel: Send + Sync {
    /// The collection name the API reports: the view's name.
    fn name(&self) -> &str;
    async fn verify(&self) -> Result<Verification, StorageError>;
}

/// What verifying every read model found: documents behind the journal are counted apart, only a wrong one is a
/// difference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    /// Keys of the journal checked, per read model's name.
    pub checked: BTreeMap<String, usize>,
    /// Documents behind their history and the events they still have to apply, summed over the read models.
    pub behind_documents: usize,
    pub behind_events: usize,
    pub differences: Vec<Difference>,
}

pub struct VerifyService {
    read_models: Vec<Arc<dyn VerifiedReadModel>>,
}

impl VerifyService {
    pub fn new(read_models: Vec<Arc<dyn VerifiedReadModel>>) -> Self {
        Self { read_models }
    }

    /// Verifies every read model, one after the other.
    pub async fn run(&self) -> Result<VerifyReport, StorageError> {
        let mut report = VerifyReport {
            checked: BTreeMap::new(),
            behind_documents: 0,
            behind_events: 0,
            differences: Vec::new(),
        };
        for read_model in &self.read_models {
            let verification = read_model.verify().await?;
            report.checked.insert(read_model.name().to_owned(), verification.checked);
            report.behind_documents += verification.behind_documents;
            report.behind_events += verification.behind_events;
            report.differences.extend(verification.differences);
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::view::verify::{Difference, DifferenceKind, Verification};

    struct Fixed(&'static str, Result<Verification, StorageError>);

    #[async_trait]
    impl VerifiedReadModel for Fixed {
        fn name(&self) -> &str {
            self.0
        }
        async fn verify(&self) -> Result<Verification, StorageError> {
            self.1.clone()
        }
    }

    #[tokio::test]
    async fn the_read_models_add_up_and_one_difference_fails_the_verification() {
        let halls = Fixed(
            "concert_halls",
            Ok(Verification {
                checked: 3,
                behind_documents: 1,
                behind_events: 2,
                differences: vec![],
            }),
        );
        let wallets = Fixed(
            "fan_wallets",
            Ok(Verification {
                checked: 5,
                behind_documents: 2,
                behind_events: 2,
                differences: vec![],
            }),
        );
        let service = VerifyService::new(vec![Arc::new(halls), Arc::new(wallets)]);
        assert_eq!(
            service.run().await.unwrap(),
            VerifyReport {
                checked: BTreeMap::from([("concert_halls".to_owned(), 3), ("fan_wallets".to_owned(), 5)]),
                behind_documents: 3,
                behind_events: 4,
                differences: vec![],
            }
        );
        let wrong = Fixed(
            "concert_halls",
            Ok(Verification {
                checked: 1,
                behind_documents: 0,
                behind_events: 0,
                differences: vec![Difference {
                    view: "concert_halls",
                    id: "c-1".into(),
                    kind: DifferenceKind::Differs,
                }],
            }),
        );
        let report = VerifyService::new(vec![Arc::new(wrong)]).run().await.unwrap();
        assert_eq!(
            report.differences,
            [Difference {
                view: "concert_halls",
                id: "c-1".into(),
                kind: DifferenceKind::Differs
            }]
        );
        let broken = VerifyService::new(vec![Arc::new(Fixed("concert_halls", Err(StorageError("down".into()))))]);
        assert_eq!(broken.run().await.unwrap_err(), StorageError("down".into()));
    }
}
