//! 🔍 Verify (D-20): every read model against the history of its aggregate (see
//! [`verification`](crate::application::projections::verification)). Documents behind the journal are
//! counted apart; only a wrong document is a difference.
use crate::application::{StorageError, projections::verification::VerifiedReadModel};
use serde_json::{Map, Value, json};
use std::sync::Arc;

pub struct VerifyService {
    read_models: Vec<Arc<dyn VerifiedReadModel>>,
}

impl VerifyService {
    pub fn new(read_models: Vec<Arc<dyn VerifiedReadModel>>) -> Self {
        Self { read_models }
    }

    /// `{ ok, checked: { collection: keys }, behind: { documents, events }, differences }`.
    pub async fn run(&self) -> Result<Value, StorageError> {
        let mut checked = Map::new();
        let (mut documents, mut events) = (0, 0);
        let mut differences = Vec::new();
        for read_model in &self.read_models {
            let verification = read_model.verify().await?;
            checked.insert(read_model.name().to_owned(), json!(verification.checked));
            documents += verification.behind_documents;
            events += verification.behind_events;
            differences.extend(verification.differences);
        }
        Ok(json!({
            "ok": differences.is_empty(),
            "checked": checked,
            "behind": { "documents": documents, "events": events },
            "differences": differences,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::projections::verification::{Difference, Verification};
    use async_trait::async_trait;

    struct Fixed(&'static str, Result<Verification, StorageError>);

    #[async_trait]
    impl VerifiedReadModel for Fixed {
        fn name(&self) -> &'static str {
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
            json!({ "ok": true, "checked": { "concert_halls": 3, "fan_wallets": 5 }, "behind": { "documents": 3, "events": 4 }, "differences": [] })
        );
        let wrong = Fixed(
            "concert_halls",
            Ok(Verification {
                checked: 1,
                behind_documents: 0,
                behind_events: 0,
                differences: vec![Difference {
                    collection: "concert_halls",
                    id: "c-1".into(),
                    kind: "differs",
                }],
            }),
        );
        let report = VerifyService::new(vec![Arc::new(wrong)]).run().await.unwrap();
        assert_eq!(report["ok"], false);
        assert_eq!(
            report["differences"][0],
            json!({ "collection": "concert_halls", "id": "c-1", "kind": "differs" })
        );
        let broken = VerifyService::new(vec![Arc::new(Fixed("concert_halls", Err(StorageError("down".into()))))]);
        assert_eq!(broken.run().await.unwrap_err(), StorageError("down".into()));
    }
}
