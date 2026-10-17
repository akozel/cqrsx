//! 🔍 Verify \[P1\]: rebuild every read model into a scratch copy with the very same
//! `ViewProjection` and compare it with the live documents (without `version`).
//! No in-memory fakes at runtime (rule §12.4): the scratch copies live in MongoDB.
//!
//! Known limitation: verify does not take the box-office lock, so a command committed while the
//! scratch copy is being built shows up as an honest `differs` (the copy is a moment behind).
//! Every run uses its own scratch copies, so concurrent runs never disturb each other.
use crate::application::{StorageError, projections::maintenance::MaintainedReadModel};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use uuid::Uuid;

pub struct VerifyService {
    read_models: Vec<Arc<dyn MaintainedReadModel>>,
}

impl VerifyService {
    pub fn new(read_models: Vec<Arc<dyn MaintainedReadModel>>) -> Self {
        Self { read_models }
    }

    pub async fn run(&self) -> Result<Value, StorageError> {
        let run_id = Uuid::now_v7();
        let mut checked = Map::new();
        let mut differences = Vec::new();
        for read_model in &self.read_models {
            let verification = read_model.verify(run_id).await?;
            checked.insert(read_model.name().to_owned(), json!(verification.checked));
            differences.extend(verification.differences);
        }
        Ok(json!({
            "ok": differences.is_empty(),
            "checked": checked,
            "differences": differences,
        }))
    }
}
