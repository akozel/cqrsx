//! Causation metadata: who caused a command, carried by its events and by the tasks it publishes.
//!
//! A chain starts with a [`root`](Causation::root) command whose three ids are equal. Every task
//! the command's planner plans carries the ids: the core stamps them ([`stamp_task`](Causation::stamp_task))
//! and derives the task's id from the command's ([`task_id`](Causation::task_id)). A command issued
//! while handling such a task continues the chain ([`from_task`](Causation::from_task)): same
//! correlation id, caused by the task. Filtering events and tasks by the correlation id shows the
//! whole chain, across aggregates and services that share these keys.
use crate::task::{NewTask, attempt::TaskAttempt, format::TaskFormat};
use serde_json::{Map, Value};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

/// Metadata key of the command's own id.
pub const COMMAND_ID: &str = "command_id";
/// Metadata key of the id shared by every command, event and task of one chain.
pub const CORRELATION_ID: &str = "correlation_id";
/// Metadata key of the direct cause: the command itself for a root, otherwise the task.
pub const CAUSATION_ID: &str = "causation_id";
/// Event metadata key of the origin: chosen by the application for a root, `task:<type>` otherwise.
pub const SOURCE: &str = "source";

/// The namespace of the task ids the core derives ([`Causation::task_id`]): part of the id contract, never
/// changed.
pub const TASK_ID_NAMESPACE: Uuid = Uuid::from_u128(0x54d8_4115_fda8_4ff1_bc65_dd24_f9a6_a084);

/// Who caused a command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Causation {
    pub command_id: Uuid,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub source: String,
}

/// Metadata that names a command ([`COMMAND_ID`]) without carrying its causation.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum CausationError {
    #[error("causation key `{0}` is missing")]
    Missing(&'static str),
    #[error("causation key `{0}` is not a UUID")]
    NotUuid(&'static str),
}

impl Causation {
    /// A new chain: all three ids are one fresh, time-ordered (v7) command id.
    pub fn root(source: impl Into<String>) -> Self {
        let command_id = Uuid::now_v7();
        Self {
            command_id,
            correlation_id: command_id,
            causation_id: command_id,
            source: source.into(),
        }
    }

    /// A command issued while handling `attempt`: inherits the task's correlation id, is caused by
    /// the task and is sourced `task:<type>`. A task without a valid correlation id (made by hand,
    /// or by a producer outside the convention) starts its chain with its own task id.
    pub fn from_task(attempt: &TaskAttempt) -> Self {
        let correlation_id = attempt
            .metadata()
            .get(CORRELATION_ID)
            .and_then(Value::as_str)
            .and_then(|id| Uuid::parse_str(id).ok())
            .unwrap_or(attempt.task_id());
        Self {
            command_id: Uuid::now_v7(),
            correlation_id,
            causation_id: attempt.task_id(),
            source: format!("task:{}", attempt.format().task_type()),
        }
    }

    /// The causation in a command's metadata, the inverse of [`event_metadata`](Self::event_metadata).
    /// Metadata without [`COMMAND_ID`] has none; with it, the three ids must be UUIDs. [`SOURCE`] is
    /// optional and empty when absent.
    pub fn from_event_metadata(metadata: &HashMap<String, String>) -> Result<Option<Self>, CausationError> {
        if !metadata.contains_key(COMMAND_ID) {
            return Ok(None);
        }
        let id = |key: &'static str| {
            let value = metadata.get(key).ok_or(CausationError::Missing(key))?;
            Uuid::parse_str(value).map_err(|_| CausationError::NotUuid(key))
        };
        Ok(Some(Self {
            command_id: id(COMMAND_ID)?,
            correlation_id: id(CORRELATION_ID)?,
            causation_id: id(CAUSATION_ID)?,
            source: metadata.get(SOURCE).cloned().unwrap_or_default(),
        }))
    }

    /// The four keys for [`execute_with_metadata`](crate::cqrs::command::CommandExecutor::execute_with_metadata);
    /// applications may add their own.
    pub fn event_metadata(&self) -> HashMap<String, String> {
        HashMap::from([
            (COMMAND_ID.to_owned(), self.command_id.to_string()),
            (CORRELATION_ID.to_owned(), self.correlation_id.to_string()),
            (CAUSATION_ID.to_owned(), self.causation_id.to_string()),
            (SOURCE.to_owned(), self.source.clone()),
        ])
    }

    /// `task` with the three ids in its metadata, replacing only those keys. The core calls it on
    /// every planned task, after the planner's [`NewTask::with_metadata`], which replaces the whole
    /// object. The source is not stamped: the task's format already says what it is.
    #[must_use = "the stamped task is the one returned"]
    pub fn stamp_task(&self, mut task: NewTask) -> NewTask {
        task.merge_metadata(Map::from_iter([
            (COMMAND_ID.to_owned(), Value::String(self.command_id.to_string())),
            (CORRELATION_ID.to_owned(), Value::String(self.correlation_id.to_string())),
            (CAUSATION_ID.to_owned(), Value::String(self.causation_id.to_string())),
        ]));
        task
    }

    /// The id of the `ordinal`-th task of `format` (counted from 0) this command plans on one aggregate:
    /// a UUID v5 over [`TASK_ID_NAMESPACE`], the aggregate type and id, the command id, the format and
    /// the ordinal. Every attempt of the command plans the same ids, and the same command id on the same
    /// aggregate a second time collides with the first one's tasks in the outbox.
    #[must_use]
    pub fn task_id(&self, aggregate_type: &str, aggregate_id: &str, format: &TaskFormat, ordinal: usize) -> Uuid {
        // Every variable-length field is length-prefixed, so no two inputs share a name.
        let mut name = Vec::with_capacity(64 + aggregate_type.len() + aggregate_id.len() + format.task_type().len());
        for field in [aggregate_type, aggregate_id] {
            name.extend_from_slice(&(field.len() as u64).to_be_bytes());
            name.extend_from_slice(field.as_bytes());
        }
        name.extend_from_slice(self.command_id.as_bytes());
        name.extend_from_slice(&(format.task_type().len() as u64).to_be_bytes());
        name.extend_from_slice(format.task_type().as_bytes());
        name.extend_from_slice(&format.task_version().to_be_bytes());
        name.extend_from_slice(&(ordinal as u64).to_be_bytes());
        uuid_v5(&TASK_ID_NAMESPACE, &name)
    }
}

/// RFC 9562 version 5, as `Uuid::new_v5` computes it: the first 16 bytes of SHA-1 over the namespace and
/// the name, with the version and variant bits set.
fn uuid_v5(namespace: &Uuid, name: &[u8]) -> Uuid {
    let mut hash = Sha1::new();
    hash.update(namespace.as_bytes());
    hash.update(name);
    let digest: [u8; 20] = hash.finalize().into();
    let [bytes @ .., _, _, _, _] = digest;
    uuid::Builder::from_sha1_bytes(bytes).into_uuid()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::format::{TaskFormat, TaskPayload};
    use proptest::prelude::*;
    use serde_json::json;

    #[derive(serde::Serialize)]
    struct Ship {}

    impl TaskPayload for Ship {
        const TASK_TYPE: &'static str = "ship_order";
        const TASK_VERSION: i32 = 1;
    }

    fn attempt(metadata: Value) -> TaskAttempt {
        TaskAttempt::new(Uuid::new_v4(), TaskFormat::of::<Ship>().unwrap(), json!({}), metadata, 2).unwrap()
    }

    #[test]
    fn a_root_uses_one_time_ordered_id_for_all_three_roles() {
        let root = Causation::root("ui");
        assert_eq!(root.command_id.get_version_num(), 7);
        assert_eq!((root.correlation_id, root.causation_id), (root.command_id, root.command_id));
        assert_eq!(root.source, "ui");
        assert_ne!(Causation::root("ui").command_id, root.command_id);
    }

    #[test]
    fn a_command_from_a_task_inherits_the_correlation_and_is_caused_by_the_task() {
        let correlation_id = Uuid::now_v7();
        let task = attempt(json!({ CORRELATION_ID: correlation_id.to_string(), "tenant": "t-1" }));
        let causation = Causation::from_task(&task);
        assert_eq!(causation.correlation_id, correlation_id);
        assert_eq!(causation.causation_id, task.task_id());
        assert_eq!(causation.source, "task:ship_order");
        assert_eq!(causation.command_id.get_version_num(), 7);
        assert_ne!(causation.command_id, correlation_id);
        assert_ne!(causation.command_id, task.task_id());
    }

    #[test]
    fn a_task_without_a_valid_correlation_id_starts_the_chain_with_its_own_id() {
        for metadata in [
            json!({}),
            json!({ CORRELATION_ID: 42 }),
            json!({ CORRELATION_ID: "not-a-uuid" }),
        ] {
            let task = attempt(metadata);
            let causation = Causation::from_task(&task);
            assert_eq!(
                (causation.correlation_id, causation.causation_id),
                (task.task_id(), task.task_id())
            );
        }
    }

    #[test]
    fn events_get_exactly_the_four_keys() {
        let causation = Causation::root("seed");
        let id = causation.command_id.to_string();
        assert_eq!(
            causation.event_metadata(),
            HashMap::from([
                ("command_id".to_owned(), id.clone()),
                ("correlation_id".to_owned(), id.clone()),
                ("causation_id".to_owned(), id),
                ("source".to_owned(), "seed".to_owned()),
            ])
        );
    }

    #[test]
    fn stamping_a_task_replaces_only_the_three_ids_and_keeps_application_keys() {
        let causation = Causation::from_task(&attempt(json!({})));
        let task = NewTask::new(Ship {})
            .unwrap()
            .with_metadata(json!({ "order_id": "o-7", "command_id": "stale", "source": "app" }))
            .unwrap();
        let id = task.id();
        let stamped = causation.stamp_task(task);
        assert_eq!(stamped.id(), id);
        assert_eq!(
            stamped.metadata(),
            &json!({
                "order_id": "o-7",
                "source": "app",
                "command_id": causation.command_id.to_string(),
                "correlation_id": causation.correlation_id.to_string(),
                "causation_id": causation.causation_id.to_string(),
            })
        );
    }

    #[test]
    fn the_causation_round_trips_through_event_metadata() {
        let causation = Causation::from_task(&attempt(json!({ CORRELATION_ID: Uuid::now_v7().to_string() })));
        let mut metadata = causation.event_metadata();
        metadata.insert("recorded_at".to_owned(), "2026-10-26T10:00:00.000000Z".to_owned());
        assert_eq!(Causation::from_event_metadata(&metadata), Ok(Some(causation)));
    }

    #[test]
    fn metadata_names_a_command_only_with_three_valid_ids() {
        assert_eq!(Causation::from_event_metadata(&HashMap::new()), Ok(None));
        assert_eq!(
            Causation::from_event_metadata(&HashMap::from([("source".to_owned(), "ui".to_owned())])),
            Ok(None),
            "no command id, no causation"
        );
        let full = Causation::root("ui").event_metadata();
        for key in [COMMAND_ID, CORRELATION_ID, CAUSATION_ID] {
            if key != COMMAND_ID {
                let mut missing = full.clone();
                missing.remove(key);
                assert_eq!(Causation::from_event_metadata(&missing), Err(CausationError::Missing(key)));
            }
            let mut malformed = full.clone();
            malformed.insert(key.to_owned(), "cmd-1".to_owned());
            assert_eq!(Causation::from_event_metadata(&malformed), Err(CausationError::NotUuid(key)));
        }
        let mut without_source = full.clone();
        without_source.remove(SOURCE);
        assert_eq!(
            Causation::from_event_metadata(&without_source).unwrap().map(|c| c.source),
            Some(String::new())
        );
        assert_eq!(
            CausationError::NotUuid(COMMAND_ID).to_string(),
            "causation key `command_id` is not a UUID"
        );
        assert_eq!(
            CausationError::Missing(CAUSATION_ID).to_string(),
            "causation key `causation_id` is missing"
        );
    }

    #[test]
    fn uuid_v5_matches_the_fixtures_of_the_uuid_crate() {
        assert_eq!(
            uuid_v5(&Uuid::NAMESPACE_DNS, b"example.org").to_string(),
            "aad03681-8b63-5304-89e0-8ca8f49461b5"
        );
        assert_eq!(
            uuid_v5(&Uuid::NAMESPACE_DNS, b"rust-lang.org").to_string(),
            "c66bbb60-d62e-5f17-a399-3a0bd237c503"
        );
    }

    fn causation(command_id: u128) -> Causation {
        Causation {
            command_id: Uuid::from_u128(command_id),
            correlation_id: Uuid::nil(),
            causation_id: Uuid::nil(),
            source: String::new(),
        }
    }

    #[test]
    fn task_ids_are_stable_v5_and_change_with_every_input() {
        let format = TaskFormat::new("ship_order", 1).unwrap();
        let id = causation(7).task_id("order", "o-1", &format, 0);
        assert_eq!(
            id,
            causation(7).task_id("order", "o-1", &format, 0),
            "the same inputs, the same id"
        );
        assert_eq!(id.get_version_num(), 5);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(
            id.to_string(),
            "869d67a2-dc88-5dcc-a3a3-37962d8ab1a1",
            "the derivation is a contract: ids of queued tasks must not change"
        );
        let others = [
            causation(7).task_id("invoice", "o-1", &format, 0),
            causation(7).task_id("order", "o-2", &format, 0),
            causation(8).task_id("order", "o-1", &format, 0),
            causation(7).task_id("order", "o-1", &TaskFormat::new("ship_parcel", 1).unwrap(), 0),
            causation(7).task_id("order", "o-1", &TaskFormat::new("ship_order", 2).unwrap(), 0),
            causation(7).task_id("order", "o-1", &format, 1),
        ];
        for other in others {
            assert_ne!(other, id);
        }
        // The correlation, the cause and the source are not part of the id.
        let mut relabelled = causation(7);
        (relabelled.correlation_id, relabelled.source) = (Uuid::now_v7(), "ui".to_owned());
        assert_eq!(relabelled.task_id("order", "o-1", &format, 0), id);
    }

    #[test]
    fn the_task_id_name_cannot_shift_between_fields() {
        let format = TaskFormat::new("ship_order", 1).unwrap();
        assert_ne!(
            causation(7).task_id("ab", "c", &format, 0),
            causation(7).task_id("a", "bc", &format, 0)
        );
        assert_ne!(
            causation(7).task_id("", "abc", &format, 0),
            causation(7).task_id("abc", "", &format, 0)
        );
    }

    proptest! {
        #[test]
        fn distinct_task_inputs_give_distinct_ids(
            left in ("[a-z]{0,6}", "[a-z0-9:-]{0,12}", any::<u128>(), "[a-z_]{1,8}", 1..4_i32, 0..4_usize),
            right in ("[a-z]{0,6}", "[a-z0-9:-]{0,12}", any::<u128>(), "[a-z_]{1,8}", 1..4_i32, 0..4_usize),
        ) {
            let id = |(aggregate_type, aggregate_id, command_id, task_type, version, ordinal): &(String, String, u128, String, i32, usize)| {
                causation(*command_id).task_id(aggregate_type, aggregate_id, &TaskFormat::new(task_type.clone(), *version).unwrap(), *ordinal)
            };
            prop_assert_eq!(id(&left) == id(&right), left == right);
        }
    }
}
