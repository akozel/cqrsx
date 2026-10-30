//! The checkpoints of projections in MongoDB: one document per projection and aggregate type in
//! [`PROJECTION_CHECKPOINTS_COLLECTION`], the keys requested for a rebuild inside it. Every operation is one
//! atomic statement on that document; the writes of an owner are fenced by its lease token, and the lease
//! compares against the clock of the server (`$$NOW`).
use crate::{
    cqrs::{
        projection::checkpoint::{
            CheckpointError, CheckpointKey, CheckpointStatus, CheckpointStore, Claimed, LeaseToken, RebuildRequest,
            RebuildRequests, RebuildState,
        },
        store::feed::Position,
    },
    diagnostics,
    mongodb::is_duplicate_key,
};
use ::mongodb::{
    Collection, Database,
    bson::{self, Bson, Document, doc},
    options::ReturnDocument,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{error::Error, time::Duration};
use uuid::Uuid;

/// The collection of the checkpoint documents in the read database.
pub const PROJECTION_CHECKPOINTS_COLLECTION: &str = "projection_checkpoints";

/// The checkpoint store over one read database: the documents live in [`PROJECTION_CHECKPOINTS_COLLECTION`],
/// next to the read models they describe.
///
/// ```text
/// { _id: { projection, aggregate_type },
///   position, applied, rebuild_requested, rebuild_pace_ms, rebuild_cursor, rebuilt,
///   owner, lock_token, locked_until, failures, last_error, updated_at, rebuild_keys: [aggregate ids] }
/// ```
///
/// Numbers are `Int64`, `lock_token` a UUID string, `locked_until` and `updated_at` dates by the server's
/// clock. The rules are those of `PostgresCheckpointStore` with one difference: a rebuild cursor is accepted
/// again only when it is the same cursor (the journal orders the keys, the document cannot), so a page
/// repeated after a lost reply is counted once and never refused. Needs MongoDB 4.2 or later.
pub struct MongoCheckpointStore {
    collection: Collection<Document>,
}

impl MongoCheckpointStore {
    pub fn new(database: &Database) -> Self {
        Self {
            collection: database.collection(PROJECTION_CHECKPOINTS_COLLECTION),
        }
    }
}

/// A stored checkpoint as the owner and the status read it.
#[derive(Debug, Deserialize)]
struct Row {
    #[serde(rename = "_id")]
    id: RowId,
    position: i64,
    applied: i64,
    rebuild_requested: bool,
    rebuild_pace_ms: i64,
    rebuild_cursor: Option<String>,
    rebuilt: i64,
    owner: Option<String>,
    lock_token: Option<String>,
    locked_until: Option<bson::DateTime>,
    failures: i64,
    last_error: Option<String>,
    updated_at: bson::DateTime,
}

#[derive(Debug, Deserialize)]
struct RowId {
    projection: String,
    aggregate_type: String,
}

/// The requests a take found on the document before it removed the taken keys.
#[derive(Debug, Deserialize)]
struct Requests {
    rebuild_requested: bool,
    rebuild_pace_ms: i64,
    rebuild_keys: Vec<String>,
}

/// The rebuild state a request found.
#[derive(Debug, Deserialize)]
struct Requested {
    rebuild_requested: bool,
    rebuild_cursor: Option<String>,
}

fn storage(operation: &'static str) -> impl FnOnce(::mongodb::error::Error) -> CheckpointError {
    move |error| {
        diagnostics::log_mongodb_failure(operation, &error);
        CheckpointError::Storage(Box::new(error))
    }
}

fn corrupt(error: impl Error + Send + Sync + 'static) -> CheckpointError {
    CheckpointError::Corrupt(Box::new(error))
}

/// The `_id` of a key's document; the field order is part of the identity.
fn id(key: &CheckpointKey) -> Document {
    doc! { "projection": key.projection(), "aggregate_type": key.aggregate_type() }
}

fn row(key: &CheckpointKey) -> Document {
    doc! { "_id": id(key) }
}

fn owned(key: &CheckpointKey, token: LeaseToken) -> Document {
    doc! { "_id": id(key), "lock_token": token.value().to_string() }
}

/// A value an update pipeline must not read as an expression (an id may start with `$`).
fn literal(value: &str) -> Document {
    doc! { "$literal": value }
}

fn count_in(value: u64, what: &'static str) -> Result<i64, CheckpointError> {
    i64::try_from(value).map_err(|_| CheckpointError::Invalid(what))
}

fn lease_millis(lease: Duration) -> Result<i64, CheckpointError> {
    i64::try_from(lease.as_millis()).map_err(|_| CheckpointError::Invalid("lease does not fit the row"))
}

/// A pace in the range of the PostgreSQL row, so both stores accept the same requests.
fn pace_millis(pace: Duration) -> Result<i64, CheckpointError> {
    i32::try_from(pace.as_millis())
        .map(i64::from)
        .map_err(|_| CheckpointError::Invalid("rebuild pace does not fit the row"))
}

/// `$slice` takes 32-bit operands.
fn limit_in(limit: usize) -> Result<i32, CheckpointError> {
    i32::try_from(limit).map_err(|_| CheckpointError::Invalid("request limit does not fit the row"))
}

fn count_out(value: i64) -> Result<u64, CheckpointError> {
    u64::try_from(value).map_err(corrupt)
}

fn failures_out(value: i64) -> Result<u32, CheckpointError> {
    u32::try_from(value).map_err(corrupt)
}

fn pace_out(millis: i64) -> Result<Duration, CheckpointError> {
    count_out(millis).map(Duration::from_millis)
}

fn date_out(date: bson::DateTime) -> Result<DateTime<Utc>, CheckpointError> {
    DateTime::from_timestamp_millis(date.timestamp_millis()).ok_or_else(|| CheckpointError::Corrupt("date out of range".into()))
}

fn token_out(token: Option<String>) -> Result<LeaseToken, CheckpointError> {
    let token = token.ok_or_else(|| CheckpointError::Corrupt("a claimed row has no lease token".into()))?;
    Uuid::parse_str(&token).map(LeaseToken::new).map_err(corrupt)
}

fn decode<T: serde::de::DeserializeOwned>(document: Document) -> Result<T, CheckpointError> {
    bson::from_document(document).map_err(corrupt)
}

fn decode_claim(document: Document) -> Result<Claimed, CheckpointError> {
    let row: Row = decode(document)?;
    let rebuild = match row.rebuild_cursor {
        Some(cursor) => Some(RebuildState {
            cursor,
            pace: pace_out(row.rebuild_pace_ms)?,
            rebuilt: count_out(row.rebuilt)?,
        }),
        None => None,
    };
    Ok(Claimed {
        token: token_out(row.lock_token)?,
        position: Position::new(count_out(row.position)?),
        failures: failures_out(row.failures)?,
        rebuild,
    })
}

fn decode_status(document: Document) -> Result<CheckpointStatus, CheckpointError> {
    let row: Row = decode(document)?;
    Ok(CheckpointStatus {
        projection: row.id.projection,
        aggregate_type: row.id.aggregate_type,
        position: Position::new(count_out(row.position)?),
        applied: count_out(row.applied)?,
        rebuild_requested: row.rebuild_requested,
        rebuild_pace: pace_out(row.rebuild_pace_ms)?,
        rebuild_cursor: row.rebuild_cursor,
        rebuilt: count_out(row.rebuilt)?,
        owner: row.owner,
        locked_until: row.locked_until.map(date_out).transpose()?,
        failures: failures_out(row.failures)?,
        last_error: row.last_error,
        updated_at: date_out(row.updated_at)?,
    })
}

/// Every field of a new document; an existing document keeps what it has.
fn defaults() -> Document {
    let mut fields = Document::new();
    for (field, default) in [
        ("position", Bson::Int64(0)),
        ("applied", Bson::Int64(0)),
        ("rebuild_requested", Bson::Boolean(false)),
        ("rebuild_pace_ms", Bson::Int64(0)),
        ("rebuild_cursor", Bson::Null),
        ("rebuilt", Bson::Int64(0)),
        ("owner", Bson::Null),
        ("lock_token", Bson::Null),
        ("locked_until", Bson::Null),
        ("failures", Bson::Int64(0)),
        ("last_error", Bson::Null),
        ("rebuild_keys", Bson::Array(Vec::new())),
        ("updated_at", Bson::String("$$NOW".into())),
    ] {
        fields.insert(field, doc! { "$ifNull": [format!("${field}"), default] });
    }
    fields
}

impl MongoCheckpointStore {
    /// One fenced write: `true` when the document was the owner's.
    async fn fenced(&self, operation: &'static str, filter: Document, update: Document) -> Result<bool, CheckpointError> {
        let result = self
            .collection
            .update_one(filter, vec![doc! { "$set": update }])
            .await
            .map_err(storage(operation))?;
        Ok(result.matched_count == 1)
    }
}

impl CheckpointStore for MongoCheckpointStore {
    async fn register(&self, key: &CheckpointKey) -> Result<(), CheckpointError> {
        match self
            .collection
            .update_one(row(key), vec![doc! { "$set": defaults() }])
            .upsert(true)
            .await
        {
            // Two nodes may race to create the document: either way it exists.
            Err(error) if !is_duplicate_key(&error) => Err(storage("register")(error)),
            _ => Ok(()),
        }
    }

    async fn claim(&self, key: &CheckpointKey, owner: &str, lease: Duration) -> Result<Option<Claimed>, CheckpointError> {
        let lease = lease_millis(lease)?;
        let free = doc! {
            "_id": id(key),
            "$or": [{ "locked_until": null }, { "$expr": { "$lt": ["$locked_until", "$$NOW"] } }],
        };
        let claim = doc! {
            "owner": literal(owner),
            "lock_token": literal(&Uuid::new_v4().to_string()),
            "locked_until": { "$add": ["$$NOW", lease] },
            "updated_at": "$$NOW",
        };
        let claimed = self
            .collection
            .find_one_and_update(free, vec![doc! { "$set": claim }])
            .projection(doc! { "rebuild_keys": 0 })
            .return_document(ReturnDocument::After)
            .await
            .map_err(storage("claim"))?;
        claimed.map(decode_claim).transpose()
    }

    async fn extend(&self, key: &CheckpointKey, token: LeaseToken, lease: Duration) -> Result<bool, CheckpointError> {
        let lease = lease_millis(lease)?;
        self.fenced(
            "extend",
            owned(key, token),
            doc! { "locked_until": { "$add": ["$$NOW", lease] } },
        )
        .await
    }

    async fn advance(&self, key: &CheckpointKey, token: LeaseToken, to: Position, applied: u64) -> Result<bool, CheckpointError> {
        let to = count_in(to.value(), "position does not fit the row")?;
        let applied = count_in(applied, "applied count does not fit the row")?;
        let mut filter = owned(key, token);
        filter.insert("position", doc! { "$lte": to });
        let update = doc! {
            "applied": { "$add": ["$applied", { "$cond": [{ "$lt": ["$position", to] }, applied, 0_i64] }] },
            "position": to,
            "failures": 0_i64,
            "last_error": null,
            "updated_at": "$$NOW",
        };
        self.fenced("advance", filter, update).await
    }

    async fn record_failure(&self, key: &CheckpointKey, token: LeaseToken, error: &str) -> Result<bool, CheckpointError> {
        let update = doc! {
            "failures": { "$add": ["$failures", 1_i64] },
            "last_error": literal(error),
            "updated_at": "$$NOW",
        };
        self.fenced("record_failure", owned(key, token), update).await
    }

    async fn request_rebuild(&self, key: &CheckpointKey, pace: Duration) -> Result<RebuildRequest, CheckpointError> {
        let pace = pace_millis(pace)?;
        let request = doc! {
            "rebuild_requested": { "$or": ["$rebuild_requested", { "$eq": ["$rebuild_cursor", null] }] },
            "rebuild_pace_ms": {
                "$cond": [{ "$or": ["$rebuild_requested", { "$ne": ["$rebuild_cursor", null] }] }, "$rebuild_pace_ms", pace]
            },
            "updated_at": "$$NOW",
        };
        let before = self
            .collection
            .find_one_and_update(row(key), vec![doc! { "$set": request }])
            .projection(doc! { "rebuild_requested": 1, "rebuild_cursor": 1 })
            .return_document(ReturnDocument::Before)
            .await
            .map_err(storage("request_rebuild"))?;
        let Some(before) = before else {
            return Err(CheckpointError::Invalid("projection is not registered"));
        };
        let before: Requested = decode(before)?;
        Ok(if !before.rebuild_requested && before.rebuild_cursor.is_none() {
            RebuildRequest::Accepted
        } else {
            RebuildRequest::AlreadyRunning
        })
    }

    async fn request_rebuild_key(&self, key: &CheckpointKey, aggregate_id: &str) -> Result<(), CheckpointError> {
        let result = self
            .collection
            .update_one(row(key), doc! { "$addToSet": { "rebuild_keys": aggregate_id } })
            .await
            .map_err(storage("request_rebuild_key"))?;
        if result.matched_count == 0 {
            return Err(CheckpointError::Invalid("projection is not registered"));
        }
        Ok(())
    }

    async fn take_requests(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        limit: usize,
    ) -> Result<Option<RebuildRequests>, CheckpointError> {
        let slice = limit_in(limit)?;
        let rest = doc! {
            "$cond": [
                { "$gt": [{ "$size": "$rebuild_keys" }, slice] },
                { "$slice": ["$rebuild_keys", slice, { "$subtract": [{ "$size": "$rebuild_keys" }, slice] }] },
                [],
            ]
        };
        let before = self
            .collection
            .find_one_and_update(owned(key, token), vec![doc! { "$set": { "rebuild_keys": rest } }])
            .projection(doc! { "rebuild_requested": 1, "rebuild_pace_ms": 1, "rebuild_keys": 1 })
            .return_document(ReturnDocument::Before)
            .await
            .map_err(storage("take_requests"))?;
        let Some(before) = before else {
            return Ok(None);
        };
        let before: Requests = decode(before)?;
        let pace = pace_out(before.rebuild_pace_ms)?;
        Ok(Some(RebuildRequests {
            rebuild: before.rebuild_requested.then_some(pace),
            keys: before.rebuild_keys.into_iter().take(limit).collect(),
        }))
    }

    async fn begin_rebuild(&self, key: &CheckpointKey, token: LeaseToken) -> Result<bool, CheckpointError> {
        let update = doc! { "rebuild_requested": false, "rebuild_cursor": literal(""), "rebuilt": 0_i64, "updated_at": "$$NOW" };
        self.fenced("begin_rebuild", owned(key, token), update).await
    }

    async fn advance_rebuild(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        cursor: &str,
        keys: u64,
    ) -> Result<bool, CheckpointError> {
        let keys = count_in(keys, "rebuilt count does not fit the row")?;
        let mut filter = owned(key, token);
        filter.insert("rebuild_cursor", doc! { "$ne": null });
        let update = doc! {
            "rebuilt": { "$add": ["$rebuilt", { "$cond": [{ "$ne": ["$rebuild_cursor", literal(cursor)] }, keys, 0_i64] }] },
            "rebuild_cursor": literal(cursor),
            "updated_at": "$$NOW",
        };
        self.fenced("advance_rebuild", filter, update).await
    }

    async fn finish_rebuild(&self, key: &CheckpointKey, token: LeaseToken) -> Result<bool, CheckpointError> {
        self.fenced(
            "finish_rebuild",
            owned(key, token),
            doc! { "rebuild_cursor": null, "updated_at": "$$NOW" },
        )
        .await
    }

    async fn release(&self, key: &CheckpointKey, token: LeaseToken) -> Result<(), CheckpointError> {
        let update = doc! { "owner": null, "lock_token": null, "locked_until": null, "updated_at": "$$NOW" };
        self.fenced("release", owned(key, token), update).await?;
        Ok(())
    }

    async fn status(&self, projection: Option<&str>) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        let filter = projection.map_or_else(Document::new, |projection| doc! { "_id.projection": projection });
        let mut cursor = self
            .collection
            .find(filter)
            .sort(doc! { "_id": 1 })
            .projection(doc! { "rebuild_keys": 0 })
            .await
            .map_err(storage("status"))?;
        let mut rows = Vec::new();
        while cursor.advance().await.map_err(storage("status"))? {
            rows.push(decode_status(cursor.deserialize_current().map_err(corrupt)?)?);
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored() -> Document {
        doc! {
            "_id": { "projection": "halls", "aggregate_type": "Concert" },
            "position": 9_i64, "applied": 7_i64,
            "rebuild_requested": false, "rebuild_pace_ms": 80_i64, "rebuild_cursor": "c3", "rebuilt": 3_i64,
            "owner": "node-a", "lock_token": "00000000-0000-0000-0000-000000000007",
            "locked_until": bson::DateTime::from_millis(2_000), "failures": 2_i64, "last_error": "halls: connection",
            "updated_at": bson::DateTime::from_millis(1_000), "rebuild_keys": ["c9"],
        }
    }

    fn with(field: &str, value: impl Into<Bson>) -> Document {
        let mut document = stored();
        document.insert(field, value.into());
        document
    }

    #[test]
    fn values_that_do_not_fit_are_invalid_and_values_out_of_range_are_corrupt() {
        assert!(matches!(
            count_in(u64::MAX, "too big"),
            Err(CheckpointError::Invalid("too big"))
        ));
        assert_eq!(count_in(7, "x").unwrap(), 7);
        assert!(matches!(
            lease_millis(Duration::MAX),
            Err(CheckpointError::Invalid("lease does not fit the row"))
        ));
        assert_eq!(lease_millis(Duration::from_millis(1500)).unwrap(), 1500);
        assert!(matches!(
            pace_millis(Duration::from_millis(u64::from(u32::MAX))),
            Err(CheckpointError::Invalid("rebuild pace does not fit the row"))
        ));
        assert_eq!(pace_millis(Duration::from_millis(80)).unwrap(), 80);
        assert!(matches!(
            limit_in(usize::MAX),
            Err(CheckpointError::Invalid("request limit does not fit the row"))
        ));
        assert_eq!(limit_in(100).unwrap(), 100);
        assert!(matches!(count_out(-1), Err(CheckpointError::Corrupt(_))));
        assert!(matches!(failures_out(-1), Err(CheckpointError::Corrupt(_))));
        assert!(matches!(failures_out(i64::MAX), Err(CheckpointError::Corrupt(_))));
        assert!(matches!(pace_out(-1), Err(CheckpointError::Corrupt(_))));
        assert!(matches!(date_out(bson::DateTime::MAX), Err(CheckpointError::Corrupt(_))));
        assert_eq!(
            (count_out(3).unwrap(), failures_out(2).unwrap(), pace_out(5).unwrap()),
            (3, 2, Duration::from_millis(5))
        );
        assert_eq!(
            date_out(bson::DateTime::from_millis(1_000)).unwrap().timestamp_millis(),
            1_000
        );
        assert!(matches!(token_out(None), Err(CheckpointError::Corrupt(_))));
        assert!(matches!(token_out(Some("x".into())), Err(CheckpointError::Corrupt(_))));
    }

    #[test]
    fn a_stored_document_reads_as_a_claim_and_as_a_status() {
        let claimed = decode_claim(stored()).unwrap();
        assert_eq!(
            claimed,
            Claimed {
                token: LeaseToken::new(Uuid::from_u128(7)),
                position: Position::new(9),
                failures: 2,
                rebuild: Some(RebuildState {
                    cursor: "c3".into(),
                    pace: Duration::from_millis(80),
                    rebuilt: 3
                }),
            }
        );
        assert_eq!(decode_claim(with("rebuild_cursor", Bson::Null)).unwrap().rebuild, None);
        let status = decode_status(stored()).unwrap();
        assert_eq!(
            (
                status.projection.as_str(),
                status.aggregate_type.as_str(),
                status.position,
                status.applied
            ),
            ("halls", "Concert", Position::new(9), 7)
        );
        assert_eq!(
            (
                status.rebuild_requested,
                status.rebuild_pace,
                status.rebuild_cursor.as_deref(),
                status.rebuilt
            ),
            (false, Duration::from_millis(80), Some("c3"), 3)
        );
        assert_eq!(
            (status.owner.as_deref(), status.failures, status.last_error.as_deref()),
            (Some("node-a"), 2, Some("halls: connection"))
        );
        assert_eq!(status.locked_until.map(|date| date.timestamp_millis()), Some(2_000));
        assert_eq!(status.updated_at.timestamp_millis(), 1_000);
        let free = decode_status(with("locked_until", Bson::Null)).unwrap();
        assert_eq!(free.locked_until, None);
    }

    #[test]
    fn a_document_the_store_cannot_read_is_corrupt() {
        let unreadable = [
            with("position", -1_i64),
            with("position", "9"),
            with("rebuilt", -1_i64),
            with("rebuild_pace_ms", -1_i64),
            with("failures", -1_i64),
        ];
        let status_only = [
            with("applied", -1_i64),
            with("updated_at", "yesterday"),
            with("updated_at", bson::DateTime::MAX),
            with("locked_until", bson::DateTime::MAX),
        ];
        for broken in unreadable.iter().chain(&status_only) {
            assert!(
                matches!(decode_status(broken.clone()), Err(CheckpointError::Corrupt(_))),
                "{broken}"
            );
        }
        let claim_only = [with("lock_token", "not a uuid"), with("lock_token", Bson::Null)];
        for broken in unreadable.iter().chain(&claim_only) {
            assert!(
                matches!(decode_claim(broken.clone()), Err(CheckpointError::Corrupt(_))),
                "{broken}"
            );
        }
        assert!(matches!(
            decode::<Requests>(doc! { "rebuild_requested": true }),
            Err(CheckpointError::Corrupt(_))
        ));
    }

    #[tokio::test]
    async fn an_unreachable_server_fails_every_operation_with_a_logged_storage_error() {
        let store = MongoCheckpointStore::new(&crate::mongodb::test_support::unreachable().await);
        let key = CheckpointKey::new("halls", "Concert");
        let token = LeaseToken::new(Uuid::new_v4());
        let (_, log) = diagnostics::test_support::capture(async {
            let errors = [
                store.register(&key).await.err(),
                store.claim(&key, "a", Duration::from_secs(1)).await.err(),
                store.extend(&key, token, Duration::from_secs(1)).await.err(),
                store.advance(&key, token, Position::new(1), 1).await.err(),
                store.record_failure(&key, token, "halls: connection").await.err(),
                store.request_rebuild(&key, Duration::ZERO).await.err(),
                store.request_rebuild_key(&key, "k").await.err(),
                store.take_requests(&key, token, 1).await.err(),
                store.begin_rebuild(&key, token).await.err(),
                store.advance_rebuild(&key, token, "k", 1).await.err(),
                store.finish_rebuild(&key, token).await.err(),
                store.release(&key, token).await.err(),
                store.status(None).await.err(),
            ];
            assert!(
                errors.iter().all(|error| matches!(error, Some(CheckpointError::Storage(_)))),
                "{errors:?}"
            );
            // Checked before any statement: no log line for these.
            for invalid in [
                store.claim(&key, "a", Duration::MAX).await.err(),
                store.extend(&key, token, Duration::MAX).await.err(),
                store.advance(&key, token, Position::new(u64::MAX), 1).await.err(),
                store.advance(&key, token, Position::new(1), u64::MAX).await.err(),
                store.request_rebuild(&key, Duration::MAX).await.err(),
                store.take_requests(&key, token, usize::MAX).await.err(),
                store.advance_rebuild(&key, token, "k", u64::MAX).await.err(),
            ] {
                assert!(matches!(invalid, Some(CheckpointError::Invalid(_))), "{invalid:?}");
            }
        })
        .await;
        assert_eq!(log.matches("storage operation failed").count(), 13, "{log}");
        for operation in [
            "register",
            "claim",
            "extend",
            "advance",
            "record_failure",
            "request_rebuild",
            "request_rebuild_key",
            "take_requests",
            "begin_rebuild",
            "advance_rebuild",
            "finish_rebuild",
            "release",
            "status",
        ] {
            assert!(log.contains(&format!("operation=\"{operation}\"")), "{operation}: {log}");
        }
        assert_eq!(log.matches("category=\"server_selection\"").count(), 13, "{log}");
    }
}
