//! `MongoCheckpointStore`: one atomic statement per operation, fenced by the lease token, by the server's
//! clock, and the documented document.
use crate::mongo_container::Mongo;
use cqrsx::{
    cqrs::{
        projection::checkpoint::{
            CheckpointError, CheckpointKey, CheckpointStore, LeaseToken, RebuildRequest, RebuildRequests, RebuildState,
        },
        store::feed::Position,
    },
    mongodb::checkpoint_store::{MongoCheckpointStore, PROJECTION_CHECKPOINTS_COLLECTION},
};
use mongodb::{
    Collection,
    bson::{Bson, Document, doc},
};
use std::time::Duration;
use uuid::Uuid;

fn key() -> CheckpointKey {
    CheckpointKey::new("halls", "Concert")
}

fn other() -> CheckpointKey {
    CheckpointKey::new("wallets", "Fan")
}

const LONG: Duration = Duration::from_secs(60);

fn documents(mongo: &Mongo) -> Collection<Document> {
    mongo.database.collection(PROJECTION_CHECKPOINTS_COLLECTION)
}

async fn raw(mongo: &Mongo, key: &CheckpointKey) -> Document {
    documents(mongo)
        .find_one(doc! { "_id": { "projection": key.projection(), "aggregate_type": key.aggregate_type() } })
        .await
        .unwrap()
        .expect("a registered row")
}

#[tokio::test]
async fn a_row_is_claimed_by_one_owner_until_it_is_released_or_its_lease_runs_out() {
    let mongo = Mongo::new().await;
    let store = MongoCheckpointStore::new(&mongo.database);
    assert!(store.claim(&key(), "a", LONG).await.unwrap().is_none(), "no row yet");
    store.register(&key()).await.unwrap();
    store.register(&key()).await.unwrap();
    let first = store.claim(&key(), "a", LONG).await.unwrap().expect("a free row");
    assert_eq!((first.position, first.failures, first.rebuild), (Position::START, 0, None));
    assert!(store.claim(&key(), "b", LONG).await.unwrap().is_none(), "held by a");
    let stale = LeaseToken::new(Uuid::new_v4());
    assert!(!store.extend(&key(), stale, LONG).await.unwrap());
    assert!(!store.advance(&key(), stale, Position::new(1), 1).await.unwrap());
    assert!(!store.record_failure(&key(), stale, "halls: connection").await.unwrap());
    assert!(!store.begin_rebuild(&key(), stale).await.unwrap());
    assert!(!store.advance_rebuild(&key(), stale, "k", 1).await.unwrap());
    assert!(!store.finish_rebuild(&key(), stale).await.unwrap());
    assert!(store.take_requests(&key(), stale, 5).await.unwrap().is_none());
    store.release(&key(), stale).await.unwrap();
    assert!(
        store.claim(&key(), "b", LONG).await.unwrap().is_none(),
        "a stale release changes nothing"
    );
    assert!(store.extend(&key(), first.token, LONG).await.unwrap());
    store.release(&key(), first.token).await.unwrap();
    let second = store
        .claim(&key(), "b", Duration::from_secs(1))
        .await
        .unwrap()
        .expect("released");
    assert_ne!(first.token, second.token);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let third = store
        .claim(&key(), "c", LONG)
        .await
        .unwrap()
        .expect("expired by the server clock");
    assert!(
        !store.extend(&key(), second.token, LONG).await.unwrap(),
        "the expired owner is fenced out"
    );
    assert!(store.extend(&key(), third.token, LONG).await.unwrap());
    let rows = store.status(None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (
            rows[0].owner.as_deref(),
            rows[0].projection.as_str(),
            rows[0].aggregate_type.as_str()
        ),
        (Some("c"), "halls", "Concert")
    );
    assert!(rows[0].locked_until.unwrap() > rows[0].updated_at);
    mongo.close().await;
}

#[tokio::test]
async fn advance_moves_only_forward_counts_a_repeat_once_and_clears_the_failures() {
    let mongo = Mongo::new().await;
    let store = MongoCheckpointStore::new(&mongo.database);
    store.register(&key()).await.unwrap();
    let owner = store.claim(&key(), "a", LONG).await.unwrap().unwrap();
    assert!(store.record_failure(&key(), owner.token, "halls: connection").await.unwrap());
    assert!(store.record_failure(&key(), owner.token, "halls: panic").await.unwrap());
    let row = store.status(Some("halls")).await.unwrap().remove(0);
    assert_eq!((row.failures, row.last_error.as_deref()), (2, Some("halls: panic")));
    assert!(store.advance(&key(), owner.token, Position::new(5), 7).await.unwrap());
    assert!(
        store.advance(&key(), owner.token, Position::new(5), 7).await.unwrap(),
        "a repeat is accepted"
    );
    assert!(
        !store.advance(&key(), owner.token, Position::new(4), 1).await.unwrap(),
        "never back"
    );
    assert!(store.advance(&key(), owner.token, Position::new(9), 2).await.unwrap());
    let row = store.status(None).await.unwrap().remove(0);
    assert_eq!(
        (row.position, row.applied, row.failures, row.last_error),
        (Position::new(9), 9, 0, None)
    );
    store.release(&key(), owner.token).await.unwrap();
    let next = store.claim(&key(), "b", LONG).await.unwrap().unwrap();
    assert_eq!((next.position, next.failures), (Position::new(9), 0));
    assert!(store.status(Some("none")).await.unwrap().is_empty());
    mongo.close().await;
}

#[tokio::test]
async fn rebuild_requests_run_one_at_a_time_and_keys_are_taken_in_request_order_by_the_owner() {
    let mongo = Mongo::new().await;
    let store = MongoCheckpointStore::new(&mongo.database);
    assert!(matches!(
        store.request_rebuild(&key(), Duration::ZERO).await,
        Err(CheckpointError::Invalid("projection is not registered"))
    ));
    assert!(matches!(
        store.request_rebuild_key(&key(), "c1").await,
        Err(CheckpointError::Invalid("projection is not registered"))
    ));
    store.register(&key()).await.unwrap();
    store.register(&other()).await.unwrap();
    assert!(matches!(
        store
            .request_rebuild(&key(), Duration::from_millis(u64::from(u32::MAX)))
            .await,
        Err(CheckpointError::Invalid(_))
    ));
    assert_eq!(
        store.request_rebuild(&key(), Duration::from_millis(80)).await.unwrap(),
        RebuildRequest::Accepted
    );
    assert_eq!(
        store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::AlreadyRunning
    );
    for id in ["c2", "c1", "c2", "c3"] {
        store.request_rebuild_key(&key(), id).await.unwrap();
    }
    store.request_rebuild_key(&other(), "z").await.unwrap();
    let owner = store.claim(&key(), "a", LONG).await.unwrap().unwrap();
    let first = store.take_requests(&key(), owner.token, 2).await.unwrap().unwrap();
    assert_eq!(
        first,
        RebuildRequests {
            rebuild: Some(Duration::from_millis(80)),
            keys: vec!["c2".into(), "c1".into()]
        }
    );
    assert!(store.begin_rebuild(&key(), owner.token).await.unwrap());
    let rest = store.take_requests(&key(), owner.token, 2).await.unwrap().unwrap();
    assert_eq!(
        rest,
        RebuildRequests {
            rebuild: None,
            keys: vec!["c3".into()]
        }
    );
    assert_eq!(
        store.take_requests(&key(), owner.token, 2).await.unwrap().unwrap(),
        RebuildRequests::default(),
        "nothing left"
    );
    assert_eq!(
        store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::AlreadyRunning,
        "running"
    );
    assert!(store.advance_rebuild(&key(), owner.token, "c2", 2).await.unwrap());
    assert!(
        store.advance_rebuild(&key(), owner.token, "c2", 2).await.unwrap(),
        "a repeat counts once"
    );
    assert!(store.advance_rebuild(&key(), owner.token, "c3", 1).await.unwrap());
    let row = store.status(Some("halls")).await.unwrap().remove(0);
    assert_eq!(
        (
            row.rebuild_cursor.as_deref(),
            row.rebuilt,
            row.rebuild_requested,
            row.rebuild_pace
        ),
        (Some("c3"), 3, false, Duration::from_millis(80))
    );
    // The document does not know the journal's order of keys: another cursor is another page.
    assert!(store.advance_rebuild(&key(), owner.token, "b9", 1).await.unwrap());
    assert_eq!(
        store.status(Some("halls")).await.unwrap()[0].rebuilt,
        4,
        "counted as a new page"
    );
    assert!(store.advance_rebuild(&key(), owner.token, "c3", 0).await.unwrap());
    store.release(&key(), owner.token).await.unwrap();
    let next = store.claim(&key(), "b", LONG).await.unwrap().unwrap();
    assert_eq!(
        next.rebuild,
        Some(RebuildState {
            cursor: "c3".into(),
            pace: Duration::from_millis(80),
            rebuilt: 4
        }),
        "a new owner continues from the cursor"
    );
    assert!(store.finish_rebuild(&key(), next.token).await.unwrap());
    assert!(
        !store.advance_rebuild(&key(), next.token, "c4", 1).await.unwrap(),
        "no rebuild runs"
    );
    assert_eq!(
        store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    let wallets = store.claim(&other(), "w", LONG).await.unwrap().unwrap();
    assert_eq!(
        store.take_requests(&other(), wallets.token, 10).await.unwrap().unwrap().keys,
        ["z"],
        "another row's keys were not taken"
    );
    for row in [key(), other()] {
        assert_eq!(
            raw(&mongo, &row).await.get_array("rebuild_keys").unwrap(),
            &Vec::<Bson>::new(),
            "taken keys are gone"
        );
    }
    mongo.close().await;
}

#[tokio::test]
async fn the_document_keeps_the_documented_shape_and_values_that_look_like_expressions() {
    let mongo = Mongo::new().await;
    let store = MongoCheckpointStore::new(&mongo.database);
    store.register(&other()).await.unwrap();
    store.register(&key()).await.unwrap();
    let registered = raw(&mongo, &key()).await;
    let mut fields: Vec<_> = registered.keys().cloned().collect();
    fields.sort();
    assert_eq!(
        fields,
        [
            "_id",
            "applied",
            "failures",
            "last_error",
            "lock_token",
            "locked_until",
            "owner",
            "position",
            "rebuild_cursor",
            "rebuild_keys",
            "rebuild_pace_ms",
            "rebuild_requested",
            "rebuilt",
            "updated_at"
        ]
    );
    assert_eq!(
        registered.get_document("_id").unwrap().keys().collect::<Vec<_>>(),
        ["projection", "aggregate_type"],
        "the identity's field order is part of it"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    store.register(&key()).await.unwrap();
    assert_eq!(
        raw(&mongo, &key()).await,
        registered,
        "a repeated registration changes nothing"
    );
    // Values an update pipeline could read as expressions are stored as they are.
    let owner = store.claim(&key(), "$owner", LONG).await.unwrap().unwrap();
    store.request_rebuild_key(&key(), "$key").await.unwrap();
    assert!(store.record_failure(&key(), owner.token, "$error").await.unwrap());
    assert!(store.advance(&key(), owner.token, Position::new(7), 3).await.unwrap());
    assert_eq!(
        store.request_rebuild(&key(), Duration::from_millis(80)).await.unwrap(),
        RebuildRequest::Accepted
    );
    assert!(store.record_failure(&key(), owner.token, "$error").await.unwrap());
    let requests = store.take_requests(&key(), owner.token, 0).await.unwrap().unwrap();
    assert_eq!(requests.keys, Vec::<String>::new(), "a limit of zero takes nothing");
    assert!(store.begin_rebuild(&key(), owner.token).await.unwrap());
    assert!(store.advance_rebuild(&key(), owner.token, "$cursor", 1).await.unwrap());
    let stored = raw(&mongo, &key()).await;
    for field in ["position", "applied", "rebuild_pace_ms", "rebuilt", "failures"] {
        assert!(matches!(stored.get(field), Some(Bson::Int64(_))), "{field}: {stored}");
    }
    assert!(matches!(stored.get("locked_until"), Some(Bson::DateTime(_))), "{stored}");
    assert!(matches!(stored.get("updated_at"), Some(Bson::DateTime(_))), "{stored}");
    assert_eq!(
        (
            stored.get_str("owner").unwrap(),
            stored.get_str("last_error").unwrap(),
            stored.get_str("rebuild_cursor").unwrap(),
        ),
        ("$owner", "$error", "$cursor")
    );
    assert_eq!(stored.get_array("rebuild_keys").unwrap(), &vec![Bson::String("$key".into())]);
    assert_eq!(
        Uuid::parse_str(stored.get_str("lock_token").unwrap()).unwrap(),
        owner.token.value()
    );
    let names: Vec<_> = store
        .status(None)
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.projection, row.aggregate_type))
        .collect();
    assert_eq!(
        names,
        [
            ("halls".to_owned(), "Concert".to_owned()),
            ("wallets".to_owned(), "Fan".to_owned())
        ]
    );
    let row = store.status(Some("halls")).await.unwrap().remove(0);
    assert_eq!(
        (row.owner.as_deref(), row.failures, row.rebuild_cursor.as_deref(), row.rebuilt),
        (Some("$owner"), 1, Some("$cursor"), 1)
    );
    mongo.close().await;
}

#[tokio::test]
async fn a_dropped_collection_fences_the_old_owner_out_and_register_recreates_the_row() {
    let mongo = Mongo::new().await;
    let store = MongoCheckpointStore::new(&mongo.database);
    store.register(&key()).await.unwrap();
    let owner = store.claim(&key(), "a", LONG).await.unwrap().unwrap();
    assert!(store.advance(&key(), owner.token, Position::new(5), 5).await.unwrap());
    documents(&mongo).drop().await.unwrap();
    assert!(!store.extend(&key(), owner.token, LONG).await.unwrap());
    assert!(!store.advance(&key(), owner.token, Position::new(6), 1).await.unwrap());
    assert!(store.take_requests(&key(), owner.token, 10).await.unwrap().is_none());
    assert!(
        store.claim(&key(), "b", LONG).await.unwrap().is_none(),
        "no row until it is registered"
    );
    assert!(store.status(None).await.unwrap().is_empty());
    let row = key();
    let (one, two) = tokio::join!(store.register(&row), store.register(&row));
    one.unwrap();
    two.unwrap();
    let next = store.claim(&key(), "b", LONG).await.unwrap().unwrap();
    assert_eq!((next.position, next.failures, next.rebuild), (Position::START, 0, None));
    assert_eq!(store.status(None).await.unwrap().len(), 1);
    mongo.close().await;
}
