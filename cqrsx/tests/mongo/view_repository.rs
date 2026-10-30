//! `MongoViewRepository`: the documented document, optimistic versions and explicit failures.
use crate::{
    mongo_container::Mongo,
    support::{Balance, NAME, history, views},
};
use cqrsx::cqrs::{
    event_handler::EventHandler,
    projection::{
        ViewProjection,
        repository::{StoredView, ViewContext, ViewRepository},
    },
    store::PersistenceError,
};
use mongodb::bson::{Bson, Document, doc};
use std::sync::Arc;
use tokio::{sync::Barrier, task::JoinSet};

#[tokio::test]
async fn a_view_is_inserted_updated_and_loaded_with_its_context() {
    let mongo = Mongo::new().await;
    let views = views(&mongo.database);
    let first = Balance {
        total: 1,
        history: vec![(1, 1)],
    };
    views
        .update_view(first.clone(), ViewContext::new("l1".into(), 0, 1))
        .await
        .unwrap();
    assert_eq!(
        views.load_with_context("l1").await.unwrap(),
        Some(StoredView {
            id: "l1".into(),
            version: 1,
            last_sequence: 1,
            view: first
        })
    );
    let second = Balance {
        total: 3,
        history: vec![(1, 1), (2, 2)],
    };
    views
        .update_view(second.clone(), ViewContext::new("l1".into(), 1, 2))
        .await
        .unwrap();
    assert_eq!(views.load("l1").await.unwrap(), Some(second.clone()));
    assert_eq!(
        views.load_with_context("l1").await.unwrap(),
        Some(StoredView {
            id: "l1".into(),
            version: 2,
            last_sequence: 2,
            view: second
        })
    );
    let raw = mongo
        .database
        .collection::<Document>(NAME)
        .find_one(doc! { "_id": "l1" })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw.keys().collect::<Vec<_>>(), ["_id", "version", "last_sequence", "view"]);
    assert_eq!(
        (raw.get("_id"), raw.get("version"), raw.get("last_sequence")),
        (Some(&Bson::String("l1".into())), Some(&Bson::Int64(2)), Some(&Bson::Int64(2)))
    );
    assert!(matches!(raw.get("view"), Some(Bson::Document(_))));
    mongo.close().await;
}

#[tokio::test]
async fn a_duplicate_insert_a_stale_version_and_an_overflow_are_explicit() {
    let mongo = Mongo::new().await;
    let views = views(&mongo.database);
    let view = Balance::default();
    views
        .update_view(view.clone(), ViewContext::new("l1".into(), 0, 1))
        .await
        .unwrap();
    assert!(matches!(
        views.update_view(view.clone(), ViewContext::new("l1".into(), 0, 1)).await,
        Err(PersistenceError::OptimisticLockError)
    ));
    for stale in [ViewContext::new("l1".into(), 5, 2), ViewContext::new("none".into(), 1, 2)] {
        assert!(matches!(
            views.update_view(view.clone(), stale).await,
            Err(PersistenceError::OptimisticLockError)
        ));
    }
    assert!(matches!(
        views
            .update_view(view.clone(), ViewContext::new("l1".into(), i64::MAX, 2))
            .await,
        Err(PersistenceError::UnknownError(_))
    ));
    #[cfg(target_pointer_width = "64")]
    assert!(matches!(
        views
            .update_view(view.clone(), ViewContext::new("l1".into(), 1, usize::MAX))
            .await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert_eq!(
        views.load_with_context("l1").await.unwrap(),
        Some(StoredView {
            id: "l1".into(),
            version: 1,
            last_sequence: 1,
            view
        }),
        "nothing was written by the refused updates"
    );
    assert!(views.load("none").await.unwrap().is_none());
    mongo.close().await;
}

#[tokio::test]
async fn missing_and_corrupt_documents_are_explicit() {
    let mongo = Mongo::new().await;
    let views = views(&mongo.database);
    assert!(views.load("none").await.unwrap().is_none());
    assert!(views.load_with_context("none").await.unwrap().is_none());
    let valid = doc! { "total": 1_i64, "history": [[1_i64, 1_i32]] };
    mongo
        .database
        .collection::<Document>(NAME)
        .insert_many([
            doc! { "_id": "not-a-balance", "version": 1_i64, "last_sequence": 1_i64, "view": {} },
            doc! { "_id": "negative", "version": 1_i64, "last_sequence": -1_i64, "view": valid.clone() },
            doc! { "_id": "no-sequence", "version": 1_i64, "view": valid.clone() },
            doc! { "_id": "no-version", "last_sequence": 1_i64, "view": valid.clone() },
            doc! { "_id": "valid", "version": 1_i64, "last_sequence": 1_i64, "view": valid },
        ])
        .await
        .unwrap();
    for id in ["not-a-balance", "negative", "no-sequence", "no-version"] {
        assert!(
            matches!(views.load(id).await, Err(PersistenceError::DeserializationError(_))),
            "{id}"
        );
        assert!(
            matches!(
                views.load_with_context(id).await,
                Err(PersistenceError::DeserializationError(_))
            ),
            "{id}"
        );
    }
    assert_eq!(
        views.load("valid").await.unwrap(),
        Some(Balance {
            total: 1,
            history: vec![(1, 1)]
        })
    );
    mongo.close().await;
}

/// The ids of every repository's ordering test: in the order of their bytes (`F` < `f`; `-` < `:` < `;` < `_` < `y`).
const IDS: [&str; 8] = [
    "Fox:muse-2027-01-16",
    "fox-2:radiohead-2026-11-21",
    "fox:muse-2027-01-16",
    "fox:radiohead-2026-11-21",
    "fox;",
    "fox_1:c-1",
    "foxy:c-1",
    "panda:c-1",
];

/// Documents of the library's shape, inserted as they are stored, read back as stored views: every document, a range
/// of ids, in the order of the ids' bytes.
#[tokio::test]
async fn documents_are_listed_and_ranged_in_the_byte_order_of_their_ids() {
    let mongo = Mongo::new().await;
    let views = views(&mongo.database);
    assert!(views.list().await.unwrap().is_empty());
    assert!(views.range("fox:", "fox;").await.unwrap().is_empty());
    let collection = mongo.database.collection::<Document>(NAME);
    for (n, id) in IDS.iter().rev().enumerate() {
        let history: Vec<Bson> = (1..=n as i64)
            .map(|s| Bson::Array(vec![Bson::Int64(s), Bson::Int32(1)]))
            .collect();
        collection
            .insert_one(doc! { "_id": *id, "version": n as i64 + 1, "last_sequence": n as i64,
            "view": { "total": n as i64, "history": history } })
            .await
            .unwrap();
    }
    let ids = |stored: Vec<StoredView<Balance>>| stored.into_iter().map(|stored| stored.id).collect::<Vec<_>>();
    assert_eq!(ids(views.list().await.unwrap()), IDS);
    assert_eq!(
        ids(views.range("fox:", "fox;").await.unwrap()),
        ["fox:muse-2027-01-16", "fox:radiohead-2026-11-21"]
    );
    // The lower bound is in, the upper one out; an empty or inverted range holds nothing.
    assert_eq!(ids(views.range("fox;", "foxy:c-1").await.unwrap()), ["fox;", "fox_1:c-1"]);
    for (lower, upper) in [("fox;", "fox:"), ("fox;", "fox;"), ("zebra", "a")] {
        assert!(views.range(lower, upper).await.unwrap().is_empty(), "{lower}..{upper}");
    }
    assert_eq!(
        views.range("fox;", "fox_").await.unwrap(),
        [StoredView {
            id: "fox;".into(),
            version: 4,
            last_sequence: 3,
            view: Balance {
                total: 3,
                history: vec![(1, 1), (2, 1), (3, 1)]
            },
        }]
    );
    // A document that is no view fails the listing and every range that holds it.
    collection
        .insert_one(doc! { "_id": "zz-broken", "version": 1_i64, "last_sequence": -1_i64, "view": {} })
        .await
        .unwrap();
    assert!(matches!(views.list().await, Err(PersistenceError::DeserializationError(_))));
    assert!(matches!(
        views.range("zz", "zz~").await,
        Err(PersistenceError::DeserializationError(_))
    ));
    assert_eq!(ids(views.range("F", "zz").await.unwrap()), IDS);
    mongo.close().await;
}

/// The contiguity rule on a real server without any lock: writers of nested prefixes of one aggregate's
/// history race for one document; whoever loses the optimistic check rereads and applies only what the
/// winner left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_concurrent_writers_of_one_mongo_document_converge() {
    let mongo = Mongo::new().await;
    let repository = Arc::new(views(&mongo.database));
    for initial in [0usize, 1] {
        for writers in [2usize, 4] {
            for round in 0..16 {
                let id = format!("converge-{initial}-{writers}-{round}");
                // One history of `total` events; writer `w` delivers the prefix `initial+1 ..= initial+2+w`.
                let total = initial + writers + 1;
                let source = history(&id, total);
                if initial == 1 {
                    ViewProjection::new(repository.clone())
                        .handle(&id, &source[..1])
                        .await
                        .unwrap();
                }
                let start = Arc::new(Barrier::new(writers));
                let mut tasks = JoinSet::new();
                for writer in 0..writers {
                    let (projection, start) = (ViewProjection::new(repository.clone()), start.clone());
                    let (id, batch) = (id.clone(), source[initial..initial + 2 + writer].to_vec());
                    tasks.spawn(async move {
                        start.wait().await;
                        projection.handle(&id, &batch).await
                    });
                }
                while let Some(result) = tasks.join_next().await {
                    result.unwrap().unwrap();
                }
                let stored = repository.load_with_context(&id).await.unwrap().unwrap();
                let expected: Vec<_> = (1..=total).map(|sequence| (sequence, sequence as i32)).collect();
                assert_eq!(
                    stored.view.history, expected,
                    "{id}: the fold of the whole history, every event once"
                );
                assert_eq!(stored.last_sequence, total, "{id}");
                let versions = (initial as i64 + 1)..=((initial + writers) as i64);
                assert!(
                    versions.contains(&stored.version),
                    "{id}: version {} outside {versions:?}",
                    stored.version
                );
            }
        }
    }
    mongo.close().await;
}
