//! The projection processor with its read side in MongoDB: the views and the checkpoint next to each other,
//! the journal in memory.
use crate::{
    mongo_container::Mongo,
    support::{Balance, NAME, Node, Recording, caught_up, commit_all, key, mem_ledgers, until, views},
};
use cqrsx::{
    journal::wakeup::CommitWakeup,
    memory::event_repository::MemEventRepository,
    mongodb::checkpoint_store::{MongoCheckpointStore, PROJECTION_CHECKPOINTS_COLLECTION},
    projection::checkpoint::{CheckpointStore, RebuildRequest},
    view::repository::ViewRepository,
};
use mongodb::bson::Document;
use std::{sync::Arc, time::Duration};

/// Commits `per_key` commands on every key through the in-memory journal.
async fn commit(journal: &MemEventRepository, wakeup: &CommitWakeup, keys: &[&str], per_key: i32) {
    let executor = mem_ledgers(journal, wakeup);
    commit_all(keys, per_key, move |id, value| {
        let executor = executor.clone();
        async move {
            executor.execute(&id, value).await.unwrap();
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forty_commands_over_five_ledgers_reach_the_mongo_views_in_order() {
    let mongo = Mongo::new().await;
    let (journal, observer, wakeup) = (
        MemEventRepository::default(),
        Arc::new(Recording::default()),
        CommitWakeup::new(),
    );
    let node = Node::start(Arc::new(journal.clone()), &mongo.database, "node-a", &observer, &wakeup);
    let keys = ["l1", "l2", "l3", "l4", "l5"];
    commit(&journal, &wakeup, &keys, 8).await;
    let status = caught_up(&journal, &mongo.database, &keys).await;
    assert_eq!(
        (status.applied, status.owner.as_deref(), status.failures),
        (40, Some("node-a"), 0)
    );
    for id in keys {
        let balance = views(&mongo.database).load(id).await.unwrap().unwrap();
        assert_eq!(balance.history, (1..=8).map(|n| (n, n as i32)).collect::<Vec<_>>());
    }
    node.stop().await;
    let after = MongoCheckpointStore::new(&mongo.database)
        .status(Some(NAME))
        .await
        .unwrap()
        .remove(0);
    assert_eq!((after.owner, after.position), (None, status.position), "released on stop");
    assert_eq!((observer.count("lease:Acquired"), observer.count("lease:Released")), (1, 1));
    mongo.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuild_under_concurrent_commits_ends_in_the_fold_of_the_journal() {
    let mongo = Mongo::new().await;
    let (journal, observer, wakeup) = (
        MemEventRepository::default(),
        Arc::new(Recording::default()),
        CommitWakeup::new(),
    );
    let node = Node::start(Arc::new(journal.clone()), &mongo.database, "node-a", &observer, &wakeup);
    let keys = ["l1", "l2", "l3", "l4", "l5"];
    commit(&journal, &wakeup, &keys, 4).await;
    caught_up(&journal, &mongo.database, &keys).await;
    let store = MongoCheckpointStore::new(&mongo.database);
    assert_eq!(
        store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    commit(&journal, &wakeup, &keys, 4).await;
    let status = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = caught_up(&journal, &mongo.database, &keys).await;
            if status.rebuild_cursor.is_none() && status.rebuilt == 5 {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the rebuild finishes");
    assert_eq!(status.applied, 40);
    assert_eq!(
        (observer.count("rebuild:Started"), observer.count("rebuild:Finished")),
        (1, 1)
    );
    assert!(observer.count("rebuilt:") >= 5);
    assert_eq!(observer.count("failed:"), 0);
    node.stop().await;
    mongo.close().await;
}

/// The read side dropped as a whole, as the demo's wipe and reset do: the documents first, the checkpoint
/// after them. The processor loses its row, registers it again and reads the journal from the start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wipe_of_both_collections_is_healed_by_a_full_self_rebuild() {
    let mongo = Mongo::new().await;
    let (journal, observer, wakeup) = (
        MemEventRepository::default(),
        Arc::new(Recording::default()),
        CommitWakeup::new(),
    );
    let node = Node::start(Arc::new(journal.clone()), &mongo.database, "node-a", &observer, &wakeup);
    let keys = ["l1", "l2", "l3"];
    commit(&journal, &wakeup, &keys, 3).await;
    caught_up(&journal, &mongo.database, &keys).await;
    mongo.database.collection::<Document>(NAME).drop().await.unwrap();
    mongo
        .database
        .collection::<Document>(PROJECTION_CHECKPOINTS_COLLECTION)
        .drop()
        .await
        .unwrap();
    until("the lost lease", || observer.count("lease:Lost") == 1).await;
    until("the second claim", || observer.count("lease:Acquired") == 2).await;
    commit(&journal, &wakeup, &keys, 1).await;
    let status = caught_up(&journal, &mongo.database, &keys).await;
    assert_eq!(
        (status.applied, status.failures),
        (12, 0),
        "the journal was read again from the start"
    );
    let first: Balance = views(&mongo.database).load("l1").await.unwrap().unwrap();
    assert_eq!(first.history, [(1, 1), (2, 2), (3, 3), (4, 1)]);
    assert_eq!(observer.count("failed:"), 0);
    node.stop().await;
    mongo.close().await;
}
