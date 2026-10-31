//! The assembly the MongoDB read side exists for: the journal in PostgreSQL, the views and the checkpoint
//! in MongoDB, the checkpoint holding PostgreSQL transaction ids.
use crate::{
    mongo_container::Mongo,
    postgres_container::Postgres,
    support::{Ledger, NAME, Node, Recording, caught_up, commit_all, until},
};
use cqrsx::{
    command::{executor::StoreCommandExecutor, locks::LockLimits},
    journal::{feed::Position, persisted::PersistedEventStore, repository::PersistedEventRepository, wakeup::CommitWakeup},
    mongodb::checkpoint_store::PROJECTION_CHECKPOINTS_COLLECTION,
    postgres::{event_repository::PostgresEventRepository, outbox::PostgresEventRepositoryWithOutbox},
};
use mongodb::bson::Document;
use sqlx::PgPool;
use std::sync::Arc;

/// Commits `per_key` commands on every key through PostgreSQL.
async fn commit(pool: &PgPool, wakeup: &CommitWakeup, keys: &[&str], per_key: i32) {
    let repository = PostgresEventRepositoryWithOutbox::new(pool.clone());
    let executor = Arc::new(
        StoreCommandExecutor::new(
            PersistedEventStore::<_, Ledger>::new(repository),
            (),
            Default::default(),
            LockLimits::default(),
        )
        .with_commit_wakeup(wakeup.clone()),
    );
    commit_all(keys, per_key, move |id, value| {
        let executor = executor.clone();
        async move {
            executor.execute(&id, value).await.unwrap();
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commands_in_postgres_reach_views_and_checkpoints_in_mongo() {
    let (postgres, mongo) = (Postgres::new().await, Mongo::new().await);
    let (observer, wakeup) = (Arc::new(Recording::default()), CommitWakeup::new());
    let journal = Arc::new(PostgresEventRepository::new(postgres.pool.clone()));
    let node = Node::start(journal.clone(), &mongo.database, "node-a", &observer, &wakeup);
    let keys = ["l1", "l2", "l3", "l4", "l5"];
    commit(&postgres.pool, &wakeup, &keys, 8).await;
    let status = caught_up(journal.as_ref(), &mongo.database, &keys).await;
    assert_eq!((status.applied, status.failures), (40, 0));
    assert!(status.position > Position::START);
    assert_eq!(
        status.position,
        journal.head::<Ledger>().await.unwrap(),
        "a PostgreSQL transaction id"
    );
    // The read side is dropped as a whole: the documents first, the checkpoint after them.
    mongo.database.collection::<Document>(NAME).drop().await.unwrap();
    mongo
        .database
        .collection::<Document>(PROJECTION_CHECKPOINTS_COLLECTION)
        .drop()
        .await
        .unwrap();
    until("the second claim", || observer.count("lease:Acquired") == 2).await;
    commit(&postgres.pool, &wakeup, &keys, 2).await;
    let status = caught_up(journal.as_ref(), &mongo.database, &keys).await;
    assert_eq!(
        (status.applied, status.failures),
        (50, 0),
        "every document came back from the journal"
    );
    assert_eq!(observer.count("failed:"), 0);
    node.stop().await;
    postgres.close().await;
    mongo.close().await;
}
