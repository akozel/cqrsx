use super::*;
use cqrsx::{cqrs::store::feed::Position, postgres::event_repository::LedgerTotals};

#[tokio::test]
async fn totals_count_the_events_and_aggregates_of_one_type_only() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    assert_eq!(repo.totals::<Ledger>().await.unwrap(), LedgerTotals::default(), "empty table");

    repo.persist::<Ledger>(&[event_for("a", 1, 1), event_for("a", 2, 2)], None)
        .await
        .unwrap();
    repo.persist::<Ledger>(&[event_for("b", 1, 3)], None).await.unwrap();
    let other = SerializedEvent {
        aggregate_id: "c".into(),
        sequence: 1,
        aggregate_type: OtherLedger::TYPE.into(),
        event_type: "delta".into(),
        event_version: "1".into(),
        payload: json!(5),
        metadata: json!({}),
    };
    repo.persist::<OtherLedger>(&[other], None).await.unwrap();

    assert_eq!(
        repo.totals::<Ledger>().await.unwrap(),
        LedgerTotals {
            events: 3,
            aggregates: 2
        },
        "two aggregates of one type, three events between them"
    );
    assert_eq!(
        repo.totals::<OtherLedger>().await.unwrap(),
        LedgerTotals {
            events: 1,
            aggregates: 1
        },
        "another aggregate type is counted on its own"
    );

    // A table that does not exist is a repository error, never a count.
    let missing = PostgresEventRepository::new(db.pool.clone()).with_tables("missing_events", "snapshots");
    assert!(matches!(
        missing.totals::<Ledger>().await,
        Err(PersistenceError::UnknownError(_))
    ));
    db.close().await;
}

#[tokio::test]
async fn count_after_counts_the_events_of_one_type_above_a_position_up_to_the_cap() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    assert_eq!(
        repo.count_after::<Ledger>(Position::START, 10).await.unwrap(),
        0,
        "empty journal"
    );
    repo.persist::<Ledger>(&[event_for("a", 1, 1), event_for("a", 2, 2)], None)
        .await
        .unwrap();
    let first_commit = repo.head::<Ledger>().await.unwrap();
    repo.persist::<Ledger>(&[event_for("b", 1, 3)], None).await.unwrap();
    repo.persist::<Ledger>(&[event_for("a", 3, 4)], None).await.unwrap();
    let other = SerializedEvent {
        aggregate_id: "c".into(),
        sequence: 1,
        aggregate_type: OtherLedger::TYPE.into(),
        event_type: "delta".into(),
        event_version: "1".into(),
        payload: json!(5),
        metadata: json!({}),
    };
    repo.persist::<OtherLedger>(&[other], None).await.unwrap();
    let head = repo.head::<Ledger>().await.unwrap();
    assert_eq!(repo.count_after::<Ledger>(Position::START, 100).await.unwrap(), 4);
    assert_eq!(
        repo.count_after::<Ledger>(first_commit, 100).await.unwrap(),
        2,
        "the events committed after the first commit"
    );
    assert_eq!(
        repo.count_after::<Ledger>(head, 100).await.unwrap(),
        0,
        "a reader at the head"
    );
    assert_eq!(
        repo.count_after::<Ledger>(Position::START, 3).await.unwrap(),
        3,
        "stopped at the cap"
    );
    assert_eq!(
        repo.count_after::<OtherLedger>(Position::START, 100).await.unwrap(),
        1,
        "another aggregate type is counted on its own"
    );
    for cap in [0, u64::MAX] {
        assert!(
            matches!(
                repo.count_after::<Ledger>(Position::START, cap).await,
                Err(PersistenceError::UnknownError(_))
            ),
            "cap {cap}"
        );
    }
    let missing = PostgresEventRepository::new(db.pool.clone()).with_tables("missing_events", "snapshots");
    assert!(matches!(
        missing.count_after::<Ledger>(Position::START, 10).await,
        Err(PersistenceError::UnknownError(_))
    ));
    db.close().await;
}
