use super::*;
use cqrsx::postgres::event_repository::LedgerTotals;

#[tokio::test]
async fn totals_count_the_events_and_aggregates_of_one_type_only() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    assert_eq!(repo.totals::<Ledger>().await.unwrap(), LedgerTotals::default(), "empty table");

    repo.persist::<Ledger>(&[event_for("a", 1, 1), event_for("a", 2, 2)], None)
        .await
        .unwrap();
    repo.persist::<Ledger>(&[event_for("b", 1, 3)], None).await.unwrap();
    let other = SerializedEvent::new(
        "c".into(),
        1,
        OtherLedger::TYPE.into(),
        "delta".into(),
        "1".into(),
        json!(5),
        json!({}),
    );
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
