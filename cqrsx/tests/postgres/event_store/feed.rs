//! The journal of one aggregate type read in commit order: pages below the snapshot horizon, the
//! head and the aggregate ids page by page.
use super::*;
use cqrsx::cqrs::store::feed::{FeedEntry, Position};
use std::collections::BTreeSet;

/// Writes one `Ledger` event inside `tx` without committing: the transaction keeps its id and holds
/// the horizon of every feed until it ends. Returns that id as a position.
async fn insert_in(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, id: &str, sequence: i64) -> Position {
    sqlx::query(
        "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
         VALUES ($1, $2, $3, 'delta', '1', '0', '{}')",
    )
    .bind(Ledger::TYPE)
    .bind(id)
    .bind(sequence)
    .execute(&mut **tx)
    .await
    .unwrap();
    let text: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut **tx)
        .await
        .unwrap();
    text.parse().unwrap()
}

fn keys(page: &[FeedEntry]) -> Vec<(Position, &str, usize)> {
    page.iter()
        .map(|entry| (entry.position, entry.event.aggregate_id.as_str(), entry.event.sequence))
        .collect()
}

/// Reads the whole `Ledger` journal page by page like a projection would: every page starts above
/// the last position seen and never goes backwards.
async fn everything<R: PersistedEventRepository>(repo: &R, limit: usize) -> Vec<FeedEntry> {
    let mut all = Vec::new();
    let mut after = Position::START;
    loop {
        let page = repo.feed_after::<Ledger>(after, limit).await.unwrap();
        let Some(last) = page.last() else {
            return all;
        };
        assert!(page.iter().all(|entry| entry.position > after), "{page:?}");
        assert!(page.windows(2).all(|pair| pair[0].position <= pair[1].position), "{page:?}");
        after = last.position;
        all.extend(page);
    }
}

#[tokio::test]
async fn a_slower_transaction_with_a_lower_number_is_never_skipped() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    let mut slow = db.pool.begin().await.unwrap();
    let x = insert_in(&mut slow, "a", 1).await;
    repo.persist::<Ledger>(&[event_for("b", 1, 1)], None).await.unwrap();
    let y = repo.head::<Ledger>().await.unwrap();
    assert!(y > x, "the later commit got the higher transaction id");
    assert!(
        repo.feed_after::<Ledger>(Position::START, 10).await.unwrap().is_empty(),
        "the open transaction holds the horizon: the head is ahead of the feed"
    );
    slow.commit().await.unwrap();
    assert_eq!(
        keys(&repo.feed_after::<Ledger>(Position::START, 10).await.unwrap()),
        [(x, "a", 1), (y, "b", 1)]
    );
    assert_eq!(keys(&repo.feed_after::<Ledger>(x, 10).await.unwrap()), [(y, "b", 1)]);
    assert!(repo.feed_after::<Ledger>(y, 10).await.unwrap().is_empty());
    db.close().await;
}

#[tokio::test]
async fn a_rolled_back_transaction_leaves_no_hole() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    let mut doomed = db.pool.begin().await.unwrap();
    let x = insert_in(&mut doomed, "a", 1).await;
    repo.persist::<Ledger>(&[event_for("b", 1, 1)], None).await.unwrap();
    let y = repo.head::<Ledger>().await.unwrap();
    assert!(repo.feed_after::<Ledger>(Position::START, 10).await.unwrap().is_empty());
    doomed.rollback().await.unwrap();
    assert_eq!(
        keys(&repo.feed_after::<Ledger>(Position::START, 10).await.unwrap()),
        [(y, "b", 1)]
    );
    assert!(
        repo.feed_after::<Ledger>(x, 10)
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.position == y)
    );
    assert!(repo.feed_after::<Ledger>(y, 10).await.unwrap().is_empty());
    assert_eq!(repo.head::<Ledger>().await.unwrap(), y);
    db.close().await;
}

#[tokio::test]
async fn a_read_only_transaction_does_not_hold_the_horizon() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    let mut reader = db.pool.begin().await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    repo.persist::<Ledger>(&[event_for("b", 1, 1)], None).await.unwrap();
    let head = repo.head::<Ledger>().await.unwrap();
    assert_eq!(
        keys(&repo.feed_after::<Ledger>(Position::START, 10).await.unwrap()),
        [(head, "b", 1)],
        "a transaction without a transaction id holds nothing"
    );
    reader.rollback().await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn an_idle_writing_transaction_holds_the_feed_until_it_ends() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    let mut idle = db.pool.begin().await.unwrap();
    let held = insert_in(&mut idle, "idle", 1).await;
    for id in ["a", "b", "c"] {
        repo.persist::<Ledger>(&[event_for(id, 1, 1)], None).await.unwrap();
    }
    assert!(
        repo.head::<Ledger>().await.unwrap() > held,
        "the head moves while the feed waits"
    );
    for limit in [1, 2, 10] {
        assert!(repo.feed_after::<Ledger>(Position::START, limit).await.unwrap().is_empty());
    }
    idle.rollback().await.unwrap();
    let all = everything(&repo, 2).await;
    assert_eq!(
        all.iter().map(|entry| entry.event.aggregate_id.as_str()).collect::<Vec<_>>(),
        ["a", "b", "c"],
        "commit order, nothing skipped"
    );
    assert!(all.windows(2).all(|pair| pair[0].position < pair[1].position));
    assert_eq!(repo.head::<Ledger>().await.unwrap(), all[2].position);
    db.close().await;
}

#[tokio::test]
async fn a_page_ends_on_a_transaction_boundary() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    repo.persist::<Ledger>(&[event_for("a", 1, 1), event_for("a", 2, 2), event_for("a", 3, 3)], None)
        .await
        .unwrap();
    repo.persist::<Ledger>(&[event_for("b", 1, 1)], None).await.unwrap();
    let first = repo.feed_after::<Ledger>(Position::START, 2).await.unwrap();
    let p = first[0].position;
    assert_eq!(
        keys(&first),
        [(p, "a", 1), (p, "a", 2), (p, "a", 3)],
        "a limit of 2 still returns the whole commit"
    );
    assert_eq!(
        repo.feed_after::<Ledger>(Position::START, 3).await.unwrap().len(),
        3,
        "an exactly full page adds nothing"
    );
    let second = repo.feed_after::<Ledger>(p, 2).await.unwrap();
    assert_eq!(second.len(), 1);
    assert!(second[0].position > p);
    assert_eq!(second[0].event.aggregate_id, "b");
    assert_eq!(repo.feed_after::<Ledger>(Position::START, 4).await.unwrap().len(), 4);
    assert_eq!(repo.feed_after::<Ledger>(Position::START, 10).await.unwrap().len(), 4);
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forty_parallel_commands_over_five_aggregates_feed_in_sequence_order() {
    let db = Database::new().await;
    let mut writers = JoinSet::new();
    for id in ["a", "b", "c", "d", "e"] {
        let pool = db.pool.clone();
        writers.spawn(async move {
            let repo = PostgresEventRepository::new(pool);
            for sequence in 1..=8 {
                repo.persist::<Ledger>(&[event_for(id, sequence, sequence as i32)], None)
                    .await
                    .unwrap();
            }
        });
    }
    while let Some(writer) = writers.join_next().await {
        writer.unwrap();
    }
    // Read through the outbox repository: it delegates the journal reads to the plain one.
    let repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let all = everything(&repo, 7).await;
    assert_eq!(all.len(), 40);
    let mut next_sequence: HashMap<&str, usize> = HashMap::new();
    for entry in &all {
        let next = next_sequence.entry(entry.event.aggregate_id.as_str()).or_insert(0);
        *next += 1;
        assert_eq!(entry.event.sequence, *next, "{} out of order", entry.event.aggregate_id);
    }
    assert!(next_sequence.values().all(|count| *count == 8));
    assert_eq!(
        all.iter().map(|entry| entry.position).collect::<BTreeSet<_>>().len(),
        40,
        "one commit, one position"
    );
    assert_eq!(repo.head::<Ledger>().await.unwrap(), all[39].position);
    assert_eq!(
        repo.aggregate_ids_after::<Ledger>("", 10).await.unwrap(),
        ["a", "b", "c", "d", "e"]
    );
    db.close().await;
}

#[tokio::test]
async fn aggregate_ids_are_listed_in_order_page_by_page() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    for id in ["e", "c", "a", "d", "b"] {
        repo.persist::<Ledger>(&[event_for(id, 1, 1), event_for(id, 2, 2)], None)
            .await
            .unwrap();
    }
    repo.persist::<OtherLedger>(&[event_for("z", 1, 1)], None).await.unwrap();
    assert_eq!(repo.aggregate_ids_after::<Ledger>("", 2).await.unwrap(), ["a", "b"]);
    assert_eq!(repo.aggregate_ids_after::<Ledger>("b", 2).await.unwrap(), ["c", "d"]);
    assert_eq!(repo.aggregate_ids_after::<Ledger>("d", 2).await.unwrap(), ["e"]);
    assert!(repo.aggregate_ids_after::<Ledger>("e", 2).await.unwrap().is_empty());
    assert_eq!(repo.aggregate_ids_after::<OtherLedger>("", 2).await.unwrap(), ["z"]);
    db.close().await;
}

#[tokio::test]
async fn an_empty_journal_has_no_head() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    assert_eq!(repo.head::<Ledger>().await.unwrap(), Position::START);
    assert!(repo.feed_after::<Ledger>(Position::START, 10).await.unwrap().is_empty());
    assert!(repo.aggregate_ids_after::<Ledger>("", 10).await.unwrap().is_empty());
    db.close().await;
}

#[tokio::test]
async fn the_feed_filters_by_aggregate_type() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    repo.persist::<Ledger>(&[event_for("a", 1, 1)], None).await.unwrap();
    repo.persist::<OtherLedger>(&[event_for("z", 1, 1)], None).await.unwrap();
    let ledger = repo.feed_after::<Ledger>(Position::START, 10).await.unwrap();
    let other = repo.feed_after::<OtherLedger>(Position::START, 10).await.unwrap();
    assert_eq!(ledger.len(), 1);
    assert_eq!(other.len(), 1);
    assert_eq!(ledger[0].event.aggregate_type, Ledger::TYPE);
    assert_eq!(other[0].event.aggregate_type, OtherLedger::TYPE);
    assert!(ledger[0].position < other[0].position);
    assert_eq!(repo.head::<Ledger>().await.unwrap(), ledger[0].position);
    assert_eq!(repo.head::<OtherLedger>().await.unwrap(), other[0].position);
    assert_eq!(repo.aggregate_ids_after::<Ledger>("", 10).await.unwrap(), ["a"]);
    assert_eq!(repo.aggregate_ids_after::<OtherLedger>("", 10).await.unwrap(), ["z"]);
    db.close().await;
}

#[tokio::test]
async fn custom_table_names_feed_too() {
    let db = Database::new().await;
    sqlx::query("CREATE TABLE custom_events (LIKE events INCLUDING ALL)")
        .execute(&db.pool)
        .await
        .unwrap();
    let custom = PostgresEventRepository::new(db.pool.clone()).with_tables("custom_events", "snapshots");
    custom.persist::<Ledger>(&[event_for("a", 1, 1)], None).await.unwrap();
    custom.persist::<Ledger>(&[event_for("a", 2, 2)], None).await.unwrap();
    custom.persist::<Ledger>(&[event_for("b", 1, 1)], None).await.unwrap();
    let first = custom.feed_after::<Ledger>(Position::START, 2).await.unwrap();
    assert_eq!(first.len(), 2, "two commits of one event each fill the page exactly");
    let second = custom.feed_after::<Ledger>(first[1].position, 2).await.unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].event.aggregate_id, "b");
    assert_eq!(custom.head::<Ledger>().await.unwrap(), second[0].position);
    assert_eq!(custom.aggregate_ids_after::<Ledger>("", 10).await.unwrap(), ["a", "b"]);
    let default = PostgresEventRepository::new(db.pool.clone());
    assert_eq!(default.head::<Ledger>().await.unwrap(), Position::START);
    assert!(default.feed_after::<Ledger>(Position::START, 10).await.unwrap().is_empty());
    db.close().await;
}

#[tokio::test]
async fn invalid_feed_arguments_are_refused() {
    let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/unused").unwrap();
    pool.close().await;
    let repo = PostgresEventRepository::new(pool);
    for limit in [0, usize::MAX] {
        if limit == usize::MAX && usize::BITS < 64 {
            continue;
        }
        let page = repo.feed_after::<Ledger>(Position::START, limit).await;
        let ids = repo.aggregate_ids_after::<Ledger>("", limit).await;
        for result in [page.map(|_| ()), ids.map(|_| ())] {
            let error = result.unwrap_err();
            assert!(matches!(error, PersistenceError::UnknownError(_)), "{error}");
            let message = error.to_string();
            assert!(
                message == "feed page limit must be positive" || message.contains("out of range"),
                "{message}: refused before any statement, not by the closed pool"
            );
        }
    }
}
