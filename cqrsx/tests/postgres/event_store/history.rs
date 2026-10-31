//! The saved history of one aggregate: error categories of both adapters, ids and types kept apart, decoding
//! and upcasting on load.
use super::*;
use cqrsx::postgres::event_repository::PostgresEventRepository;

fn assert_sql_error(error: &PersistenceError, decode: bool) {
    match (error, decode) {
        (PersistenceError::UnknownError(source), false) | (PersistenceError::DeserializationError(source), true) => {
            assert!(source.is::<sqlx::Error>())
        }
        _ => panic!("incorrect history error category: {error:?}"),
    }
}

#[tokio::test]
async fn both_adapters_report_missing_tables_and_row_decode_failures() {
    let db = Database::new().await;
    for decode in [false, true] {
        if decode {
            sqlx::raw_sql(EVENT_STORE_SCHEMA).execute(&db.pool).await.unwrap();
            PostgresEventRepository::new(db.pool.clone())
                .persist::<TestAggregate>(&[event("fault", 1)], None)
                .await
                .unwrap();
            sqlx::raw_sql("ALTER TABLE events RENAME TO valid_events; CREATE VIEW events AS SELECT aggregate_type,aggregate_id,sequence,event_type,event_version,payload,NULL::json AS metadata FROM valid_events").execute(&db.pool).await.unwrap();
        } else {
            sqlx::query("DROP TABLE events").execute(&db.pool).await.unwrap();
        }
        let plain = PostgresEventRepository::new(db.pool.clone())
            .events::<TestAggregate>("fault")
            .await;
        assert_sql_error(&plain.unwrap_err(), decode);
        let outbox = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
            .events::<TestAggregate>("fault")
            .await;
        assert_sql_error(&outbox.unwrap_err(), decode);
    }
    db.close().await;
}

mod contracts {
    struct DeltaUpcaster(&'static str, &'static str, fn(Value) -> Value);
    impl cqrsx::journal::upcaster::EventUpcaster for DeltaUpcaster {
        fn can_upcast(&self, format: EventFormat<'_>) -> bool {
            format.event_type == "delta" && format.event_version == self.0
        }
        fn upcast(&self, mut event: SerializedEvent) -> SerializedEvent {
            event.event_version = self.1.into();
            event.payload = (self.2)(event.payload);
            event
        }
    }

    use super::*;
    #[tokio::test]
    async fn histories_keep_ids_and_types_apart_and_a_broken_row_fails_the_load() {
        let db = Database::new().await;
        let repo = PostgresEventRepository::new(db.pool.clone());
        assert!(repo.events::<Ledger>("empty").await.unwrap().is_empty());
        let mut expected = HashMap::new();
        for id in ["one", "two"] {
            let events: Vec<_> = (1..=5).map(|i| event_for(id, i, i as i32)).collect();
            repo.persist::<Ledger>(&events, None).await.unwrap();
            expected.insert(id.to_owned(), events);
        }
        repo.persist::<OtherLedger>(&[event_for("one", 1, 999)], None).await.unwrap();
        for id in ["one", "two"] {
            assert_eq!(repo.events::<Ledger>(id).await.unwrap(), expected[id]);
            assert_eq!(repo.events_after::<Ledger>(id, 3).await.unwrap(), expected[id][3..]);
        }
        assert_eq!(
            repo.events::<OtherLedger>("one").await.unwrap()[0].aggregate_type,
            OtherLedger::TYPE
        );
        sqlx::query("UPDATE events SET payload = '{}'::json WHERE aggregate_type=$1 AND aggregate_id='one' AND sequence=2")
            .bind(Ledger::TYPE)
            .execute(&db.pool)
            .await
            .unwrap();
        assert!(matches!(
            PersistedEventStore::<_, Ledger>::new(repo).load_aggregate("one").await,
            Err(cqrsx::journal::PersistenceError::DeserializationError(_))
        ));
        db.close().await;
    }

    #[tokio::test]
    async fn a_supported_upcaster_chain_preserves_full_envelopes_on_load() {
        let db = Database::new().await;
        let mut original = event_for("upcast", 37, 3);
        original.event_version = "0".into();
        original.payload = json!({"old":3});
        original.metadata = json!({"trace":"данные"});
        let mut expected = original.clone();
        expected.event_version = "1".into();
        expected.payload = json!(4);
        PostgresEventRepository::new(db.pool.clone())
            .persist::<Ledger>(&[original], None)
            .await
            .unwrap();
        let store = PersistedEventStore::<_, Ledger>::new(PostgresEventRepository::new(db.pool.clone()))
            .with_upcasters(delta_upcasters());
        let loaded = store.load_events("upcast").await.unwrap();
        assert_eq!(SerializedEvent::try_from(&loaded[0]).unwrap(), expected);
        assert_eq!(store.load_aggregate("upcast").await.unwrap().aggregate.0, [4]);
        sqlx::query("UPDATE events SET event_version='unsupported'")
            .execute(&db.pool)
            .await
            .unwrap();
        assert!(matches!(
            store.load_aggregate("upcast").await,
            Err(cqrsx::journal::PersistenceError::DeserializationError(_))
        ));
        db.close().await;
    }

    fn delta_upcasters() -> Vec<Box<dyn cqrsx::journal::upcaster::EventUpcaster>> {
        vec![
            Box::new(DeltaUpcaster("0", "0.5", |v| v["old"].clone())),
            Box::new(DeltaUpcaster("0.5", "1", |v| json!(v.as_i64().unwrap() + 1))),
        ]
    }
}
