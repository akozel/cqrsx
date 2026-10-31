//! The read side of the process: one processor per read model under its name, the documents as MongoDB stores them,
//! the verification of a read model by the engine.
use crate::{
    concert::{
        application::views::concert_hall::{ConcertHallView, ConcertSummary, SeatEntry, SeatState},
        domain::{Concert, ConcertEvent, events::ConcertAnnounced},
    },
    fan::application::views::fan_wallet::{FanWalletView, TicketEntry, TicketEntryState},
    operations::application::{engine::EngineView, verify::VerifiedReadModel},
    shared::{application::StorageError, domain::venue::Zone},
    testing::test_app,
};
use chrono::NaiveDate;
use cqrsx::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{repository::PersistedEventRepository, serialized_event::SerializedEvent},
    view::{
        View,
        verify::{Difference, DifferenceKind, Verification, verify},
    },
};
use mongodb::bson::{from_bson, to_bson};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

/// The engine of the process runs one projection processor per read model the modules register.
#[tokio::test]
async fn the_read_side_is_wired_with_one_processor_per_read_model() {
    let (cqrs, _) = test_app().await;
    assert_eq!(cqrs.views().keys().len(), 2, "the concert halls and the fan wallets");
}

#[tokio::test]
async fn the_read_models_keep_their_collection_and_checkpoint_names() {
    assert_eq!((ConcertHallView::NAME, FanWalletView::NAME), ("concert_halls", "fan_wallets"));
    let (cqrs, _) = test_app().await;
    let rows: Vec<(String, &str)> = cqrs
        .views()
        .keys()
        .iter()
        .map(|key| (key.projection().to_owned(), key.aggregate_type()))
        .collect();
    assert_eq!(
        rows,
        [("concert_halls".to_owned(), "Concert"), ("fan_wallets".to_owned(), "Fan")]
    );
}

/// The views are stored as BSON: seat and ticket keys stay strings, options stay optional.
#[test]
fn both_views_survive_the_bson_round_trip() {
    let hall = ConcertHallView {
        concert: Some(ConcertSummary {
            artist: "A".into(),
            city: "C".into(),
            date: "2026-11-21".into(),
        }),
        capacity: 40,
        seats: BTreeMap::from([(
            "B4".to_owned(),
            SeatEntry {
                state: SeatState::Booked,
                fan_id: Some("fox".into()),
                fan_name: Some("Fox".into()),
                zone: Some(Zone::Standard),
                price: Some(80),
                booking_id: Some("b-1".into()),
                sequence: 2,
                changed_at: "2026-10-17T09:00:02.000000Z".into(),
            },
        )]),
        sold: 1,
        revenue: 80,
        ..ConcertHallView::default()
    };
    assert_eq!(from_bson::<ConcertHallView>(to_bson(&hall).unwrap()).unwrap(), hall);
    let wallet = FanWalletView {
        fan_id: "fox".into(),
        fan_name: "Fox".into(),
        tickets: BTreeMap::from([(
            "b-1".to_owned(),
            TicketEntry {
                concert_id: "c-1".into(),
                seat: "B4".into(),
                price: 80,
                state: TicketEntryState::Cancelled,
                refund: Some(80),
                sequence: 2,
                changed_at: "2026-10-17T09:00:02.000000Z".into(),
            },
        )]),
        ..FanWalletView::default()
    };
    assert_eq!(from_bson::<FanWalletView>(to_bson(&wallet).unwrap()).unwrap(), wallet);
}

/// The operations verify a read model with the engine's verify, under the view's name; a failed read is a
/// storage error with the engine's text.
#[tokio::test]
async fn a_read_model_is_verified_by_the_engine_under_the_name_of_its_view() {
    let announced = EventEnvelope::<Concert> {
        aggregate_id: "c-1".into(),
        sequence: 1,
        payload: ConcertEvent::ConcertAnnounced(ConcertAnnounced {
            artist: "A".into(),
            city: "C".into(),
            date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
            capacity: 40,
        }),
        metadata: HashMap::new(),
    };
    let (cqrs, _) = test_app().await;
    let cqrs = Arc::new(cqrs);
    let journal = cqrs.journal();
    journal
        .persist::<Concert>(&[SerializedEvent::try_from(&announced).unwrap()], None)
        .await
        .unwrap();
    let halls = cqrs.documents::<ConcertHallView, Concert>().unwrap();
    let mut hall = ConcertHallView::default();
    hall.update(&announced);
    halls.put("c-1", 1, 1, &hall).unwrap();
    let verification = EngineView::new(cqrs.clone(), ConcertHallView::NAME);
    assert_eq!(verification.name(), "concert_halls");
    assert_eq!(
        verification.verify().await.unwrap(),
        Verification {
            checked: 1,
            behind_documents: 0,
            behind_events: 0,
            differences: vec![]
        }
    );
    halls.put("c-1", 2, 1, &ConcertHallView::default()).unwrap();
    assert_eq!(
        verification.verify().await.unwrap().differences,
        [Difference {
            collection: "concert_halls",
            id: "c-1".into(),
            kind: DifferenceKind::Differs
        }]
    );
    let broken = SerializedEvent {
        aggregate_id: "c-2".into(),
        sequence: 1,
        aggregate_type: Concert::TYPE.into(),
        event_type: "ConcertAnnounced".into(),
        event_version: "1".into(),
        payload: serde_json::json!("not an event"),
        metadata: serde_json::json!({}),
    };
    journal.persist::<Concert>(&[broken], None).await.unwrap();
    let expected = verify::<ConcertHallView, Concert>(journal, halls)
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(verification.verify().await.unwrap_err(), StorageError(expected));
}
