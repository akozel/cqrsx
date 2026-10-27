//! One module per read model: its MongoDB collection (the view's name, `View::NAME`), the projection processor that
//! feeds it from the journal of its aggregate type and the verification of it. The index collects them for the
//! aggregates (whose executors wake the processors after a commit, see [`aggregates`](super::aggregates)), the
//! operations and the HTTP queries. A new read model gets a module like these two, listed here.
pub mod concert_halls;
pub mod fan_wallets;

use crate::{
    bootstrap::read_models::{Documents, ReadModels},
    concert::{application::views::concert_hall::ConcertHallView, domain::Concert},
    fan::{application::views::fan_wallet::FanWalletView, domain::Fan},
    operations::application::{activity_observer::ActivityObserver, verify::VerifiedReadModel},
    process::Process,
    shared::application::activity::ActivityHub,
};
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        command::wakeup::CommitWakeup,
        projection::{
            View, ViewProjection, checkpoint::CheckpointKey, processor::ProjectionProcessor, runtime::ProjectionRuntime,
        },
    },
    mongodb::{projection_checkpoints::MongoCheckpointStore, view_repository::MongoViewRepository},
    postgres::event_repository::PostgresEventRepository,
};
use std::sync::Arc;

/// The processor of a read model: the journal on the projection pool, the checkpoint next to the views.
pub type Processor<A> = ProjectionProcessor<A, PostgresEventRepository, MongoCheckpointStore>;

/// One read model as its module wires it.
pub struct ReadModel<V, A: Aggregate> {
    /// The collection, written by the processor, read by the HTTP queries and verify.
    pub repository: Arc<MongoViewRepository<V, A>>,
    pub verification: Arc<dyn VerifiedReadModel>,
    pub processor: Processor<A>,
}

/// The read side as the composition root sees it.
pub struct ReadSideWiring {
    /// What the HTTP queries read through: the engine's repositories.
    pub queries: ReadModels,
    /// Verify over every read model, from the history pool.
    pub verification: Vec<Arc<dyn VerifiedReadModel>>,
    /// Both processors, started next to the task runtime by `process`.
    pub runtime: ProjectionRuntime,
    /// The checkpoint rows of both projections in the read database.
    pub checkpoints: Arc<MongoCheckpointStore>,
    /// What the executors wake after a commit: one per aggregate type, shared with its processor.
    pub concert_wakeup: CommitWakeup,
    pub fan_wakeup: CommitWakeup,
}

/// The checkpoint rows of the read models, the concert halls first: the projection names are the views' names, the
/// collection names.
pub fn checkpoint_keys() -> Vec<CheckpointKey> {
    vec![
        CheckpointKey::of::<ConcertHallView, Concert>(),
        CheckpointKey::of::<FanWalletView, Fan>(),
    ]
}

/// Every read model and its checkpoint live in `ENCORE_READ_DB`.
pub fn read_models(process: &Process, activity: &Arc<ActivityHub>) -> ReadSideWiring {
    let read_db = process.mongo.database(&process.config.read_db);
    let checkpoints = Arc::new(MongoCheckpointStore::new(&read_db));
    let (concert_wakeup, fan_wakeup) = (CommitWakeup::new(), CommitWakeup::new());
    let halls = concert_halls::wire(process, &read_db, &checkpoints, &concert_wakeup, activity);
    let wallets = fan_wallets::wire(process, &read_db, &checkpoints, &fan_wakeup, activity);
    ReadSideWiring {
        queries: ReadModels {
            halls: Arc::new(Documents::MongoDb(halls.repository)),
            wallets: Arc::new(Documents::MongoDb(wallets.repository)),
        },
        verification: vec![halls.verification, wallets.verification],
        runtime: ProjectionRuntime::new()
            .with_processor(halls.processor)
            .with_processor(wallets.processor),
        checkpoints,
        concert_wakeup,
        fan_wakeup,
    }
}

/// A read model's processor: the projection of the view `V` under its name (the name the feed and the checkpoint
/// carry), the journal of `A` through the projection pool, this node as the owner.
pub fn processor<V, A>(
    process: &Process,
    repository: Arc<MongoViewRepository<V, A>>,
    checkpoints: &Arc<MongoCheckpointStore>,
    wakeup: &CommitWakeup,
    activity: &Arc<ActivityHub>,
) -> Processor<A>
where
    V: View<A> + 'static,
    A: Aggregate + 'static,
{
    let config = &process.config;
    ProjectionProcessor::new(
        V::NAME,
        Arc::new(PostgresEventRepository::new(process.projection_pool.clone())),
        checkpoints.clone(),
        Arc::new(ViewProjection::new(repository)),
        config.projection,
    )
    .with_observer(Arc::new(ActivityObserver::new(
        V::NAME,
        config.node_name.clone(),
        activity.clone(),
    )))
    .with_wakeup(wakeup.clone())
    .with_owner(config.node_name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        concert::{
            application::views::concert_hall::{ConcertSummary, SeatEntry, SeatState},
            domain::{ConcertEvent, events::ConcertAnnounced},
        },
        fan::application::views::fan_wallet::{TicketEntry, TicketEntryState},
        operations::application::verify::ProjectionVerification,
        shared::{application::StorageError, domain::venue::Zone},
    };
    use chrono::NaiveDate;
    use cqrsx::cqrs::{
        event::EventEnvelope,
        projection::verify::{Difference, Verification, verify},
        store::{repository::PersistedEventRepository, serialized_event::SerializedEvent},
        testing::{mem_event_repository::MemEventRepository, mem_view_repository::MemViewRepository},
    };
    use mongodb::bson::{from_bson, to_bson};
    use std::collections::{BTreeMap, HashMap};

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
        let journal = Arc::new(MemEventRepository::default());
        journal
            .persist::<Concert>(&[SerializedEvent::try_from(&announced).unwrap()], None)
            .await
            .unwrap();
        let halls = Arc::new(MemViewRepository::<ConcertHallView, Concert>::default());
        let mut hall = ConcertHallView::default();
        hall.update(&announced);
        halls.put("c-1", 1, 1, &hall).unwrap();
        let verification = ProjectionVerification::<_, _, ConcertHallView, Concert>::new(journal.clone(), halls.clone());
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
                kind: "differs"
            }]
        );
        let broken = SerializedEvent::new(
            "c-2".into(),
            1,
            Concert::TYPE.into(),
            "ConcertAnnounced".into(),
            "1".into(),
            serde_json::json!("not an event"),
            serde_json::json!({}),
        );
        journal.persist::<Concert>(&[broken], None).await.unwrap();
        let expected = verify::<ConcertHallView, Concert>(&*journal, &*halls)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(verification.verify().await.unwrap_err(), StorageError(expected));
    }

    #[test]
    fn the_read_models_keep_their_collection_and_checkpoint_names() {
        assert_eq!((ConcertHallView::NAME, FanWalletView::NAME), ("concert_halls", "fan_wallets"));
        let rows: Vec<(String, &str)> = checkpoint_keys()
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
}
