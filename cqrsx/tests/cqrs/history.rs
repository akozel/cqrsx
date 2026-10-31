use std::collections::HashMap;
use {
    cqrsx::command::AggregateError, cqrsx::cqrs::testing::mem_event_repository::MemEventRepository,
    cqrsx::cqrs::testing::mem_store::MemStore, cqrsx::domain::event::EventEnvelope, cqrsx::journal::EventStore,
    cqrsx::journal::persisted::PersistedEventStore, cqrsx::journal::repository::PersistedEventRepository,
    cqrsx::journal::serialized_event::SerializedEvent,
};

mod snapshots {
    use super::*;
    use proptest::prelude::*;
    use {crate::support::ledger::*, crate::support::property_config, cqrsx::command::framework::CqrsFramework};
    proptest! {
        #![proptest_config(property_config())]
        #[test]
        fn property_snapshot_histories_match_prefix_model(interval in 1usize..26, initial in 0usize..8, framework in any::<bool>(), batches in prop::collection::vec(prop::collection::vec(0i32..30,0..12),1..20)) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let repo=MemEventRepository::default();
                let mut model=vec![1;initial];
                let seed:Vec<_>=model.iter().enumerate().map(|(i,v)|SerializedEvent::try_from(&EventEnvelope::<Ledger>{aggregate_id:"history".into(),sequence:i+1,payload:Delta(*v),metadata:HashMap::new()}).unwrap()).collect();
                repo.persist::<Ledger>(&seed,None).await.unwrap();
                let (mut watermark,mut version)=(0,0);
                for batch in batches {
                    // Through the framework every value is a command of its own and a commit of its own; directly, the
                    // batch is one commit.
                    let commits:Vec<Vec<i32>>=if framework { batch.iter().map(|v|vec![*v]).collect() } else { vec![batch] };
                    for batch in commits {
                        let before=model.len();
                        let store=PersistedEventStore::<_,Ledger>::new(repo.clone()).with_snapshots(std::num::NonZeroUsize::new(interval).unwrap());
                        if framework { CqrsFramework::new(store,Services::default()).execute(&"history".into(),Command::Adjust(batch[0])).await.unwrap(); }
                        else { let context=store.load_aggregate("history").await.unwrap(); store.commit(batch.iter().map(|v|Delta(*v)).collect(), context, HashMap::new(), &[]).await.unwrap(); }
                        model.extend(batch);
                        let boundaries:Vec<_>=(before+1..=model.len()).filter(|n|n%interval==0).collect();
                        if let Some(last)=boundaries.last() { watermark = *last;version+=1; }
                        let snapshot=repo.snapshot::<Ledger>("history").await.unwrap();
                        if watermark==0 { assert!(snapshot.is_none()); } else { let snapshot=snapshot.unwrap(); assert_eq!(snapshot.aggregate,serde_json::json!({"history":&model[..watermark], "balance":model[..watermark].iter().map(|v|i64::from(*v)).sum::<i64>()})); assert_eq!(snapshot.last_sequence,watermark);assert_eq!(snapshot.snapshot_version,version); }
                        let restored=PersistedEventStore::<_,Ledger>::new(repo.clone()).with_snapshots(std::num::NonZeroUsize::new(interval).unwrap()).load_aggregate("history").await.unwrap();
                        let replay=PersistedEventStore::<_,Ledger>::new(repo.clone()).load_aggregate("history").await.unwrap();
                        assert_eq!(restored.aggregate.history,model);assert_eq!(replay.aggregate.history,model);
                        assert_eq!(restored.aggregate.balance,model.iter().map(|v|i64::from(*v)).sum::<i64>());
                        assert_eq!(restored.current_sequence,model.len());
                    }
                }
            });
        }
    }
}
mod memory {
    use super::*;
    use proptest::prelude::*;
    use {crate::support::ledger::*, crate::support::property_config, cqrsx::command::framework::CqrsFramework};

    proptest! {
        #![proptest_config(property_config())]
        #[test]
        fn property_command_histories_match_independent_balance_model(
            history in prop::collection::vec((0usize..3, -20i32..21, ".{0,12}"), 0..45)
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let store = MemStore::<Ledger>::default();
                let framework = CqrsFramework::new(store.clone(), Services::default());
                let mut balances = [0i64; 3];
                let mut accepted: [Vec<i32>; 3] = Default::default();
                let mut metadata_history: [Vec<HashMap<String,String>>; 3] = Default::default();
                for (id, delta, trace) in history {
                    let key = format!("account-{id}");
                    let candidate = balances[id] + i64::from(delta);
                    let valid = candidate >= 0;
                    let metadata = HashMap::from([("trace".into(), trace)]);
                    let result = framework.execute_with_metadata(&key, Command::Adjust(delta), metadata.clone()).await;
                    if valid {
                        result.unwrap(); balances[id] = candidate;
                        metadata_history[id].push(metadata);
                        accepted[id].push(delta);
                    } else { assert!(matches!(result, Err(AggregateError::Domain(Rejected)))); }
                    for other in 0..3 {
                        let other_key = format!("account-{other}");
                        let context = store.load_aggregate(&other_key).await.unwrap();
                        assert_eq!(context.aggregate.balance, balances[other]);
                        assert_eq!(context.aggregate.history, accepted[other]);
                        assert_eq!(context.current_sequence, accepted[other].len());
                        let envelopes = store.load_events(&other_key).await.unwrap();
                        for (i, envelope) in envelopes.iter().enumerate() {
                            assert_eq!(envelope.aggregate_id, other_key); assert_eq!(envelope.sequence, i+1);
                            assert_eq!(envelope.payload, Delta(accepted[other][i]));
                            assert_eq!(envelope.metadata, metadata_history[other][i]);
                        }
                    }
                }
            });
        }
    }
}

mod upcasting {
    use super::*;
    use crate::projection::support::{Heal, delta, old_delta};
    use crate::support::ledger::{Delta, Ledger};
    use std::num::NonZeroUsize;

    async fn journal() -> MemEventRepository {
        let repo = MemEventRepository::default();
        repo.persist::<Ledger>(&[old_delta("k", 1), delta("k", 2, 5)], None)
            .await
            .unwrap();
        repo
    }

    #[tokio::test]
    async fn the_upcasters_of_a_store_reshape_old_events_on_load_in_the_aggregate_and_the_event_list() {
        let repo = journal().await;
        let plain = PersistedEventStore::<_, Ledger>::new(repo.clone());
        assert!(plain.load_events("k").await.is_err(), "nobody reads the version-0 event");

        let store = PersistedEventStore::<_, Ledger>::new(repo).with_upcasters(vec![Box::new(Heal)]);
        let events = store.load_events("k").await.unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| (event.sequence, event.payload.clone()))
                .collect::<Vec<_>>(),
            [(1, Delta(0)), (2, Delta(5))]
        );
        let context = store.load_aggregate("k").await.unwrap();
        assert_eq!(
            (context.aggregate.balance, context.aggregate.history.clone()),
            (5, vec![0, 5])
        );
    }

    #[tokio::test]
    async fn a_cloned_store_reads_the_same_journal_through_the_same_upcasters_and_keeps_its_policy() {
        let store = PersistedEventStore::<_, Ledger>::new(journal().await)
            .with_snapshots(NonZeroUsize::new(3).unwrap())
            .with_upcasters(vec![Box::new(Heal)]);
        let clone = store.clone();
        assert_eq!(format!("{clone:?}"), format!("{store:?}"));
        assert!(format!("{clone:?}").contains("Every(3)") && format!("{clone:?}").contains("upcasters: 1"));
        assert_eq!(clone.load_aggregate("k").await.unwrap().aggregate.history, [0, 5]);
        // A commit through the clone is visible through the original: one journal.
        let context = clone.load_aggregate("k").await.unwrap();
        clone.commit(vec![Delta(2)], context, HashMap::new(), &[]).await.unwrap();
        assert_eq!(store.load_aggregate("k").await.unwrap().aggregate.history, [0, 5, 2]);
    }
}
