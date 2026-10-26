use std::collections::HashMap;
use {
    cqrsx::cqrs::AggregateError, cqrsx::cqrs::event::EventEnvelope, cqrsx::cqrs::store::EventStore,
    cqrsx::cqrs::store::persisted::PersistedEventStore, cqrsx::cqrs::store::repository::PersistedEventRepository,
    cqrsx::cqrs::store::serialized_event::SerializedEvent, cqrsx::cqrs::testing::mem_event_repository::MemEventRepository,
    cqrsx::cqrs::testing::mem_store::MemStore,
};

mod snapshots {
    use super::*;
    use proptest::prelude::*;
    use {crate::support::ledger::*, crate::support::property_config, cqrsx::cqrs::CqrsFramework};
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
                    let before=model.len();
                    let store=PersistedEventStore::<_,Ledger>::new_snapshot_store(repo.clone(),interval);
                    if framework { CqrsFramework::new(store,Services::default()).execute("history",Command::Adjust(batch.clone())).await.unwrap(); }
                    else { let context=store.load_aggregate("history").await.unwrap(); store.commit(batch.iter().map(|v|Delta(*v)).collect(), context, HashMap::new(), &[]).await.unwrap(); }
                    model.extend(batch);
                    let boundaries:Vec<_>=(before+1..=model.len()).filter(|n|n%interval==0).collect();
                    if let Some(last)=boundaries.last() { watermark = *last;version+=1; }
                    let snapshot=repo.get_snapshot::<Ledger>("history").await.unwrap();
                    if watermark==0 { assert!(snapshot.is_none()); } else { let snapshot=snapshot.unwrap(); assert_eq!(snapshot.aggregate,serde_json::json!({"history":&model[..watermark], "balance":model[..watermark].iter().map(|v|i64::from(*v)).sum::<i64>()})); assert_eq!(snapshot.last_sequence,watermark);assert_eq!(snapshot.snapshot_version,version); }
                    let restored=PersistedEventStore::<_,Ledger>::new_snapshot_store(repo.clone(),interval).load_aggregate("history").await.unwrap();
                    let replay=PersistedEventStore::<_,Ledger>::new_event_store(repo.clone()).load_aggregate("history").await.unwrap();
                    assert_eq!(restored.aggregate.history,model);assert_eq!(replay.aggregate.history,model);
                    assert_eq!(restored.aggregate.balance,model.iter().map(|v|i64::from(*v)).sum::<i64>());
                    assert_eq!(restored.current_sequence,model.len());
                }
            });
        }
    }
}
mod memory {
    use super::*;
    use proptest::prelude::*;
    use {crate::support::ledger::*, crate::support::property_config, cqrsx::cqrs::CqrsFramework};

    proptest! {
        #![proptest_config(property_config())]
        #[test]
        fn property_command_histories_match_independent_balance_model(
            history in prop::collection::vec((0usize..3, prop::collection::vec(-20i32..21, 0..8), ".{0,12}"), 0..45)
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let store = MemStore::<Ledger>::default();
                let framework = CqrsFramework::new(store.clone(), Services::default());
                let mut balances = [0i64; 3];
                let mut accepted: [Vec<i32>; 3] = Default::default();
                let mut metadata_history: [Vec<HashMap<String,String>>; 3] = Default::default();
                for (id, changes, trace) in history {
                    let key = format!("account-{id}");
                    let mut candidate = balances[id];
                    let valid = changes.iter().all(|delta| { candidate += i64::from(*delta); candidate >= 0 });
                    let metadata = HashMap::from([("trace".into(), trace)]);
                    let result = framework.execute_with_metadata(&key, Command::Adjust(changes.clone()), metadata.clone()).await;
                    if valid {
                        result.unwrap(); balances[id] = candidate;
                        metadata_history[id].extend(vec![metadata; changes.len()]);
                        accepted[id].extend(changes);
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
