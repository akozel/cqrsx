// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::{
    domain::{
        aggregate::Aggregate,
        event::{EventEnvelope, Events},
    },
    journal::{PersistenceError, upcaster::EventUpcaster},
};
use serde_json::Value;
use std::convert::TryFrom;

/// A serialized version of an event with metadata.
/// Used by repositories to store and load events from a database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SerializedEvent {
    /// The id of the aggregate instance.
    pub aggregate_id: String,
    /// The sequence number of the event for this aggregate instance.
    pub sequence: usize,
    /// The type of aggregate the event applies to.
    pub aggregate_type: String,
    /// The type of event that is serialized.
    pub event_type: String,
    /// The version of event that is serialized.
    pub event_version: String,
    /// The serialized domain event.
    pub payload: Value,
    /// Additional metadata, serialized from a HashMap<String,String>.
    pub metadata: Value,
}

/// The type and the version of a serialized event, side by side so that the two strings cannot be swapped: what an
/// [`EventUpcaster`] decides on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventFormat<'a> {
    pub event_type: &'a str,
    pub event_version: &'a str,
}

impl SerializedEvent {
    /// The type and the version of this event.
    pub fn format(&self) -> EventFormat<'_> {
        EventFormat {
            event_type: &self.event_type,
            event_version: &self.event_version,
        }
    }

    fn validate_aggregate<A: Aggregate>(&self) -> Result<(), PersistenceError> {
        if self.aggregate_type != A::TYPE {
            return Err(PersistenceError::DeserializationError(EnvelopeMismatch::AggregateType.into()));
        }
        Ok(())
    }

    pub(crate) fn decode<A: Aggregate>(self, upcasters: &[Box<dyn EventUpcaster>]) -> Result<EventEnvelope<A>, PersistenceError> {
        self.validate_aggregate::<A>()?;
        self.upcast(upcasters).try_into()
    }

    pub(crate) fn upcast(self, upcasters: &[Box<dyn EventUpcaster>]) -> Self {
        upcasters.iter().fold(self, |event, upcaster| {
            if upcaster.can_upcast(event.format()) {
                upcaster.upcast(event)
            } else {
                event
            }
        })
    }
}

pub(crate) fn serialize_events<A: Aggregate>(events: &[EventEnvelope<A>]) -> Result<Vec<SerializedEvent>, PersistenceError> {
    let mut result = Vec::default();
    for event in events {
        result.push(SerializedEvent::try_from(event)?);
    }
    Ok(result)
}

pub(crate) fn deserialize_events<A: Aggregate>(
    events: Vec<SerializedEvent>,
    upcasters: &[Box<dyn EventUpcaster>],
) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
    events.into_iter().map(|event| event.decode(upcasters)).collect()
}

impl<A: Aggregate> TryFrom<&EventEnvelope<A>> for SerializedEvent {
    type Error = PersistenceError;

    fn try_from(event: &EventEnvelope<A>) -> Result<Self, Self::Error> {
        let aggregate_type = A::TYPE.to_string();
        let event_type = event.payload.event_type().to_owned();
        let event_version = event.payload.event_version().to_owned();
        let payload = serde_json::to_value(&event.payload)?;
        let metadata = serde_json::to_value(&event.metadata)?;
        Ok(Self {
            aggregate_id: event.aggregate_id.clone(),
            sequence: event.sequence,
            aggregate_type,
            event_type,
            event_version,
            payload,
            metadata,
        })
    }
}

/// The typed envelope of a stored event: its aggregate type, event type and event version must be `A`'s, and its
/// payload and metadata must decode.
impl<A: Aggregate> TryFrom<SerializedEvent> for EventEnvelope<A> {
    type Error = PersistenceError;

    fn try_from(event: SerializedEvent) -> Result<Self, Self::Error> {
        event.validate_aggregate::<A>()?;
        let payload: A::Event = serde_json::from_value(event.payload)?;
        if event.event_type != payload.event_type() {
            return Err(PersistenceError::DeserializationError(EnvelopeMismatch::EventType.into()));
        }
        if event.event_version != payload.event_version() {
            return Err(PersistenceError::DeserializationError(EnvelopeMismatch::EventVersion.into()));
        }
        let metadata = serde_json::from_value(event.metadata)?;
        Ok(Self {
            aggregate_id: event.aggregate_id,
            sequence: event.sequence,
            payload,
            metadata,
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum EnvelopeMismatch {
    #[error("aggregate type does not match the typed envelope")]
    AggregateType,
    #[error("event type does not match the decoded event")]
    EventType,
    #[error("event version is unsupported by the decoded event")]
    EventVersion,
}

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) fn property_config() -> proptest::test_runner::Config {
        proptest::test_runner::Config {
            cases: 1024,
            failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
                "../target/stage4/proptest-regressions.txt",
            ))),
            ..Default::default()
        }
    }

    use super::*;
    use crate::domain::aggregate::tests::*;
    use crate::journal::upcaster::NumericVersionUpcaster;
    use proptest::prelude::*;
    use std::collections::HashMap;

    proptest! {
        #![proptest_config(property_config())]
        #[test]
        fn property_envelopes_round_trip_all_fields(id in ".{0,30}", sequence in any::<usize>(), delta in any::<i32>(), metadata in prop::collection::hash_map(".{0,12}", ".{0,24}", 0..8)) {
            let envelope = EventEnvelope::<Ledger> { aggregate_id: id.clone(), sequence, payload: Delta(delta), metadata: metadata.clone() };
            let serialized = SerializedEvent::try_from(&envelope).unwrap();
            prop_assert_eq!(&serialized.aggregate_id, &id); prop_assert_eq!(serialized.sequence, sequence);
            prop_assert_eq!(&serialized.aggregate_type, Ledger::TYPE);
            prop_assert_eq!(&serialized.event_type, "delta"); prop_assert_eq!(&serialized.event_version, "1");
            prop_assert_eq!(&serialized.payload, &serde_json::json!(delta));
            prop_assert_eq!(&serialized.metadata, &serde_json::json!(metadata));
            let decoded = EventEnvelope::<Ledger>::try_from(serialized).unwrap();
            prop_assert_eq!(decoded.aggregate_id, id); prop_assert_eq!(decoded.sequence, sequence);
            prop_assert_eq!(decoded.payload, Delta(delta)); prop_assert_eq!(decoded.metadata, metadata);
        }
    }
    proptest! {
        #![proptest_config(property_config())]
        #[test]
        fn property_version_chains_match_independent_upgrade_model(initial in 0usize..9, value in -1000i64..1000, increments in prop::collection::vec(-20i64..20, 0..8)) {
            let original=SerializedEvent { aggregate_id: "🦀".into(), sequence: 37, aggregate_type: Ledger::TYPE.into(), event_type: "delta".into(), event_version: format!("0.{initial}.0"), payload: serde_json::json!(value), metadata: serde_json::json!({"trace":"данные"}) };
            let mut upcasters:Vec<Box<dyn EventUpcaster>>=Vec::new();
            for (index,increment) in increments.iter().copied().enumerate() {
                upcasters.push(Box::new(NumericVersionUpcaster::try_new("delta",&format!("0.{}.0",index+1),Box::new(move |v|serde_json::json!(v.as_i64().unwrap()+increment))).unwrap()));
            }
            let expected=value+increments.iter().skip(initial).sum::<i64>();
            let upgraded=original.clone().upcast(&upcasters);
            prop_assert_eq!(upgraded.payload,serde_json::json!(expected));
            prop_assert_eq!(upgraded.event_version,format!("0.{}.0",initial.max(increments.len())));
            prop_assert_eq!(upgraded.aggregate_id,original.aggregate_id);prop_assert_eq!(upgraded.sequence,original.sequence);
            prop_assert_eq!(upgraded.aggregate_type,original.aggregate_type);prop_assert_eq!(upgraded.event_type,original.event_type);prop_assert_eq!(upgraded.metadata,original.metadata);
        }
    }
    #[test]
    fn corrupted_payload_and_metadata_fail_instead_of_defaulting() {
        let original = SerializedEvent::try_from(&EventEnvelope::<Ledger> {
            aggregate_id: "🦀".into(),
            sequence: 1,
            payload: Delta(7),
            metadata: HashMap::new(),
        })
        .unwrap();
        for corrupt_metadata in [false, true] {
            let mut event = original.clone();
            if corrupt_metadata {
                event.metadata = serde_json::json!({"trace": 12});
            } else {
                event.payload = serde_json::json!({"unknown": true});
            }
            assert!(matches!(
                EventEnvelope::<Ledger>::try_from(event),
                Err(PersistenceError::DeserializationError(_))
            ));
        }
    }
    #[test]
    fn upcaster_chain_preserves_identity_metadata_and_uses_each_previous_version() {
        let original = SerializedEvent {
            aggregate_id: "🦀".into(),
            sequence: 42,
            aggregate_type: Ledger::TYPE.into(),
            event_type: "delta".into(),
            event_version: "0.1.0".into(),
            payload: serde_json::json!({"old": 3}),
            metadata: serde_json::json!({"trace": "данные"}),
        };
        let upcasters: Vec<Box<dyn EventUpcaster>> = vec![
            Box::new(
                NumericVersionUpcaster::try_new(
                    "delta",
                    "0.2.0",
                    Box::new(|v| serde_json::json!({"next": v["old"].as_i64().unwrap()+1})),
                )
                .unwrap(),
            ),
            Box::new(
                NumericVersionUpcaster::try_new(
                    "delta",
                    "1.0.0",
                    Box::new(|v| serde_json::json!(v["next"].as_i64().unwrap() * 2)),
                )
                .unwrap(),
            ),
        ];
        let upcasted = original.clone().upcast(&upcasters);
        assert_eq!(upcasted.aggregate_id, original.aggregate_id);
        assert_eq!(upcasted.sequence, original.sequence);
        assert_eq!(upcasted.aggregate_type, original.aggregate_type);
        assert_eq!(upcasted.event_type, original.event_type);
        assert_eq!(upcasted.event_version, "1.0.0");
        assert_eq!(upcasted.payload, serde_json::json!(8));
        assert_eq!(upcasted.metadata, original.metadata);
        for (kind, version) in [("other", "0.1.0"), ("delta", "2.0.0"), ("delta", "unknown")] {
            let mut unmatched = original.clone();
            unmatched.event_type = kind.into();
            unmatched.event_version = version.into();
            assert_eq!(unmatched.clone().upcast(&upcasters), unmatched);
        }
    }
    fn valid_envelope() -> SerializedEvent {
        SerializedEvent::try_from(&EventEnvelope::<Ledger> {
            aggregate_id: "a".into(),
            sequence: 37,
            payload: Delta(4),
            metadata: HashMap::from([("trace".into(), "данные".into())]),
        })
        .unwrap()
    }
    #[test]
    fn typed_envelope_rejects_foreign_aggregate_before_upcasting() {
        struct MustNotRun;
        impl EventUpcaster for MustNotRun {
            fn can_upcast(&self, _: EventFormat<'_>) -> bool {
                panic!("foreign aggregate reached upcaster")
            }
            fn upcast(&self, _: SerializedEvent) -> SerializedEvent {
                panic!("foreign aggregate reached upcaster")
            }
        }
        let mut event = valid_envelope();
        event.aggregate_type = "foreign".into();
        assert!(matches!(
            EventEnvelope::<Ledger>::try_from(event.clone()),
            Err(PersistenceError::DeserializationError(_))
        ));
        assert!(matches!(
            deserialize_events::<Ledger>(vec![event], &[Box::new(MustNotRun)]),
            Err(PersistenceError::DeserializationError(_))
        ));
    }
    #[test]
    fn typed_envelope_rejects_a_different_event_type() {
        let mut event = valid_envelope();
        event.event_type = "foreign".into();
        assert!(matches!(
            EventEnvelope::<Ledger>::try_from(event),
            Err(PersistenceError::DeserializationError(_))
        ));
    }
    #[test]
    fn typed_envelope_rejects_an_unsupported_version() {
        let mut event = valid_envelope();
        event.event_version = "999".into();
        assert!(matches!(
            EventEnvelope::<Ledger>::try_from(event.clone()),
            Err(PersistenceError::DeserializationError(_))
        ));
        assert!(matches!(
            deserialize_events::<Ledger>(vec![event], &[Box::new(Upgrade("0", "1"))]),
            Err(PersistenceError::DeserializationError(_))
        ));
    }
    struct Upgrade(&'static str, &'static str);
    impl EventUpcaster for Upgrade {
        fn can_upcast(&self, format: EventFormat<'_>) -> bool {
            format.event_type == "delta" && format.event_version == self.0
        }
        fn upcast(&self, mut event: SerializedEvent) -> SerializedEvent {
            event.event_version = self.1.into();
            event.payload = serde_json::json!(event.payload.as_i64().unwrap() + 1);
            event
        }
    }
    #[test]
    fn typed_envelope_accepts_a_chain_ending_at_the_domain_contract() {
        let mut event = valid_envelope();
        event.event_version = "0".into();
        let decoded = deserialize_events::<Ledger>(
            vec![event.clone()],
            &[Box::new(Upgrade("0", "0.5")), Box::new(Upgrade("0.5", "1"))],
        )
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(decoded.payload, Delta(6));
        assert_eq!(decoded.aggregate_id, event.aggregate_id);
        assert_eq!(decoded.sequence, event.sequence);
        assert_eq!(serde_json::to_value(decoded.metadata).unwrap(), event.metadata);
    }
    proptest! {
        #![proptest_config(property_config())]
        #[test]
        fn property_envelope_identity_corruption_is_rejected(mask in 1u8..8, suffix in ".{0,30}", value in any::<i32>()) {
            let mut event=valid_envelope();event.payload=serde_json::json!(value);
            let invalid=format!("unsupported-{suffix}");
            if mask&1!=0 { event.aggregate_type=invalid.clone(); }
            if mask&2!=0 { event.event_type=invalid.clone(); }
            if mask&4!=0 { event.event_version=invalid; }
            prop_assert!(matches!(EventEnvelope::<Ledger>::try_from(event),Err(PersistenceError::DeserializationError(_))));
        }
    }
}
