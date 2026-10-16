use super::*;
use cqrsx::cqrs::event::EventEnvelope;
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ledger(pub(crate) Vec<i32>);
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delta(pub(crate) i32);
impl DomainEvent for Delta {
    fn event_type(&self) -> String {
        "delta".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}
#[derive(Debug, thiserror::Error)]
#[error("negative balance")]
pub(crate) struct Rejected;
impl Aggregate for Ledger {
    const TYPE: &'static str = "contract_ledger";
    type Command = Vec<i32>;
    type Event = Delta;
    type Error = Rejected;
    type Services = ();
    async fn handle(&mut self, changes: Vec<i32>, _: &(), sink: &EventSink<Self>) -> Result<(), Rejected> {
        for change in changes {
            if self.0.iter().map(|v| i64::from(*v)).sum::<i64>() + i64::from(change) < 0 {
                return Err(Rejected);
            }
            sink.write(Delta(change), self).await;
        }
        Ok(())
    }
    fn apply(&mut self, event: Delta) {
        self.0.push(event.0);
    }
}
pub(crate) fn event_for(id: &str, sequence: usize, value: i32) -> SerializedEvent {
    SerializedEvent::try_from(&EventEnvelope::<Ledger> {
        aggregate_id: id.into(),
        sequence,
        payload: Delta(value),
        metadata: HashMap::from([("command".into(), value.to_string())]),
    })
    .unwrap()
}
use std::collections::HashMap;

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct OtherLedger;
impl Aggregate for OtherLedger {
    const TYPE: &'static str = "other_ledger";
    type Command = ();
    type Event = Delta;
    type Error = Rejected;
    type Services = ();
    async fn handle(&mut self, _: (), _: &(), _: &EventSink<Self>) -> Result<(), Rejected> {
        Ok(())
    }
    fn apply(&mut self, _: Delta) {}
}
