use super::*;
use cqrsx::domain::event::EventEnvelope;
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ledger(pub(crate) Vec<i32>);
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delta(pub(crate) i32);
impl Events for Delta {
    fn event_type(&self) -> &'static str {
        "delta"
    }
    fn event_version(&self) -> &'static str {
        "1"
    }
}
#[derive(Debug, thiserror::Error)]
#[error("negative balance")]
pub(crate) struct Rejected;
impl Aggregate for Ledger {
    const TYPE: &'static str = "contract_ledger";
    type Id = String;
    type Command = i32;
    type Event = Delta;
    type Error = Rejected;
    type Rules = ();
    fn apply(&mut self, event: Delta) {
        self.0.push(event.0);
    }
}
impl Decide<Ledger> for i32 {
    fn decide(self, ledger: &Ledger, _: &()) -> Result<Delta, Rejected> {
        if ledger.0.iter().map(|v| i64::from(*v)).sum::<i64>() + i64::from(self) < 0 {
            return Err(Rejected);
        }
        Ok(Delta(self))
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
    type Id = String;
    type Command = ();
    type Event = Delta;
    type Error = Rejected;
    type Rules = ();
    fn apply(&mut self, _: Delta) {}
}
impl Decide<OtherLedger> for () {
    fn decide(self, _: &OtherLedger, _: &()) -> Result<Delta, Rejected> {
        Ok(Delta(0))
    }
}
