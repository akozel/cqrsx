//! The aggregate the unit tests of the tools write and read: a ledger of deltas. Its one command settles the ledger:
//! the delta that brings the balance to zero, refused for a negative balance.
use cqrsx::domain::{aggregate::Aggregate, command::Decide, event::Events};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ledger {
    pub balance: i64,
    pub history: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delta(pub i32);

impl Events for Delta {
    fn event_type(&self) -> &'static str {
        "delta"
    }
    fn event_version(&self) -> &'static str {
        "1"
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("insufficient balance")]
pub(crate) struct Rejected;

impl Aggregate for Ledger {
    const TYPE: &'static str = "ledger";
    type Id = String;
    type Command = ();
    type Event = Delta;
    type Rejection = Rejected;
    type Rules = ();
    fn apply(&mut self, event: Delta) {
        self.balance += i64::from(event.0);
        self.history.push(event.0);
    }
}

impl Decide<Ledger> for () {
    fn decide(self, ledger: &Ledger, _: &()) -> Result<Delta, Rejected> {
        match i32::try_from(ledger.balance) {
            Ok(balance) if balance >= 0 => Ok(Delta(-balance)),
            _ => Err(Rejected),
        }
    }
}
