use cqrsx::domain::{aggregate::Aggregate, command::Decide, event::DomainEvent};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ledger {
    pub balance: i64,
    pub history: Vec<i32>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delta(pub i32);
impl DomainEvent for Delta {
    fn event_type(&self) -> &'static str {
        "delta"
    }
    fn event_version(&self) -> &'static str {
        "1"
    }
}
/// One delta: refused when it would take the balance below zero.
#[derive(Clone, Debug)]
pub(crate) enum Command {
    Adjust(i32),
}
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("insufficient balance")]
pub(crate) struct Rejected;
#[derive(Clone, Default)]
pub(crate) struct Services(pub Arc<Mutex<Vec<&'static str>>>);
impl Aggregate for Ledger {
    const TYPE: &'static str = "ledger";
    type Id = String;
    type Command = Command;
    type Event = Delta;
    type Error = Rejected;
    type Rules = Services;
    fn apply(&mut self, event: Delta) {
        self.balance += i64::from(event.0);
        self.history.push(event.0);
    }
}
impl Decide<Ledger> for Command {
    fn decide(self, ledger: &Ledger, services: &Services) -> Result<Delta, Rejected> {
        services.0.lock().unwrap().push("decide");
        let Command::Adjust(value) = self;
        if ledger.balance + i64::from(value) < 0 {
            return Err(Rejected);
        }
        Ok(Delta(value))
    }
}
