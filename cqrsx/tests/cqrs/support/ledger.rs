use cqrsx::cqrs::{aggregate::Aggregate, event::DomainEvent, event_sink::EventSink};
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
    fn event_type(&self) -> String {
        "delta".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}
#[derive(Clone, Debug)]
pub(crate) enum Command {
    Adjust(Vec<i32>),
    RejectAfterWrite,
}
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("insufficient balance")]
pub(crate) struct Rejected;
#[derive(Clone, Default)]
pub(crate) struct Services(pub Arc<Mutex<Vec<&'static str>>>);
impl Aggregate for Ledger {
    const TYPE: &'static str = "ledger";
    type Command = Command;
    type Event = Delta;
    type Error = Rejected;
    type Services = Services;
    async fn handle(&mut self, command: Command, services: &Services, sink: &EventSink<Self>) -> Result<(), Rejected> {
        services.0.lock().unwrap().push("handle");
        let values = match command {
            Command::Adjust(values) => values,
            Command::RejectAfterWrite => {
                sink.write(Delta(7), self).await;
                return Err(Rejected);
            }
        };
        for value in values {
            if self.balance + i64::from(value) < 0 {
                return Err(Rejected);
            }
            sink.write(Delta(value), self).await;
        }
        Ok(())
    }
    fn apply(&mut self, event: Delta) {
        self.balance += i64::from(event.0);
        self.history.push(event.0);
    }
}
