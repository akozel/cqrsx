//! The one fact of a probe, stored flat (`#[serde(tag = "type")]`) like the box office's.
use cqrsx::{Event, Events};
use serde::{Deserialize, Serialize};

/// Fact: a probe was pinged; its reaction plans `tasks` tasks (the rules of the ping).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Event)]
#[event(name = "Pinged", version = "1.0")]
pub struct Pinged {
    pub tasks: u8,
}

/// The facts of a probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Events)]
#[serde(tag = "type")]
pub enum ProbeEvent {
    Pinged(Pinged),
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::cqrs::event::DomainEvent;
    use serde_json::json;

    #[test]
    fn a_ping_is_stored_as_a_small_flat_document() {
        let event = ProbeEvent::Pinged(Pinged { tasks: 4 });
        assert_eq!(serde_json::to_value(&event).unwrap(), json!({ "type": "Pinged", "tasks": 4 }));
        assert_eq!(
            (event.event_type(), event.event_version()),
            ("Pinged".to_owned(), "1.0".to_owned())
        );
    }
}
