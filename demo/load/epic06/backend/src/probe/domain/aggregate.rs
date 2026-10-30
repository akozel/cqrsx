//! The `Probe` state and its `impl Aggregate`: it counts its pings, so its snapshot stays a few bytes.
use crate::probe::domain::{Ping, ProbeError, ProbeEvent, ProbeId, events::Pinged};
use cqrsx::cqrs::aggregate::Aggregate;
use serde::{Deserialize, Serialize};

/// What a ping depends on besides the state: how many tasks it plans (`ENCORE_LOAD_TASKS`).
#[derive(Debug, Clone, Default)]
pub struct ProbeRules {
    pub tasks: u8,
}

/// One probe: the count of its pings.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Probe {
    pings: u64,
}

impl Probe {
    /// The pings applied so far.
    pub fn pings(&self) -> u64 {
        self.pings
    }
}

impl Aggregate for Probe {
    const TYPE: &'static str = "Probe";
    type Id = ProbeId;
    type Command = Ping;
    type Event = ProbeEvent;
    type Error = ProbeError;
    type Rules = ProbeRules;

    fn apply(&mut self, ProbeEvent::Pinged(Pinged { .. }): ProbeEvent) {
        self.pings = self.pings.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_probe_counts_its_pings_and_its_snapshot_is_the_count() {
        let mut probe = Probe::default();
        probe.apply(ProbeEvent::Pinged(Pinged { tasks: 4 }));
        probe.apply(ProbeEvent::Pinged(Pinged { tasks: 0 }));
        assert_eq!(probe.pings(), 2);
        assert_eq!(serde_json::to_value(&probe).unwrap(), serde_json::json!({ "pings": 2 }));
    }
}
