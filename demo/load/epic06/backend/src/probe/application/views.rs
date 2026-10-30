//! The views of a probe: one fold, the count of its pings, under as many names as the stand asks for
//! (`ENCORE_LOAD_VIEWS`). Each name is a collection and a projection of its own, so each reads and decodes the
//! journal itself, as every view does.
use crate::probe::domain::Probe;
use cqrsx::cqrs::{event::EventEnvelope, projection::View};
use serde::{Deserialize, Serialize};

/// The pings of one probe, as the view `SLOT` (1–4) counts them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingCount<const SLOT: u8> {
    pub pings: u64,
}

/// `impl View<Probe>` for every slot, under its name.
macro_rules! named_views {
    ($($slot:literal => $name:literal),+ $(,)?) => {
        $(
            impl View<Probe> for PingCount<$slot> {
                const NAME: &'static str = $name;

                fn update(&mut self, _event: &EventEnvelope<Probe>) {
                    self.pings = self.pings.saturating_add(1);
                }
            }
        )+
    };
}

named_views!(1 => "probe_pings_1", 2 => "probe_pings_2", 3 => "probe_pings_3", 4 => "probe_pings_4");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::domain::{Pinged, ProbeEvent};
    use std::collections::HashMap;

    #[test]
    fn every_view_counts_the_pings_under_a_name_of_its_own() {
        let ping = EventEnvelope::<Probe> {
            aggregate_id: "p-1".into(),
            sequence: 1,
            payload: ProbeEvent::Pinged(Pinged { tasks: 1 }),
            metadata: HashMap::new(),
        };
        let mut count = PingCount::<3>::default();
        count.update(&ping);
        count.update(&ping);
        assert_eq!(count.pings, 2);
        let names = [
            <PingCount<1> as View<Probe>>::NAME,
            <PingCount<2> as View<Probe>>::NAME,
            <PingCount<3> as View<Probe>>::NAME,
            <PingCount<4> as View<Probe>>::NAME,
        ];
        assert_eq!(names, ["probe_pings_1", "probe_pings_2", "probe_pings_3", "probe_pings_4"]);
    }
}
