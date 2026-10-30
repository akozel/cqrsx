//! What follows a ping: the tasks its rules asked for, in order, each with its place among them.
use crate::probe::{
    application::echo::ProbeEchoV1,
    domain::{Pinged, Probe, ProbeEvent},
};
use cqrsx::cqrs::reactions::{Decided, Reactions, Then};

impl Reactions for Probe {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            ProbeEvent::Pinged(Pinged { tasks }) => {
                for slot in 0..*tasks {
                    then.task(ProbeEchoV1 {
                        probe_id: decided.id.to_string(),
                        slot,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::domain::{Ping, ProbeId};
    use cqrsx::task::NewTask;
    use serde_json::{Value, json};
    use std::collections::HashMap;

    /// The tasks a ping that asked for `tasks` plans.
    fn planned(tasks: u8) -> Vec<NewTask> {
        let (id, state, metadata): (ProbeId, Probe, HashMap<String, String>) =
            ("p-1".parse().unwrap(), Probe::default(), HashMap::new());
        let event = ProbeEvent::Pinged(Pinged { tasks });
        let mut then = Then::default();
        Probe::react(&Decided::new(&id, &state, &Ping, &event, &metadata), &mut then);
        then.into_tasks().unwrap()
    }

    #[test]
    fn a_ping_plans_the_tasks_it_asked_for_each_in_its_place() {
        assert!(planned(0).is_empty());
        let tasks = planned(4);
        let slots: Vec<&Value> = tasks.iter().map(|task| &task.payload()["slot"]).collect();
        assert_eq!(slots, [&json!(0), &json!(1), &json!(2), &json!(3)]);
        assert!(tasks.iter().all(|task| task.metadata() == &json!({ "probe_id": "p-1" })));
    }
}
