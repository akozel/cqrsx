//! The one command of a probe: `Ping`, decided by [`Decide`]; the accepted fact is [`Pinged`].
use crate::probe::domain::{Probe, ProbeError, ProbeEvent, ProbeRules, events::Pinged};
use cqrsx::{Command, cqrs::command::decide::Decide};

/// A hit of the load stand: always accepted, with the tasks the rules ask for.
#[derive(Debug, Clone, PartialEq, Eq, Command)]
pub struct Ping;

impl Decide<Probe> for Ping {
    fn decide(self, _probe: &Probe, rules: &ProbeRules) -> Result<ProbeEvent, ProbeError> {
        Ok(ProbeEvent::Pinged(Pinged { tasks: rules.tasks }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::cqrs::{command::CommandInfo, testing::TestFramework};

    #[test]
    fn every_ping_is_accepted_with_the_tasks_of_the_rules() {
        TestFramework::<Probe>::new(ProbeRules { tasks: 3 })
            .given(vec![ProbeEvent::Pinged(Pinged { tasks: 0 })])
            .when(Ping)
            .then_expect_event(&ProbeEvent::Pinged(Pinged { tasks: 3 }));
        assert_eq!((Ping.name(), Ping.labels(), Ping.command_id()), ("Ping", vec![], None));
    }
}
