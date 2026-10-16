// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::aggregate::Aggregate;
use tokio::sync::Mutex;

/// Buffers new events while applying them immediately to a working aggregate.
/// Persistence occurs later, after command handling succeeds.
#[derive(Debug)]
pub struct EventSink<A: Aggregate> {
    events: Mutex<Vec<A::Event>>,
}

impl<A: Aggregate> Default for EventSink<A> {
    fn default() -> Self {
        Self {
            events: Default::default(),
        }
    }
}

impl<A: Aggregate> EventSink<A> {
    /// Apply the event in memory and buffer it; this does not persist the event.
    pub async fn write(&self, event: A::Event, aggregate: &mut A) {
        aggregate.apply(event.clone());
        self.events.lock().await.push(event);
    }

    pub async fn collect(self) -> Vec<A::Event> {
        self.events.into_inner()
    }
}

#[cfg(test)]
pub(in crate::cqrs) mod tests {
    use crate::cqrs::{aggregate::Aggregate, event::DomainEvent, event_sink::EventSink};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    pub(in crate::cqrs) struct Ledger {
        pub balance: i64,
        pub history: Vec<i32>,
    }
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub(in crate::cqrs) struct Delta(pub i32);
    impl DomainEvent for Delta {
        fn event_type(&self) -> String {
            "delta".into()
        }
        fn event_version(&self) -> String {
            "1".into()
        }
    }
    #[derive(Debug, PartialEq, thiserror::Error)]
    #[error("insufficient balance")]
    pub(in crate::cqrs) struct Rejected;
    impl Aggregate for Ledger {
        const TYPE: &'static str = "ledger";
        type Command = ();
        type Event = Delta;
        type Error = Rejected;
        type Services = ();
        async fn handle(&mut self, _: Self::Command, _: &Self::Services, _: &EventSink<Self>) -> Result<(), Self::Error> {
            Ok(())
        }
        fn apply(&mut self, event: Delta) {
            self.balance += i64::from(event.0);
            self.history.push(event.0);
        }
    }

    #[tokio::test]
    async fn applies_immediately_and_collects_in_order_including_empty() {
        assert!(EventSink::<Ledger>::default().collect().await.is_empty());
        let sink = EventSink::default();
        let mut ledger = Ledger::default();
        sink.write(Delta(10), &mut ledger).await;
        assert_eq!(ledger.balance, 10);
        sink.write(Delta(-(ledger.balance as i32)), &mut ledger).await;
        assert_eq!(ledger.balance, 0);
        sink.write(Delta(3), &mut ledger).await;
        assert_eq!(ledger.history, [10, -10, 3]);
        assert_eq!(sink.collect().await, [Delta(10), Delta(-10), Delta(3)]);
    }
}
