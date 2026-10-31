// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx-postgres/licenses/.

use cqrsx::domain::aggregate::Aggregate;
use cqrsx::domain::command::Decide;
use cqrsx::domain::event::EventEnvelope;
use cqrsx::domain::event::Events;
use cqrsx::journal::serialized_event::SerializedEvent;
use cqrsx::journal::snapshot::SerializedSnapshot;
use cqrsx::view::View;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub(crate) struct TestAggregate {
    pub(crate) id: String,
    pub(crate) description: String,
    pub(crate) tests: Vec<String>,
}

impl Aggregate for TestAggregate {
    const TYPE: &'static str = "TestAggregate";
    type Id = String;
    type Command = TestCommand;
    type Event = TestEvent;
    type Error = TestError;
    type Rules = TestServices;

    fn apply(&mut self, _e: Self::Event) {}
}

impl Decide<TestAggregate> for TestCommand {
    fn decide(self, _: &TestAggregate, _: &TestServices) -> Result<TestEvent, TestError> {
        Ok(TestEvent::Created(Created { id: String::new() }))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) enum TestEvent {
    Created(Created),
    Tested(Tested),
    SomethingElse(SomethingElse),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct Created {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct Tested {
    pub test_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SomethingElse {
    pub description: String,
}

impl Events for TestEvent {
    fn event_type(&self) -> &'static str {
        match self {
            Self::Created(_) => "Created",
            Self::Tested(_) => "Tested",
            Self::SomethingElse(_) => "SomethingElse",
        }
    }

    fn event_version(&self) -> &'static str {
        "1.0"
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct TestError(String);

#[derive(Debug)]
pub(crate) struct TestServices;

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

pub(crate) enum TestCommand {}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct TestView {
    pub(crate) events: Vec<TestEvent>,
}

impl View<TestAggregate> for TestView {
    const NAME: &'static str = "test_view";
    fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
        self.events.push(event.payload.clone());
    }
}

pub(crate) fn test_serialized_event(id: &str, sequence: usize, event: &TestEvent) -> SerializedEvent {
    let payload: Value = serde_json::to_value(event).unwrap();
    SerializedEvent {
        aggregate_id: id.to_string(),
        sequence,
        aggregate_type: TestAggregate::TYPE.to_string(),
        event_type: event.event_type().to_owned(),
        event_version: event.event_version().to_owned(),
        payload,
        metadata: serde_json::json!({}),
    }
}

pub(crate) fn serialized_snapshot(
    aggregate_id: String,
    last_sequence: usize,
    snapshot_version: usize,
    aggregate: Value,
) -> SerializedSnapshot {
    SerializedSnapshot {
        aggregate_id,
        aggregate,
        last_sequence,
        snapshot_version,
    }
}

pub(crate) mod customer {
    use cqrsx::domain::aggregate::Aggregate;
    use cqrsx::domain::command::Decide;
    use cqrsx::domain::event::Events;
    use serde::Deserialize;
    use serde::Serialize;

    #[derive(Clone, Serialize, Deserialize, Default)]
    pub struct Customer {
        pub customer_id: String,
        pub name: String,
        pub email: String,
        pub data_populated: bool,
    }

    impl Aggregate for Customer {
        const TYPE: &'static str = "Customer";
        type Id = String;
        type Command = CustomerCommand;
        type Event = CustomerEvent;
        type Error = CustomerError;
        type Rules = CustomerService;

        fn apply(&mut self, event: Self::Event) {
            match event {
                CustomerEvent::NameAdded { name: changed_name } => self.name = changed_name,
                CustomerEvent::EmailUpdated { new_email } => self.email = new_email,
                CustomerEvent::CustomerDataPopulated => self.data_populated = true,
            }
        }
    }

    impl Decide<Customer> for CustomerCommand {
        fn decide(self, customer: &Customer, _rules: &CustomerService) -> Result<CustomerEvent, CustomerError> {
            match self {
                CustomerCommand::AddCustomerName { .. } if customer.name.as_str() != "" => {
                    Err("a name has already been added for this customer".into())
                }
                CustomerCommand::AddCustomerName { name: changed_name } => Ok(CustomerEvent::NameAdded {
                    name: changed_name.to_string(),
                }),
                CustomerCommand::UpdateEmail { new_email } => Ok(CustomerEvent::EmailUpdated {
                    new_email: new_email.to_string(),
                }),
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
    #[error("{0}")]
    pub struct CustomerError(String);

    impl From<&str> for CustomerError {
        fn from(message: &str) -> Self {
            Self(message.to_string())
        }
    }

    #[derive(Clone, Default)]
    pub struct CustomerService;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub enum CustomerEvent {
        NameAdded { name: String },
        EmailUpdated { new_email: String },
        CustomerDataPopulated,
    }

    impl Events for CustomerEvent {
        fn event_type(&self) -> &'static str {
            match self {
                Self::NameAdded { .. } => "NameAdded",
                Self::EmailUpdated { .. } => "EmailUpdated",
                CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated",
            }
        }

        fn event_version(&self) -> &'static str {
            "1.0.1"
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub enum CustomerCommand {
        AddCustomerName { name: String },
        UpdateEmail { new_email: String },
    }
}

/// Bound and observe all competing tasks, including cleanup after a failure.
pub(crate) async fn finish_writers<T: Send + 'static>(tasks: &mut tokio::task::JoinSet<T>) -> Vec<T> {
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut results = Vec::new();
        while let Some(result) = tasks.join_next().await {
            results.push(result?);
        }
        Ok::<_, tokio::task::JoinError>(results)
    })
    .await;
    match result {
        Ok(Ok(results)) => results,
        other => {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            match other {
                Ok(Err(error)) => panic!("writer failed: {error}"),
                _ => panic!("writers exceeded their deadline"),
            }
        }
    }
}

pub(crate) async fn wait_for_writers<T: Send + 'static>(
    pool: &sqlx::PgPool,
    fragment: &str,
    count: i64,
    tasks: &mut tokio::task::JoinSet<T>,
) {
    use futures::FutureExt;
    let result = std::panic::AssertUnwindSafe(super::support::wait_for_blocked_queries(pool, fragment, count))
        .catch_unwind()
        .await;
    if let Err(panic) = result {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        std::panic::resume_unwind(panic);
    }
}
