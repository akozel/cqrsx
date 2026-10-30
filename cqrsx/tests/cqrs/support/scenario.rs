// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use cqrsx::cqrs::aggregate::Aggregate;
use cqrsx::cqrs::command::decide::Decide;
use cqrsx::cqrs::event::DomainEvent;

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
pub struct TestAggregate {
    pub(crate) id: String,
    pub(crate) description: String,
    pub(crate) tests: Vec<String>,
}
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TestError(pub(crate) String);

impl From<&str> for TestError {
    fn from(msg: &str) -> Self {
        Self(msg.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct TestService;

impl Aggregate for TestAggregate {
    const TYPE: &'static str = "TestAggregate";
    type Id = String;
    type Command = TestCommand;
    type Event = TestEvent;
    type Error = TestError;
    type Rules = TestService;

    fn apply(&mut self, event: Self::Event) {
        match event {
            TestEvent::Created(e) => {
                self.id = e.id;
            }
            TestEvent::Tested(e) => {
                self.tests.push(e.test_name);
            }
            TestEvent::SomethingElse(e) => {
                self.description = e.description;
            }
        }
    }
}

impl Decide<TestAggregate> for TestCommand {
    fn decide(self, aggregate: &TestAggregate, _service: &TestService) -> Result<TestEvent, TestError> {
        match &self {
            TestCommand::CreateTest(command) => Ok(TestEvent::Created(Created {
                id: command.id.to_string(),
            })),

            TestCommand::ConfirmTest(command) => {
                for test in &aggregate.tests {
                    if test == &command.test_name {
                        return Err("test already performed".into());
                    }
                }
                Ok(TestEvent::Tested(Tested {
                    test_name: command.test_name.to_string(),
                }))
            }

            TestCommand::DoSomethingElse(command) => Ok(TestEvent::SomethingElse(SomethingElse {
                description: command.description.clone(),
            })),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TestEvent {
    Created(Created),
    Tested(Tested),
    SomethingElse(SomethingElse),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Created {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Tested {
    pub test_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SomethingElse {
    pub description: String,
}

impl DomainEvent for TestEvent {
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

pub enum TestCommand {
    CreateTest(CreateTest),
    ConfirmTest(ConfirmTest),
    DoSomethingElse(DoSomethingElse),
}

pub struct CreateTest {
    pub id: String,
}

pub struct ConfirmTest {
    pub test_name: String,
}

pub struct DoSomethingElse {
    pub description: String,
}

pub(crate) fn metadata() -> HashMap<String, String> {
    let now = "2021-03-18T12:32:45.930Z".to_string();
    let mut metadata = HashMap::new();
    metadata.insert("time".to_string(), now);
    metadata
}
