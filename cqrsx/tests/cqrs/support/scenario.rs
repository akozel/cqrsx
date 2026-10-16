// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use cqrsx::cqrs::aggregate::Aggregate;
use cqrsx::cqrs::event::DomainEvent;
use cqrsx::cqrs::event_sink::EventSink;

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
    type Command = TestCommand;
    type Event = TestEvent;
    type Error = TestError;
    type Services = TestService;

    async fn handle(
        &mut self,
        command: Self::Command,
        _service: &Self::Services,
        sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
        match &command {
            TestCommand::CreateTest(command) => {
                sink.write(
                    TestEvent::Created(Created {
                        id: command.id.to_string(),
                    }),
                    self,
                )
                .await;
            }

            TestCommand::ConfirmTest(command) => {
                for test in &self.tests {
                    if test == &command.test_name {
                        return Err("test already performed".into());
                    }
                }
                sink.write(
                    TestEvent::Tested(Tested {
                        test_name: command.test_name.to_string(),
                    }),
                    self,
                )
                .await;
            }

            TestCommand::DoSomethingElse(command) => {
                sink.write(
                    TestEvent::SomethingElse(SomethingElse {
                        description: command.description.clone(),
                    }),
                    self,
                )
                .await;
            }
        };
        Ok(())
    }

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
    fn event_type(&self) -> String {
        match self {
            Self::Created(_) => "Created".to_string(),
            Self::Tested(_) => "Tested".to_string(),
            Self::SomethingElse(_) => "SomethingElse".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
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
