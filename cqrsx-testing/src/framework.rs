// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx-testing/licenses/.
//! The given/when/then test of an aggregate: [`TestFramework`].

use crate::validator::AggregateResultValidator;
use cqrsx::domain::{aggregate::Aggregate, command::Decide};
use std::fmt;

/// A framework for rigorously testing the aggregate logic, one of the *most important*
/// parts of any DDD system: the history it is given, the command that decides on its fold.
#[must_use = "a test checks nothing until `when` decides a command and its validator checks the result"]
pub struct TestFramework<A: Aggregate> {
    rules: A::Rules,
    events: Vec<A::Event>,
}

// By hand: the rules need not be `Debug`.
impl<A: Aggregate> fmt::Debug for TestFramework<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestFramework")
            .field("aggregate_type", &A::TYPE)
            .field("events", &self.events)
            .finish_non_exhaustive()
    }
}

impl<A: Aggregate> TestFramework<A> {
    /// Create a test framework using the provided rules, with no previous events.
    pub fn new(rules: A::Rules) -> Self {
        Self {
            rules,
            events: Vec::new(),
        }
    }

    /// Initiates an aggregate test with no previous events.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::EventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewMeta;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # pub enum MyCommands {
    /// #     DoSomething,
    /// #     BadCommand,
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyAggregate;
    /// #
    /// # impl Aggregate for MyAggregate {
    /// #     const TYPE: &'static str = "MyAggregate";
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Rejection = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct MyUserError(pub String);
    /// #
    /// # impl From<&str> for MyUserError {
    /// #     fn from(msg: &str) -> Self {
    /// #         Self(msg.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Clone, Default)]
    /// # pub struct MyService;
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyService;
    /// use cqrsx_testing::framework::TestFramework;
    ///
    /// let framework = TestFramework::<MyAggregate>::new(MyService)
    ///     .given_no_previous_events();
    /// ```
    pub fn given_no_previous_events(self) -> Self {
        self.given(Vec::new())
    }

    /// Initiates an aggregate test with a collection of previous events: the history, replacing what was given
    /// before.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::EventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewMeta;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # pub enum MyCommands {
    /// #     DoSomething,
    /// #     BadCommand,
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyAggregate;
    /// #
    /// # impl Aggregate for MyAggregate {
    /// #     const TYPE: &'static str = "MyAggregate";
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Rejection = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct MyUserError(pub String);
    /// #
    /// # impl From<&str> for MyUserError {
    /// #     fn from(msg: &str) -> Self {
    /// #         Self(msg.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Clone, Default)]
    /// # pub struct MyService;
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyService;
    /// # use example::MyEvents;
    /// use cqrsx_testing::framework::TestFramework;
    ///
    /// let framework = TestFramework::<MyAggregate>::new(MyService)
    ///     .given(vec![MyEvents::SomethingWasDone]);
    /// ```
    pub fn given(self, events: Vec<A::Event>) -> Self {
        Self { events, ..self }
    }

    /// Adds additional events to an aggregate test.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::EventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewMeta;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # pub enum MyCommands {
    /// #     DoSomething,
    /// #     BadCommand,
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyAggregate;
    /// #
    /// # impl Aggregate for MyAggregate {
    /// #     const TYPE: &'static str = "MyAggregate";
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Rejection = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct MyUserError(pub String);
    /// #
    /// # impl From<&str> for MyUserError {
    /// #     fn from(msg: &str) -> Self {
    /// #         Self(msg.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Clone, Default)]
    /// # pub struct MyService;
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyService;
    /// # use example::MyEvents;
    /// use cqrsx_testing::framework::TestFramework;
    ///
    /// let framework = TestFramework::<MyAggregate>::new(MyService)
    ///     .given(vec![MyEvents::SomethingWasDone])
    ///     .and(vec![MyEvents::SomethingElseWasDone]);
    /// ```
    pub fn and(mut self, events: Vec<A::Event>) -> Self {
        self.events.extend(events);
        self
    }

    /// Decides `command` on the fold of the events given and provides a validator object to test against.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::EventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewMeta;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # pub enum MyCommands {
    /// #     DoSomething,
    /// #     BadCommand,
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyAggregate;
    /// #
    /// # impl Aggregate for MyAggregate {
    /// #     const TYPE: &'static str = "MyAggregate";
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Rejection = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct MyUserError(pub String);
    /// #
    /// # impl From<&str> for MyUserError {
    /// #     fn from(msg: &str) -> Self {
    /// #         Self(msg.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Clone, Default)]
    /// # pub struct MyService;
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyService;
    /// # use example::MyCommands;
    /// use cqrsx_testing::framework::TestFramework;
    ///
    /// let validator = TestFramework::<MyAggregate>::new(MyService)
    ///     .given_no_previous_events()
    ///     .when(MyCommands::DoSomething);
    /// ```
    pub fn when(self, command: A::Command) -> AggregateResultValidator<A> {
        let mut state = A::default();
        for event in self.events {
            state.apply(event);
        }
        AggregateResultValidator::new(command.decide(&state, &self.rules))
    }
}
