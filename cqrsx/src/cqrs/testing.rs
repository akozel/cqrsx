// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
//! This module provides a test framework for building a resilient test base around aggregates.
//!
//! A `TestFramework` should be used to build a comprehensive set of aggregate tests to verify
//! your application logic: given the events of the history, when a command decides, then one event or
//! the rejection. It is synchronous: no runtime is needed.
//!
//! ```rust
//! # mod example {
//! # use async_trait::async_trait;
//! # use serde::Deserialize;
//! # use serde::Serialize;
//! # use serde_json::Value;
//! # use cqrsx::cqrs::aggregate::Aggregate;
//! # use cqrsx::cqrs::event::DomainEvent;
//! # use cqrsx::cqrs::event::EventEnvelope;
//! # use cqrsx::cqrs::event_handler::EventHandler;
//! # use cqrsx::cqrs::projection::View;
//! # use cqrsx::cqrs::command::decide::Decide;
//! # use cqrsx::cqrs::store::repository::PersistedEventRepository;
//! # use cqrsx::cqrs::store::PersistenceError;
//! # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
//! # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
//! # use cqrsx::cqrs::projection::repository::ViewContext;
//! # use cqrsx::cqrs::projection::repository::ViewRepository;
//! #
//! # #[derive(Serialize, Deserialize, Default)]
//! # pub struct Customer {
//! #     pub customer_id: String,
//! #     pub name: String,
//! #     pub email: String,
//! #     pub data_populated: bool,
//! # }
//! #
//! # impl Aggregate for Customer {
//! #     const TYPE: &'static str = "Customer";
//! #     type Id = String;
//! #     type Command = CustomerCommand;
//! #     type Event = CustomerEvent;
//! #     type Error = CustomerError;
//! #     type Rules = CustomerService;
//! #
//! #     fn apply(&mut self, event: Self::Event) {
//! #         match event {
//! #             CustomerEvent::NameAdded { name: changed_name } => self.name = changed_name,
//! #             CustomerEvent::EmailUpdated { new_email } => self.email = new_email,
//! #             CustomerEvent::CustomerDataPopulated => self.data_populated = true,
//! #         }
//! #     }
//! # }
//! #
//! # impl Decide<Customer> for CustomerCommand {
//! #     fn decide(self, customer: &Customer, _rules: &CustomerService) -> Result<CustomerEvent, CustomerError> {
//! #         match self {
//! #             CustomerCommand::AddCustomerName { .. } if customer.name.as_str() != "" => {
//! #                 Err("a name has already been added for this customer".into())
//! #             }
//! #             CustomerCommand::AddCustomerName { name: changed_name } => Ok(CustomerEvent::NameAdded {
//! #                 name: changed_name.to_string(),
//! #             }),
//! #             CustomerCommand::UpdateEmail { new_email } => Ok(CustomerEvent::EmailUpdated {
//! #                 new_email: new_email.to_string(),
//! #             }),
//! #         }
//! #     }
//! # }
//! #
//! # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
//! # #[error("{0}")]
//! # pub struct CustomerError(String);
//! #
//! # impl From<&str> for CustomerError {
//! #     fn from(message: &str) -> Self {
//! #         Self(message.to_string())
//! #     }
//! # }
//! #
//! # #[derive(Clone, Default)]
//! # pub struct CustomerService;
//! #
//! # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
//! # pub enum CustomerEvent {
//! #     NameAdded { name: String },
//! #     EmailUpdated { new_email: String },
//! #     CustomerDataPopulated,
//! # }
//! #
//! # impl DomainEvent for CustomerEvent {
//! #     fn event_type(&self) -> String {
//! #         match self {
//! #             Self::NameAdded { .. } => "NameAdded".to_string(),
//! #             Self::EmailUpdated { .. } => "EmailUpdated".to_string(),
//! #             CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated".to_string(),
//! #         }
//! #     }
//! #
//! #     fn event_version(&self) -> String {
//! #         "1.0".to_string()
//! #     }
//! # }
//! #
//! # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
//! # pub enum CustomerCommand {
//! #     AddCustomerName { name: String },
//! #     UpdateEmail { new_email: String },
//! # }
//! # }
//! # use example::Customer;
//! # use example::CustomerEvent;
//! # use example::CustomerCommand;
//! # use example::CustomerService;
//! # use cqrsx::cqrs::testing::TestFramework;
//! # fn test() {
//! type CustomerTestFramework = TestFramework<Customer>;
//!
//! CustomerTestFramework::with(CustomerService::default())
//!     .given_no_previous_events()
//!     .when(CustomerCommand::AddCustomerName{
//!             name: "John Doe".to_string()
//!         })
//!     .then_expect_event(CustomerEvent::NameAdded{
//!         name: "John Doe".to_string()
//!     });
//! # }
//! ```

pub mod command;
pub mod mem_checkpoint_store;
pub mod mem_event_repository;
pub mod mem_store;
pub mod mem_view_repository;
pub mod unavailable_store;
pub mod validator;

use crate::cqrs::{aggregate::Aggregate, command::decide::Decide, testing::validator::AggregateResultValidator};

/// A framework for rigorously testing the aggregate logic, one of the *most important*
/// parts of any DDD system: the history it is given, the command that decides on its fold.
pub struct TestFramework<A: Aggregate> {
    rules: A::Rules,
    events: Vec<A::Event>,
}

impl<A: Aggregate> TestFramework<A> {
    /// Create a test framework using the provided rules, with no previous events.
    pub fn with(rules: A::Rules) -> Self {
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
    /// # use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::event::DomainEvent;
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl DomainEvent for MyEvents {
    /// #     fn event_type(&self) -> String {
    /// #         format!("{:?}", &self)
    /// #     }
    /// #     fn event_version(&self) -> String {
    /// #         "0.1.0".to_string()
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
    /// #     type Error = MyUserError;
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
    /// use cqrsx::cqrs::testing::TestFramework;
    ///
    /// let framework = TestFramework::<MyAggregate>::with(MyService)
    ///     .given_no_previous_events();
    /// ```
    #[must_use]
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
    /// # use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::event::DomainEvent;
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl DomainEvent for MyEvents {
    /// #     fn event_type(&self) -> String {
    /// #         format!("{:?}", &self)
    /// #     }
    /// #     fn event_version(&self) -> String {
    /// #         "0.1.0".to_string()
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
    /// #     type Error = MyUserError;
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
    /// use cqrsx::cqrs::testing::TestFramework;
    ///
    /// let framework = TestFramework::<MyAggregate>::with(MyService)
    ///     .given(vec![MyEvents::SomethingWasDone]);
    /// ```
    #[must_use]
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
    /// # use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::event::DomainEvent;
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl DomainEvent for MyEvents {
    /// #     fn event_type(&self) -> String {
    /// #         format!("{:?}", &self)
    /// #     }
    /// #     fn event_version(&self) -> String {
    /// #         "0.1.0".to_string()
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
    /// #     type Error = MyUserError;
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
    /// use cqrsx::cqrs::testing::TestFramework;
    ///
    /// let framework = TestFramework::<MyAggregate>::with(MyService)
    ///     .given(vec![MyEvents::SomethingWasDone])
    ///     .and(vec![MyEvents::SomethingElseWasDone]);
    /// ```
    #[must_use]
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
    /// # use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::event::DomainEvent;
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl DomainEvent for MyEvents {
    /// #     fn event_type(&self) -> String {
    /// #         format!("{:?}", &self)
    /// #     }
    /// #     fn event_version(&self) -> String {
    /// #         "0.1.0".to_string()
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
    /// #     type Error = MyUserError;
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
    /// use cqrsx::cqrs::testing::TestFramework;
    ///
    /// let validator = TestFramework::<MyAggregate>::with(MyService)
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
