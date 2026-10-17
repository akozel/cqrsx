// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
//! This module provides a test framework for building a resilient test base around aggregates.
//!
//! A `TestFramework` should be used to build a comprehensive set of aggregate tests to verify
//! your application logic.
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
//! # use cqrsx::cqrs::event_sink::EventSink;
//! # use cqrsx::cqrs::store::repository::PersistedEventRepository;
//! # use cqrsx::cqrs::store::PersistenceError;
//! # use cqrsx::cqrs::store::stream::ReplayStream;
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
//! #     type Command = CustomerCommand;
//! #     type Event = CustomerEvent;
//! #     type Error = CustomerError;
//! #     type Services = CustomerService;
//! #
//! #     async fn handle(
//! #         &mut self,
//! #         command: Self::Command,
//! #         _service: &Self::Services,
//! #         sink: &EventSink<Self>,
//! #     ) -> Result<(), Self::Error> {
//! #         match command {
//! #             CustomerCommand::AddCustomerName { .. } if self.name.as_str() != "" => {
//! #                 return Err("a name has already been added for this customer".into())
//! #             }
//! #             CustomerCommand::AddCustomerName { name: changed_name } => {
//! #                 sink.write(
//! #                     CustomerEvent::NameAdded {
//! #                         name: changed_name.to_string(),
//! #                     },
//! #                     self,
//! #                 )
//! #                 .await;
//! #             }
//! #             CustomerCommand::UpdateEmail { new_email } => {
//! #                 sink.write(
//! #                     CustomerEvent::EmailUpdated {
//! #                         new_email: new_email.to_string(),
//! #                     },
//! #                     self,
//! #                 )
//! #                 .await;
//! #             }
//! #         };
//! #         if !self.data_populated && !self.name.is_empty() && !self.email.is_empty() {
//! #             sink.write(CustomerEvent::CustomerDataPopulated, self).await;
//! #         }
//! #         Ok(())
//! #     }
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
//!     .then_expect_events(vec![
//!         CustomerEvent::NameAdded{
//!             name: "John Doe".to_string()
//!         }]);
//! # }
//! ```

pub mod command;
pub mod executor;
pub mod mem_store;
pub mod mem_view_repository;
pub mod unavailable_store;
pub mod validator;

use crate::cqrs::{aggregate::Aggregate, testing::executor::AggregateTestExecutor};

/// A framework for rigorously testing the aggregate logic, one of the *most important*
/// parts of any DDD system.
pub struct TestFramework<A: Aggregate> {
    service: A::Services,
}

impl<A: Aggregate> TestFramework<A> {
    /// Create a test framework using the provided service.
    pub fn with(service: A::Services) -> Self {
        Self { service }
    }
}

impl<A> TestFramework<A>
where
    A: Aggregate,
{
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
    /// # use cqrsx::cqrs::event_sink::EventSink;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::stream::ReplayStream;
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
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Services = MyService;
    /// #
    /// #     async fn handle(
    /// #         &mut self,
    /// #         command: Self::Command,
    /// #         _service: &Self::Services,
    /// #         sink: &EventSink<Self>,
    /// #     ) -> Result<(), Self::Error> {
    /// #         match command {
    /// #             MyCommands::DoSomething => {
    /// #                 sink.write(MyEvents::SomethingWasDone, self).await;
    /// #                 Ok(())
    /// #             }
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
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
    /// let executor = TestFramework::<MyAggregate>::with(MyService)
    ///     .given_no_previous_events();
    /// ```
    #[must_use]
    pub fn given_no_previous_events(self) -> AggregateTestExecutor<A> {
        AggregateTestExecutor::new(Vec::new(), self.service)
    }
    /// Initiates an aggregate test with a collection of previous events.
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
    /// # use cqrsx::cqrs::event_sink::EventSink;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::stream::ReplayStream;
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
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Services = MyService;
    /// #
    /// #     async fn handle(
    /// #         &mut self,
    /// #         command: Self::Command,
    /// #         _service: &Self::Services,
    /// #         sink: &EventSink<Self>,
    /// #     ) -> Result<(), Self::Error> {
    /// #         match command {
    /// #             MyCommands::DoSomething => {
    /// #                 sink.write(MyEvents::SomethingWasDone, self).await;
    /// #                 Ok(())
    /// #             }
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
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
    /// # use example::MyEvents;
    /// # use example::MyService;
    /// use cqrsx::cqrs::testing::TestFramework;
    ///
    /// let executor = TestFramework::<MyAggregate>::with(MyService)
    ///     .given(vec![MyEvents::SomethingWasDone]);
    /// ```
    #[must_use]
    pub fn given(self, events: Vec<A::Event>) -> AggregateTestExecutor<A> {
        AggregateTestExecutor::new(events, self.service)
    }
}
