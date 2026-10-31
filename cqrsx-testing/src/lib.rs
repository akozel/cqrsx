// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx-testing/licenses/.
//! Test tools for the code that uses `cqrsx`, written on its public API: [`framework::TestFramework`] (an aggregate's
//! given/when/then), [`command_executor::TestCommandExecutor`] (a command executor for use-case tests) and the event
//! stores [`mem_store::MemStore`] and [`unavailable_store::UnavailableStore`].
//!
//! A `TestFramework` should be used to build a comprehensive set of aggregate tests to verify
//! your application logic: given the events of the history, when a command decides, then one event or
//! the rejection. It is synchronous: no runtime is needed. The stores in memory stand in for the databases; the
//! journal in memory, [`MemEventRepository`](cqrsx::memory::event_repository::MemEventRepository), has the task queue
//! [`MemTaskQueue`](cqrsx::memory::task_queue::MemTaskQueue) over its outbox, and the engine in memory
//! (`cqrsx_engine::Cqrs::in_memory`) assembles all of them.
//!
//! A failed check is a panic (rule 8 of `.claude/rules/rust-style.md`): the tools say so in `# Panics`.
//!
//! Derived in part from `cqrs-es` 0.5.0 (Apache-2.0), copyright 2023 Serverless Technology Consulting, LLC; the
//! text of the license is in `cqrsx-testing/licenses/`, the notice in `doc/components.md#upstream-attribution`.
//! ```rust
//! # mod example {
//! # use async_trait::async_trait;
//! # use serde::Deserialize;
//! # use serde::Serialize;
//! # use serde_json::Value;
//! # use cqrsx::domain::aggregate::Aggregate;
//! # use cqrsx::domain::event::Events;
//! # use cqrsx::domain::event::EventEnvelope;
//! # use cqrsx::projection::EventHandler;
//! # use cqrsx::view::View;
//! # use cqrsx::domain::command::Decide;
//! # use cqrsx::journal::repository::PersistedEventRepository;
//! # use cqrsx::journal::PersistenceError;
//! # use cqrsx::journal::serialized_event::SerializedEvent;
//! # use cqrsx::journal::snapshot::SerializedSnapshot;
//! # use cqrsx::view::repository::ViewContext;
//! # use cqrsx::view::repository::ViewRepository;
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
//! # impl Events for CustomerEvent {
//! #     fn event_type(&self) -> &'static str {
//! #         match self {
//! #             Self::NameAdded { .. } => "NameAdded",
//! #             Self::EmailUpdated { .. } => "EmailUpdated",
//! #             CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated",
//! #         }
//! #     }
//! #
//! #     fn event_version(&self) -> &'static str {
//! #         "1.0"
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
//! # use cqrsx_testing::framework::TestFramework;
//! # fn test() {
//! type CustomerTestFramework = TestFramework<Customer>;
//!
//! CustomerTestFramework::new(CustomerService::default())
//!     .given_no_previous_events()
//!     .when(CustomerCommand::AddCustomerName{
//!             name: "John Doe".to_string()
//!         })
//!     .then_expect_event(&CustomerEvent::NameAdded{
//!         name: "John Doe".to_string()
//!     });
//! # }
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test tools: a panic is the failed check (rule 8 of .claude/rules/rust-style.md)"
)]

pub mod command_executor;
pub mod framework;
pub mod mem_store;
pub mod unavailable_store;
pub mod validator;

#[cfg(test)]
mod test_support;
