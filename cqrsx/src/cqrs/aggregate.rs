// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{event::DomainEvent, event_sink::EventSink};
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;

/// In CQRS (and Domain Driven Design) an `Aggregate` is the fundamental component that
/// encapsulates the state and application logic (aka business rules) for the application.
///
/// An `Aggregate` is always composed of a
/// [DDD entity](https://docs.microsoft.com/en-us/dotnet/architecture/microservices/microservice-ddd-cqrs-patterns/microservice-domain-model#the-domain-entity-pattern)
/// along with all entities and
/// [value objects](https://docs.microsoft.com/en-us/dotnet/architecture/microservices/microservice-ddd-cqrs-patterns/microservice-domain-model#the-value-object-pattern)
/// associated with it.
///
/// # Example of a 'Customer' aggregate
/// ```rust
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
/// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
/// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
/// # use cqrsx::cqrs::projection::repository::ViewContext;
/// # use cqrsx::cqrs::projection::repository::ViewRepository;
/// #
/// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
/// # #[error("{0}")]
/// # pub struct CustomerError(String);
/// #
/// # impl From<&str> for CustomerError {
/// #     fn from(message: &str) -> Self {
/// #         Self(message.to_string())
/// #     }
/// # }
/// #
/// # #[derive(Clone, Default)]
/// # pub struct CustomerService;
/// #
/// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// # pub enum CustomerEvent {
/// #     NameAdded { name: String },
/// #     EmailUpdated { new_email: String },
/// #     CustomerDataPopulated,
/// # }
/// #
/// # impl DomainEvent for CustomerEvent {
/// #     fn event_type(&self) -> String {
/// #         match self {
/// #             Self::NameAdded { .. } => "NameAdded".to_string(),
/// #             Self::EmailUpdated { .. } => "EmailUpdated".to_string(),
/// #             CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated".to_string(),
/// #         }
/// #     }
/// #
/// #     fn event_version(&self) -> String {
/// #         "1.0".to_string()
/// #     }
/// # }
/// #
/// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// # pub enum CustomerCommand {
/// #     AddCustomerName { name: String },
/// #     UpdateEmail { new_email: String },
/// # }
/// # }
/// # use example::CustomerEvent;
/// # use example::CustomerError;
/// # use example::CustomerCommand;
/// # use example::CustomerService;
/// # use cqrsx::cqrs::aggregate::Aggregate;
/// # use cqrsx::cqrs::AggregateError;
/// # use serde::Serialize;
/// # use serde::Deserialize;
/// # use cqrsx::cqrs::event_sink::EventSink;
/// #[derive(Default,Serialize,Deserialize)]
/// struct Customer {
///     name: Option<String>,
///     email: Option<String>,
///     data_populated: bool,
/// }
///
/// impl Aggregate for Customer {
///     const TYPE: &'static str = "customer";
///     type Command = CustomerCommand;
///     type Event = CustomerEvent;
///     type Error = CustomerError;
///     type Services = CustomerService;
///
///     async fn handle(&mut self, command: Self::Command, service: &Self::Services, sink: &EventSink<Self>) -> Result<(), Self::Error> {
///         match command {
///             CustomerCommand::AddCustomerName{name: changed_name} => {
///                 if self.name.is_some() {
///                     return Err("a name has already been added".into());
///                 }
///                 sink.write(CustomerEvent::NameAdded{name: changed_name.to_string()}, self).await;
///             }
///
///             CustomerCommand::UpdateEmail { new_email } => {
///                 sink.write(CustomerEvent::EmailUpdated { new_email: new_email.to_string()}, self).await;
///             }
///         };
///         Ok(())
///     }
///
///     fn apply(&mut self, event: Self::Event) {
///         match event {
///             CustomerEvent::NameAdded{name: changed_name} => self.name = Some(changed_name),
///             CustomerEvent::EmailUpdated{new_email} => self.email = Some(new_email),
///             CustomerEvent::CustomerDataPopulated => self.data_populated = true,
///         }
///     }
/// }
/// ```
pub trait Aggregate: Default + Serialize + DeserializeOwned + Sync + Send {
    /// The stable aggregate type discriminator, distinct from an aggregate instance ID.
    /// This is used for persisting the events and snapshots to a database.
    const TYPE: &'static str;
    /// Specifies the inbound command used to make changes in the state of the Aggregate.
    type Command;
    /// Specifies the published events representing some change in state of the Aggregate.
    type Event: DomainEvent;
    /// The error returned when a command fails due to business logic.
    /// This is used to provide feedback to the user as to the nature of why the command was refused.
    type Error: std::error::Error;
    /// Dependencies required by command handling, separate from application services.
    type Services: Send + Sync;

    /// This method consumes and processes commands.
    /// Emit accepted events through the sink and return `Ok(())`, or reject the
    /// command with a domain error. A rejected command commits none of its events.
    ///
    /// _All business logic belongs in this method_.
    ///
    /// ```rust
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
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct CustomerError(String);
    /// #
    /// # impl From<&str> for CustomerError {
    /// #     fn from(message: &str) -> Self {
    /// #         Self(message.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Clone, Default)]
    /// # pub struct CustomerService;
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    /// # pub enum CustomerEvent {
    /// #     NameAdded { name: String },
    /// #     EmailUpdated { new_email: String },
    /// #     CustomerDataPopulated,
    /// # }
    /// #
    /// # impl DomainEvent for CustomerEvent {
    /// #     fn event_type(&self) -> String {
    /// #         match self {
    /// #             Self::NameAdded { .. } => "NameAdded".to_string(),
    /// #             Self::EmailUpdated { .. } => "EmailUpdated".to_string(),
    /// #             CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated".to_string(),
    /// #         }
    /// #     }
    /// #
    /// #     fn event_version(&self) -> String {
    /// #         "1.0".to_string()
    /// #     }
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    /// # pub enum CustomerCommand {
    /// #     AddCustomerName { name: String },
    /// #     UpdateEmail { new_email: String },
    /// # }
    /// # }
    /// # use example::CustomerCommand;
    /// # use example::CustomerError;
    /// # use example::CustomerEvent;
    /// # use example::CustomerService;
    /// # use std::sync::Arc;
    /// use cqrsx::cqrs::aggregate::Aggregate;
    /// use cqrsx::cqrs::AggregateError;
    /// # use serde::Serialize;
    /// # use serde::Deserialize;
    /// # use serde::de::DeserializeOwned;
    /// # use cqrsx::cqrs::event_sink::EventSink;
    /// #[derive(Default,Serialize,Deserialize)]
    /// # struct Customer {
    /// #     name: Option<String>,
    /// #     email: Option<String>,
    /// # }
    /// # impl Aggregate for Customer {
    /// #     const TYPE: &'static str = "customer";
    /// #     type Command = CustomerCommand;
    /// #     type Event = CustomerEvent;
    /// #     type Error = CustomerError;
    /// #     type Services = CustomerService;
    /// async fn handle(&mut self, command: Self::Command, service: &Self::Services, sink: &EventSink<Self>) -> Result<(), Self::Error> {
    ///     match command {
    ///         CustomerCommand::AddCustomerName{name: changed_name} => {
    ///             if self.name.is_some() {
    ///                 return Err("a name has already been added".into());
    ///             }
    ///             sink.write(CustomerEvent::NameAdded{ name: changed_name.to_string()}, self).await;
    ///         }
    ///
    ///         CustomerCommand::UpdateEmail { new_email } => {
    ///             sink.write(CustomerEvent::EmailUpdated { new_email: new_email.to_string() }, self).await;
    ///         }
    ///     };
    ///     Ok(())
    /// }
    /// # fn apply(&mut self, event: Self::Event) {}
    /// # }
    /// ```
    fn handle(
        &mut self,
        command: Self::Command,
        service: &Self::Services,
        sink: &EventSink<Self>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// Deterministically applies an event to aggregate state.
    ///
    /// The event sink calls this on the command's working copy before persistence.
    /// The store also calls it when replaying history or constructing a snapshot.
    /// A snapshot represents an event prefix; remaining events rebuild current state.
    /// Validation belongs in command handling, so replay never rechecks business rules.
    ///
    /// ```rust
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
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct CustomerError(String);
    /// #
    /// # impl From<&str> for CustomerError {
    /// #     fn from(message: &str) -> Self {
    /// #         Self(message.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Clone, Default)]
    /// # pub struct CustomerService;
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    /// # pub enum CustomerEvent {
    /// #     NameAdded { name: String },
    /// #     EmailUpdated { new_email: String },
    /// #     CustomerDataPopulated,
    /// # }
    /// #
    /// # impl DomainEvent for CustomerEvent {
    /// #     fn event_type(&self) -> String {
    /// #         match self {
    /// #             Self::NameAdded { .. } => "NameAdded".to_string(),
    /// #             Self::EmailUpdated { .. } => "EmailUpdated".to_string(),
    /// #             CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated".to_string(),
    /// #         }
    /// #     }
    /// #
    /// #     fn event_version(&self) -> String {
    /// #         "1.0".to_string()
    /// #     }
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    /// # pub enum CustomerCommand {
    /// #     AddCustomerName { name: String },
    /// #     UpdateEmail { new_email: String },
    /// # }
    /// # }
    /// # use example::CustomerCommand;
    /// # use example::CustomerError;
    /// # use example::CustomerEvent;
    /// # use example::CustomerService;
    /// # use std::sync::Arc;
    /// # use serde::Serialize;
    /// # use serde::Deserialize;
    /// # use serde::de::DeserializeOwned;
    /// # use cqrsx::cqrs::event_sink::EventSink;
    /// use cqrsx::cqrs::aggregate::Aggregate;
    /// use cqrsx::cqrs::AggregateError;
    /// #[derive(Default,Serialize,Deserialize)]
    /// # struct Customer {
    /// #     name: Option<String>,
    /// #     email: Option<String>,
    /// #     data_populated: bool,
    /// # }
    /// # impl Aggregate for Customer {
    /// #     const TYPE: &'static str = "customer";
    /// #     type Command = CustomerCommand;
    /// #     type Event = CustomerEvent;
    /// #     type Error = CustomerError;
    /// #     type Services = CustomerService;
    /// # async fn handle(&mut self, command: Self::Command, service: &Self::Services, sink: &EventSink<Self>) -> Result<(), Self::Error> {
    /// # Ok(())
    /// # }
    /// fn apply(&mut self, event: Self::Event) {
    ///     match event {
    ///         CustomerEvent::NameAdded{name: changed_name} => self.name = Some(changed_name),
    ///         CustomerEvent::EmailUpdated{new_email} => self.email = Some(new_email),
    ///         CustomerEvent::CustomerDataPopulated => self.data_populated = true,
    ///     }
    /// }
    /// # }
    /// ```
    fn apply(&mut self, event: Self::Event);
}

/// One aggregate instance, unique across aggregate types: the same ID under two types is two keys.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AggregateKey {
    aggregate_type: &'static str,
    aggregate_id: String,
}

impl AggregateKey {
    pub fn new(aggregate_type: &'static str, aggregate_id: impl Into<String>) -> Self {
        Self {
            aggregate_type,
            aggregate_id: aggregate_id.into(),
        }
    }

    /// The key of `aggregate_id` under [`A::TYPE`](Aggregate::TYPE).
    pub fn of<A: Aggregate>(aggregate_id: &str) -> Self {
        Self::new(A::TYPE, aggregate_id)
    }

    pub fn aggregate_type(&self) -> &'static str {
        self.aggregate_type
    }

    pub fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }
}
