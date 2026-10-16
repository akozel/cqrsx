// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::aggregate::Aggregate;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::HashMap, fmt};

/// A `DomainEvent` represents any business change in the state of an `Aggregate`.
///
/// `DomainEvent`s are immutable, and when
/// [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
/// is used they are the single source of truth.
///
/// The name of a `DomainEvent` should always be in the past tense, e.g.,
/// - AdminPrivilegesGranted
/// - EmailAddressChanged
/// - DependencyAdded
///
/// To simplify serialization, an event should be an enum, and each variant should carry any
/// important information.
///
/// The `DomainEvent` trait provides event type and version; events must also derive a
/// number of standard traits.
/// - `Clone` - events may be cloned throughout the framework, particularly when applied to event_handlers
/// - `Serialize` and `Deserialize` - required for persistence
/// - `PartialEq` and `Debug` - needed for effective testing
///
/// # Examples
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
/// # #[derive(Serialize, Deserialize, Default)]
/// # pub struct Customer {
/// #     pub customer_id: String,
/// #     pub name: String,
/// #     pub email: String,
/// #     pub data_populated: bool,
/// # }
/// #
/// # impl Aggregate for Customer {
/// #     const TYPE: &'static str = "Customer";
/// #     type Command = CustomerCommand;
/// #     type Event = CustomerEvent;
/// #     type Error = CustomerError;
/// #     type Services = CustomerService;
/// #
/// #     async fn handle(
/// #         &mut self,
/// #         command: Self::Command,
/// #         _service: &Self::Services,
/// #         sink: &EventSink<Self>,
/// #     ) -> Result<(), Self::Error> {
/// #         match command {
/// #             CustomerCommand::AddCustomerName { .. } if self.name.as_str() != "" => {
/// #                 return Err("a name has already been added for this customer".into())
/// #             }
/// #             CustomerCommand::AddCustomerName { name: changed_name } => {
/// #                 sink.write(
/// #                     CustomerEvent::NameAdded {
/// #                         name: changed_name.to_string(),
/// #                     },
/// #                     self,
/// #                 )
/// #                 .await;
/// #             }
/// #             CustomerCommand::UpdateEmail { new_email } => {
/// #                 sink.write(
/// #                     CustomerEvent::EmailUpdated {
/// #                         new_email: new_email.to_string(),
/// #                     },
/// #                     self,
/// #                 )
/// #                 .await;
/// #             }
/// #         };
/// #         if !self.data_populated && !self.name.is_empty() && !self.email.is_empty() {
/// #             sink.write(CustomerEvent::CustomerDataPopulated, self).await;
/// #         }
/// #         Ok(())
/// #     }
/// #
/// #     fn apply(&mut self, event: Self::Event) {
/// #         match event {
/// #             CustomerEvent::NameAdded { name: changed_name } => self.name = changed_name,
/// #             CustomerEvent::EmailUpdated { new_email } => self.email = new_email,
/// #             CustomerEvent::CustomerDataPopulated => self.data_populated = true,
/// #         }
/// #     }
/// # }
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
/// # use example::Customer;
/// # use cqrsx::cqrs::aggregate::Aggregate;
/// # use cqrsx::cqrs::event::DomainEvent;
/// # use serde::Serialize;
/// # use serde::Deserialize;
/// #[derive(Clone,Debug,Serialize,Deserialize,PartialEq)]
/// pub enum CustomerEvent {
///     NameChanged{ changed_name: String },
///     EmailUpdated{ new_email: String },
/// }
/// ```
pub trait DomainEvent: Serialize + DeserializeOwned + Clone + PartialEq + fmt::Debug + Sync + Send {
    /// A name specifying the event, used for event upcasting.
    fn event_type(&self) -> String;
    /// A version of the `event_type`, used for event upcasting.
    fn event_version(&self) -> String;
}

/// `EventEnvelope` is a data structure that encapsulates an event with its pertinent
/// information.
/// All of the associated data will be transported and persisted together and will be available
/// for event_handlers.
///
/// Within any system an event must be unique based on the compound key composed of its:
/// - [`Aggregate::TYPE`]
/// - `aggregate_id`
/// - `sequence`
///
/// Thus an `EventEnvelope` provides a uniqueness value along with an event `payload` and
/// `metadata`.
#[derive(Debug)]
pub struct EventEnvelope<A>
where
    A: Aggregate,
{
    /// The id of the aggregate instance.
    pub aggregate_id: String,
    /// The sequence number for an aggregate instance.
    pub sequence: usize,
    /// The event payload with all business information.
    pub payload: A::Event,
    /// Additional metadata for use in auditing, logging or debugging purposes.
    pub metadata: HashMap<String, String>,
}

impl<A: Aggregate> Clone for EventEnvelope<A> {
    fn clone(&self) -> Self {
        Self {
            aggregate_id: self.aggregate_id.clone(),
            sequence: self.sequence,
            payload: self.payload.clone(),
            metadata: self.metadata.clone(),
        }
    }
}
