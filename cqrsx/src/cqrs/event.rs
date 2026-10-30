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
/// important information. With one event type per variant (`SeatBooked(SeatBooked)`),
/// `#[derive(cqrsx::Events)]` implements this trait from the [`EventKind`] of each variant's type;
/// the example below implements it by hand.
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
/// # use cqrsx::cqrs::command::decide::Decide;
/// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
/// # use cqrsx::cqrs::store::PersistenceError;
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
/// #     type Id = String;
/// #     type Command = CustomerCommand;
/// #     type Event = CustomerEvent;
/// #     type Error = CustomerError;
/// #     type Rules = CustomerService;
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
/// # impl Decide<Customer> for CustomerCommand {
/// #     fn decide(self, customer: &Customer, _rules: &CustomerService) -> Result<CustomerEvent, CustomerError> {
/// #         match self {
/// #             CustomerCommand::AddCustomerName { .. } if customer.name.as_str() != "" => {
/// #                 Err("a name has already been added for this customer".into())
/// #             }
/// #             CustomerCommand::AddCustomerName { name: changed_name } => Ok(CustomerEvent::NameAdded {
/// #                 name: changed_name.to_string(),
/// #             }),
/// #             CustomerCommand::UpdateEmail { new_email } => Ok(CustomerEvent::EmailUpdated {
/// #                 new_email: new_email.to_string(),
/// #             }),
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
/// #     fn event_type(&self) -> &'static str {
/// #         match self {
/// #             Self::NameAdded { .. } => "NameAdded",
/// #             Self::EmailUpdated { .. } => "EmailUpdated",
/// #             CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated",
/// #         }
/// #     }
/// #
/// #     fn event_version(&self) -> &'static str {
/// #         "1.0"
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
    /// A name specifying the event, used for event upcasting: a constant of the type, so encoding and decoding an
    /// event never allocate it.
    fn event_type(&self) -> &'static str;
    /// A version of the `event_type`, used for event upcasting; a constant as well.
    fn event_version(&self) -> &'static str;
}

/// The stored name and version of one event type, what `#[derive(cqrsx::Event)]` writes from its
/// `#[event(name = "…", version = "…")]`.
///
/// Both are stored with every event of the type (`event_type`, `event_version`) and read back by the
/// upcasters, so a published name and version never change: a new meaning is a new version. An
/// aggregate's enum of events takes them from the type of each variant with `#[derive(cqrsx::Events)]`,
/// which implements [`DomainEvent`].
///
/// ```
/// use cqrsx::{
///     Event, Events,
///     cqrs::event::{DomainEvent, EventKind},
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
/// #[event(name = "SeatBooked", version = "1.0")]
/// pub struct SeatBooked {
///     pub seat: String,
/// }
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
/// #[event(name = "BookingCancelled", version = "2.0")]
/// pub struct BookingCancelled {
///     pub seat: String,
///     pub refund: u32,
/// }
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
/// #[serde(tag = "type")]
/// pub enum ConcertEvent {
///     SeatBooked(SeatBooked),
///     BookingCancelled(BookingCancelled),
/// }
///
/// assert_eq!((SeatBooked::NAME, SeatBooked::VERSION), ("SeatBooked", "1.0"));
/// let cancelled = ConcertEvent::BookingCancelled(BookingCancelled { seat: "B4".into(), refund: 80 });
/// assert_eq!(cancelled.event_type(), "BookingCancelled");
/// assert_eq!(cancelled.event_version(), "2.0");
/// let booked = ConcertEvent::SeatBooked(SeatBooked { seat: "B4".into() });
/// assert_eq!((booked.event_type(), booked.event_version()), ("SeatBooked", "1.0"));
/// ```
///
/// Every variant carries one event type, so a variant with fields of its own does not compile:
///
/// ```compile_fail
/// # use cqrsx::{Event, Events};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
/// # #[event(name = "SeatBooked", version = "1.0")]
/// # pub struct SeatBooked {
/// #     pub seat: String,
/// # }
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
/// pub enum ConcertEvent {
///     SeatBooked(SeatBooked),
///     BookingCancelled { seat: String, refund: u32 },
/// }
/// ```
///
/// Nor does a variant whose type has no stored name and version, so a new event never takes the
/// version of its neighbour:
///
/// ```compile_fail
/// # use cqrsx::{Event, Events};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
/// # #[event(name = "SeatBooked", version = "1.0")]
/// # pub struct SeatBooked {
/// #     pub seat: String,
/// # }
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// pub struct BookingCancelled {
///     pub seat: String,
/// }
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
/// pub enum ConcertEvent {
///     SeatBooked(SeatBooked),
///     BookingCancelled(BookingCancelled),
/// }
/// ```
///
/// And `Event` needs its attribute:
///
/// ```compile_fail
/// # use cqrsx::Event;
/// # use serde::{Deserialize, Serialize};
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
/// pub struct SeatBooked {
///     pub seat: String,
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no stored event name and version",
    label = "not an event type",
    note = "derive `cqrsx::Event` on it, with `#[event(name = \"…\", version = \"…\")]`"
)]
pub trait EventKind {
    /// The name stored with every event of the type: `event_type`.
    const NAME: &'static str;
    /// The version stored with every event of the type: `event_version`.
    const VERSION: &'static str;
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
/// `metadata`. `Clone`, `Debug` and `PartialEq` need only the event's, not the aggregate's.
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

impl<A: Aggregate> fmt::Debug for EventEnvelope<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventEnvelope")
            .field("aggregate_id", &self.aggregate_id)
            .field("sequence", &self.sequence)
            .field("payload", &self.payload)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl<A: Aggregate> PartialEq for EventEnvelope<A> {
    fn eq(&self, other: &Self) -> bool {
        (&self.aggregate_id, self.sequence, &self.payload, &self.metadata)
            == (&other.aggregate_id, other.sequence, &other.payload, &other.metadata)
    }
}
