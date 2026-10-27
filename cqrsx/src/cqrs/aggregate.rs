// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{command::decide::Decide, event::DomainEvent};
use serde::{Serialize, de::DeserializeOwned};

/// In CQRS (and Domain Driven Design) an `Aggregate` is the fundamental component that
/// encapsulates the state and application logic (aka business rules) for the application.
///
/// An `Aggregate` is always composed of a
/// [DDD entity](https://docs.microsoft.com/en-us/dotnet/architecture/microservices/microservice-ddd-cqrs-patterns/microservice-domain-model#the-domain-entity-pattern)
/// along with all entities and
/// [value objects](https://docs.microsoft.com/en-us/dotnet/architecture/microservices/microservice-ddd-cqrs-patterns/microservice-domain-model#the-value-object-pattern)
/// associated with it.
///
/// The aggregate holds the state and how every event changes it ([`apply`](Self::apply)). The rules are the
/// commands': every command decides itself on the committed state ([`Decide`]), one event or a rejection, and the
/// aggregate's [`Command`](Self::Command), usually an enum with `#[derive(cqrsx::Commands)]`, routes each to its
/// own decision.
///
/// # Example of a 'Customer' aggregate
/// ```rust
/// # mod example {
/// # use serde::Deserialize;
/// # use serde::Serialize;
/// # use cqrsx::cqrs::event::DomainEvent;
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
/// use cqrsx::cqrs::{aggregate::Aggregate, command::decide::Decide};
/// # use serde::Serialize;
/// # use serde::Deserialize;
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
///     type Rules = CustomerService;
///
///     fn apply(&mut self, event: Self::Event) {
///         match event {
///             CustomerEvent::NameAdded{name: changed_name} => self.name = Some(changed_name),
///             CustomerEvent::EmailUpdated{new_email} => self.email = Some(new_email),
///             CustomerEvent::CustomerDataPopulated => self.data_populated = true,
///         }
///     }
/// }
///
/// // The rules: every command decides one event or a rejection, on the committed state.
/// impl Decide<Customer> for CustomerCommand {
///     fn decide(self, customer: &Customer, _rules: &CustomerService) -> Result<CustomerEvent, CustomerError> {
///         match self {
///             CustomerCommand::AddCustomerName{name: changed_name} => {
///                 if customer.name.is_some() {
///                     return Err("a name has already been added".into());
///                 }
///                 Ok(CustomerEvent::NameAdded{name: changed_name})
///             }
///             CustomerCommand::UpdateEmail { new_email } => Ok(CustomerEvent::EmailUpdated { new_email }),
///         }
///     }
/// }
/// ```
pub trait Aggregate: Default + Serialize + DeserializeOwned + Sync + Send {
    /// The stable aggregate type discriminator, distinct from an aggregate instance ID.
    /// This is used for persisting the events and snapshots to a database.
    const TYPE: &'static str;
    /// Specifies the inbound command used to make changes in the state of the Aggregate: it decides itself
    /// ([`Decide`]), and it is `Send`, so that the future that executes it is.
    type Command: Decide<Self> + Send;
    /// Specifies the published events representing some change in state of the Aggregate.
    type Event: DomainEvent;
    /// The error returned when a command fails due to business logic.
    /// This is used to provide feedback to the user as to the nature of why the command was refused.
    type Error: std::error::Error;
    /// What the rules of the commands depend on besides the state, e.g. a limit from the configuration; the
    /// history does not depend on it (a replay calls only [`apply`](Self::apply)).
    type Rules: Send + Sync;

    /// Deterministically applies an event to aggregate state.
    ///
    /// The store calls it when replaying history or constructing a snapshot, and the framework on a copy of the
    /// committed state when a task planner needs the state the event leaves.
    /// A snapshot represents an event prefix; remaining events rebuild current state.
    /// Validation belongs in the command's decision, so replay never rechecks business rules.
    ///
    /// ```rust
    /// # mod example {
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use cqrsx::cqrs::event::DomainEvent;
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
    /// # use serde::Serialize;
    /// # use serde::Deserialize;
    /// use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// #[derive(Default,Serialize,Deserialize)]
    /// # struct Customer {
    /// #     name: Option<String>,
    /// #     email: Option<String>,
    /// #     data_populated: bool,
    /// # }
    /// # impl Decide<Customer> for CustomerCommand {
    /// #     fn decide(self, _: &Customer, _: &CustomerService) -> Result<CustomerEvent, CustomerError> {
    /// #         Ok(CustomerEvent::NameAdded { name: String::new() })
    /// #     }
    /// # }
    /// # impl Aggregate for Customer {
    /// #     const TYPE: &'static str = "customer";
    /// #     type Command = CustomerCommand;
    /// #     type Event = CustomerEvent;
    /// #     type Error = CustomerError;
    /// #     type Rules = CustomerService;
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

/// A ledger of deltas, the aggregate the unit tests of the stores, the projections and the testing tools write and
/// read; its one command decides a zero delta and is never executed.
#[cfg(test)]
pub(crate) mod tests {
    use crate::cqrs::{aggregate::Aggregate, command::decide::Decide, event::DomainEvent};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    pub(crate) struct Ledger {
        pub balance: i64,
        pub history: Vec<i32>,
    }
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub(crate) struct Delta(pub i32);
    impl DomainEvent for Delta {
        fn event_type(&self) -> String {
            "delta".into()
        }
        fn event_version(&self) -> String {
            "1".into()
        }
    }
    #[derive(Debug, PartialEq, thiserror::Error)]
    #[error("insufficient balance")]
    pub(crate) struct Rejected;
    impl Aggregate for Ledger {
        const TYPE: &'static str = "ledger";
        type Command = ();
        type Event = Delta;
        type Error = Rejected;
        type Rules = ();
        fn apply(&mut self, event: Delta) {
            self.balance += i64::from(event.0);
            self.history.push(event.0);
        }
    }
    impl Decide<Ledger> for () {
        fn decide(self, _: &Ledger, _: &()) -> Result<Delta, Rejected> {
            Ok(Delta(0))
        }
    }
}
