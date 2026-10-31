// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use cqrsx::domain::aggregate::Aggregate;
use cqrsx::domain::command::Decide;
use cqrsx::domain::event::Events;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Customer {
    pub customer_id: String,
    pub name: String,
    pub email: String,
    pub data_populated: bool,
}

impl Aggregate for Customer {
    const TYPE: &'static str = "Customer";
    type Id = String;
    type Command = CustomerCommand;
    type Event = CustomerEvent;
    type Error = CustomerError;
    type Rules = CustomerService;

    fn apply(&mut self, event: Self::Event) {
        match event {
            CustomerEvent::NameAdded { name: changed_name } => self.name = changed_name,
            CustomerEvent::EmailUpdated { new_email } => self.email = new_email,
            CustomerEvent::CustomerDataPopulated => self.data_populated = true,
        }
    }
}

impl Decide<Customer> for CustomerCommand {
    fn decide(self, customer: &Customer, _rules: &CustomerService) -> Result<CustomerEvent, CustomerError> {
        match self {
            CustomerCommand::AddCustomerName { .. } if customer.name.as_str() != "" => {
                Err("a name has already been added for this customer".into())
            }
            CustomerCommand::AddCustomerName { name: changed_name } => Ok(CustomerEvent::NameAdded {
                name: changed_name.to_string(),
            }),
            CustomerCommand::UpdateEmail { new_email } => Ok(CustomerEvent::EmailUpdated {
                new_email: new_email.to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct CustomerError(String);

impl From<&str> for CustomerError {
    fn from(message: &str) -> Self {
        Self(message.to_string())
    }
}

#[derive(Clone, Default)]
pub struct CustomerService;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CustomerEvent {
    NameAdded { name: String },
    EmailUpdated { new_email: String },
    CustomerDataPopulated,
}

impl Events for CustomerEvent {
    fn event_type(&self) -> &'static str {
        match self {
            Self::NameAdded { .. } => "NameAdded",
            Self::EmailUpdated { .. } => "EmailUpdated",
            CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated",
        }
    }

    fn event_version(&self) -> &'static str {
        "1.0"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CustomerCommand {
    AddCustomerName { name: String },
    UpdateEmail { new_email: String },
}
