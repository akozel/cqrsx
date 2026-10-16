// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use cqrsx::cqrs::aggregate::Aggregate;
use cqrsx::cqrs::event::DomainEvent;
use cqrsx::cqrs::event_sink::EventSink;
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
    type Command = CustomerCommand;
    type Event = CustomerEvent;
    type Error = CustomerError;
    type Services = CustomerService;

    async fn handle(
        &mut self,
        command: Self::Command,
        _service: &Self::Services,
        sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
        match command {
            CustomerCommand::AddCustomerName { .. } if self.name.as_str() != "" => {
                return Err("a name has already been added for this customer".into());
            }
            CustomerCommand::AddCustomerName { name: changed_name } => {
                sink.write(
                    CustomerEvent::NameAdded {
                        name: changed_name.to_string(),
                    },
                    self,
                )
                .await;
            }
            CustomerCommand::UpdateEmail { new_email } => {
                sink.write(
                    CustomerEvent::EmailUpdated {
                        new_email: new_email.to_string(),
                    },
                    self,
                )
                .await;
            }
        };
        if !self.data_populated && !self.name.is_empty() && !self.email.is_empty() {
            sink.write(CustomerEvent::CustomerDataPopulated, self).await;
        }
        Ok(())
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            CustomerEvent::NameAdded { name: changed_name } => self.name = changed_name,
            CustomerEvent::EmailUpdated { new_email } => self.email = new_email,
            CustomerEvent::CustomerDataPopulated => self.data_populated = true,
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

impl DomainEvent for CustomerEvent {
    fn event_type(&self) -> String {
        match self {
            Self::NameAdded { .. } => "NameAdded".to_string(),
            Self::EmailUpdated { .. } => "EmailUpdated".to_string(),
            CustomerEvent::CustomerDataPopulated => "CustomerDataPoplated".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CustomerCommand {
    AddCustomerName { name: String },
    UpdateEmail { new_email: String },
}
