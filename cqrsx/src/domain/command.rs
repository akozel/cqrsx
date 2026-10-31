//! What a command is to the domain: what is known of it ([`CommandInfo`], [`Label`]) and how it decides ([`Decide`]).
use crate::domain::aggregate::Aggregate;
use uuid::Uuid;

/// What is known of a command besides its decision: what an observer reports it by and what an assembly takes its id
/// from. `#[derive(cqrsx::Command)]` implements it on a command type, `#[derive(cqrsx::Commands)]` on the aggregate's
/// enum of commands, by the variant's type; the core does not read it.
///
/// - [`name`](Self::name): the command type's name, `"CancelBooking"`.
/// - [`command_id`](Self::command_id): the field marked `#[command_id]`, when the business id is the id of the command
///   (a booking, a cancellation); a `Uuid`, at most one field. Without it, `None`: the id is not the command's.
/// - [`labels`](Self::labels): the fields marked `#[label]`, in field order, each as its name and its `Display`; what
///   an activity feed shows and filters a command by.
///
/// ```
/// use cqrsx::{
///     Command, Commands,
///     domain::command::{CommandInfo, Label},
/// };
/// use uuid::Uuid;
///
/// #[derive(Command)]
/// struct CancelBooking {
///     seat: String,
///     #[label]
///     fan_id: String,
///     #[command_id]
///     cancellation_id: Uuid,
///     #[label]
///     reason: u8,
/// }
///
/// #[derive(Command)]
/// struct AnnounceConcert {
///     artist: String,
/// }
///
/// let cancellation_id = Uuid::now_v7();
/// let cancel = CancelBooking { seat: "B4".into(), fan_id: "fox".into(), cancellation_id, reason: 2 };
/// assert_eq!(cancel.name(), "CancelBooking");
/// assert_eq!(cancel.command_id(), Some(cancellation_id));
/// assert_eq!(
///     cancel.labels(),
///     [
///         Label { name: "fan_id", value: "fox".to_owned() },
///         Label { name: "reason", value: "2".to_owned() },
///     ]
/// );
/// let announce = AnnounceConcert { artist: "Radiohead".into() };
/// assert_eq!((announce.name(), announce.command_id(), announce.labels()), ("AnnounceConcert", None, vec![]));
/// ```
///
/// One field at most is the command id:
///
/// ```compile_fail
/// #[derive(cqrsx::Command)]
/// struct BookSeat {
///     #[command_id]
///     booking_id: uuid::Uuid,
///     #[command_id]
///     request_id: uuid::Uuid,
/// }
/// ```
///
/// and it is a `Uuid`:
///
/// ```compile_fail
/// #[derive(cqrsx::Command)]
/// struct BookSeat {
///     #[command_id]
///     booking_id: String,
/// }
/// ```
pub trait CommandInfo {
    /// The name of the command's type.
    fn name(&self) -> &'static str;

    /// The id of the command, when a field of it is (`#[command_id]`); `None` by default.
    fn command_id(&self) -> Option<Uuid> {
        None
    }

    /// The labels of the command in field order (`#[label]`); none by default.
    fn labels(&self) -> Vec<Label> {
        Vec::new()
    }
}

/// A labelled field of a command (`#[label]`): its name and its value as its `Display` writes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Label {
    pub name: &'static str,
    pub value: String,
}

/// The rules of a command: what it decides on the committed state of its aggregate `A`, with the aggregate's
/// [`Rules`](Aggregate::Rules).
///
/// A decision is pure and synchronous: no I/O, no clock, no randomness. It runs under the aggregate's lock, and
/// a conflict with another process makes the executor load the aggregate again and decide again on the fresh
/// state, so the same command on the same state must decide the same. Data from outside comes in the command.
///
/// It decides one event or a rejection (decision D-02: the event, its snapshot and its tasks commit in one
/// statement). A rejection is an outcome, not a failure: nothing is written, and the executor answers with the
/// aggregate's error. The event is applied with [`Aggregate::apply`] when the history is replayed, so the
/// decision reads the state and never changes it.
///
/// Each command type decides in its own file; the aggregate's enum of commands, one command type per variant,
/// routes every variant to its type's decision with `#[derive(cqrsx::Commands)]`, which is what
/// `type Command` of the aggregate names:
///
/// ```
/// use cqrsx::{
///     Command, Commands,
///     domain::{aggregate::Aggregate, command::Decide, event::DomainEvent}, testing::TestFramework,
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Default, Serialize, Deserialize)]
/// struct Shelf {
///     items: Vec<String>,
/// }
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// enum ShelfEvent {
///     Put(String),
///     Taken(String),
/// }
///
/// impl DomainEvent for ShelfEvent {
///     fn event_type(&self) -> &'static str {
///         match self {
///             Self::Put(_) => "put",
///             Self::Taken(_) => "taken",
///         }
///     }
///     fn event_version(&self) -> &'static str {
///         "1"
///     }
/// }
///
/// #[derive(Debug, PartialEq, thiserror::Error)]
/// enum ShelfError {
///     #[error("the shelf is full")]
///     Full,
///     #[error("{0} is not on the shelf")]
///     Missing(String),
/// }
///
/// /// What the rules depend on: the capacity of the shelf.
/// struct Capacity(usize);
///
/// impl Aggregate for Shelf {
///     const TYPE: &'static str = "shelf";
///     type Id = String;
///     type Command = ShelfCommand;
///     type Event = ShelfEvent;
///     type Error = ShelfError;
///     type Rules = Capacity;
///
///     fn apply(&mut self, event: ShelfEvent) {
///         match event {
///             ShelfEvent::Put(item) => self.items.push(item),
///             ShelfEvent::Taken(item) => self.items.retain(|kept| *kept != item),
///         }
///     }
/// }
///
/// #[derive(Command)]
/// struct Put {
///     item: String,
/// }
///
/// impl Decide<Shelf> for Put {
///     fn decide(self, shelf: &Shelf, capacity: &Capacity) -> Result<ShelfEvent, ShelfError> {
///         if shelf.items.len() >= capacity.0 {
///             return Err(ShelfError::Full);
///         }
///         Ok(ShelfEvent::Put(self.item))
///     }
/// }
///
/// #[derive(Command)]
/// struct Take {
///     item: String,
/// }
///
/// impl Decide<Shelf> for Take {
///     fn decide(self, shelf: &Shelf, _: &Capacity) -> Result<ShelfEvent, ShelfError> {
///         if !shelf.items.contains(&self.item) {
///             return Err(ShelfError::Missing(self.item));
///         }
///         Ok(ShelfEvent::Taken(self.item))
///     }
/// }
///
/// /// The commands of the shelf: the derive routes each to its own `decide`.
/// #[derive(Commands)]
/// enum ShelfCommand {
///     Put(Put),
///     Take(Take),
/// }
///
/// TestFramework::<Shelf>::new(Capacity(1))
///     .given(vec![ShelfEvent::Put("jam".into())])
///     .when(ShelfCommand::Put(Put { item: "tea".into() }))
///     .then_expect_error(&ShelfError::Full);
/// TestFramework::<Shelf>::new(Capacity(1))
///     .given(vec![ShelfEvent::Put("jam".into())])
///     .when(ShelfCommand::Take(Take { item: "jam".into() }))
///     .then_expect_event(&ShelfEvent::Taken("jam".into()));
/// ```
///
/// Every variant carries one command type, and a struct variant does not compile:
///
/// ```compile_fail
/// #[derive(cqrsx::Commands)]
/// enum ShelfCommand {
///     Put { item: String },
/// }
/// ```
///
/// Nor does a variant whose type does not decide the aggregate (`Take` decides nothing here):
///
/// ```compile_fail
/// use cqrsx::{Command, Commands, domain::{aggregate::Aggregate, command::Decide, event::DomainEvent}};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Default, Serialize, Deserialize)]
/// struct Shelf;
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// struct Put;
/// impl DomainEvent for Put {
///     fn event_type(&self) -> &'static str { "put" }
///     fn event_version(&self) -> &'static str { "1" }
/// }
/// #[derive(Debug, thiserror::Error)]
/// #[error("full")]
/// struct Full;
/// impl Aggregate for Shelf {
///     const TYPE: &'static str = "shelf";
///     type Id = String;
///     type Command = ShelfCommand;
///     type Event = Put;
///     type Error = Full;
///     type Rules = ();
///     fn apply(&mut self, _: Put) {}
/// }
/// #[derive(Command)]
/// struct PutOne;
/// impl Decide<Shelf> for PutOne {
///     fn decide(self, _: &Shelf, _: &()) -> Result<Put, Full> { Ok(Put) }
/// }
/// #[derive(Command)]
/// struct Take;
/// #[derive(Commands)]
/// enum ShelfCommand {
///     Put(PutOne),
///     Take(Take),
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not decide the commands of `{A}`",
    label = "no `Decide<{A}>` for this type",
    note = "implement `Decide<{A}>` for it, or `#[derive(cqrsx::Commands)]` on an enum of command types that do"
)]
pub trait Decide<A: Aggregate>: Sized {
    /// The event this command decides on `state`, the committed state of its aggregate, or the rejection.
    fn decide(self, state: &A, rules: &A::Rules) -> Result<A::Event, A::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Ping;

    impl CommandInfo for Ping {
        fn name(&self) -> &'static str {
            "Ping"
        }
    }

    #[test]
    fn a_command_info_written_by_hand_has_no_id_and_no_labels_unless_it_says_so() {
        assert_eq!((Ping.name(), Ping.command_id(), Ping.labels()), ("Ping", None, vec![]));
    }
}
