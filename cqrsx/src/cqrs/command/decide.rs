//! How a command decides: the rules of one command on the state of its aggregate, one event or a rejection.
use crate::cqrs::aggregate::Aggregate;

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
///     cqrs::{aggregate::Aggregate, command::decide::Decide, event::DomainEvent, testing::TestFramework},
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
///     fn event_type(&self) -> String {
///         match self {
///             Self::Put(_) => "put".into(),
///             Self::Taken(_) => "taken".into(),
///         }
///     }
///     fn event_version(&self) -> String {
///         "1".into()
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
///     .then_expect_error(ShelfError::Full);
/// TestFramework::<Shelf>::new(Capacity(1))
///     .given(vec![ShelfEvent::Put("jam".into())])
///     .when(ShelfCommand::Take(Take { item: "jam".into() }))
///     .then_expect_event(ShelfEvent::Taken("jam".into()));
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
/// use cqrsx::{Command, Commands, cqrs::{aggregate::Aggregate, command::decide::Decide, event::DomainEvent}};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Default, Serialize, Deserialize)]
/// struct Shelf;
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// struct Put;
/// impl DomainEvent for Put {
///     fn event_type(&self) -> String { "put".into() }
///     fn event_version(&self) -> String { "1".into() }
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
