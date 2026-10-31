// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx-testing/licenses/.
//! The checks of [`TestFramework`](crate::framework::TestFramework): a failed check panics (`# Panics` of each method).
use cqrsx::domain::aggregate::Aggregate;
use std::fmt;

/// Validation object for the `TestFramework` package.
#[must_use = "the result is checked only by a `then_expect_*` method"]
pub struct AggregateResultValidator<A>
where
    A: Aggregate,
{
    result: Result<A::Event, A::Error>,
}

// By hand: a derive would require `A` itself to be `Debug`, not only its event and its error.
impl<A: Aggregate> fmt::Debug for AggregateResultValidator<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateResultValidator")
            .field("result", &self.result)
            .finish()
    }
}

impl<A: Aggregate> AggregateResultValidator<A> {
    /// Verifies that the command decided the expected event.
    ///
    /// # Panics
    ///
    /// When the command was rejected or decided another event: the check failed.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewContext;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
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
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
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
    /// # use example::MyCommands;
    /// # use example::MyEvents;
    /// # use example::MyService;
    /// # async fn test() {
    /// use cqrsx_testing::framework::TestFramework;
    ///
    /// let validator = TestFramework::<MyAggregate>::new(MyService)
    ///     .given_no_previous_events()
    ///     .when(MyCommands::DoSomething);
    ///
    /// validator.then_expect_event(&MyEvents::SomethingWasDone);
    /// # }
    /// ```
    pub fn then_expect_event(self, expected_event: &A::Event) {
        let event = self.result.unwrap_or_else(|err| {
            panic!("expected success, received aggregate error: '{err}'");
        });
        assert_eq!(&event, expected_event, "the command decided another event");
    }

    /// Verifies that the command was rejected with an error whose message is `error_message`.
    ///
    /// # Panics
    ///
    /// When the command decided an event or was rejected with another message: the check failed.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewContext;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
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
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
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
    /// # use example::MyCommands;
    /// # use example::MyEvents;
    /// # use example::MyService;
    /// use cqrsx_testing::framework::TestFramework;
    ///
    /// let validator = TestFramework::<MyAggregate>::new(MyService)
    ///     .given_no_previous_events()
    ///     .when(MyCommands::BadCommand);
    ///
    /// validator.then_expect_error_message("the expected error message");
    /// ```
    pub fn then_expect_error_message(self, error_message: &str) {
        match self.result {
            Ok(event) => {
                panic!("expected error, received event: '{event:?}'");
            }
            Err(err) => assert_eq!(
                err.to_string(),
                error_message,
                "the command was rejected with another message"
            ),
        }
    }

    /// Returns the internal error payload for validation by the user.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewContext;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
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
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
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
    /// # use example::MyCommands;
    /// # use example::MyEvents;
    /// # use example::MyService;
    /// # use example::MyUserError;
    /// use cqrsx_testing::framework::TestFramework;
    ///
    /// let validator = TestFramework::<MyAggregate>::new(MyService)
    ///     .given_no_previous_events()
    ///     .when(MyCommands::BadCommand);
    ///
    /// let expected = MyUserError("the expected error message".to_string());
    /// assert_eq!(expected,validator.into_result().unwrap_err());
    /// ```
    pub fn into_result(self) -> Result<A::Event, A::Error> {
        self.result
    }

    pub(crate) fn new(result: Result<A::Event, A::Error>) -> Self {
        Self { result }
    }
}
impl<A> AggregateResultValidator<A>
where
    A: Aggregate,
    A::Error: PartialEq,
{
    /// Verifies that the result is the expected error.
    ///
    /// # Panics
    ///
    /// When the command decided an event or was rejected with another error: the check failed.
    ///
    /// > Note that the configured Error *must* implement `std::cmp::PartialEq`.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::Events;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::projection::EventHandler;
    /// # use cqrsx::view::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
    /// # use cqrsx::view::repository::ViewContext;
    /// # use cqrsx::view::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl Events for MyEvents {
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
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
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
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
    /// # use example::MyCommands;
    /// # use example::MyEvents;
    /// # use example::MyService;
    /// # use example::MyUserError;
    /// use cqrsx_testing::framework::TestFramework;
    ///
    /// let validator = TestFramework::<MyAggregate>::new(MyService)
    ///     .given_no_previous_events()
    ///     .when(MyCommands::BadCommand);
    ///
    /// let expected = MyUserError("the expected error message".to_string());
    /// validator.then_expect_error(&expected);
    /// ```
    pub fn then_expect_error(self, expected_error: &A::Error) {
        match self.result {
            Ok(event) => {
                panic!("expected error, received event: '{event:?}'");
            }
            Err(err) => {
                assert_eq!(&err, expected_error, "the command was rejected with another error");
            }
        }
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use crate::test_support::*;
    use cqrsx::domain::command::Decide;

    #[test]
    #[should_panic(expected = "expected error, received event")]
    fn typed_error_validator_rejects_success() {
        AggregateResultValidator::<Ledger>::new(Ok(Delta(0))).then_expect_error(&Rejected);
    }
    /// An aggregate whose errors carry a message, so two errors can differ.
    #[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
    struct Messaged;

    #[derive(Debug, PartialEq, thiserror::Error)]
    #[error("{0}")]
    struct Message(&'static str);

    impl Aggregate for Messaged {
        const TYPE: &'static str = "messaged";
        type Id = String;
        type Command = ();
        type Event = Delta;
        type Error = Message;
        type Rules = ();
        fn apply(&mut self, _: Delta) {}
    }

    impl Decide<Messaged> for () {
        fn decide(self, _: &Messaged, _: &()) -> Result<Delta, Message> {
            Ok(Delta(0))
        }
    }

    #[test]
    #[should_panic(expected = "assertion `left == right` failed")]
    fn typed_error_validator_checks_expected_payload() {
        AggregateResultValidator::<Messaged>::new(Err(Message("actual error"))).then_expect_error(&Message("expected error"));
    }
    #[test]
    #[should_panic(expected = "assertion `left == right` failed")]
    fn event_validator_checks_payload_not_just_count() {
        AggregateResultValidator::<Ledger>::new(Ok(Delta(2))).then_expect_event(&Delta(1));
    }
    #[test]
    #[should_panic(expected = "assertion `left == right` failed")]
    fn error_message_validator_checks_expected_text() {
        AggregateResultValidator::<Ledger>::new(Err(Rejected)).then_expect_error_message("wrong");
    }
}

#[cfg(test)]
mod inspection_tests {
    use super::*;
    use crate::test_support::{Ledger, Rejected};
    #[test]
    fn a_validator_prints_its_result() {
        let validator = AggregateResultValidator::<Ledger>::new(Err(Rejected));
        assert_eq!(format!("{validator:?}"), "AggregateResultValidator { result: Err(Rejected) }");
        validator.then_expect_error(&Rejected);
    }

    #[test]
    fn constructor_retains_the_typed_error() {
        assert_eq!(
            AggregateResultValidator::<Ledger>::new(Err(Rejected))
                .into_result()
                .unwrap_err(),
            Rejected
        );
    }
}
