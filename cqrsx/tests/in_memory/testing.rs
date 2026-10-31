use crate::support::ledger::*;
use cqrsx::testing::TestFramework;

#[test]
fn a_framework_prints_its_history_and_a_validator_its_result() {
    let framework = TestFramework::<Ledger>::new(Services::default()).given(vec![Delta(4)]);
    assert_eq!(
        format!("{framework:?}"),
        r#"TestFramework { aggregate_type: "ledger", events: [Delta(4)], .. }"#
    );
    let validator = framework.when(Command::Adjust(-1));
    assert_eq!(format!("{validator:?}"), "AggregateResultValidator { result: Ok(Delta(-1)) }");
    validator.then_expect_event(&Delta(-1));
}

#[test]
fn typed_errors_and_success_can_be_inspected() {
    TestFramework::<Ledger>::new(Services::default())
        .given(vec![Delta(4)])
        .and(vec![Delta(3)])
        .when(Command::Adjust(-7))
        .then_expect_event(&Delta(-7));
    TestFramework::<Ledger>::new(Services::default())
        .given_no_previous_events()
        .when(Command::Adjust(-1))
        .then_expect_error(&Rejected);
    assert_eq!(
        TestFramework::<Ledger>::new(Services::default())
            .given_no_previous_events()
            .when(Command::Adjust(2))
            .into_result()
            .unwrap(),
        Delta(2)
    );
}

mod given {
    use cqrsx::testing::TestFramework;

    use crate::support::customer::*;

    type CustomerTestFramework = TestFramework<Customer>;

    #[test]
    fn test_add_name() {
        CustomerTestFramework::new(CustomerService)
            .given_no_previous_events()
            .when(CustomerCommand::AddCustomerName {
                name: "John Doe".to_string(),
            })
            .then_expect_event(&CustomerEvent::NameAdded {
                name: "John Doe".to_string(),
            });
    }

    #[test]
    fn test_add_name_again() {
        CustomerTestFramework::new(CustomerService)
            .given(vec![CustomerEvent::NameAdded {
                name: "John Doe".to_string(),
            }])
            .when(CustomerCommand::AddCustomerName {
                name: "John Doe".to_string(),
            })
            .then_expect_error_message("a name has already been added for this customer");
    }

    #[test]
    fn test_add_email() {
        CustomerTestFramework::new(CustomerService)
            .given_no_previous_events()
            .when(CustomerCommand::UpdateEmail {
                new_email: "john.doe@example.com".to_string(),
            })
            .then_expect_event(&CustomerEvent::EmailUpdated {
                new_email: "john.doe@example.com".to_string(),
            });
    }

    /// A command decides one event: the email that completes the customer's data is the email, and nothing else.
    #[test]
    fn test_add_email_after_the_name_is_one_event() {
        CustomerTestFramework::new(CustomerService)
            .given(vec![CustomerEvent::NameAdded {
                name: "John Doe".to_string(),
            }])
            .when(CustomerCommand::UpdateEmail {
                new_email: "john.doe@example.com".to_string(),
            })
            .then_expect_event(&CustomerEvent::EmailUpdated {
                new_email: "john.doe@example.com".to_string(),
            });
    }
}
mod framework {
    use {crate::support::scenario::*, cqrsx::testing::TestFramework};

    #[test]
    fn the_framework_applies_given_and_additional_events_before_the_command() {
        TestFramework::<TestAggregate>::new(TestService)
            .given_no_previous_events()
            .when(TestCommand::CreateTest(CreateTest { id: "created".into() }))
            .then_expect_event(&TestEvent::Created(Created { id: "created".into() }));

        TestFramework::<TestAggregate>::new(TestService)
            .given(vec![TestEvent::Created(Created { id: "created".into() })])
            .and(vec![TestEvent::Tested(Tested {
                test_name: "previous".into(),
            })])
            .when(TestCommand::DoSomethingElse(DoSomethingElse {
                description: "changed".into(),
            }))
            .then_expect_event(&TestEvent::SomethingElse(SomethingElse {
                description: "changed".into(),
            }));

        TestFramework::<TestAggregate>::new(TestService)
            .given_no_previous_events()
            .and(vec![TestEvent::Tested(Tested {
                test_name: "previous".into(),
            })])
            .when(TestCommand::ConfirmTest(ConfirmTest {
                test_name: "previous".into(),
            }))
            .then_expect_error_message("test already performed");
    }
}
mod validators {
    use crate::support::scenario::*;
    use cqrsx::testing::TestFramework;
    type ThisTestFramework = TestFramework<TestAggregate>;

    #[test]
    fn test_framework_test() {
        let test_name = "test A";

        ThisTestFramework::new(TestService)
            .given(vec![TestEvent::Created(Created {
                id: "test_id_A".to_string(),
            })])
            .when(TestCommand::ConfirmTest(ConfirmTest {
                test_name: test_name.to_string(),
            }))
            .then_expect_event(&TestEvent::Tested(Tested {
                test_name: test_name.to_string(),
            }));

        ThisTestFramework::new(TestService)
            .given(vec![TestEvent::Tested(Tested {
                test_name: test_name.to_string(),
            })])
            .when(TestCommand::ConfirmTest(ConfirmTest {
                test_name: test_name.to_string(),
            }))
            .then_expect_error_message("test already performed");
    }

    #[test]
    #[should_panic(expected = "expected success, received aggregate error")]
    fn test_framework_failure_test() {
        let test_name = "test A";

        ThisTestFramework::new(TestService)
            .given(vec![TestEvent::Tested(Tested {
                test_name: test_name.to_string(),
            })])
            .when(TestCommand::ConfirmTest(ConfirmTest {
                test_name: test_name.to_string(),
            }))
            .then_expect_event(&TestEvent::Tested(Tested {
                test_name: test_name.to_string(),
            }));
    }

    #[test]
    #[should_panic(expected = "expected error, received event")]
    fn test_framework_failure_test_b() {
        let test_name = "test A";

        ThisTestFramework::new(TestService)
            .given(vec![TestEvent::Created(Created {
                id: "test_id_A".to_string(),
            })])
            .when(TestCommand::ConfirmTest(ConfirmTest {
                test_name: test_name.to_string(),
            }))
            .then_expect_error_message("some error message");
    }
}
