use crate::support::ledger::*;
use cqrsx::cqrs::testing::TestFramework;

#[test]
fn typed_errors_and_empty_success_can_be_inspected() {
    TestFramework::<Ledger>::with(Services::default())
        .given(vec![Delta(4)])
        .and(vec![Delta(3)])
        .when(Command::Adjust(vec![-7]))
        .then_expect_events(vec![Delta(-7)]);
    TestFramework::<Ledger>::with(Services::default())
        .given_no_previous_events()
        .when(Command::Adjust(vec![-1]))
        .then_expect_error(Rejected);
    assert_eq!(
        TestFramework::<Ledger>::with(Services::default())
            .given_no_previous_events()
            .when(Command::Adjust(vec![]))
            .inspect_result()
            .unwrap(),
        vec![]
    );
}

#[tokio::test]
async fn async_dsl_observes_dependent_events_and_discards_rejected_events() {
    TestFramework::<Ledger>::with(Services::default())
        .given(vec![Delta(4)])
        .and(vec![Delta(3)])
        .when_async(Command::Adjust(vec![-7]))
        .await
        .then_expect_events(vec![Delta(-7)]);
    TestFramework::<Ledger>::with(Services::default())
        .given_no_previous_events()
        .when_async(Command::Adjust(vec![3, -4]))
        .await
        .then_expect_error(Rejected);
    TestFramework::<Ledger>::with(Services::default())
        .given_no_previous_events()
        .when_async(Command::RejectAfterWrite)
        .await
        .then_expect_error(Rejected);
}

mod given {
    use cqrsx::cqrs::testing::TestFramework;

    use crate::support::customer::*;

    type CustomerTestFramework = TestFramework<Customer>;

    #[test]
    fn test_add_name() {
        CustomerTestFramework::with(CustomerService)
            .given_no_previous_events()
            .when(CustomerCommand::AddCustomerName {
                name: "John Doe".to_string(),
            })
            .then_expect_events(vec![CustomerEvent::NameAdded {
                name: "John Doe".to_string(),
            }]);
    }

    #[test]
    fn test_add_name_again() {
        CustomerTestFramework::with(CustomerService)
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
        CustomerTestFramework::with(CustomerService)
            .given_no_previous_events()
            .when(CustomerCommand::UpdateEmail {
                new_email: "john.doe@example.com".to_string(),
            })
            .then_expect_events(vec![CustomerEvent::EmailUpdated {
                new_email: "john.doe@example.com".to_string(),
            }]);
    }

    #[test]
    fn test_add_email_name_populted() {
        CustomerTestFramework::with(CustomerService)
            .given(vec![CustomerEvent::NameAdded {
                name: "John Doe".to_string(),
            }])
            .when(CustomerCommand::UpdateEmail {
                new_email: "john.doe@example.com".to_string(),
            })
            .then_expect_events(vec![
                CustomerEvent::EmailUpdated {
                    new_email: "john.doe@example.com".to_string(),
                },
                CustomerEvent::CustomerDataPopulated,
            ]);
    }
}
mod executor {
    use {crate::support::scenario::*, cqrsx::cqrs::testing::TestFramework};

    #[tokio::test]
    async fn async_executor_applies_given_and_additional_events_before_the_command() {
        TestFramework::<TestAggregate>::with(TestService)
            .given_no_previous_events()
            .when_async(TestCommand::CreateTest(CreateTest { id: "created".into() }))
            .await
            .then_expect_events(vec![TestEvent::Created(Created { id: "created".into() })]);

        TestFramework::<TestAggregate>::with(TestService)
            .given(vec![TestEvent::Created(Created { id: "created".into() })])
            .and(vec![TestEvent::Tested(Tested {
                test_name: "previous".into(),
            })])
            .when_async(TestCommand::DoSomethingElse(DoSomethingElse {
                description: "changed".into(),
            }))
            .await
            .then_expect_events(vec![TestEvent::SomethingElse(SomethingElse {
                description: "changed".into(),
            })]);

        TestFramework::<TestAggregate>::with(TestService)
            .given_no_previous_events()
            .and(vec![TestEvent::Tested(Tested {
                test_name: "previous".into(),
            })])
            .when_async(TestCommand::ConfirmTest(ConfirmTest {
                test_name: "previous".into(),
            }))
            .await
            .then_expect_error_message("test already performed");
    }
}
mod validators {
    use crate::support::scenario::*;
    use cqrsx::cqrs::testing::TestFramework;
    type ThisTestFramework = TestFramework<TestAggregate>;

    #[test]
    fn test_framework_test() {
        let test_name = "test A";

        ThisTestFramework::with(TestService)
            .given(vec![TestEvent::Created(Created {
                id: "test_id_A".to_string(),
            })])
            .when(TestCommand::ConfirmTest(ConfirmTest {
                test_name: test_name.to_string(),
            }))
            .then_expect_events(vec![TestEvent::Tested(Tested {
                test_name: test_name.to_string(),
            })]);

        ThisTestFramework::with(TestService)
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

        ThisTestFramework::with(TestService)
            .given(vec![TestEvent::Tested(Tested {
                test_name: test_name.to_string(),
            })])
            .when(TestCommand::ConfirmTest(ConfirmTest {
                test_name: test_name.to_string(),
            }))
            .then_expect_events(vec![TestEvent::Tested(Tested {
                test_name: test_name.to_string(),
            })]);
    }

    #[test]
    #[should_panic(expected = "expected error, received events")]
    fn test_framework_failure_test_b() {
        let test_name = "test A";

        ThisTestFramework::with(TestService)
            .given(vec![TestEvent::Created(Created {
                id: "test_id_A".to_string(),
            })])
            .when(TestCommand::ConfirmTest(ConfirmTest {
                test_name: test_name.to_string(),
            }))
            .then_expect_error_message("some error message");
    }
}
