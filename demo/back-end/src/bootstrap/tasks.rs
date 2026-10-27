//! The outbox tasks: the policy of every task type and its handler, one line per task type. A fresh dispatcher
//! (fresh handlers) is built for every worker and every worker restart; the mailers share one HTTP client to the
//! external mail service, since a connection pool is meant to be shared.
use super::WiringError;
use crate::{
    config::Config,
    fan::{
        application::{
            chain::CHAIN_RETRY_AFTER,
            issue_refund::{IssueRefundHandler, IssueRefundV1},
            record_cancellation::{RecordCancellationHandler, RecordCancellationV1},
            record_ticket::{RecordTicketHandler, RecordTicketV1},
            send_e_ticket::{SendETicketHandler, SendETicketV1},
        },
        domain::Fan,
    },
    process::Process,
    shared::{
        application::{
            activity::ActivityHub,
            policies::{MAIL, STEP},
        },
        infrastructure::http_mailer::{HttpMailer, MailClientSettings},
    },
};
use cqrsx::{
    postgres::command::PostgresCommandExecutor,
    task::{
        adapter::boxed::BoxedTaskAdapter,
        dispatcher::TaskDispatcher,
        policy::{Policies, PolicyError, Retry, TaskPolicy},
        runtime::dispatcher_factory::TaskDispatcherFactory,
    },
};
use std::sync::Arc;

/// The policies of the four task types, from the configuration: a step of the chain waits [`CHAIN_RETRY_AFTER`]
/// between attempts, an e-mail backs off from `ENCORE_TASK_RETRY_BASE_MS` to `ENCORE_TASK_RETRY_MAX_MS`; both get
/// `ENCORE_TASK_MAX_ATTEMPTS` attempts. The reactions of both aggregates plan with them; the pause is still the one
/// the handlers ask for (backlog 05/09 hands it to the policies).
pub fn policies(config: &Config) -> Result<Policies, PolicyError> {
    let attempts = config.task_max_attempts;
    let mail = Retry::backoff(config.task_retry_base, config.task_retry_max);
    Policies::default()
        .with_policy(STEP, TaskPolicy::new(Retry::fixed(CHAIN_RETRY_AFTER), attempts)?)?
        .with_policy(MAIL, TaskPolicy::new(mail, attempts)?)?
        .with_task::<RecordTicketV1>(STEP)?
        .with_task::<RecordCancellationV1>(STEP)?
        .with_task::<SendETicketV1>(MAIL)?
        .with_task::<IssueRefundV1>(MAIL)
}

/// The client of the external mail service: its address, a request timeout that ends before the handler's, and the
/// backoff an e-mail task asks for after a refused delivery.
pub fn mail_settings(config: &Config) -> MailClientSettings {
    MailClientSettings {
        base_url: config.mail_service_url.clone(),
        timeout: config.mail_request_timeout(),
        retry_base: config.task_retry_base,
        retry_max: config.task_retry_max,
    }
}

/// The chain steps between the two aggregates, on the ledgers' executor, and the e-mails they end in.
pub fn dispatcher(
    process: &Process,
    fans: Arc<PostgresCommandExecutor<Fan>>,
    activity: Arc<ActivityHub>,
) -> Result<TaskDispatcherFactory<BoxedTaskAdapter>, WiringError> {
    let mail = mail_settings(&process.config);
    let http = HttpMailer::client(&mail)?;
    Ok(Arc::new(move || {
        let mailer = || HttpMailer::new(http.clone(), mail.clone(), activity.clone());
        TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::json(RecordTicketHandler { fans: fans.clone() })?,
            BoxedTaskAdapter::json(RecordCancellationHandler { fans: fans.clone() })?,
            BoxedTaskAdapter::json(SendETicketHandler { mailer: mailer() })?,
            BoxedTaskAdapter::json(IssueRefundHandler { mailer: mailer() })?,
        ])
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        concert::domain::{
            Concert, ConcertCommand, ConcertRules,
            commands::{AnnounceConcert, BookSeat, CancelBooking},
        },
        fan::domain::{
            Fan, FanCommand,
            commands::{RecordCancellation, RecordTicket},
        },
        shared::domain::{
            ConcertId, FanId, FanLedgerId,
            venue::{SeatId, Zone},
        },
    };
    use chrono::NaiveDate;
    use cqrsx::{
        cqrs::{
            command::{CommandExecutor, causation::Causation},
            reactions::ReactionsPlanner,
            testing::{command::TestCommandExecutor, mem_store::MemStore},
        },
        task::{TaskSchedule, format::TaskFormat},
    };
    use serde::{Serialize, de::DeserializeOwned};
    use serde_json::json;
    use std::time::Duration;
    use uuid::Uuid;

    /// The formats of the four task types, in the order of the policies.
    fn formats() -> [TaskFormat; 4] {
        [
            TaskFormat::of::<RecordTicketV1>().unwrap(),
            TaskFormat::of::<RecordCancellationV1>().unwrap(),
            TaskFormat::of::<SendETicketV1>().unwrap(),
            TaskFormat::of::<IssueRefundV1>().unwrap(),
        ]
    }

    #[test]
    fn formats_are_snake_case_v1_and_tasks_carry_the_budget_of_their_policy() {
        for (format, expected) in [
            (TaskFormat::of::<RecordTicketV1>(), "encore_record_ticket v1"),
            (TaskFormat::of::<SendETicketV1>(), "encore_send_e_ticket v1"),
            (TaskFormat::of::<RecordCancellationV1>(), "encore_record_cancellation v1"),
            (TaskFormat::of::<IssueRefundV1>(), "encore_issue_refund v1"),
        ] {
            assert_eq!(format.unwrap().to_string(), expected);
        }
        let policies = policies(&crate::testing::config()).unwrap();
        for format in formats() {
            assert_eq!(policies.max_attempts(&format), 6, "{format}");
        }
        assert!(
            TaskPolicy::new(Retry::fixed(CHAIN_RETRY_AFTER), 0).is_err(),
            "an attempt budget is positive"
        );
    }

    fn round_trip<T: Serialize + DeserializeOwned>(document: serde_json::Value) {
        let payload: T = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(serde_json::to_value(&payload).unwrap(), document);
    }

    /// Wire contract of the task payloads (`transactional_outbox.payload`): tasks already queued
    /// must still decode after a deploy, so the literals must never change.
    #[test]
    fn task_payloads_keep_their_queued_json() {
        let booking_id = "00000000-0000-0000-0000-000000000001";
        let cancellation_id = "00000000-0000-0000-0000-000000000002";
        let concert_id = "radiohead-2026-11-21";
        round_trip::<RecordTicketV1>(json!({ "booking_id": booking_id, "concert_id": concert_id, "seat": "B4",
            "zone": "standard", "price": 80, "fan_id": "fox", "fan_name": "Fox" }));
        round_trip::<SendETicketV1>(json!({ "booking_id": booking_id, "concert_id": concert_id, "seat": "B4",
            "fan_id": "fox", "fan_name": "Fox", "price": 80 }));
        round_trip::<RecordCancellationV1>(json!({ "booking_id": booking_id, "cancellation_id": cancellation_id,
            "concert_id": concert_id, "seat": "B4", "fan_id": "fox", "refund": 80 }));
        round_trip::<IssueRefundV1>(json!({ "cancellation_id": cancellation_id, "booking_id": booking_id,
            "concert_id": concert_id, "seat": "B4", "fan_id": "fox", "amount": 80 }));
    }

    /// The steps of the chain are `STEP`, the e-mails `MAIL`; their pauses and their budget come from the
    /// configuration.
    #[test]
    fn every_task_type_has_its_policy_from_the_configuration() {
        let defaults = policies(&crate::testing::config()).unwrap();
        assert_eq!(
            formats().map(|format| defaults.name_of(&format)),
            [Some(STEP), Some(STEP), Some(MAIL), Some(MAIL)]
        );
        assert_eq!(
            (defaults.policy(STEP), defaults.policy(MAIL)),
            (
                Some(&TaskPolicy::new(Retry::fixed(Duration::from_secs(1)), 6).unwrap()),
                Some(&TaskPolicy::new(Retry::backoff(Duration::from_secs(1), Duration::from_secs(8)), 6).unwrap())
            )
        );
        let mut config = crate::testing::config();
        config.task_max_attempts = 3;
        config.task_retry_base = Duration::from_millis(300);
        config.task_retry_max = Duration::from_secs(5);
        let changed = policies(&config).unwrap();
        assert_eq!(
            (changed.policy(STEP), changed.policy(MAIL)),
            (
                Some(&TaskPolicy::new(Retry::fixed(Duration::from_secs(1)), 3).unwrap()),
                Some(&TaskPolicy::new(Retry::backoff(Duration::from_millis(300), Duration::from_secs(5)), 3).unwrap())
            )
        );
        config.task_max_attempts = 0;
        assert_eq!(policies(&config).unwrap_err(), PolicyError::MaxAttempts(0));
    }

    /// The mail policy pauses exactly as the mail client asks today, and the step policy as a step does: the
    /// formula is the client's, word for word.
    #[test]
    fn the_mail_policy_pauses_as_the_mail_client_asks() {
        let attempts: Vec<i32> = (-3..=40).chain([i32::MIN, i32::MAX]).collect();
        for (base, max) in [
            (Duration::from_secs(1), Duration::from_secs(8)),
            (Duration::from_millis(300), Duration::from_secs(5)),
            (Duration::from_millis(1), Duration::from_secs(3600)),
            (Duration::ZERO, Duration::ZERO),
            (Duration::from_secs(5), Duration::from_secs(2)),
        ] {
            let mut config = crate::testing::config();
            (config.task_retry_base, config.task_retry_max) = (base, max);
            let mail = policies(&config).unwrap().policy(MAIL).unwrap().retry();
            let client = mail_settings(&config);
            for &attempt in &attempts {
                assert_eq!(
                    mail.pause(attempt),
                    client.retry_after(attempt),
                    "{base:?} to {max:?}, attempt {attempt}"
                );
            }
        }
        let step = policies(&crate::testing::config()).unwrap().policy(STEP).unwrap().retry();
        for &attempt in &attempts {
            assert_eq!(step.pause(attempt), CHAIN_RETRY_AFTER, "attempt {attempt}");
        }
    }

    /// The outbox row of every task as an executor publishes it: its format, its body, its metadata (the causation of
    /// the command that planned it), its schedule, its attempt budget and its id, derived from the command. The four
    /// tasks the two aggregates plan, under the configuration without environment.
    #[tokio::test]
    async fn every_task_row_keeps_its_format_body_schedule_budget_and_derived_id() {
        let config = Config::from_lookup(|_| None).unwrap();
        let policies = policies(&config).unwrap();
        let concerts = TestCommandExecutor::new(MemStore::<Concert>::default(), ConcertRules::default())
            .with_task_planner(ReactionsPlanner::<Concert>::new(policies.clone()));
        let fans =
            TestCommandExecutor::new(MemStore::<Fan>::default(), ()).with_task_planner(ReactionsPlanner::<Fan>::new(policies));
        let concert_id = ConcertId::parse("radiohead-2026-11-21").unwrap();
        let fox = FanId::parse("fox").unwrap();
        let ledger = FanLedgerId::new(&fox, &concert_id);
        let (booking_id, cancellation_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let announce = AnnounceConcert {
            artist: "Radiohead".into(),
            city: "Tbilisi".into(),
            date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
        };
        concerts
            .execute(&concert_id, ConcertCommand::AnnounceConcert(announce))
            .await
            .unwrap();

        // The box office books Fox's A1: the ledger records the ticket.
        let booking = Causation::root("ui");
        let book = BookSeat {
            seat: "A1".into(),
            fan_id: fox.clone(),
            fan_name: "Fox".into(),
            booking_id,
        };
        let booked = concerts
            .execute_with_metadata(&concert_id, ConcertCommand::BookSeat(book), booking.event_metadata())
            .await
            .unwrap();
        let [task] = &booked.tasks[..] else {
            panic!("one task per booking: {:?}", booked.tasks);
        };
        let format = TaskFormat::new("encore_record_ticket", 1).unwrap();
        assert_eq!(
            (task.id(), task.format(), task.schedule(), task.max_attempts()),
            (
                booking.task_id("Concert", "radiohead-2026-11-21", &format, 0),
                &format,
                &TaskSchedule::Now,
                6
            )
        );
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "A1", "zone": "vip",
                     "price": 120, "fan_id": "fox", "fan_name": "Fox" })
        );
        assert_eq!(
            task.metadata(),
            &json!({ "command_id": booking.command_id, "correlation_id": booking.correlation_id,
                     "causation_id": booking.causation_id })
        );

        // The box office cancels it: the ledger records the cancellation with the price paid.
        let cancelling = Causation::root("ui");
        let cancel = CancelBooking {
            seat: "A1".into(),
            fan_id: fox.clone(),
            cancellation_id,
        };
        let cancelled = concerts
            .execute_with_metadata(
                &concert_id,
                ConcertCommand::CancelBooking(cancel),
                cancelling.event_metadata(),
            )
            .await
            .unwrap();
        let [task] = &cancelled.tasks[..] else {
            panic!("one task per cancellation: {:?}", cancelled.tasks);
        };
        let format = TaskFormat::new("encore_record_cancellation", 1).unwrap();
        assert_eq!(
            (task.id(), task.format(), task.schedule(), task.max_attempts()),
            (
                cancelling.task_id("Concert", "radiohead-2026-11-21", &format, 0),
                &format,
                &TaskSchedule::Now,
                6
            )
        );
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "cancellation_id": cancellation_id, "concert_id": "radiohead-2026-11-21",
                     "seat": "A1", "fan_id": "fox", "refund": 120 })
        );
        assert_eq!(
            task.metadata(),
            &json!({ "command_id": cancelling.command_id, "correlation_id": cancelling.correlation_id,
                     "causation_id": cancelling.causation_id })
        );

        // The ledger records the ticket: the fan gets the e-ticket.
        let recording = Causation::root("ui");
        let record = RecordTicket {
            booking_id,
            concert_id: concert_id.clone(),
            seat: SeatId::parse("A1").unwrap(),
            zone: Zone::Vip,
            price: 120,
            fan_name: "Fox".into(),
        };
        let recorded = fans
            .execute_with_metadata(&ledger, FanCommand::RecordTicket(record), recording.event_metadata())
            .await
            .unwrap();
        let [task] = &recorded.tasks[..] else {
            panic!("one e-ticket per recorded ticket: {:?}", recorded.tasks);
        };
        let format = TaskFormat::new("encore_send_e_ticket", 1).unwrap();
        assert_eq!(
            (task.id(), task.format(), task.schedule(), task.max_attempts()),
            (
                recording.task_id("Fan", "fox:radiohead-2026-11-21", &format, 0),
                &format,
                &TaskSchedule::Now,
                6
            )
        );
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "A1", "fan_id": "fox",
                     "fan_name": "Fox", "price": 120 })
        );
        assert_eq!(
            task.metadata(),
            &json!({ "command_id": recording.command_id, "correlation_id": recording.correlation_id,
                     "causation_id": recording.causation_id })
        );

        // The ledger records the cancellation: the fan gets the refund.
        let refunding = Causation::root("ui");
        let record = RecordCancellation {
            booking_id,
            cancellation_id,
            refund: 120,
        };
        let refunded = fans
            .execute_with_metadata(&ledger, FanCommand::RecordCancellation(record), refunding.event_metadata())
            .await
            .unwrap();
        let [task] = &refunded.tasks[..] else {
            panic!("one refund per cancelled ticket: {:?}", refunded.tasks);
        };
        let format = TaskFormat::new("encore_issue_refund", 1).unwrap();
        assert_eq!(
            (task.id(), task.format(), task.schedule(), task.max_attempts()),
            (
                refunding.task_id("Fan", "fox:radiohead-2026-11-21", &format, 0),
                &format,
                &TaskSchedule::Now,
                6
            )
        );
        assert_eq!(
            task.payload(),
            &json!({ "cancellation_id": cancellation_id, "booking_id": booking_id, "concert_id": "radiohead-2026-11-21",
                     "seat": "A1", "fan_id": "fox", "amount": 120 })
        );
        assert_eq!(
            task.metadata(),
            &json!({ "command_id": refunding.command_id, "correlation_id": refunding.correlation_id,
                     "causation_id": refunding.causation_id })
        );
    }

    #[test]
    fn the_mail_settings_come_from_the_configuration() {
        let mut config = crate::testing::config();
        config.mail_service_url = "http://mail:8025".into();
        config.task_retry_base = Duration::from_millis(300);
        config.task_retry_max = Duration::from_secs(5);
        let settings = mail_settings(&config);
        assert_eq!(settings.base_url, "http://mail:8025");
        assert_eq!(settings.timeout, config.mail_request_timeout());
        assert_eq!(
            (settings.retry_base, settings.retry_max),
            (Duration::from_millis(300), Duration::from_secs(5))
        );
    }
}
