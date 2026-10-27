//! Building an outbox task. The reactions of each aggregate plan their tasks from the aggregate's
//! decisions, and the core commits them with the decision's events: a
//! rejected command writes nothing, so a task is never published without its event. A task carries the
//! application keys `concert_id` and `fan_id`; the core adds the causation ids of the command
//! (`command_id`, `correlation_id`, `causation_id`) and derives the task id from the command id. The task
//! contracts live with their handlers.
use crate::shared::domain::{ConcertId, FanId};
use cqrsx::task::{NewTask, TaskValidationError, format::TaskPayload};
use serde_json::json;

/// A task of `payload` with the application keys `concert_id` and `fan_id` and an attempt budget.
pub fn new_task<T: TaskPayload>(
    payload: T,
    concert_id: &ConcertId,
    fan_id: &FanId,
    max_attempts: i32,
) -> Result<NewTask, TaskValidationError> {
    NewTask::new(payload)?
        .with_metadata(json!({ "concert_id": concert_id, "fan_id": fan_id }))?
        .with_max_attempts(max_attempts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fan::application::{
            issue_refund::IssueRefundV1, record_cancellation::RecordCancellationV1, record_ticket::RecordTicketV1,
            send_e_ticket::SendETicketV1,
        },
        shared::domain::venue::SeatId,
    };
    use cqrsx::task::format::TaskFormat;
    use serde::{Serialize, de::DeserializeOwned};
    use uuid::Uuid;

    #[test]
    fn formats_are_snake_case_v1_and_tasks_carry_the_application_keys() {
        for (format, expected) in [
            (TaskFormat::of::<RecordTicketV1>(), "encore_record_ticket v1"),
            (TaskFormat::of::<SendETicketV1>(), "encore_send_e_ticket v1"),
            (TaskFormat::of::<RecordCancellationV1>(), "encore_record_cancellation v1"),
            (TaskFormat::of::<IssueRefundV1>(), "encore_issue_refund v1"),
        ] {
            assert_eq!(format.unwrap().to_string(), expected);
        }
        let concert_id = ConcertId::parse("radiohead-2026-11-21").unwrap();
        let fan_id = FanId::parse("fox").unwrap();
        let task = new_task(
            SendETicketV1 {
                booking_id: Uuid::now_v7(),
                concert_id: concert_id.clone(),
                seat: SeatId::parse("B4").unwrap(),
                fan_id: fan_id.clone(),
                fan_name: "Fox".into(),
                price: 80,
            },
            &concert_id,
            &fan_id,
            6,
        )
        .unwrap();
        assert_eq!(task.max_attempts(), 6);
        assert_eq!(
            task.metadata(),
            &json!({ "concert_id": "radiohead-2026-11-21", "fan_id": "fox" }),
            "the application keys; the core adds the causation ids"
        );
        assert_eq!(task.payload()["seat"], "B4");
        assert!(
            new_task(task_payload(), &concert_id, &fan_id, 0).is_err(),
            "an attempt budget is positive"
        );
    }

    fn task_payload() -> SendETicketV1 {
        SendETicketV1 {
            booking_id: Uuid::nil(),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            fan_id: FanId::parse("fox").unwrap(),
            fan_name: "Fox".into(),
            price: 80,
        }
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
}
