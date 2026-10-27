//! The derive macros as a downstream crate uses them: what their expansions do once compiled, on the edges the
//! examples leave out (generic types, raw field names, enums without variants, an overridden code).
use cqrsx::{
    Event, Events, Rejection, Task,
    cqrs::{
        event::{DomainEvent, EventKind},
        rejection::Rejection as _,
    },
    task::{
        NewTask,
        format::{TaskFormat, TaskPayload},
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "SeatBooked", version = "1.0")]
struct SeatBooked {
    seat: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "Noted", version = "3")]
struct Noted<T>
where
    T: Clone,
{
    note: T,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
#[serde(tag = "type", bound(deserialize = "T: DeserializeOwned"))]
enum ConcertEvent<T: Clone + Debug + PartialEq + Send + Sync + Serialize + DeserializeOwned> {
    SeatBooked(SeatBooked),
    Noted(Noted<T>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
enum NoEvent {}

#[test]
fn derived_events_store_the_name_and_the_version_of_each_payload() {
    assert_eq!((SeatBooked::NAME, SeatBooked::VERSION), ("SeatBooked", "1.0"));
    assert_eq!((Noted::<u8>::NAME, Noted::<u8>::VERSION), ("Noted", "3"));
    let booked = ConcertEvent::<String>::SeatBooked(SeatBooked { seat: "B4".into() });
    let noted = ConcertEvent::Noted(Noted { note: "late".to_owned() });
    assert_eq!(
        (booked.event_type(), booked.event_version()),
        ("SeatBooked".to_owned(), "1.0".to_owned())
    );
    assert_eq!(
        (noted.event_type(), noted.event_version()),
        ("Noted".to_owned(), "3".to_owned())
    );
    // The derive leaves serde's attributes alone: the stored payload is still tagged by the variant.
    assert_eq!(
        serde_json::to_value(&noted).unwrap(),
        serde_json::json!({ "type": "Noted", "note": "late" })
    );
    // An enum without events dispatches nothing; it only has to compile.
    let _: fn(&NoEvent) -> (String, String) = |never| (never.event_type(), never.event_version());
}

#[derive(Serialize, Task)]
#[task(name = "send_e_ticket", version = 2)]
struct SendETicket<'a> {
    seat: &'a str,
}

#[test]
fn a_derived_task_has_its_format_and_publishes() {
    assert_eq!((SendETicket::TASK_TYPE, SendETicket::TASK_VERSION), ("send_e_ticket", 2));
    assert_eq!(TaskFormat::of::<SendETicket>().unwrap().to_string(), "send_e_ticket v2");
    let task = NewTask::new(SendETicket { seat: "B4" }).unwrap();
    assert_eq!(
        (task.format().to_string(), task.payload()),
        ("send_e_ticket v2".to_owned(), &json!({ "seat": "B4" }))
    );
}

#[derive(Debug, Serialize)]
struct Seat {
    row: char,
    number: u8,
}

#[derive(Debug, thiserror::Error, Rejection)]
enum BookingError<T: Serialize + Debug> {
    #[error("sold out")]
    SoldOut,
    #[error("seat taken")]
    SeatTaken { seat: Seat, by: String, booking_id: Uuid },
    #[error("wrong")]
    #[rejection(code = "r3_unknown")]
    Unknown { r#type: T },
    #[error("nothing")]
    Nothing {},
}

#[derive(Debug, Rejection)]
enum NoRejection {}

#[test]
fn derived_rejections_report_their_codes_and_fields() {
    let reported = [
        BookingError::SoldOut,
        BookingError::SeatTaken {
            seat: Seat { row: 'B', number: 4 },
            by: "Fox".into(),
            booking_id: Uuid::from_u128(1),
        },
        BookingError::Unknown { r#type: 9_u32 },
        BookingError::Nothing {},
    ]
    .iter()
    .map(|error| (error.code(), error.params()))
    .collect::<Vec<_>>();
    assert_eq!(
        reported,
        [
            ("sold_out", json!({})),
            (
                "seat_taken",
                json!({ "seat": { "row": "B", "number": 4 }, "by": "Fox", "booking_id": "00000000-0000-0000-0000-000000000001" })
            ),
            ("r3_unknown", json!({ "type": 9 })),
            ("nothing", json!({})),
        ]
    );
    // An enum without rejections reports nothing; it only has to compile.
    let _: fn(&NoRejection) -> (&'static str, serde_json::Value) = |never| (never.code(), never.params());
}
