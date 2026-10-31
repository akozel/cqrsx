//! `CancelBooking`: the owner gives a ticket back. Rules R1, R3, R7, R8; the accepted fact is
//! [`BookingCancelled`].
use crate::{
    concert::domain::{Concert, ConcertError, ConcertEvent, ConcertRules, events::BookingCancelled},
    shared::domain::{FanId, venue::VENUE},
};
use cqrsx::domain::command::{Command, Decide};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Command)]
pub struct CancelBooking {
    /// Raw seat text from the UI; the aggregate validates it (rule R3).
    pub seat: String,
    /// The activity feed shows and filters a cancellation by its fan.
    #[label]
    pub fan_id: FanId,
    /// The cancellation is identified by its command: the cancellation id is the command id.
    #[command_id]
    pub cancellation_id: Uuid,
}

impl Decide<Concert> for CancelBooking {
    fn decide(self, concert: &Concert, _rules: &ConcertRules) -> Result<ConcertEvent, ConcertError> {
        let CancelBooking {
            seat,
            fan_id,
            cancellation_id,
        } = self;
        if concert.info().is_none() {
            return Err(ConcertError::ConcertNotFound); // R1
        }
        let seat = VENUE.seat(&seat).ok_or(ConcertError::UnknownSeat { seat })?; // R3
        let booking = concert
            .bookings()
            .get(&seat.id)
            .ok_or_else(|| ConcertError::SeatNotBooked { seat: seat.id.clone() })?; // R7
        if booking.fan_id != fan_id {
            return Err(ConcertError::NotYourTicket); // R8
        }
        // The refund is the price that was actually paid (a fact from history), not a formula
        // over today's price list.
        Ok(ConcertEvent::BookingCancelled(BookingCancelled {
            seat: seat.id,
            fan_id,
            booking_id: booking.booking_id,
            cancellation_id,
            refund: booking.price,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        concert::domain::{
            Concert, ConcertError, ConcertEvent,
            events::BookingCancelled,
            test_support::{announced, booked, cancel, fan, rules},
        },
        shared::domain::venue::SeatId,
    };
    use cqrsx_testing::framework::TestFramework;
    use uuid::Uuid;

    #[test]
    fn cancel_booking_happy_path_refunds_the_price_paid_in_history() {
        let booking_id = Uuid::new_v4();
        let cancellation_id = Uuid::new_v4();
        // A VIP seat booked at 120: the refund comes from the SeatBooked fact.
        TestFramework::<Concert>::new(rules())
            .given(vec![announced(), booked("A1", "fox", "Fox", booking_id)])
            .when(cancel("A1", "fox", cancellation_id))
            .then_expect_event(&ConcertEvent::BookingCancelled(BookingCancelled {
                seat: SeatId::parse("A1").unwrap(),
                fan_id: fan("fox"),
                booking_id,
                cancellation_id,
                refund: 120,
            }));
    }

    #[test]
    fn r1_rejects_cancelling_in_an_unannounced_concert() {
        TestFramework::<Concert>::new(rules())
            .given_no_previous_events()
            .when(cancel("B4", "fox", Uuid::new_v4()))
            .then_expect_error(&ConcertError::ConcertNotFound);
    }

    #[test]
    fn r3_rejects_an_unknown_seat() {
        TestFramework::<Concert>::new(rules())
            .given(vec![announced()])
            .when(cancel("Z9", "fox", Uuid::new_v4()))
            .then_expect_error(&ConcertError::UnknownSeat { seat: "Z9".into() });
    }

    #[test]
    fn r7_rejects_cancelling_a_free_seat() {
        TestFramework::<Concert>::new(rules())
            .given(vec![announced()])
            .when(cancel("B4", "fox", Uuid::new_v4()))
            .then_expect_error(&ConcertError::SeatNotBooked {
                seat: SeatId::parse("B4").unwrap(),
            });
    }

    #[test]
    fn r8_rejects_cancelling_somebody_elses_ticket() {
        TestFramework::<Concert>::new(rules())
            .given(vec![announced(), booked("B4", "fox", "Fox", Uuid::new_v4())])
            .when(cancel("B4", "panda", Uuid::new_v4()))
            .then_expect_error(&ConcertError::NotYourTicket);
    }
}
