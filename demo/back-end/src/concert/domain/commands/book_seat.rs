//! `BookSeat`: a fan takes a free seat. Rules R1, R3–R6; the accepted fact is [`SeatBooked`].
use crate::{
    concert::domain::{Concert, ConcertError, ConcertEvent, ConcertRules, events::SeatBooked},
    shared::domain::{FanId, venue::VENUE},
};
use cqrsx::{Command, cqrs::command::decide::Decide};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Command)]
pub struct BookSeat {
    /// Raw seat text from the UI; the aggregate validates it (rule R3).
    pub seat: String,
    /// The activity feed shows and filters a booking by its fan.
    #[label]
    pub fan_id: FanId,
    pub fan_name: String,
    /// The booking is identified by its command: the booking id is the command id.
    #[command_id]
    pub booking_id: Uuid,
}

impl Decide<Concert> for BookSeat {
    /// Rules are checked in the documented order, from "does the concert exist" to the most
    /// specific check, so the first failing rule is the most fundamental one.
    fn decide(self, concert: &Concert, rules: &ConcertRules) -> Result<ConcertEvent, ConcertError> {
        let BookSeat {
            seat,
            fan_id,
            fan_name,
            booking_id,
        } = self;
        if concert.info().is_none() {
            return Err(ConcertError::ConcertNotFound); // R1
        }
        let seat = VENUE.seat(&seat).ok_or(ConcertError::UnknownSeat { seat })?; // R3
        if concert.is_sold_out() {
            return Err(ConcertError::SoldOut); // R4
        }
        if let Some(booking) = concert.bookings().get(&seat.id) {
            return Err(ConcertError::SeatTaken {
                seat: seat.id,
                by: booking.fan_name.clone(),
            }); // R5
        }
        if concert.tickets_of(&fan_id) >= rules.ticket_limit {
            return Err(ConcertError::TicketLimit {
                limit: rules.ticket_limit,
            }); // R6
        }
        // Sold-out is derived state, not a second event (D-02/D-03).
        Ok(ConcertEvent::SeatBooked(SeatBooked {
            seat: seat.id,
            zone: seat.zone,
            price: seat.price,
            fan_id,
            fan_name,
            booking_id,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        concert::domain::{
            Concert, ConcertError, ConcertEvent, ConcertRules,
            events::{BookingCancelled, SeatBooked},
            test_support::{announced, book, booked, fan, full_house, rules},
        },
        shared::domain::venue::{SeatId, Zone},
    };
    use cqrsx::cqrs::testing::TestFramework;
    use uuid::Uuid;

    #[test]
    fn book_seat_happy_path_records_zone_and_price() {
        let id = Uuid::new_v4();
        TestFramework::<Concert>::new(rules())
            .given(vec![announced()])
            .when(book("b4", "fox", "Fox", id))
            .then_expect_event(ConcertEvent::SeatBooked(SeatBooked {
                seat: SeatId::parse("B4").unwrap(),
                zone: Zone::Standard,
                price: 80,
                fan_id: fan("fox"),
                fan_name: "Fox".into(),
                booking_id: id,
            }));
    }

    #[test]
    fn r1_rejects_booking_in_an_unannounced_concert() {
        TestFramework::<Concert>::new(rules())
            .given_no_previous_events()
            .when(book("B4", "fox", "Fox", Uuid::new_v4()))
            .then_expect_error(ConcertError::ConcertNotFound);
    }

    #[test]
    fn r3_rejects_an_unknown_seat() {
        TestFramework::<Concert>::new(rules())
            .given(vec![announced()])
            .when(book("Z9", "fox", "Fox", Uuid::new_v4()))
            .then_expect_error(ConcertError::UnknownSeat { seat: "Z9".into() });
    }

    /// Sold-out wins over "seat taken": on a full hall even a taken seat is reported as sold out.
    #[test]
    fn r4_rejects_booking_when_sold_out() {
        TestFramework::<Concert>::new(rules())
            .given(full_house())
            .when(book("B4", "fox", "Fox", Uuid::new_v4()))
            .then_expect_error(ConcertError::SoldOut);
    }

    #[test]
    fn r5_rejects_a_taken_seat() {
        TestFramework::<Concert>::new(rules())
            .given(vec![announced(), booked("B4", "fox", "Fox", Uuid::new_v4())])
            .when(book("B4", "panda", "Panda", Uuid::new_v4()))
            .then_expect_error(ConcertError::SeatTaken {
                seat: SeatId::parse("B4").unwrap(),
                by: "Fox".into(),
            });
    }

    #[test]
    fn r6_rejects_the_fifth_ticket_and_the_limit_comes_from_the_rules() {
        let history = vec![
            announced(),
            booked("A1", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()),
            booked("A2", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()),
            booked("A3", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()),
            booked("A4", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()),
        ];
        TestFramework::<Concert>::new(rules())
            .given(history.clone())
            .when(book("A5", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()))
            .then_expect_error(ConcertError::TicketLimit { limit: 4 });
        // Another fan is unaffected.
        assert!(
            TestFramework::<Concert>::new(rules())
                .given(history.clone())
                .when(book("A5", "fox", "Fox", Uuid::new_v4()))
                .into_result()
                .is_ok()
        );
        // A looser limit from the rules accepts the fifth ticket.
        assert!(
            TestFramework::<Concert>::new(ConcertRules { ticket_limit: 5 })
                .given(history)
                .when(book("A5", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()))
                .into_result()
                .is_ok()
        );
    }

    #[test]
    fn a_cancelled_seat_can_be_booked_again() {
        let booking_id = Uuid::new_v4();
        let history = vec![
            announced(),
            booked("B4", "fox", "Fox", booking_id),
            ConcertEvent::BookingCancelled(BookingCancelled {
                seat: SeatId::parse("B4").unwrap(),
                fan_id: fan("fox"),
                booking_id,
                cancellation_id: Uuid::new_v4(),
                refund: 80,
            }),
        ];
        assert!(
            TestFramework::<Concert>::new(rules())
                .given(history)
                .when(book("B4", "panda", "Panda", Uuid::new_v4()))
                .into_result()
                .is_ok()
        );
    }
}
