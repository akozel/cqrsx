//! `RecordTicket`: a booked seat enters the fan's ledger. Rule R9; the accepted fact is
//! [`TicketRecorded`].
use crate::{
    fan::domain::{Fan, FanError, FanEvent, events::TicketRecorded},
    shared::domain::{
        ConcertId,
        venue::{SeatId, Zone},
    },
};
use cqrsx::domain::command::{Command, Decide};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Command)]
pub struct RecordTicket {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub zone: Zone,
    pub price: u32,
    pub fan_name: String,
}

impl Decide<Fan> for RecordTicket {
    fn decide(self, fan: &Fan, _rules: &()) -> Result<FanEvent, FanError> {
        let RecordTicket {
            booking_id,
            concert_id,
            seat,
            zone,
            price,
            fan_name,
        } = self;
        if fan.tickets().contains_key(&booking_id) {
            return Err(FanError::AlreadyRecorded { booking_id }); // R9: redelivered task
        }
        Ok(FanEvent::TicketRecorded(TicketRecorded {
            booking_id,
            concert_id,
            seat,
            zone,
            price,
            fan_name,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::fan::domain::{
        Fan, FanError,
        test_support::{record, recorded},
    };
    use cqrsx_testing::framework::TestFramework;
    use uuid::Uuid;

    #[test]
    fn record_ticket_happy_path() {
        let id = Uuid::new_v4();
        TestFramework::<Fan>::new(())
            .given_no_previous_events()
            .when(record(id))
            .then_expect_event(&recorded(id));
    }

    #[test]
    fn r9_rejects_a_repeated_record_ticket() {
        let id = Uuid::new_v4();
        TestFramework::<Fan>::new(())
            .given(vec![recorded(id)])
            .when(record(id))
            .then_expect_error(&FanError::AlreadyRecorded { booking_id: id });
    }
}
