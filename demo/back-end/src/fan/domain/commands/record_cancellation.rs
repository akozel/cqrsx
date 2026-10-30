//! `RecordCancellation`: a cancelled booking is marked in the fan's ledger. Rules R10, R11; the
//! accepted fact is [`TicketCancelled`].
use crate::fan::domain::{Fan, FanError, FanEvent, TicketState, events::TicketCancelled};
use cqrsx::{Command, cqrs::command::decide::Decide};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Command)]
pub struct RecordCancellation {
    pub booking_id: Uuid,
    pub cancellation_id: Uuid,
    pub refund: u32,
}

impl Decide<Fan> for RecordCancellation {
    fn decide(self, fan: &Fan, _rules: &()) -> Result<FanEvent, FanError> {
        let RecordCancellation {
            booking_id,
            cancellation_id,
            refund,
        } = self;
        // R10: the cancellation task overtook the record task → the handler retries later.
        let ticket = fan.tickets().get(&booking_id).ok_or(FanError::UnknownTicket { booking_id })?;
        if ticket.state == TicketState::Cancelled {
            return Err(FanError::AlreadyCancelled { booking_id }); // R11: redelivered task
        }
        Ok(FanEvent::TicketCancelled(TicketCancelled {
            booking_id,
            cancellation_id,
            concert_id: ticket.concert_id.clone(),
            seat: ticket.seat.clone(),
            refund,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::fan::domain::{
        Fan, FanError,
        test_support::{cancel, cancelled, recorded},
    };
    use cqrsx::cqrs::testing::TestFramework;
    use uuid::Uuid;

    #[test]
    fn record_cancellation_happy_path_copies_concert_and_seat_from_the_ticket() {
        let id = Uuid::new_v4();
        let cancellation_id = Uuid::new_v4();
        TestFramework::<Fan>::new(())
            .given(vec![recorded(id)])
            .when(cancel(id, cancellation_id))
            .then_expect_event(&cancelled(id, cancellation_id));
    }

    #[test]
    fn r10_rejects_cancellation_of_an_unknown_ticket() {
        let id = Uuid::new_v4();
        TestFramework::<Fan>::new(())
            .given_no_previous_events()
            .when(cancel(id, Uuid::new_v4()))
            .then_expect_error(&FanError::UnknownTicket { booking_id: id });
    }

    #[test]
    fn r11_rejects_a_repeated_cancellation() {
        let id = Uuid::new_v4();
        let cancellation_id = Uuid::new_v4();
        TestFramework::<Fan>::new(())
            .given(vec![recorded(id), cancelled(id, cancellation_id)])
            .when(cancel(id, cancellation_id))
            .then_expect_error(&FanError::AlreadyCancelled { booking_id: id });
    }
}
