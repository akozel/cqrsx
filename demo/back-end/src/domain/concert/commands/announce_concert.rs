//! `AnnounceConcert`: the organiser opens a concert for booking. Rule R2; the accepted fact is
//! [`ConcertAnnounced`].
use crate::domain::{
    concert::{Concert, ConcertError, ConcertEvent, ConcertServices, events::ConcertAnnounced},
    shared::venue::VENUE,
};
use chrono::NaiveDate;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceConcert {
    pub artist: String,
    pub city: String,
    pub date: NaiveDate,
}

impl AnnounceConcert {
    pub(in crate::domain::concert) fn decide(
        self,
        concert: &Concert,
        _services: &ConcertServices,
    ) -> Result<ConcertEvent, ConcertError> {
        if concert.info().is_some() {
            return Err(ConcertError::AlreadyAnnounced); // R2
        }
        let AnnounceConcert { artist, city, date } = self;
        Ok(ConcertEvent::ConcertAnnounced(ConcertAnnounced {
            artist,
            city,
            date,
            capacity: VENUE.capacity(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::concert::{
        Concert, ConcertError,
        test_support::{announce, announced, services},
    };
    use cqrsx::cqrs::testing::TestFramework;

    #[test]
    fn announce_concert_happy_path() {
        TestFramework::<Concert>::with(services())
            .given_no_previous_events()
            .when(announce())
            .then_expect_events(vec![announced()]);
    }

    #[test]
    fn r2_rejects_a_second_announcement() {
        TestFramework::<Concert>::with(services())
            .given(vec![announced()])
            .when(announce())
            .then_expect_error(ConcertError::AlreadyAnnounced);
    }
}
