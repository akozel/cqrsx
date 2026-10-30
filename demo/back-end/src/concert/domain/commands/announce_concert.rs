//! `AnnounceConcert`: the organiser opens a concert for booking. Rule R2; the accepted fact is
//! [`ConcertAnnounced`].
use crate::{
    concert::domain::{Concert, ConcertError, ConcertEvent, ConcertRules, events::ConcertAnnounced},
    shared::domain::venue::VENUE,
};
use chrono::NaiveDate;
use cqrsx::{Command, cqrs::command::decide::Decide};

/// The organiser's concert: nobody's, so it has no label for the activity feed.
#[derive(Debug, Clone, PartialEq, Eq, Command)]
pub struct AnnounceConcert {
    pub artist: String,
    pub city: String,
    pub date: NaiveDate,
}

impl Decide<Concert> for AnnounceConcert {
    fn decide(self, concert: &Concert, _rules: &ConcertRules) -> Result<ConcertEvent, ConcertError> {
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
    use crate::concert::domain::{
        Concert, ConcertError,
        test_support::{announce, announced, rules},
    };
    use cqrsx::cqrs::testing::TestFramework;

    #[test]
    fn announce_concert_happy_path() {
        TestFramework::<Concert>::new(rules())
            .given_no_previous_events()
            .when(announce())
            .then_expect_event(&announced());
    }

    #[test]
    fn r2_rejects_a_second_announcement() {
        TestFramework::<Concert>::new(rules())
            .given(vec![announced()])
            .when(announce())
            .then_expect_error(&ConcertError::AlreadyAnnounced);
    }
}
