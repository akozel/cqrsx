//! Seed: announce the three default concerts with ordinary commands. Business-level idempotency:
//! a repeated `AnnounceConcert` is rejected by rule R2 and the rejection is simply ignored.
use crate::{
    app::command_service::{CommandMetadata, CommandOutcome, ConcertCommands},
    domain::concert::{ConcertCommand, ConcertError},
};
use chrono::NaiveDate;

pub struct SeedConcert {
    pub concert_id: &'static str,
    pub artist: &'static str,
    pub city: &'static str,
    pub date: (i32, u32, u32),
}

pub const SEED_CONCERTS: [SeedConcert; 3] = [
    SeedConcert {
        concert_id: "tbilisi-2026",
        artist: "The Event Sourcerers",
        city: "Tbilisi",
        date: (2026, 11, 21),
    },
    SeedConcert {
        concert_id: "berlin-2026",
        artist: "Eventually Consistent",
        city: "Berlin",
        date: (2026, 12, 5),
    },
    SeedConcert {
        concert_id: "lisbon-2027",
        artist: "The Idempotents",
        city: "Lisbon",
        date: (2027, 1, 16),
    },
];

pub async fn run(commands: &ConcertCommands) {
    for concert in &SEED_CONCERTS {
        let Some(date) = NaiveDate::from_ymd_opt(concert.date.0, concert.date.1, concert.date.2) else {
            tracing::error!("seed: invalid date for {}", concert.concert_id);
            continue;
        };
        let command = ConcertCommand::AnnounceConcert {
            artist: concert.artist.to_owned(),
            city: concert.city.to_owned(),
            date,
        };
        match commands
            .execute(concert.concert_id, command, CommandMetadata::with_source("seed"))
            .await
            .outcome
        {
            CommandOutcome::Accepted { .. } => tracing::info!("seed: announced {}", concert.concert_id),
            CommandOutcome::Rejected(ConcertError::AlreadyAnnounced) => {
                tracing::info!("seed: {} already announced", concert.concert_id)
            }
            other => tracing::warn!("seed: {} not announced: {other:?}", concert.concert_id),
        }
    }
}
