//! Fact: a concert is open for booking. Read models start the seat map from it.
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcertAnnounced {
    pub artist: String,
    pub city: String,
    pub date: NaiveDate,
    pub capacity: u32,
}
