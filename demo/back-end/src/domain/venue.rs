//! The hall: rows `A`–`E`, 8 seats each, three price zones. Reference data, not events.
use serde::{Deserialize, Serialize};
use std::fmt;

pub const ROWS: [char; 5] = ['A', 'B', 'C', 'D', 'E'];
pub const SEATS_PER_ROW: u8 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Zone {
    Vip,
    Standard,
    Balcony,
}

impl Zone {
    pub fn id(self) -> &'static str {
        match self {
            Zone::Vip => "vip",
            Zone::Standard => "standard",
            Zone::Balcony => "balcony",
        }
    }

    pub fn price(self) -> u32 {
        match self {
            Zone::Vip => 120,
            Zone::Standard => 80,
            Zone::Balcony => 50,
        }
    }

    pub fn rows(self) -> &'static [char] {
        match self {
            Zone::Vip => &['A'],
            Zone::Standard => &['B', 'C'],
            Zone::Balcony => &['D', 'E'],
        }
    }

    pub const ALL: [Zone; 3] = [Zone::Vip, Zone::Standard, Zone::Balcony];

    fn of_row(row: char) -> Option<Zone> {
        Zone::ALL.into_iter().find(|zone| zone.rows().contains(&row))
    }
}

/// A validated seat identifier such as `B4`. Serialized as a plain string, so it can be a
/// `BTreeMap` key inside the aggregate snapshot and inside the MongoDB read model.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SeatId(String);

impl SeatId {
    /// Accepts `^[A-E][1-8]$` (case-insensitive on the row letter).
    pub fn parse(raw: &str) -> Option<SeatId> {
        let raw = raw.trim();
        let mut chars = raw.chars();
        let row = chars.next()?.to_ascii_uppercase();
        let number = chars.next()?.to_digit(10)?;
        if chars.next().is_some() || !ROWS.contains(&row) || !(1..=u32::from(SEATS_PER_ROW)).contains(&number) {
            return None;
        }
        Some(SeatId(format!("{row}{number}")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn row(&self) -> char {
        self.0.chars().next().unwrap_or('A')
    }
}

impl fmt::Display for SeatId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatInfo {
    pub id: SeatId,
    pub zone: Zone,
    pub price: u32,
}

/// The one hall shared by every concert.
pub struct Venue;

pub static VENUE: Venue = Venue;

impl Venue {
    pub fn capacity(&self) -> u32 {
        ROWS.len() as u32 * u32::from(SEATS_PER_ROW)
    }

    /// `None` means rule R3 (`unknown_seat`).
    pub fn seat(&self, raw: &str) -> Option<SeatInfo> {
        let id = SeatId::parse(raw)?;
        let zone = Zone::of_row(id.row())?;
        Some(SeatInfo {
            id,
            zone,
            price: zone.price(),
        })
    }

    pub fn seats(&self) -> impl Iterator<Item = SeatInfo> {
        ROWS.into_iter()
            .flat_map(|row| (1..=SEATS_PER_ROW).filter_map(move |number| VENUE.seat(&format!("{row}{number}"))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_seats_and_rejects_everything_else() {
        assert_eq!(SeatId::parse("B4").map(|s| s.to_string()), Some("B4".into()));
        assert_eq!(SeatId::parse(" e8 ").map(|s| s.to_string()), Some("E8".into()));
        for bad in ["Z9", "A0", "A9", "AA", "B", "B44", "", "4B"] {
            assert_eq!(SeatId::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn zones_and_prices_follow_the_rows() {
        assert_eq!(VENUE.capacity(), 40);
        assert_eq!(VENUE.seats().count(), 40);
        let a1 = VENUE.seat("A1").unwrap();
        assert_eq!((a1.zone, a1.price), (Zone::Vip, 120));
        let c7 = VENUE.seat("C7").unwrap();
        assert_eq!((c7.zone, c7.price), (Zone::Standard, 80));
        let e8 = VENUE.seat("E8").unwrap();
        assert_eq!((e8.zone, e8.price), (Zone::Balcony, 50));
        assert_eq!(serde_json::to_string(&Zone::Vip).unwrap(), "\"vip\"");
        assert_eq!(serde_json::to_string(&e8.id).unwrap(), "\"E8\"");
    }
}
