//! The journal of one aggregate type read in commit order.
//!
//! Every stored event remembers the id of the transaction that wrote it (PostgreSQL `xid8`; a commit
//! counter in memory): its [`Position`] in the journal of its aggregate type. Order contract: inside
//! one aggregate the order of positions is the order of sequences, because event `n + 1` is written
//! by a command that loaded event `n`, after its commit; between aggregates it is the order in which
//! the transactions got their ids, the same at every read. A reader remembers the greatest position
//! it applied and asks for what comes after it
//! ([`PersistedEventRepository::feed_after`](crate::cqrs::store::repository::PersistedEventRepository::feed_after)).
use crate::cqrs::store::serialized_event::SerializedEvent;
use std::{fmt, num::ParseIntError, str::FromStr};

/// A position in the journal of one aggregate type; [`Position::START`] lies before every event.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(u64);

impl Position {
    /// Before the first event of any journal: where a new reader starts.
    pub const START: Position = Position(0);

    /// A position from its raw value: a transaction id or a commit count.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Decimal, the text form PostgreSQL gives an `xid8`.
impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Position {
    type Err = ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse().map(Self)
    }
}

/// One event of a feed page together with its position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedEntry {
    /// Where the event stands in the journal of its aggregate type.
    pub position: Position,
    /// The event as stored.
    pub event: SerializedEvent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_start_at_zero_order_by_value_and_round_trip_as_decimal_text() {
        assert_eq!(Position::START, Position::default());
        assert_eq!(Position::START.value(), 0);
        assert!(Position::START < Position::new(1));
        assert!(Position::new(9) < Position::new(10));
        for value in [0, 1, 4_294_967_296, u64::MAX] {
            let text = Position::new(value).to_string();
            assert_eq!(text, value.to_string());
            assert_eq!(text.parse::<Position>().unwrap(), Position::new(value));
        }
        for broken in ["", "x", "-1", "1.5", "18446744073709551616"] {
            assert!(broken.parse::<Position>().is_err(), "{broken}");
        }
    }
}
