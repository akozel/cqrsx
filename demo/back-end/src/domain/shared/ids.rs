//! Validated identifiers shared by both aggregates, the task payloads and the HTTP layer. Both are
//! plain strings on the wire (`#[serde(try_from = "String", into = "String")]`), so a document or a
//! request that carries a malformed id is rejected at the boundary, never deep inside a handler.
use serde::{Deserialize, Serialize};
use std::fmt;

/// A validated fan identifier (`fox`, `crowd-12`, `arbitrage-bot`): `[a-z0-9_-]{1,64}`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct FanId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid fan id {0:?}: expected [a-z0-9_-]{{1,64}}")]
pub struct InvalidFanId(String);

impl FanId {
    pub fn parse(raw: &str) -> Result<FanId, InvalidFanId> {
        let raw = raw.trim();
        if is_slug(raw, |c| c == '-' || c == '_') {
            Ok(FanId(raw.to_owned()))
        } else {
            Err(InvalidFanId(raw.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated concert identifier (`radiohead-2026-11-21`): `[a-z0-9-]{1,64}`. It is the
/// aggregate id of the `Concert` stream and the `_id` of its read-model document.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ConcertId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid concert id {0:?}: expected [a-z0-9-]{{1,64}}")]
pub struct InvalidConcertId(String);

impl ConcertId {
    pub fn parse(raw: &str) -> Result<ConcertId, InvalidConcertId> {
        let raw = raw.trim();
        if is_slug(raw, |c| c == '-') {
            Ok(ConcertId(raw.to_owned()))
        } else {
            Err(InvalidConcertId(raw.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A fan's ticket ledger for one concert (`fox:radiohead-2026-11-21`): the aggregate id of a `Fan`
/// stream and the `_id` of its wallet document (decision D-14). One fan has one ledger per concert,
/// so a ledger never holds more than the ticket limit and its snapshot never grows with the fan's
/// history. `:` belongs to neither id alphabet, so the two halves are unambiguous.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct FanLedgerId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid fan ledger id {0:?}: expected <fan_id>:<concert_id>")]
pub struct InvalidFanLedgerId(String);

const LEDGER_SEPARATOR: char = ':';
/// The character right after the separator: the upper bound of a fan's id range.
const AFTER_SEPARATOR: char = ';';

impl FanLedgerId {
    pub fn new(fan: &FanId, concert: &ConcertId) -> FanLedgerId {
        FanLedgerId(format!("{}{LEDGER_SEPARATOR}{}", fan.as_str(), concert.as_str()))
    }

    pub fn parse(raw: &str) -> Result<FanLedgerId, InvalidFanLedgerId> {
        let raw = raw.trim();
        let invalid = || InvalidFanLedgerId(raw.to_owned());
        let (fan, concert) = raw.split_once(LEDGER_SEPARATOR).ok_or_else(invalid)?;
        let fan = FanId::parse(fan).map_err(|_| invalid())?;
        let concert = ConcertId::parse(concert).map_err(|_| invalid())?;
        Ok(FanLedgerId::new(&fan, &concert))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn halves(&self) -> (&str, &str) {
        self.0.split_once(LEDGER_SEPARATOR).unwrap_or((&self.0, ""))
    }

    pub fn fan(&self) -> FanId {
        FanId(self.halves().0.to_owned())
    }

    pub fn concert(&self) -> ConcertId {
        ConcertId(self.halves().1.to_owned())
    }

    /// The half-open id range `[lower, upper)` that holds every ledger of `fan` and nothing else:
    /// `("fox:", "fox;")`. No id character sorts between `:` and `;`, so `fox-…`, `fox0…` and
    /// `fox_…` fall outside, and so does every other fan.
    pub fn id_range(fan: &FanId) -> (String, String) {
        (
            format!("{}{LEDGER_SEPARATOR}", fan.as_str()),
            format!("{}{AFTER_SEPARATOR}", fan.as_str()),
        )
    }
}

/// Non-empty, at most 64 bytes, lowercase ASCII letters and digits plus the given separators.
fn is_slug(raw: &str, separator: impl Fn(char) -> bool) -> bool {
    !raw.is_empty()
        && raw.len() <= 64
        && raw
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || separator(c))
}

macro_rules! string_newtype_conversions {
    ($id:ident, $error:ident) => {
        impl TryFrom<String> for $id {
            type Error = $error;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                $id::parse(&value)
            }
        }

        impl From<$id> for String {
            fn from(id: $id) -> String {
                id.0
            }
        }

        impl fmt::Display for $id {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_newtype_conversions!(FanId, InvalidFanId);
string_newtype_conversions!(ConcertId, InvalidConcertId);
string_newtype_conversions!(FanLedgerId, InvalidFanLedgerId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fan_ids_allow_underscores_and_concert_ids_do_not() {
        assert_eq!(FanId::parse(" fox ").unwrap().as_str(), "fox");
        assert!(FanId::parse("crowd_12").is_ok());
        assert!(FanId::parse("Fox!").is_err());
        assert!(FanId::parse("").is_err());
        assert!(FanId::parse(&"a".repeat(65)).is_err());
        assert_eq!(
            ConcertId::parse("radiohead-2026-11-21").unwrap().to_string(),
            "radiohead-2026-11-21"
        );
        assert!(ConcertId::parse("Tbilisi 2026").is_err());
        assert!(ConcertId::parse("under_score").is_err());
    }

    #[test]
    fn serde_round_trips_as_plain_strings_and_rejects_bad_input() {
        let fan: FanId = serde_json::from_str("\"fox\"").unwrap();
        assert_eq!(serde_json::to_string(&fan).unwrap(), "\"fox\"");
        assert!(serde_json::from_str::<FanId>("\"Fox\"").is_err());
        let concert: ConcertId = serde_json::from_str("\"c-1\"").unwrap();
        assert_eq!(String::from(concert), "c-1");
        assert!(serde_json::from_str::<ConcertId>("\"c 1\"").is_err());
    }

    #[test]
    fn a_ledger_id_joins_a_fan_and_a_concert_and_parses_back() {
        let fan = FanId::parse("fox").unwrap();
        let concert = ConcertId::parse("radiohead-2026-11-21").unwrap();
        let ledger = FanLedgerId::new(&fan, &concert);
        assert_eq!(ledger.as_str(), "fox:radiohead-2026-11-21");
        assert_eq!(FanLedgerId::parse(" fox:radiohead-2026-11-21 ").unwrap(), ledger);
        assert_eq!((ledger.fan(), ledger.concert()), (fan, concert));
        let json: FanLedgerId = serde_json::from_str("\"crowd_1:c-1\"").unwrap();
        assert_eq!(String::from(json), "crowd_1:c-1");
        for bad in ["fox", "fox:", ":c-1", "fox:c-1:x", "Fox:c-1", "fox:c_1", "fox/c-1"] {
            assert!(FanLedgerId::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_id_range_of_a_fan_holds_its_ledgers_and_no_other_fans() {
        let (lower, upper) = FanLedgerId::id_range(&FanId::parse("fox").unwrap());
        assert_eq!((lower.as_str(), upper.as_str()), ("fox:", "fox;"));
        let inside = |id: &str| lower.as_str() <= id && id < upper.as_str();
        assert!(inside("fox:c-1"));
        assert!(inside("fox:zz-9"));
        for other in ["fox-1:c-1", "fox_1:c-1", "fox0:c-1", "foxa:c-1", "fo:c-1", "panda:c-1"] {
            assert!(!inside(other), "{other}");
        }
    }
}
