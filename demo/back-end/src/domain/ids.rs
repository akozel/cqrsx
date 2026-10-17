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
}
