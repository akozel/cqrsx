//! The id of a probe: the key of its stream, of its lock and of its documents.
use std::{fmt, str::FromStr};

/// A validated probe id (`k6-run-12-p7`): `[a-z0-9-]{1,64}`, as the k6 scenario names its probes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProbeId(String);

/// A path segment that is not a probe id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid probe id {0:?}: expected [a-z0-9-]{{1,64}}")]
pub struct InvalidProbeId(String);

impl ProbeId {
    /// The id's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ProbeId {
    type Err = InvalidProbeId;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let valid =
            (1..=64).contains(&raw.len()) && raw.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if valid {
            Ok(ProbeId(raw.to_owned()))
        } else {
            Err(InvalidProbeId(raw.to_owned()))
        }
    }
}

/// The id's text, byte for byte: the key of the probe's stream.
impl fmt::Display for ProbeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_probe_id_is_a_short_lowercase_slug() {
        let id: ProbeId = "k6-run-12-p7".parse().unwrap();
        assert_eq!((id.as_str(), id.to_string().as_str()), ("k6-run-12-p7", "k6-run-12-p7"));
        for invalid in ["", "P1", "p_1", "p 1", &"p".repeat(65)] {
            assert_eq!(
                invalid.parse::<ProbeId>(),
                Err(InvalidProbeId(invalid.to_owned())),
                "{invalid:?}"
            );
        }
        assert_eq!(
            "P1".parse::<ProbeId>().unwrap_err().to_string(),
            r#"invalid probe id "P1": expected [a-z0-9-]{1,64}"#
        );
    }
}
