// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx/licenses/.
use crate::journal::serialized_event::{EventFormat, SerializedEvent};
use serde_json::Value;
use std::{
    fmt::{self, Debug, Display, Formatter},
    num::ParseIntError,
    str::FromStr,
};

/// Used to upcast an event from an older type or version to the current form. This is needed
/// to change the structure of events whose older versions are already persisted.
pub trait EventUpcaster: Send + Sync {
    /// Examines an event type and version to decide whether the event should be upcast.
    fn can_upcast(&self, format: EventFormat<'_>) -> bool;

    /// Modifies the serialized event to conform to the new structure.
    fn upcast(&self, event: SerializedEvent) -> SerializedEvent;
}

/// A helper type for creating the upcaster function for a `NumericVersionUpcaster`.
pub type EventPayloadTransform = dyn Fn(Value) -> Value + Send + Sync;

/// Numeric event version used by `NumericVersionUpcaster`, rather than SemVer.
///
/// Missing minor/patch components default to zero; components after the third are
/// ignored. The first three components must be unsigned integers. Prerelease and
/// build suffixes in those components are unsupported. Persisted versions are unchanged.
///
/// Versions compare component by component, major first: an upcaster applies to the events whose version is
/// lower than its own, so an upcaster of v0.2.3 upcasts an event of v0.2.2 and leaves one of v0.2.3 as it is.
#[derive(Debug, PartialOrd, PartialEq, Eq)]
pub struct NumericVersion {
    major: u32,
    minor: u32,
    patch: u32,
}

impl Display for NumericVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}
impl FromStr for NumericVersion {
    type Err = NumericVersionError;

    fn from_str(event_version: &str) -> Result<Self, Self::Err> {
        let mut split_version = event_version.split('.').fuse();
        // `split` yields at least one component, maybe empty: an empty major version does not parse.
        let major = u32::from_str(split_version.next().unwrap_or_default())?;
        let minor = split_version.next().map_or(Ok(0), u32::from_str)?;
        let patch = split_version.next().map_or(Ok(0), u32::from_str)?;
        Ok(Self { major, minor, patch })
    }
}

#[derive(Debug, PartialOrd, PartialEq, Eq)]
/// The first three numeric version components could not be parsed.
pub struct NumericVersionError;

impl From<ParseIntError> for NumericVersionError {
    fn from(_: ParseIntError) -> Self {
        Self
    }
}

/// This upcasts any event that has the same `event_type` and an `event_version` that is less than the
/// version configured on the upcaster.
///
/// ```
/// use cqrsx::journal::upcaster::EventUpcaster;
/// use cqrsx::journal::upcaster::NumericVersionUpcaster;
/// use serde_json::Value;
/// use cqrsx::journal::serialized_event::SerializedEvent;
///
/// let upcast_function = Box::new(|payload: Value| match payload {
///             Value::Object(mut object_map) => {
///                 object_map.insert("country".to_string(), "USA".into());
///                 Value::Object(object_map)
///             }
///             _ => {
///                 panic!("the event payload is not an object")
///             }
///         });
/// let upcaster = NumericVersionUpcaster::try_new("EventX", "2.3.4", upcast_function).unwrap();
///
/// let payload: Value = serde_json::from_str(
///             r#"{
///                     "zip code": 98103,
///                     "state": "Washington"
///                    }"#,
///         ).unwrap();
/// let event = SerializedEvent {
///     aggregate_id: String::new(),
///     sequence: 0,
///     aggregate_type: String::new(),
///     event_type: String::new(),
///     event_version: String::new(),
///     payload,
///     metadata: Value::default(),
/// };
/// let upcasted_event = upcaster.upcast(event);
///
/// let expected_payload: Value = serde_json::from_str(
///             r#"{
///                     "zip code": 98103,
///                     "state": "Washington",
///                     "country": "USA"
///                    }"#,
///         ).unwrap();
/// let expected_event = SerializedEvent {
///     aggregate_id: String::new(),
///     sequence: 0,
///     aggregate_type: String::new(),
///     event_type: String::new(),
///     event_version: "2.3.4".to_string(),
///     payload: expected_payload,
///     metadata: Value::default(),
/// };
///
/// assert_eq!(upcasted_event, expected_event);
/// ```
pub struct NumericVersionUpcaster {
    event_type: String,
    target_version: NumericVersion,
    transform: Box<EventPayloadTransform>,
}

// By hand: the transform is a closure.
impl Debug for NumericVersionUpcaster {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NumericVersionUpcaster")
            .field("event_type", &self.event_type)
            .field("target_version", &self.target_version)
            .finish_non_exhaustive()
    }
}

impl NumericVersionUpcaster {
    /// Upcasts the events of `event_type` older than `target_version` with `transform`; a `target_version` that is not
    /// a numeric version (see [`NumericVersion`]) is refused.
    pub fn try_new(
        event_type: &str,
        target_version: &str,
        transform: Box<EventPayloadTransform>,
    ) -> Result<Self, NumericVersionError> {
        Ok(Self {
            event_type: event_type.to_string(),
            target_version: NumericVersion::from_str(target_version)?,
            transform,
        })
    }
}

impl EventUpcaster for NumericVersionUpcaster {
    fn can_upcast(&self, format: EventFormat<'_>) -> bool {
        if format.event_type != self.event_type {
            return false;
        }
        let Ok(event_version) = NumericVersion::from_str(format.event_version) else {
            return false;
        };
        self.target_version > event_version
    }

    fn upcast(&self, event: SerializedEvent) -> SerializedEvent {
        let upcasted_payload = (self.transform)(event.payload);
        SerializedEvent {
            aggregate_id: event.aggregate_id,
            sequence: event.sequence,
            aggregate_type: event.aggregate_type,
            event_type: event.event_type,
            event_version: self.target_version.to_string(),
            payload: upcasted_payload,
            metadata: event.metadata,
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::journal::serialized_event::{EventFormat, SerializedEvent};
    use serde_json::Value;
    use serde_json::json;

    use crate::journal::upcaster::EventPayloadTransform;
    use crate::journal::upcaster::EventUpcaster;
    use crate::journal::upcaster::NumericVersion;
    use crate::journal::upcaster::NumericVersionError;
    use crate::journal::upcaster::NumericVersionUpcaster;

    fn semantic_version(major: u32, minor: u32, patch: u32) -> NumericVersion {
        NumericVersion { major, minor, patch }
    }

    #[test]
    fn parse_version() {
        assert_eq!(semantic_version(2, 0, 0), NumericVersion::from_str("2").unwrap());
        assert_eq!(semantic_version(2, 3, 0), NumericVersion::from_str("2.3").unwrap());
        assert_eq!(semantic_version(2, 3, 4), NumericVersion::from_str("2.3.4").unwrap());
        assert_eq!(semantic_version(2, 3, 4), NumericVersion::from_str("2.3.4.5").unwrap());
    }

    #[test]
    fn parse_version_invalid() {
        assert_eq!(Err(NumericVersionError), NumericVersion::from_str("not_a_version"));
        assert_eq!(Err(NumericVersionError), NumericVersion::from_str(""));
        assert_eq!(Err(NumericVersionError), NumericVersion::from_str("2."));
        assert_eq!(Err(NumericVersionError), NumericVersion::from_str("2.3."));
    }

    #[test]
    fn simple_upcaster_can_upcast() {
        let upcaster = NumericVersionUpcaster::try_new("EventX", "2.3.4", Box::new(|event| event)).unwrap();
        let can_upcast = |event_type, event_version| {
            upcaster.can_upcast(EventFormat {
                event_type,
                event_version,
            })
        };
        assert!(can_upcast("EventX", "1.12.35"));
        assert!(can_upcast("EventX", "2.3.3"));
        assert!(!can_upcast("AnotherEvent", "1.12.35"));
        assert!(!can_upcast("EventX", "2.3.4"));
        assert!(!can_upcast("EventX", "2.3.5"));
        assert!(!can_upcast("EventX", "2.4.0"));
        assert!(!can_upcast("EventX", "3.0.0"));
        assert!(!can_upcast("EventX", "not_a_version"));
        assert_eq!(
            format!("{upcaster:?}"),
            r#"NumericVersionUpcaster { event_type: "EventX", target_version: NumericVersion { major: 2, minor: 3, patch: 4 }, .. }"#
        );
    }

    #[test]
    fn semantic_version_upcaster_can_upcast() {
        assert!(NumericVersionUpcaster::try_new("EventX", "2.3.4", test_upcast()).is_ok());
    }

    #[test]
    fn semantic_version_upcaster_invalid_version() {
        for invalid in ["not_a_version", "", "2."] {
            assert!(
                matches!(
                    NumericVersionUpcaster::try_new("EventX", invalid, test_upcast()),
                    Err(NumericVersionError)
                ),
                "{invalid:?} is refused"
            );
        }
    }

    #[test]
    fn semantic_version_upcaster_upcast() {
        let upcaster = NumericVersionUpcaster::try_new("EventX", "2.3.4", test_upcast()).unwrap();
        let payload: Value = serde_json::from_str(
            r#"{
    "id": 4829,
    "name": "George Steinbrenner"
}"#,
        )
        .unwrap();
        let event = SerializedEvent {
            aggregate_id: String::new(),
            sequence: 0,
            aggregate_type: String::new(),
            event_type: String::new(),
            event_version: String::new(),
            payload,
            metadata: Value::default(),
        };

        let upcasted_event = upcaster.upcast(event);

        let expected: Value = serde_json::from_str(r#"{"id":"CUST4829","name":"George Steinbrenner"}"#).unwrap();
        assert_eq!(expected, upcasted_event.payload);
    }
    #[test]
    fn semantic_version_upcaster_upcast_for_documentation() {
        let upcast_function = Box::new(|payload: Value| {
            if let Value::Object(mut object_map) = payload {
                object_map.insert("country".to_string(), "USA".into());
                Value::Object(object_map)
            } else {
                panic!("the event payload is not an object")
            }
        });
        let upcaster = NumericVersionUpcaster::try_new("EventX", "2.3.4", upcast_function).unwrap();

        let payload: Value = serde_json::from_str(
            r#"{
                    "zip code": 98103,
                    "state": "Washington"
                   }"#,
        )
        .unwrap();
        let event = SerializedEvent {
            aggregate_id: String::new(),
            sequence: 0,
            aggregate_type: String::new(),
            event_type: String::new(),
            event_version: String::new(),
            payload,
            metadata: Value::default(),
        };
        let upcasted_event = upcaster.upcast(event);

        let expected_payload: Value = serde_json::from_str(
            r#"{
                    "zip code": 98103,
                    "state": "Washington",
                    "country": "USA"
                   }"#,
        )
        .unwrap();
        let expected_event = SerializedEvent {
            aggregate_id: String::new(),
            sequence: 0,
            aggregate_type: String::new(),
            event_type: String::new(),
            event_version: "2.3.4".to_string(),
            payload: expected_payload,
            metadata: Value::default(),
        };

        assert_eq!(upcasted_event, expected_event);
    }

    fn test_upcast() -> Box<EventPayloadTransform> {
        Box::new(|mut payload| {
            let current_id = payload.get("id").unwrap().to_string();
            let updated_id = format!("CUST{current_id}");
            *payload.get_mut("id").unwrap() = json!(updated_id);
            payload
        })
    }
}
