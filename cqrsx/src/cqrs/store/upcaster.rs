// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::store::serialized_event::SerializedEvent;
use serde_json::Value;
use std::{
    fmt::{Display, Formatter},
    num::ParseIntError,
    str::FromStr,
};

/// Used to upcast and event from an older type or version to the current form. This is needed
/// to modify the structure of events older versions are already persisted.
pub trait EventUpcaster: Send + Sync {
    /// Examines and event type and version to understand if the event should be upcasted.
    fn can_upcast(&self, event_type: &str, event_version: &str) -> bool;

    /// Modifies the serialized event to conform the the new structure.
    fn upcast(&self, event: SerializedEvent) -> SerializedEvent;
}

/// A helper type for creating the upcaster function for a `NumericVersionUpcaster`.
pub type EventPayloadTransform = dyn Fn(Value) -> Value + Send + Sync;

/// Numeric event version used by `NumericVersionUpcaster`, rather than SemVer.
///
/// Missing minor/patch components default to zero; components after the third are
/// ignored. The first three components must be unsigned integers. Prerelease and
/// build suffixes in those components are unsupported. Persisted versions are unchanged.
#[derive(Debug, PartialOrd, PartialEq, Eq)]
pub struct NumericVersion {
    major_version: u32,
    minor_version: u32,
    patch: u32,
}

impl NumericVersion {
    /// Identifies if one `NumericVersion` supersedes another. Used to determine whether an
    /// upcaster function should be applied.
    ///
    /// E.g.,
    /// - for upcaster v0.2.3 with code v0.2.2 --> upcaster is applied
    /// - for upcaster v0.2.2 with code v0.2.2 --> upcaster is _not_ applied
    pub fn supersedes(&self, other: &Self) -> bool {
        (self.major_version, self.minor_version, self.patch) > (other.major_version, other.minor_version, other.patch)
    }
}

impl Display for NumericVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major_version, self.minor_version, self.patch)
    }
}
impl FromStr for NumericVersion {
    type Err = NumericVersionError;

    fn from_str(event_version: &str) -> Result<Self, Self::Err> {
        let mut split_version = event_version.split('.').fuse();
        // `split` yields at least one component, maybe empty: an empty major version does not parse.
        let major_version = u32::from_str(split_version.next().unwrap_or_default())?;
        let minor_version = split_version.next().map_or(Ok(0), u32::from_str)?;
        let patch = split_version.next().map_or(Ok(0), u32::from_str)?;
        Ok(Self {
            major_version,
            minor_version,
            patch,
        })
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
/// use cqrsx::cqrs::store::upcaster::EventUpcaster;
/// use cqrsx::cqrs::store::upcaster::NumericVersionUpcaster;
/// use serde_json::Value;
/// use cqrsx::cqrs::store::serialized_event::SerializedEvent;
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
///  let event = SerializedEvent::new(
///             String::new(),
///             0,
///             String::new(),
///             String::new(),
///             String::new(),
///             payload,
///             Default::default(),
///         );
/// let upcasted_event = upcaster.upcast(event);
///
/// let expected_payload: Value = serde_json::from_str(
///             r#"{
///                     "zip code": 98103,
///                     "state": "Washington",
///                     "country": "USA"
///                    }"#,
///         ).unwrap();
/// let expected_event = SerializedEvent::new(
///             String::new(),
///             0,
///             String::new(),
///             String::new(),
///             "2.3.4".to_string(),
///             expected_payload,
///             Default::default(),
///         );
///
/// assert_eq!(upcasted_event, expected_event);
/// ```
pub struct NumericVersionUpcaster {
    event_type: String,
    target_version: NumericVersion,
    transform: Box<EventPayloadTransform>,
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
    fn can_upcast(&self, event_type: &str, event_version: &str) -> bool {
        if event_type != self.event_type {
            return false;
        }
        let Ok(event_version) = NumericVersion::from_str(event_version) else {
            return false;
        };
        self.target_version.supersedes(&event_version)
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

    use crate::cqrs::store::serialized_event::SerializedEvent;
    use serde_json::Value;
    use serde_json::json;

    use crate::cqrs::store::upcaster::EventPayloadTransform;
    use crate::cqrs::store::upcaster::EventUpcaster;
    use crate::cqrs::store::upcaster::NumericVersion;
    use crate::cqrs::store::upcaster::NumericVersionError;
    use crate::cqrs::store::upcaster::NumericVersionUpcaster;

    fn semantic_version(major_version: u32, minor_version: u32, patch: u32) -> NumericVersion {
        NumericVersion {
            major_version,
            minor_version,
            patch,
        }
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
        assert!(upcaster.can_upcast("EventX", "1.12.35"));
        assert!(upcaster.can_upcast("EventX", "2.3.3"));
        assert!(!upcaster.can_upcast("AnotherEvent", "1.12.35"));
        assert!(!upcaster.can_upcast("EventX", "2.3.4"));
        assert!(!upcaster.can_upcast("EventX", "2.3.5"));
        assert!(!upcaster.can_upcast("EventX", "2.4.0"));
        assert!(!upcaster.can_upcast("EventX", "3.0.0"));
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
        let event = SerializedEvent::new(
            String::new(),
            0,
            String::new(),
            String::new(),
            String::new(),
            payload,
            Value::default(),
        );

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
        let event = SerializedEvent::new(
            String::new(),
            0,
            String::new(),
            String::new(),
            String::new(),
            payload,
            Value::default(),
        );
        let upcasted_event = upcaster.upcast(event);

        let expected_payload: Value = serde_json::from_str(
            r#"{
                    "zip code": 98103,
                    "state": "Washington",
                    "country": "USA"
                   }"#,
        )
        .unwrap();
        let expected_event = SerializedEvent::new(
            String::new(),
            0,
            String::new(),
            String::new(),
            "2.3.4".to_string(),
            expected_payload,
            Value::default(),
        );

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
