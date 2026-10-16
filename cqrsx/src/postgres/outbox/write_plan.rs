//! Checked single-event and snapshot decisions before any SQL work.
use crate::{
    cqrs::store::{PersistenceError, serialized_event::SerializedEvent, snapshot::SnapshotUpdate},
    task::NewTask,
};
use serde_json::Value;
use thiserror::Error;
pub(super) enum WritePlan<'a> {
    NoOp,
    OneEvent {
        event: &'a SerializedEvent,
        sequence: i64,
        snapshot: SnapshotWrite,
    },
}
pub(super) enum SnapshotWrite {
    None,
    Insert {
        aggregate_id: String,
        payload: Value,
        version: i64,
        last_sequence: i64,
    },
    Update {
        aggregate_id: String,
        payload: Value,
        version: i64,
        last_sequence: i64,
    },
}
impl SnapshotWrite {
    fn new(update: Option<crate::cqrs::store::snapshot::SnapshotUpdate>) -> Result<Self, PersistenceError> {
        match update {
            None => Ok(Self::None),
            Some(SnapshotUpdate {
                aggregate_id,
                payload,
                last_sequence,
                next_version: version,
            }) => {
                let version = checked_bigint(version)?;
                let last_sequence = checked_bigint(last_sequence)?;
                if version == 0 {
                    return Err(invalid_write(WriteValidationError::InvalidSnapshotVersion));
                }
                Ok(if version == 1 {
                    Self::Insert {
                        aggregate_id,
                        payload,
                        version,
                        last_sequence,
                    }
                } else {
                    Self::Update {
                        aggregate_id,
                        payload,
                        version,
                        last_sequence,
                    }
                })
            }
        }
    }
    fn bind_count(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Insert { .. } => 5,
            Self::Update { .. } => 6,
        }
    }
}
impl<'a> WritePlan<'a> {
    pub(super) fn new(
        events: &'a [SerializedEvent],
        snapshot: Option<crate::cqrs::store::snapshot::SnapshotUpdate>,
        tasks: &[NewTask],
    ) -> Result<Self, PersistenceError> {
        match events {
            [] if snapshot.is_some() => Err(invalid_write(WriteValidationError::SnapshotWithoutEvent)),
            [] if !tasks.is_empty() => Err(invalid_write(WriteValidationError::TasksWithoutEvent)),
            [] => Ok(Self::NoOp),
            [event] => {
                let snapshot = SnapshotWrite::new(snapshot)?;
                validate_bind_count(tasks.len(), snapshot.bind_count())?;
                Ok(Self::OneEvent {
                    event,
                    sequence: checked_bigint(event.sequence)?,
                    snapshot,
                })
            }
            _ => Err(invalid_write(WriteValidationError::MultipleEvents)),
        }
    }
}
#[derive(Debug, Error)]
enum WriteValidationError {
    #[error("a command may commit at most one domain event")]
    MultipleEvents,
    #[error("a snapshot requires a committed domain event")]
    SnapshotWithoutEvent,
    #[error("outbox tasks require a committed domain event")]
    TasksWithoutEvent,
    #[error("persisted integer does not fit PostgreSQL bigint")]
    BigintOverflow,
    #[error("snapshot version must be positive")]
    InvalidSnapshotVersion,
    #[error("write batch exceeds PostgreSQL's bind parameter limit")]
    BindLimitExceeded,
}
fn invalid_write(reason: WriteValidationError) -> PersistenceError {
    PersistenceError::UnknownError(Box::new(reason))
}
fn validate_bind_count(tasks: usize, snapshot_binds: usize) -> Result<(), PersistenceError> {
    const EVENT_BINDS: usize = 7;
    const TASK_BINDS: usize = 11;
    let count = tasks
        .checked_mul(TASK_BINDS)
        .and_then(|count| count.checked_add(EVENT_BINDS))
        .and_then(|count| count.checked_add(snapshot_binds));
    if count.is_some_and(|count| count < u16::MAX as usize) {
        Ok(())
    } else {
        Err(invalid_write(WriteValidationError::BindLimitExceeded))
    }
}
fn checked_bigint(sequence: usize) -> Result<i64, PersistenceError> {
    i64::try_from(sequence).map_err(|_| invalid_write(WriteValidationError::BigintOverflow))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(sequence: usize) -> SerializedEvent {
        SerializedEvent::new(
            "aggregate".into(),
            sequence,
            "test".into(),
            "recorded".into(),
            "1".into(),
            json!({}),
            json!({}),
        )
    }

    #[test]
    fn a_write_plan_rejects_oversized_task_batches_including_snapshot_binds() {
        let task = NewTask::from_payload(crate::task::format::TaskFormat::new("test", 1).unwrap(), json!({})).unwrap();
        let events = [event(1)];
        for (count, snapshot) in [
            (5_958, None),
            (
                5_957,
                Some(SnapshotUpdate {
                    aggregate_id: "aggregate".into(),
                    payload: json!({}),
                    last_sequence: 1,
                    next_version: 1,
                }),
            ),
            (
                5_957,
                Some(SnapshotUpdate {
                    aggregate_id: "aggregate".into(),
                    payload: json!({}),
                    last_sequence: 1,
                    next_version: 2,
                }),
            ),
        ] {
            let tasks = vec![task.clone(); count];
            let Err(PersistenceError::UnknownError(source)) = WritePlan::new(&events, snapshot, &tasks) else {
                panic!("an oversized batch must be rejected before issuing SQL");
            };
            assert!(matches!(source.downcast_ref(), Some(WriteValidationError::BindLimitExceeded)));
        }
    }

    #[test]
    fn invalid_write_shapes_retain_their_validation_reason() {
        let tasks = [NewTask::from_payload(crate::task::format::TaskFormat::new("test", 1).unwrap(), json!({})).unwrap()];
        for (events, snapshot, tasks, expected) in [
            (
                vec![],
                Some(SnapshotUpdate {
                    aggregate_id: "aggregate".into(),
                    payload: json!({}),
                    last_sequence: 1,
                    next_version: 1,
                }),
                &[][..],
                "a snapshot requires a committed domain event",
            ),
            (vec![], None, &tasks[..], "outbox tasks require a committed domain event"),
            (
                vec![event(1)],
                Some(SnapshotUpdate {
                    aggregate_id: "aggregate".into(),
                    payload: json!({}),
                    last_sequence: 1,
                    next_version: 0,
                }),
                &[][..],
                "snapshot version must be positive",
            ),
            (
                vec![event(1), event(2)],
                None,
                &[][..],
                "a command may commit at most one domain event",
            ),
        ] {
            let Err(PersistenceError::UnknownError(source)) = WritePlan::new(&events, snapshot, tasks) else {
                panic!("invalid shape must be rejected before SQL");
            };
            assert_eq!(source.to_string(), expected);
        }
        assert!(matches!(WritePlan::new(&[], None, &[]), Ok(WritePlan::NoOp)));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn both_event_and_snapshot_sequence_overflows_are_rejected_before_sql() {
        let overflow = i64::MAX as usize + 1;
        for (event, snapshot) in [
            (event(overflow), None),
            (
                event(1),
                Some(SnapshotUpdate {
                    aggregate_id: "aggregate".into(),
                    payload: json!({}),
                    last_sequence: overflow,
                    next_version: 1,
                }),
            ),
            (
                event(1),
                Some(SnapshotUpdate {
                    aggregate_id: "aggregate".into(),
                    payload: json!({}),
                    last_sequence: 1,
                    next_version: overflow,
                }),
            ),
        ] {
            let Err(PersistenceError::UnknownError(source)) = WritePlan::new(&[event], snapshot, &[]) else {
                panic!("overflow must fail during validation");
            };
            assert!(matches!(source.downcast_ref(), Some(WriteValidationError::BigintOverflow)));
        }
    }
    #[test]
    fn converts_supported_sequence() {
        assert_eq!(checked_bigint(42).expect("sequence must fit"), 42);
    }

    #[test]
    fn rejects_batches_over_the_postgres_bind_limit() {
        assert!(validate_bind_count(1, 6).is_ok());
        assert!(validate_bind_count(5_957, 0).is_ok());
        assert!(matches!(
            validate_bind_count(5_958, 0),
            Err(PersistenceError::UnknownError(_))
        ));
        assert!(validate_bind_count(usize::MAX, 0).is_err());
        // Snapshot binds count toward the same statement limit as the tasks.
        for snapshot_binds in [5, 6] {
            assert!(validate_bind_count(5_956, snapshot_binds).is_ok());
            assert!(validate_bind_count(5_957, snapshot_binds).is_err());
        }
    }

    #[test]
    fn postgres_bind_limit_has_no_exact_boundary_for_supported_snapshot_shapes() {
        // SQL binds seven event fields and eleven fields per task. The only
        // caller supplies these three snapshot shapes; none can total 65535.
        let limit = usize::from(u16::MAX);
        let task = NewTask::from_payload(crate::task::format::TaskFormat::new("test", 1).unwrap(), json!({})).unwrap();
        let events = [event(1)];
        for (update, expected_binds) in [
            (None, 0),
            (
                Some(SnapshotUpdate {
                    aggregate_id: "a".into(),
                    payload: json!({}),
                    last_sequence: 1,
                    next_version: 1,
                }),
                5,
            ),
            (
                Some(SnapshotUpdate {
                    aggregate_id: "a".into(),
                    payload: json!({}),
                    last_sequence: 1,
                    next_version: 2,
                }),
                6,
            ),
        ] {
            let snapshot = SnapshotWrite::new(update.clone()).unwrap();
            assert_eq!(snapshot.bind_count(), expected_binds);
            let fixed_binds = 7 + expected_binds;
            assert_ne!((limit - fixed_binds) % 11, 0);
            let largest_valid_tasks = (limit - fixed_binds) / 11;
            assert!(validate_bind_count(largest_valid_tasks, snapshot.bind_count()).is_ok());
            assert!(validate_bind_count(largest_valid_tasks + 1, snapshot.bind_count()).is_err());
            let tasks = vec![task.clone(); largest_valid_tasks];
            assert!(matches!(
                WritePlan::new(&events, update, &tasks),
                Ok(WritePlan::OneEvent { .. })
            ));
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_overflowing_sequence() {
        let overflowing_sequence = usize::try_from(i64::MAX).expect("64-bit usize must fit i64::MAX") + 1;

        let result = checked_bigint(overflowing_sequence);

        assert!(matches!(result, Err(PersistenceError::UnknownError(_))));
    }
}
