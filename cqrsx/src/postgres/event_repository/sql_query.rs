// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
pub(crate) struct SqlQueryFactory {
    select_events: String,
    select_events_after: String,
    insert_event: String,
    all_events: String,
    count_events: String,
    count_after: String,
    feed_page: String,
    feed_tail: String,
    head: String,
    aggregate_ids_after: String,
    insert_snapshot: String,
    update_snapshot: String,
    select_snapshot: String,
}

impl SqlQueryFactory {
    pub fn new(event_table: &str, snapshot_table: &str) -> Self {
        Self {
            select_events: format!(
                "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM {event_table}
  WHERE aggregate_type = $1 AND aggregate_id = $2
  ORDER BY sequence"
            ),
            // The watermark is a parameter, not a literal: one text for every watermark, so SQLx's
            // per-connection statement cache prepares it once instead of once per new snapshot.
            select_events_after: format!(
                "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM {event_table}
  WHERE aggregate_type = $1 AND aggregate_id = $2 AND sequence > $3
  ORDER BY sequence"
            ),
            insert_event: format!(
                "
INSERT INTO {event_table} (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (aggregate_type, aggregate_id, sequence) DO NOTHING"
            ),
            all_events: format!(
                "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM {event_table}
  WHERE aggregate_type = $1
  ORDER BY sequence"
            ),
            count_events: format!(
                "
SELECT count(*) AS events, count(DISTINCT aggregate_id) AS aggregates
  FROM {event_table}
  WHERE aggregate_type = $1"
            ),
            // The events of one type above a position, at most $3 of them: the LIMIT stops the index scan,
            // so a reader far behind is counted in bounded time.
            count_after: format!(
                "
SELECT count(*) AS events
  FROM (SELECT 1 FROM {event_table} WHERE aggregate_type = $1 AND tx > $2::text::xid8 LIMIT $3) AS capped"
            ),
            // sqlx has no xid8: the position travels as decimal text in both directions. The horizon
            // `pg_snapshot_xmin(pg_current_snapshot())` belongs to this statement's snapshot: every
            // transaction below it has finished, so the page has no holes, and the LIMIT keeps the
            // planner on the feed index whatever it estimates for the range.
            feed_page: format!(
                "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata, tx::text AS position
  FROM {event_table}
  WHERE aggregate_type = $1 AND tx > $2::text::xid8 AND tx < pg_snapshot_xmin(pg_current_snapshot())
  ORDER BY tx, aggregate_id, sequence
  LIMIT $3"
            ),
            // The rest of the transaction a full page ended in; it finished before the page was read.
            feed_tail: format!(
                "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata, tx::text AS position
  FROM {event_table}
  WHERE aggregate_type = $1 AND tx = $2::text::xid8 AND (aggregate_id, sequence) > ($3, $4)
  ORDER BY aggregate_id, sequence"
            ),
            head: format!(
                "
SELECT tx::text AS position
  FROM {event_table}
  WHERE aggregate_type = $1
  ORDER BY tx DESC
  LIMIT 1"
            ),
            aggregate_ids_after: format!(
                "
SELECT DISTINCT aggregate_id
  FROM {event_table}
  WHERE aggregate_type = $1 AND aggregate_id > $2
  ORDER BY aggregate_id
  LIMIT $3"
            ),
            insert_snapshot: format!(
                "
INSERT INTO {snapshot_table} (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload)
VALUES ($1, $2, $3, $4, $5) ON CONFLICT (aggregate_type, aggregate_id, last_sequence) DO NOTHING"
            ),
            update_snapshot: format!(
                "
UPDATE {snapshot_table}
  SET last_sequence= $3 , payload= $6, current_snapshot= $4
  WHERE aggregate_type= $1 AND aggregate_id= $2 AND current_snapshot= $5"
            ),
            select_snapshot: format!(
                "
SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload
  FROM {snapshot_table}
  WHERE aggregate_type = $1 AND aggregate_id = $2"
            ),
        }
    }
    pub fn select_events(&self) -> &str {
        &self.select_events
    }
    pub fn insert_event(&self) -> &str {
        &self.insert_event
    }
    pub fn insert_snapshot(&self) -> &str {
        &self.insert_snapshot
    }
    pub fn update_snapshot(&self) -> &str {
        &self.update_snapshot
    }
    pub fn select_snapshot(&self) -> &str {
        &self.select_snapshot
    }
    pub fn all_events(&self) -> &str {
        &self.all_events
    }
    pub fn count_events(&self) -> &str {
        &self.count_events
    }
    pub fn count_after(&self) -> &str {
        &self.count_after
    }
    pub fn select_events_after(&self) -> &str {
        &self.select_events_after
    }
    pub fn feed_page(&self) -> &str {
        &self.feed_page
    }
    pub fn feed_tail(&self) -> &str {
        &self.feed_tail
    }
    pub fn head(&self) -> &str {
        &self.head
    }
    pub fn aggregate_ids_after(&self) -> &str {
        &self.aggregate_ids_after
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_queries() {
        let query_factory = SqlQueryFactory::new("my_events", "my_snapshots");
        assert_eq!(
            query_factory.select_events(),
            "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM my_events
  WHERE aggregate_type = $1 AND aggregate_id = $2
  ORDER BY sequence"
        );
        assert_eq!(
            query_factory.insert_event(),
            "
INSERT INTO my_events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (aggregate_type, aggregate_id, sequence) DO NOTHING"
        );
        assert_eq!(
            query_factory.all_events(),
            "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM my_events
  WHERE aggregate_type = $1
  ORDER BY sequence"
        );
        assert_eq!(
            query_factory.insert_snapshot(),
            "
INSERT INTO my_snapshots (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload)
VALUES ($1, $2, $3, $4, $5) ON CONFLICT (aggregate_type, aggregate_id, last_sequence) DO NOTHING"
        );
        assert_eq!(
            query_factory.update_snapshot(),
            "
UPDATE my_snapshots
  SET last_sequence= $3 , payload= $6, current_snapshot= $4
  WHERE aggregate_type= $1 AND aggregate_id= $2 AND current_snapshot= $5"
        );
        assert_eq!(
            query_factory.select_snapshot(),
            "
SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload
  FROM my_snapshots
  WHERE aggregate_type = $1 AND aggregate_id = $2"
        );
        assert_eq!(
            query_factory.select_events_after(),
            "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM my_events
  WHERE aggregate_type = $1 AND aggregate_id = $2 AND sequence > $3
  ORDER BY sequence"
        );
        assert_eq!(
            query_factory.feed_page(),
            "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata, tx::text AS position
  FROM my_events
  WHERE aggregate_type = $1 AND tx > $2::text::xid8 AND tx < pg_snapshot_xmin(pg_current_snapshot())
  ORDER BY tx, aggregate_id, sequence
  LIMIT $3"
        );
        assert_eq!(
            query_factory.feed_tail(),
            "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata, tx::text AS position
  FROM my_events
  WHERE aggregate_type = $1 AND tx = $2::text::xid8 AND (aggregate_id, sequence) > ($3, $4)
  ORDER BY aggregate_id, sequence"
        );
        assert_eq!(
            query_factory.head(),
            "
SELECT tx::text AS position
  FROM my_events
  WHERE aggregate_type = $1
  ORDER BY tx DESC
  LIMIT 1"
        );
        assert_eq!(
            query_factory.aggregate_ids_after(),
            "
SELECT DISTINCT aggregate_id
  FROM my_events
  WHERE aggregate_type = $1 AND aggregate_id > $2
  ORDER BY aggregate_id
  LIMIT $3"
        );
    }
}
