-- The event store before epic 03/02: no transaction id on events and no feed index. Mounted after the
-- stand's schema by compose.before-feed.yaml for the "before" runs of the hot-key measurement of 03/03; the
-- demo back-end does not read the column, so the write path is the only difference.
DROP INDEX IF EXISTS events_feed_idx;
ALTER TABLE events DROP COLUMN IF EXISTS tx;
