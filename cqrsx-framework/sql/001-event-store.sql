CREATE TABLE IF NOT EXISTS events
(
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    event_type     text                         NOT NULL,
    event_version  text                         NOT NULL,
    payload        json                         NOT NULL,
    metadata       json                         NOT NULL,
    tx             xid8                         NOT NULL DEFAULT pg_current_xact_id(),
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);

-- The journal of one aggregate type in commit order: feed pages, the head and the keys page by page.
CREATE INDEX IF NOT EXISTS events_feed_idx ON events (aggregate_type, tx, aggregate_id, sequence);

CREATE TABLE IF NOT EXISTS snapshots
(
    aggregate_type   text                                 NOT NULL,
    aggregate_id     text                                 NOT NULL,
    last_sequence    bigint CHECK (last_sequence >= 0)    NOT NULL,
    revision         bigint CHECK (revision >= 0)         NOT NULL,
    payload          json                                 NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, last_sequence)
);
