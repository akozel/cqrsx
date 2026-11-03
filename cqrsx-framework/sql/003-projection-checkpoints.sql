CREATE TABLE IF NOT EXISTS projection_checkpoints
(
    projection        text        NOT NULL CHECK (projection <> ''),
    aggregate_type    text        NOT NULL CHECK (aggregate_type <> ''),
    position          xid8        NOT NULL DEFAULT '0',                          -- everything up to it is applied
    delivered         bigint      NOT NULL DEFAULT 0 CHECK (delivered >= 0),     -- events delivered live
    rebuild_requested boolean     NOT NULL DEFAULT false,
    rebuild_pace_ms   integer     NOT NULL DEFAULT 0 CHECK (rebuild_pace_ms >= 0),
    rebuild_cursor    text,                                                      -- NULL: no rebuild; '': begun; else the last recorded key
    rebuilt           bigint      NOT NULL DEFAULT 0 CHECK (rebuilt >= 0),       -- keys rebuilt by the running or last rebuild
    owner             text,
    lease_token       uuid,
    lease_until       timestamptz,
    failures          integer     NOT NULL DEFAULT 0 CHECK (failures >= 0),
    last_error        text,
    updated_at        timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (projection, aggregate_type),
    CHECK ((owner IS NULL) = (lease_token IS NULL) AND (owner IS NULL) = (lease_until IS NULL)),
    CHECK (NOT rebuild_requested OR rebuild_cursor IS NULL)
);

CREATE TABLE IF NOT EXISTS projection_rebuild_keys
(
    projection       text        NOT NULL,
    aggregate_type   text        NOT NULL,
    aggregate_id     text        NOT NULL,
    requested_at     timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (projection, aggregate_type, aggregate_id)
);
