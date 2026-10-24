CREATE TABLE IF NOT EXISTS projection_checkpoints
(
    projection_name   text        NOT NULL CHECK (projection_name <> ''),
    aggregate_type    text        NOT NULL CHECK (aggregate_type <> ''),
    position          xid8        NOT NULL DEFAULT '0',                          -- everything up to it is applied
    applied           bigint      NOT NULL DEFAULT 0 CHECK (applied >= 0),       -- events delivered live
    rebuild_requested boolean     NOT NULL DEFAULT false,
    rebuild_pace_ms   integer     NOT NULL DEFAULT 0 CHECK (rebuild_pace_ms >= 0),
    rebuild_cursor    text,                                                      -- NULL: no rebuild; '': begun; else the last recorded key
    rebuilt           bigint      NOT NULL DEFAULT 0 CHECK (rebuilt >= 0),       -- keys rebuilt by the running or last rebuild
    owner             text,
    lock_token        uuid,
    locked_until      timestamptz,
    failures          integer     NOT NULL DEFAULT 0 CHECK (failures >= 0),
    last_error        text,
    updated_at        timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (projection_name, aggregate_type),
    CHECK ((owner IS NULL) = (lock_token IS NULL) AND (owner IS NULL) = (locked_until IS NULL)),
    CHECK (NOT rebuild_requested OR rebuild_cursor IS NULL)
);

CREATE TABLE IF NOT EXISTS projection_rebuild_keys
(
    projection_name  text        NOT NULL,
    aggregate_type   text        NOT NULL,
    aggregate_id     text        NOT NULL,
    requested_at     timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (projection_name, aggregate_type, aggregate_id)
);
