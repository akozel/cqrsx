-- Backlog epic 02 measurements: per-statement statistics on the load stand (compose.perf.yaml).
-- The server loads the library (shared_preload_libraries); the cqrsx schema is untouched.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
