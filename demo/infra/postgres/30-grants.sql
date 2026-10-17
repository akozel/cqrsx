-- Runs after the cqrsx schemas (10-event-store.sql, 20-outbox.sql); both live in `public`.
GRANT USAGE ON SCHEMA public TO encore_inspector;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO encore_inspector;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO encore_inspector;
