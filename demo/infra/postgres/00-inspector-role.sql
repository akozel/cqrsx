-- Roles of the demo sidecar. Runs before the schemas (alphabetical init order).
-- 1. Read-only role for the inspector (every poll of the X-ray).
CREATE ROLE encore_inspector LOGIN PASSWORD 'inspector';
-- Second line of defence: even a stray write statement fails in a read-only transaction.
ALTER ROLE encore_inspector SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE encore TO encore_inspector;
-- 2. DEMO ONLY: the role behind "reset the demo" (POST /inspect/reset). It may only empty the
--    three tables (TRUNCATE, granted in 30-grants.sql); nothing else, and it is never used for reads.
CREATE ROLE encore_demo_admin LOGIN PASSWORD 'demo-admin';
GRANT CONNECT ON DATABASE encore TO encore_demo_admin;
