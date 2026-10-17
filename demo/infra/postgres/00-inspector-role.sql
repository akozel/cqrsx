-- Read-only role for the sidecar. Runs before the schemas (alphabetical init order).
CREATE ROLE encore_inspector LOGIN PASSWORD 'inspector';
-- Second line of defence: even a stray write statement fails in a read-only transaction.
ALTER ROLE encore_inspector SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE encore TO encore_inspector;
