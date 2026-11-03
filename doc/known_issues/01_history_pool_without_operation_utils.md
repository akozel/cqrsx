# 01. The history pool is opened without the feature `operation-utils`

**What.** On PostgreSQL the framework opens three pools: main, projection and history
([`cqrsx-framework/src/postgres.rs`](../../cqrsx-framework/src/postgres.rs)). Only the operations of the feature
`operation-utils` read the history pool: `Cqrsx::history`, `Views::verify` and `Views::totals`. A framework built without
the feature still opens it, and the variable `DATABASE_HISTORY_POOL_SIZE` is read and described although it changes
nothing. The field `Registered.history` of [`cqrsx-framework/src/views.rs`](../../cqrsx-framework/src/views.rs) carries
`expect(dead_code)` in that build.

**Cost.** One connection while the framework is built, so that an unreachable database fails at once; the pool keeps no
connection (`min_connections = 0`) and closes it after 30 s idle (`HISTORY_IDLE_TIMEOUT`). One variable that does not
apply.

**Why it stays.** Opening the pool only with the feature puts the history repository of every view, the history pool
size in `PostgresSettings`, its variable and its line of the description, the variables table of the framework's README
with its test and the pool test of G-29 ("three pools, one connection each") under `cfg`: about ten conditions for one
connection of 30 seconds at the start.

**What would change it.** A deployment where one more connection at the start matters, or a fourth pool that only a
feature uses.

## Open: the value of the pool's three readers

The pool exists for three methods, and the value of each in the framework is not established. If none of them belongs in
the framework, the pool, its variable and this issue go with them.

- **`Cqrsx::history`**: every event of one aggregate, without a bound. Its one user is the demo's time machine
  (`concert/application/time_travel.rs`). The use case it serves in general is an audit trail of an entity; whether an
  application needs it from the framework rather than from a view of its own is open.
- **`Views::totals`**: `count(*)` and `count(DISTINCT aggregate_id)` over the whole journal of every aggregate type
  with a view, on each call. Its one user is the demo's Encore! (`operations/application/rebuild.rs`): `aggregates` is
  the denominator of the rebuild's progress (`keys_total`), `events` goes into the activity entry `rebuild.started`
  (`events_total`), and a failed count becomes zero. The most expensive form for a progress figure.
- **`Views::verify`**: every document of a view against the fold of the part of its history it applied (D-20). Its one
  user is the demo's `GET /api/projections/verify`. It checks the read side as a whole, the only reader of the three
  that is not one application's display.
