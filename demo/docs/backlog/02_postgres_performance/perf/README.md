# Epic 02 measurements

The measurement tools of backlog epic 02 ([`../README.md`](../README.md)), written by ticket
[01](../01_measurement_baseline.md). They run the k6 stand of `demo/load` ([`README.md`](../../../../load/README.md))
unchanged and add what the epic judges every change by: what PostgreSQL does per command, CPU time per
command, and repeats with medians. They plug into a run through the stand's extension points,
`LOAD_COMPOSE_EXTRA`, `LOAD_SERVICES_EXTRA` and `LOAD_HOOKS`; results land in `demo/load/results/` next to
the stand's own runs.

## Running

From this directory (`demo/docs/backlog/02_postgres_performance/perf`):

```bash
./run.sh announce-storm PREFIX=perf-02- NAME=02a-announce-vus100 VUS=100 POOL=20   # one run, demo/load/run.sh parameters
./matrix.sh PREFIX=perf-02- REPEAT=3 POOL=20 ONLY=02a-announce-vus100,03a-drain-w8  # the points of a ticket
./report.sh perf-02-                                    # table + demo/load/results/perf-report.html
node tools/compare.mjs ../../../../load/results perf-base- perf-02- --md            # before → after
node tools/compare.mjs ../../../../load/results e04-base- e04-03- e04-04- --md     # stages side by side
```

`run.sh` takes the parameters of `demo/load/run.sh` (`--check`, `--help`, `LOAD_HOST_DIR`, …) and five of
its own, which also go on to the stand's run.sh so that `result.json` records them (`meta.generator`,
`meta.profile`, `meta.backend_image`, `meta.nodes`, `meta.balance`):

| Parameter | Values | What it does |
|---|---|---|
| `GENERATOR` | `auto` (default), `wrk`, `k6` | `auto` takes wrk when `generator/scenarios/<test>.lua` exists (today: `announce-storm`), k6 otherwise. wrk costs ~21 ms of CPU per 1000 requests against k6's ~245, so it is never the ceiling; k6 is fine where the stand is slower than ~11k requests/s (hot key, bookings, drain). |
| `PROFILE` | `full` (default), `db`, `app` | The CPU budget. `full` is the stand's (back-end 4, PostgreSQL 4, MongoDB 3, k6 4): with wrk the announce ceiling is MongoDB. `db` gives PostgreSQL 2 cores, so the database is the bottleneck and a saving there shows in req/s; `app` gives the back-end 2 cores, the same for the application; under a CPU quota the multi-threaded back-end hits the quota in bursts and req/s swings by tens of percent, so trust only the per-command figures there unless the cores are pinned (cpuset). |
| `BACKEND_IMAGE` | an image tag | Runs an already built back-end image (`compose.image.yaml`) instead of building the tree, e.g. the code before a ticket; `BUILD=1` is ignored then, it would overwrite that image. With `NODES=2` both nodes run it. |
| `NODES` | `1` (default), `2` | Back-end nodes ([ticket 17](../17_two_backend_nodes.md)). Two nodes split the profile's back-end CPU budget (`app`: 1 core each) and the outbox workers (`WORKERS` is then the stand's total, default 2, even) between them, so one node and two run on the same back-end CPU and drain the outbox with as many workers. Without the split each node would poll and drain with the full count, and the burst would carry twice the queue work (seen on the first 02b runs: +27 % PostgreSQL CPU per command). The second node (`compose.node2.yaml`) has the image and environment of the first and starts once the first is healthy. |
| `BALANCE` | `none` (default), `round_robin`, `key` | How requests reach the nodes. `none`: straight to the one node, the stand as it always was. `round_robin`: through nginx (`compose.balancer.yaml`), the nodes in turn. `key`: through nginx, every request for one aggregate (`/api/concerts/{id}/…`, `/api/fans/{id}/…`) to the same node by consistent hashing. `NODES=2` needs a balancer; `NODES=1 BALANCE=round_robin` is one node behind nginx, the baseline for the balancer's own cost. With a balancer the stand runs under a CPU quota (`PIN=quota`, nginx gets 1 core, `PERF_CPUS_BALANCER`); on a machine that pins cores, take the single-node runs of such a comparison with `PIN=quota` too. Every response carries `X-Upstream`, the node that answered. |

The stand's own parameters pass through unchanged. Tickets 08a–08d added the back-end's knobs to
`demo/load/run.sh`: `LOCK_MAX_WAIT_MS=…` and `LOCK_MAX_WAITERS=…` (`ENCORE_COMMAND_LOCK_*`),
`PROJECTION_BUFFER=…` and `PROJECTION_MAX_BATCH=…` (`ENCORE_PROJECTION_*`); empty = the back-end's default.
Runs before 08c also had `ORDERING=per_aggregate|concurrent` (`meta.command_ordering` in `result.json`), runs
before 08d `DELIVERY=ordered|inline` (`meta.projection_delivery`). They
live in the stand's `compose.yaml`, not in an overlay here, so the second node inherits them through `extends`.
`tools/compare.mjs` shows accepted commands per second and the 409 and 503 shares next to req/s, how far the
projections trailed the journal (`lag p50/p99 events`, the status sampled once a second, and `caught up s`
after the burst) and, for runs with `VERIFY=1`, whether the read models matched the journal afterwards
(`verify ok`: 1 or 0, with no document behind). It takes two batches or more: a column per batch, `Δ` and
`≠` from the first to the last, so the stages of an epic stand side by side.

The epic README lists the point, profile and generator per ticket ("Что мерить") and how to build the
"before" image.

`matrix.sh` holds the points of `demo/load/matrix.sh` the epic measures (01, 02a, 02b, 03a) and takes
two parameters of its own; every other `KEY=VALUE` goes to each run:

- `REPEAT=N` runs the matrix N times, one whole pass after another, and suffixes every `NAME` with
  `-r1` … `-rN`. Passes rather than N runs in a row spread the repeats of a point over time, so a
  drift of the machine widens the spread instead of shifting one point. A single run moves by about
  ±4 % between identical runs (and one run in three by up to 24 % under a CPU quota), so a change
  smaller than ~5 % needs repeats to be told from noise.
- `ONLY=…` keeps only the listed lines: a series tag (01, 02a, 02b, 03a) or a whole `NAME`, e.g.
  `ONLY=02a-announce-vus100,03a-drain-w8`. The whole matrix with `REPEAT=3` takes ~28 minutes, 11 of
  them the outbox drain with two workers.

The first run that actually starts rebuilds the back-end image; `BUILD=0` skips that.

## What a run adds

| File | Content |
|---|---|
| `counters.tsv` | cumulative counters at `before` / `after` the burst (and `drained`): cgroup CPU time, CPU-quota throttling and memory (`anon`, page cache) of back-end, PostgreSQL and MongoDB (and of `backend-2` and `balancer` when they run), PostgreSQL network packets, commits, rollbacks, WAL position, `pg_stat_wal` (records, full-page images, bytes), completed tasks, MongoDB's operation counters (`opcounters`) |
| `backend-2.log`, `balancer.log` | the logs of the second node and of nginx, when they run (the stand's `LOAD_SERVICES_EXTRA`) |
| `pg_statements.tsv` | `pg_stat_statements` over the burst window: one line per normalized statement with calls, total and mean time, rows, buffer hits and reads, read time, WAL records, full-page images and bytes; most expensive first |
| `pg_statements.drain.tsv` | `DRAIN=1` only: the statements of the drain, from the end of the burst to the end of the drain |
| `wait_events.tsv` | while k6 runs, every 100 ms: the busy client sessions of PostgreSQL by state and wait event (`epoch_ms  state  wait  sessions`, a `sample` line per sample) |
| `soak.tsv` | `SOAK=…` only: the stand's slices plus the database's growth from `hook_slice` |
| `result.json` → `db`, `efficiency`, `queue`, `waits`, `drain.per_task`, `meta.git_describe` | the figures below |

| Piece | What it does |
|---|---|
| `compose.perf.yaml` | overlay: PostgreSQL with `shared_preload_libraries=pg_stat_statements`, `track_io_timing=on`, and `postgres/00-pg-stat-statements.sql`; the `mongo-probe` service (`mongosh` in a container of its own, started with every run), so that reading MongoDB's counters costs MongoDB no CPU |
| `compose.image.yaml` | overlay for `BACKEND_IMAGE`: the back-end service runs that image |
| `compose.before-feed.yaml` | overlay for the "before" runs of epic 03/03: `postgres/15-before-feed.sql` drops `events.tx` and `events_feed_idx` after the stand's schema (the back-end does not read them), so the write path is measured with and without the feed position |
| `compose.balancer.yaml` | overlay for `BALANCE`: the `balancer` service (nginx) in front of the node(s), the generator's `BASE_URL` pointed at it |
| `compose.node2.yaml` | overlay for `NODES=2`: the `backend-2` service, `extends` of the stand's `backend` |
| `nginx/` | `nginx.conf` (one worker, no access log, keep-alive both ways, `$aggregate_key` from the path, `X-Upstream`) and the upstreams `upstream-{single,round_robin,key}.conf` |
| `generator/` | the wrk image that stands in for k6 (`LOAD_K6_IMAGE`): `gen.sh` takes k6's arguments and environment, runs wrk for the warm-up and the measured window with `scenarios/<test>.lua`, and writes `result.json` in the shape of `demo/load/k6/lib/summary.js` (no projection-warning count: wrk does not read the bodies) |
| `hooks.sh` | `hook_before_k6` resets the statement statistics, snapshots the counters and starts the wait sampler; `hook_after_k6` stops it, snapshots and dumps the statements; `hook_after_drain` waits for the projections to catch up (at most `PERF_CATCH_UP_S`, 30 s), then snapshots and dumps; `hook_slice` adds the database's growth to a soak run's slice; `hook_after_collect` runs `tools/collect.mjs` |
| `tools/collect.mjs` | turns the counters into `db`, `efficiency` (with `chain`), `queue`, `waits`, `drain.per_task` |
| `tools/lib/runs.mjs` | loading results and folding the repeats of a point into its median |
| `tools/report.mjs` | terminal table (or a batch as Markdown, `--md`): one line per point, median, `n`, min–max |
| `tools/report-html.mjs` | the stand's HTML report extended with these figures: medians with min–max whiskers, "CPU per 1000 accepted commands", "Database work per command" and "per drained task" |
| `tools/compare.mjs` | two batches point by point: medians with min–max, relative change, `≠` when the two ranges do not overlap |

## Work per command

`efficiency` puts the work of the burst window (from just before k6 starts to just after it ends,
`counters.tsv` points `before` and `after`) over every command k6 got accepted in it, setup and warm-up
included (`accepted_total_all_phases`). These figures move when a change saves work even while req/s
stays at a ceiling somewhere else, e.g. PostgreSQL work saved while the back-end CPU is the limit.

| Field | Meaning |
|---|---|
| `cpu_ms_per_1k_accepted.{backend,postgres,mongo}` | CPU time per 1000 accepted commands from the container's cgroup (`cpu.stat`), exact. `backend` is every back-end node together, so one node and two compare as they are |
| `cpu_ms_per_1k_accepted.{backend_node1,backend_node2,balancer}` | with `NODES=2`: each node on its own (how evenly the balancer spread the work); with a balancer: nginx |
| `cpu_ms_per_1k_accepted.k6` | the same for k6 from the `stats` samples (its container is gone before its cgroup could be read), approximate |
| `cores.{backend,postgres,mongo,k6}` | the cores each container used on average over the window: its CPU time ÷ the window; against the profile's budget (`meta.cpu_budget_cores`) it says which container was the limit |
| `statements_per_command` | SQL statements (`pg_stat_statements` calls, `BEGIN`/`COMMIT` included) per accepted command |
| `commits_per_command` | `pg_stat_database.xact_commit` per accepted command |
| `packets_per_command` | network packets in and out of the PostgreSQL container per accepted command |
| `rollbacks_per_command` | `pg_stat_database.xact_rollback` per accepted command: write conflicts and errors |
| `wal_bytes_per_command` | WAL written per accepted command, from the WAL insert position |
| `wal_fpi_per_command` | full-page images per accepted command (`pg_stat_wal.wal_fpi`): the first change of a page after a checkpoint writes the whole 8 kB page |
| `mongo_ops_per_command.{insert,query,update,delete,getmore,command,total}` | MongoDB operations per accepted command (`opcounters`): `query` is a `find`, `update` an `update_one`; `command` counts everything else, the lease and checkpoint writes of the processors among them |
| `tasks_completed_per_command` | outbox tasks completed inside the window per accepted command |

`db` holds the raw differences behind them, with `cpu_throttled_ms` (the time a CPU quota held a container
back: under `PIN=quota` a sign that it hit its budget in bursts) and `memory_after` (`anon_bytes`, the processes'
own memory, and `file_bytes`, the page cache, of every container at `after`; the peaks of the whole run are the
stand's `memory_peak_bytes`).

**Through the chain** (`efficiency.chain`, `DRAIN=1` only). An accepted booking starts an asynchronous chain:
task `encore_record_ticket` → `Fan` command → task `encore_send_e_ticket` → mail. How much of it runs inside the
window depends on the code under test, so the window's figures put a varying part of the chain on the command:
a cheaper task lets more of the chain into the window and the command looks no cheaper. `chain` measures from
`before` to `drained` — the end of the drain, once the projections have caught up as well — and divides by the
same accepted commands, so every variant is charged the same work: all of the chain of all of its commands.

| Field | Meaning |
|---|---|
| `cpu_ms_per_1k_accepted.*` | CPU time per 1000 accepted commands, burst and drain together |
| `statements_per_command`, `commits_per_command`, `packets_per_command`, `wal_bytes_per_command`, `wal_fpi_per_command`, `mongo_ops_per_command` | the same as in the window, through the chain |
| `tasks_per_command` | outbox tasks completed per accepted command (2 for a booking, 1 for an announce: none) |
| `window_share` | the part of those tasks completed inside the window |

The window's figures stay: they are the cost of taking a command; `chain` is the cost of everything it causes.

**Queue statements** (`queue.window`, and `queue.drain` with `DRAIN=1`). Claim (`WITH ready AS … FOR UPDATE SKIP
LOCKED`) and settle (fenced by the lease token) are picked out of the statement dumps by their text: `calls`,
`mean_ms`, `blocks_per_call` (shared buffers hit and read), `wal_fpi_per_call`, `share_of_sql_time_pct` of all
SQL execution time in the dump, and for the window `settle.per_command`.

**What the sessions waited for** (`waits`). `samples` (one every 100 ms), `sessions_avg` (busy client sessions
on average: active or inside a transaction), `active_avg`, and `top`: the ten most common states and wait
events with their share of the session samples. An active session without a wait event is `CPU`;
`LWLock:WALWrite` and `IO:WalSync` wait for the WAL to be written and flushed at commit; `Lock:transactionid`
waits for another transaction's row; `idle in transaction Client:ClientRead` is a transaction left open
between two statements while the client works. The sampler's own session and the hooks' are left out.

- The database works for the whole stand: in 02b and 03a the outbox workers run during the burst, so
  their statements are part of the per-command figures. Compare like with like.
- A connection ping (`Sync` → `ReadyForQuery`) is not a statement: it shows only in packets and CPU.
  A packet is a rough unit (TCP ACKs are packets too, small messages may share one), so read
  `packets_per_command` as a difference between batches, not as an absolute count of round trips.
- `pg_stat_statements` counts only statements that succeeded; a write that lost a race shows in
  `rollbacks_per_command`, not in the statements.
- PostgreSQL publishes the commit and rollback counters per backend at most once a second, so the last
  second of a burst may be missing from them (−1.6 % commits on a check run). CPU, packets, statements
  and WAL are exact.
- `pg_stat_statements` costs a little CPU itself. Compare only with runs measured the same way, i.e.
  from the `perf-base-` batch on, never with the stand's plain runs. The same holds for the wait sampler
  (one session, a light query ten times a second) and the `mongo-probe` container (from epic 04 on, batches
  `e04-…`): both are in every run measured this way.
- The drain is measured to the end, not in a window: its rate changes as it goes (the
  `encore_record_ticket` tasks first, the `encore_send_e_ticket` tasks they create later), and the first
  30 s show a different, noisier figure than the whole drain.
