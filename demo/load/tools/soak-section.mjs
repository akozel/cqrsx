// The "over time" section of a soak run (run.sh SOAK=…, result.json `soak`) for the HTML reports: one line
// chart per figure over the run's slices. Both reports (tools/report-html.mjs and the one in
// perf/tools/) pass in their own chart helpers, so the section looks
// like the rest of the page it is on.
//
// soakSection(run, title, { lineChart, esc }) → HTML, or '' for a run that is not a soak run.

const SERVICES = [
  ['backend', 'back-end'],
  ['postgres', 'PostgreSQL'],
  ['mongo', 'MongoDB'],
  ['k6', 'k6'],
];
const TABLES = [
  ['events', 'events'],
  ['snapshots', 'snapshots'],
  ['transactional_outbox', 'outbox'],
];
const MIB = 1048576;

export function soakSection(run, title, { lineChart, esc }) {
  const soak = run.soak;
  if (!soak?.points?.length) {
    return '';
  }
  const points = soak.points;
  const axisLabel = 's since k6 started';
  const categories = points.map((p) => String(Math.round(p.t_s)));
  const series = (name, slot, pick) => ({ name, slot, values: points.map(pick) });
  const present = (pick) => points.some((p) => pick(p) != null);
  /** Per sample, the growth of a cumulative database counter since the sample before (null for the first). */
  const growth = (key, scale = 1) => (p, i) => (i === 0 || p.db[key] == null || points[i - 1].db[key] == null ? null : (p.db[key] - points[i - 1].db[key]) / scale);
  const charts = [];
  if (soak.slices?.length) {
    const sliceCategories = soak.slices.map((s) => String(Math.round(s.t_s)));
    charts.push(
      lineChart({
        title: 'Throughput over time',
        subtitle: `per ${soak.slice_s} s slice of the measured window; requested ${soak.rate} a second`,
        unit: '/s',
        axisLabel,
        categories: sliceCategories,
        series: [
          { name: 'accepted', slot: 1, values: soak.slices.map((s) => s.accepted_per_s) },
          { name: 'requests', slot: 2, values: soak.slices.map((s) => s.rps) },
        ],
      }),
    );
    charts.push(
      lineChart({
        title: 'Latency over time',
        subtitle: 'percentiles of the requests of each slice',
        unit: 'ms',
        axisLabel,
        categories: sliceCategories,
        series: [
          { name: 'p50', slot: 1, values: soak.slices.map((s) => s.p50) },
          { name: 'p99', slot: 3, values: soak.slices.map((s) => s.p99) },
        ],
      }),
    );
  }
  charts.push(
    lineChart({
      title: 'Open outbox tasks',
      subtitle: 'pending + processing at each slice; a line that keeps rising is a chain that does not keep up',
      unit: 'tasks',
      axisLabel,
      categories,
      series: [series('pending', 1, (p) => p.outbox_pending), series('processing', 2, (p) => p.outbox_processing)],
    }),
  );
  charts.push(
    lineChart({
      title: 'Outbox throughput',
      subtitle: 'tasks completed per second since the slice before',
      unit: 'tasks/s',
      axisLabel,
      categories,
      series: [series('completed', 1, (p) => p.completed_per_s)],
    }),
  );
  const projections = [...new Set(points.flatMap((p) => Object.keys(p.lag ?? {})))].sort();
  if (projections.length > 0) {
    charts.push(
      lineChart({
        title: 'Projection lag',
        subtitle: 'the most events a projection trailed the journal by since the slice before',
        unit: 'events',
        axisLabel,
        categories,
        series: projections.map((projection, i) => series(projection, i + 1, (p) => p.lag?.[projection] ?? null)),
      }),
    );
  }
  const services = SERVICES.filter(([key]) => present((p) => p.cpu_pct?.[key]));
  charts.push(
    lineChart({
      title: 'CPU over time',
      subtitle: 'average % since the slice before, 100 % = one core',
      unit: '%',
      axisLabel,
      categories,
      series: services.map(([key, name], i) => series(name, i + 1, (p) => p.cpu_pct?.[key] ?? null)),
    }),
  );
  charts.push(
    lineChart({
      title: 'Memory over time',
      subtitle: 'the most each container used since the slice before (cgroup: processes and page cache)',
      unit: 'MiB',
      axisLabel,
      categories,
      series: services.filter(([key]) => key !== 'k6').map(([key, name], i) => series(name, i + 1, (p) => p.mem_mib?.[key] ?? null)),
    }),
  );
  if (present((p) => p.db?.size_transactional_outbox)) {
    charts.push(
      lineChart({
        title: 'Table sizes',
        subtitle: 'pg_total_relation_size: table, indexes and TOAST',
        unit: 'MiB',
        axisLabel,
        categories,
        series: TABLES.map(([table, name], i) => series(name, i + 1, (p) => (p.db[`size_${table}`] == null ? null : p.db[`size_${table}`] / MIB))),
      }),
    );
    charts.push(
      lineChart({
        title: 'Dead rows',
        subtitle: 'n_dead_tup: row versions waiting for VACUUM; a drop is an autovacuum run',
        unit: 'rows',
        axisLabel,
        categories,
        series: TABLES.map(([table, name], i) => series(name, i + 1, (p) => p.db[`dead_tup_${table}`] ?? null)),
      }),
    );
    charts.push(
      lineChart({
        title: 'WAL over time',
        subtitle: 'MiB written and full-page images since the slice before; full-page images follow checkpoints',
        unit: 'per slice',
        axisLabel,
        categories,
        series: [series('WAL MiB', 1, growth('wal_bytes', MIB)), series('full-page images ÷ 1000', 2, growth('wal_fpi', 1000))],
      }),
    );
  }
  const v = soak.verdict;
  const verdict = `<p class="series-intro"><strong>${v.sustained ? 'Sustained' : 'Not sustained'}</strong> at ${esc(soak.rate)} a second: ${esc(v.accepted_per_s)} accepted a second, ${esc(v.dropped_iterations)} iterations dropped; open outbox tasks ${esc(v.open_at_60s)} at 60 s and ${esc(v.open_at_end)} at the end (${esc(v.open_growth_per_min)} a minute); the worst projection lag ${esc(v.lag_at_60s)} and ${esc(v.lag_at_end)} events.</p>`;
  return `<section class="series"><h2>${esc(title)} — over time</h2>${verdict}<div class="charts">${charts.join('')}</div></section>`;
}
