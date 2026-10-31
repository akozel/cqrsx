// Shared by the reports: loading results/*/result.json and folding the repeats of a run (`NAME-r1`
// … `NAME-rN`, matrix.sh REPEAT=N) into one group. A group is shown as its median run: every number
// of result.json is the median over the repeats, everything else comes from the first repeat. The
// spread of a figure is its min–max over the repeats.
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';

const TIMESTAMP = /^\d{8}-\d{6}-/;
const REPEAT = /-r\d+$/;

/** Every results/<run>/result.json, oldest first; an unreadable one becomes `{ meta, error }`. */
export function loadRuns(dir) {
  if (!existsSync(dir)) {
    return [];
  }
  return readdirSync(dir)
    .sort()
    .map((name) => join(dir, name, 'result.json'))
    .filter((path) => existsSync(path))
    .map((path) => {
      try {
        const run = JSON.parse(readFileSync(path, 'utf8'));
        run.meta ??= {};
        run.meta.run ??= basename(dirname(path));
        return run;
      } catch (error) {
        return { meta: { run: basename(dirname(path)) }, error: `result.json is not JSON: ${error.message}` };
      }
    });
}

/** `20261020-101500-perf-base-02a-announce-vus100-r2` → `perf-base-02a-announce-vus100-r2`. */
export const nameOf = (run) => run.meta.run.replace(TIMESTAMP, '') || run.test || 'run';

/** The name without the repeat suffix: what the repeats of one point have in common. */
export const repeatKey = (run) => nameOf(run).replace(REPEAT, '');

const numbers = (values) => values.filter((x) => typeof x === 'number' && Number.isFinite(x));

export function median(values) {
  const xs = numbers(values).sort((a, b) => a - b);
  if (xs.length === 0) {
    return null;
  }
  const mid = Math.floor(xs.length / 2);
  return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
}

/** `[min, max]` of the numbers among `values`, null when there are fewer than two. */
export function spread(values) {
  const xs = numbers(values);
  return xs.length < 2 ? null : [Math.min(...xs), Math.max(...xs)];
}

/** Numbers → their median, objects → key by key, anything else → the first value present. */
function medianOf(values) {
  const present = values.filter((x) => x !== undefined && x !== null);
  if (present.length === 0) {
    return values[0];
  }
  if (present.every((x) => typeof x === 'number')) {
    return median(present);
  }
  if (present.every((x) => typeof x === 'object' && !Array.isArray(x))) {
    const keys = [...new Set(present.flatMap((x) => Object.keys(x)))];
    return Object.fromEntries(keys.map((key) => [key, medianOf(present.map((x) => x[key]))]));
  }
  return present[0];
}

/**
 * Runs grouped by `repeatKey`, in the order of their first run: `{ key, runs, run }`, where `run` is
 * the median run (a single run stands for itself) and `runs` the repeats behind it.
 */
export function groupRepeats(runs) {
  const groups = new Map();
  for (const run of runs) {
    const key = repeatKey(run);
    if (!groups.has(key)) {
      groups.set(key, { key, runs: [] });
    }
    groups.get(key).runs.push(run);
  }
  return [...groups.values()].map((group) => {
    const run = group.runs.length === 1 ? group.runs[0] : medianOf(group.runs);
    run.meta = { ...run.meta, run: group.runs.length === 1 ? group.runs[0].meta.run : group.key, repeats: group.runs.length };
    return { ...group, run };
  });
}
