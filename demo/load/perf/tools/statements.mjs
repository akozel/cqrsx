#!/usr/bin/env node
// The SQL statements of a batch of runs as a set: the statement texts of every run's pg_statements.tsv and
// pg_statements.drain.tsv (hooks.sh dumps pg_stat_statements there), normalized, per point. With two batches it prints,
// point by point, the statements only one of them has: a change that adds, drops or rewrites a statement shows here
// even when the counts per command stay the same (backlog epic 05: the set must not change).
//
// A point is the name of a run without its timestamp, its batch prefix and its repeat suffix (`-rN`), as in
// compare.mjs; its set is the union over its repeats. A run belongs to the batch with the longest prefix it starts
// with, so a whole run name works as a prefix too: the set of that one run.
//
// Normalization: whitespace collapsed; parameters (`$1`) and literals (`'pending'`, `1`, `0.5`, `NULL`, `TRUE`) become `?`.
// pg_stat_statements keeps one text per statement (queryid), but in one of two forms: normalized (`SELECT $4 FROM …`)
// when the statement was parsed after the statistics were reset, as sent (`SELECT 1 FROM …`) when the connection had
// prepared it before; which form a run gets varies, so raw texts differ between identical runs. The price: a changed
// literal value is not seen, as queryid does not see it either.
//
// Usage: node statements.mjs <results-dir> <prefix>                # every point's set, with counts
//        node statements.mjs <results-dir> <prefix-a> <prefix-b>   # per point: statements only in a, only in b
//   e.g. node statements.mjs ../results e05-02-                 (from perf/)
//        node statements.mjs ../results e05-base- e05-
// Exit code: 0 — listed, or the same statements at every point; 1 — a statement or a point only one batch has;
// 2 — wrong usage, or a batch without a single run with statement dumps.
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { loadRuns, nameOf, repeatKey } from './lib/runs.mjs';

const DUMPS = ['pg_statements.tsv', 'pg_statements.drain.tsv'];

/** A statement text as the comparison sees it: parameters and literals are `?`. */
function normalize(text) {
  return text
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/'(?:[^']|'')*'/g, '?')
    .replace(/\$\d+/g, '?')
    .replace(/(?<![\w$.])\d+(?:\.\d+)?(?![\w.])/g, '?')
    .replace(/\b(?:NULL|TRUE|FALSE)\b/gi, '?');
}

/** The raw statement texts of one run's dumps, or null when it has none (a run measured without hooks.sh). */
function textsOf(runDir) {
  const texts = new Set();
  let dumps = 0;
  for (const file of DUMPS) {
    const path = join(runDir, file);
    if (!existsSync(path)) {
      continue;
    }
    dumps += 1;
    const [header = '', ...lines] = readFileSync(path, 'utf8').split('\n').filter((line) => line.trim() !== '');
    const columns = header.split('\t');
    const query = columns.indexOf('query') >= 0 ? columns.indexOf('query') : columns.length - 1;
    for (const line of lines) {
      texts.add(line.split('\t').slice(query).join('\t'));
    }
  }
  return dumps ? texts : null;
}

const args = process.argv.slice(2);
const [dir, ...prefixes] = args;
if (!dir || prefixes.length < 1 || prefixes.length > 2) {
  console.error('usage: statements.mjs <results-dir> <prefix> [<prefix-b>]');
  process.exit(2);
}
const all = loadRuns(dir);
const longest = [...new Set(prefixes)].sort((a, b) => b.length - a.length);

/** prefix → Map(point → { runs, raw, statements }), the points sorted by name. */
function batch(prefix) {
  const points = new Map();
  let skipped = 0;
  for (const run of all) {
    if (longest.find((candidate) => nameOf(run).startsWith(candidate)) !== prefix) {
      continue;
    }
    const raw = textsOf(join(dir, run.meta.run));
    if (!raw) {
      skipped += 1;
      continue;
    }
    const point = repeatKey(run).slice(prefix.length);
    if (!points.has(point)) {
      points.set(point, { runs: 0, raw: new Set(), statements: new Set() });
    }
    const entry = points.get(point);
    entry.runs += 1;
    for (const text of raw) {
      entry.raw.add(text);
      entry.statements.add(normalize(text));
    }
  }
  if (skipped) {
    console.error(`${prefix || '(no prefix)'}: ${skipped} run(s) without statement dumps left out`);
  }
  if (points.size === 0) {
    console.error(`${prefix || '(no prefix)'}: no run with statement dumps in ${dir}`);
    process.exit(2);
  }
  return new Map([...points.entries()].sort(([a], [b]) => a.localeCompare(b)));
}

const sorted = (set) => [...set].sort();
/** A point's name; a prefix that is the whole name of the runs leaves none. */
const label = (point, prefix) => point || `${prefix} (the whole name)`;

if (prefixes.length === 1) {
  for (const [point, { runs, raw, statements }] of batch(prefixes[0])) {
    console.log(`## ${label(point, prefixes[0])}: ${runs} run(s), ${raw.size} distinct texts, ${statements.size} after normalization`);
    for (const statement of sorted(statements)) {
      console.log(statement);
    }
    console.log();
  }
  process.exit(0);
}

const [a, b] = prefixes.map(batch);
const [nameA, nameB] = prefixes.map((prefix) => prefix || '(no prefix)');
let differences = 0;
for (const point of [...new Set([...a.keys(), ...b.keys()])].sort()) {
  const [left, right] = [a.get(point), b.get(point)];
  if (!left || !right) {
    differences += 1;
    console.log(`? ${label(point, left ? nameA : nameB)}: only in ${left ? nameA : nameB}`);
    continue;
  }
  const onlyLeft = sorted(left.statements).filter((s) => !right.statements.has(s));
  const onlyRight = sorted(right.statements).filter((s) => !left.statements.has(s));
  if (onlyLeft.length === 0 && onlyRight.length === 0) {
    console.log(`= ${label(point, `${nameA} / ${nameB}`)}: the same ${left.statements.size} statements (${left.runs} and ${right.runs} run(s))`);
    continue;
  }
  differences += 1;
  console.log(`≠ ${label(point, `${nameA} / ${nameB}`)}: ${onlyLeft.length} only in ${nameA}, ${onlyRight.length} only in ${nameB}`);
  for (const statement of onlyLeft) {
    console.log(`  - ${statement}`);
  }
  for (const statement of onlyRight) {
    console.log(`  + ${statement}`);
  }
}
process.exit(differences ? 1 : 0);
