---
name: rust-style-review
description: Check Rust code of this repository (cqrsx, cqrsx-engine, cqrsx-postgres, cqrsx-mongodb, cqrsx-macros, cqrsx-fixtures, demo) against the project's code guidelines in .claude/rules/rust-style.md, report verified findings by rule and, when asked, fix them and enable the matching lints. Use when the user asks for a guidelines, style or code-quality review or audit of the Rust code.
---

# Rust style review

The rules are in `.claude/rules/rust-style.md`; read them first and cite their numbers in every finding.
Argument: a path, a crate or `diff` (changed lines and their callers only); default `cqrsx`. The demo is outside
the Cargo workspace: run its commands from `demo/`.

## 1. Lints

Run clippy with the guideline lints that are not yet in `[lints]` of the crate's `Cargo.toml` (drop those that
are). `--lib` keeps test code out:

```bash
cargo clippy --locked -p cqrsx --lib --all-features --message-format=short -- -W clippy::wildcard_enum_match_arm
```

Already in `[lints]` of every crate (`lints.workspace = true`), so plain `cargo clippy` reports them:
- `deny` since epic 06, ticket 03: `unwrap_used`, `expect_used`, `panic`, `unreachable`, `todo`, `unimplemented`,
  `indexing_slicing` (rules 6–8). Tests are allowed by `clippy.toml` at the workspace root; the assertion helpers
  (`testing::validator`, `MemStore`) and the integration-test crates allow them with `#![allow(..)]`.
- since epic 06, ticket 06: `needless_pass_by_value` (rule 3), `missing_debug_implementations` (16) and
  `return_self_not_must_use` (20) as `deny`, `unsafe_code` (32) as `forbid`. They cover test code too.

| Lint | Rule | Note |
|---|---|---|
| `wildcard_enum_match_arm` | 21 | hint only: `_` on a foreign enum is fine |

`#[must_use]` by meaning (pure functions, guards, a result whose loss is a bug) has no lint: look at what returns a
value without side effects.

`assert!` outside tests has no lint: search for it, and for `serde_json::Value[..] = …` and sqlx `Row::get`, which
panic but are no indexing to clippy. Default clippy (`-D warnings`) already covers rule 4
(`ptr_arg`, `borrowed_box`), rule 9 (`unnecessary_unwrap`), rules 15–16 (`should_implement_trait`,
`new_without_default`) and rule 22 (`wrong_self_convention`).

## 2. What lints cannot see

Go through the code for the rest, outside `#[cfg(test)]`:

- `.clone()` of owned data (2) and `Arc` in signatures and fields (1): is a borrow enough?
- functions returning `Vec` that callers only walk (5);
- functions taking several parameters of the same primitive type, mixed ids and numbers (13); tuples in
  signatures (14);
- traits with one implementation and no generic or `dyn` use (17); `impl Into<T>` outside entry API (19);
- `PoisonError`, `lock()` and `catch_unwind` (10, 11);
- parent modules, `foo.rs` next to `foo/`, and what they implement (26); file names repeating their path (25);
- chains in `for` headers or mutating outer state (29); undocumented modules, types and methods (34).

## 3. Verify

Read every site and its callers before reporting it. Drop what a rule's exception covers and say which
exception. Sort every finding into a **defect** (a panic, a lost value, a wrong result, a wrong doc, a measurable cost)
or **style** (names, types that only read better, Debug, imports, chains, module layout): the rules file says style
applies to new and changed code only. A finding names: rule, `file:line`, what is wrong, the fix, and whether the fix changes the public
API. Never promise a speed-up without a measurement.

## 4. Report

A short result goes in the chat. A full audit is a standalone HTML report in `demo/docs/`: a summary table by
rule (count, cost of the fix), then findings grouped by rule.

## 5. Fix (only when asked)

- Fix defects. Style findings in old code are not swept: they are fixed when the file is changed for another
  reason; propose a style change of old code only when its benefit is concrete, with the cost in files and lines.
- Fix one rule group at a time. Run what `CLAUDE.md` requires before finishing (fmt, clippy, tests, coverage).
- When a group reaches zero hits, enable its lints in the same change, in `[lints]` of the crate's `Cargo.toml`
  (`unsafe_code = "forbid"` belongs there too). Test code stays allowed: `clippy.toml` at the workspace root
  (`allow-unwrap-in-tests`, `allow-expect-in-tests`, `allow-panic-in-tests`, `allow-indexing-slicing-in-tests`);
  `#![allow(..)]` only on the assertion helpers and at the root of each integration-test crate. Clippy counts as
  test code only `#[test]` functions and modules under a plain `#[cfg(test)]`: write `#[cfg(test)] #[cfg(feature =
  "…")]`, not `#[cfg(all(test, …))]`, and `panic!` instead of `unreachable!` in tests (no setting allows it).
- Then drop those lints from the command in step 1 and record the change in this skill.
- Propose the commit message; commit only when the user asks.
