//! The table of variables in the crate's README against the code: every variable the engine reads, with the default
//! `Settings::description` writes for it. Built with every feature, the only build that reads all of them.
#![cfg(all(feature = "postgres", feature = "mongodb", feature = "http"))]
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md).
#![allow(clippy::unwrap_used, clippy::panic)]

use cqrsx_engine::settings::{Defaults, Settings};
use std::collections::BTreeMap;

const README: &str = include_str!("../README.md");
/// The header row that opens the table.
const HEADER: &str = "| Variable | Default | Setting |";

/// The rows of the README's table: a variable and its default, without their backticks, in the order written.
fn readme_rows() -> Vec<(String, String)> {
    let mut lines = README.lines().skip_while(|line| *line != HEADER);
    assert_eq!(lines.next(), Some(HEADER), "the README has no table of variables");
    let mut rows = Vec::new();
    for line in lines.skip(1).take_while(|line| line.starts_with('|')) {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        let [_, variable, default, ..] = cells.as_slice() else {
            panic!("a row of the table has no variable and default: {line}");
        };
        rows.push((variable.trim_matches('`').to_owned(), default.trim_matches('`').to_owned()));
    }
    rows
}

/// What the engine reads with no variable set: every variable (an upper-case key; the derived values have lower-case
/// dotted keys) with the library's default.
fn code_rows() -> BTreeMap<String, String> {
    let settings = Settings::from_lookup("", |_| None, &Defaults::LIBRARY).unwrap();
    settings
        .description()
        .into_iter()
        .filter(|(key, _)| key.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
        .collect()
}

#[test]
fn the_readme_lists_every_variable_once_with_its_default() {
    let rows = readme_rows();
    let written = rows.len();
    let listed: BTreeMap<String, String> = rows.into_iter().collect();
    assert_eq!(listed.len(), written, "a variable is listed twice in the README");
    assert_eq!(listed, code_rows(), "the README's table (left) differs from the code (right)");
}
