//! Layer rules of the clean architecture, checked on the source text.
//!
//! Every `use` tree is expanded to full paths and every other qualified path is collected, with
//! comments and string literals removed first (event docs link to the workflows they start).
//! `bootstrap.rs` with `bootstrap/`, `process.rs`, `main.rs`, `config.rs`, `lib.rs` and `testing.rs`
//! compose the layers and are exempt. A few rules apply to production code only: test modules may use test tooling.
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

struct Layer {
    dir: &'static str,
    /// Path prefixes the layer must not use anywhere, tests included.
    forbidden: &'static [&'static str],
    /// Path prefixes only test code of the layer may use.
    test_only: &'static [&'static str],
    /// When set, every `cqrsx` path must start with one of these prefixes.
    cqrsx_allowed: Option<&'static [&'static str]>,
}

const LAYERS: &[Layer] = &[
    Layer {
        dir: "domain",
        forbidden: &[
            "crate::application",
            "crate::presentation",
            "crate::infrastructure",
            "crate::bootstrap",
            "crate::config",
            "sqlx",
            "mongodb",
            "axum",
            "tokio",
        ],
        test_only: &["serde_json", "cqrsx::cqrs::testing"],
        cqrsx_allowed: Some(&[
            "cqrsx::cqrs::aggregate",
            "cqrsx::cqrs::event",
            "cqrsx::cqrs::event_sink",
            "cqrsx::cqrs::testing",
        ]),
    },
    Layer {
        dir: "application",
        forbidden: &[
            "crate::presentation",
            "crate::infrastructure",
            "crate::bootstrap",
            "crate::config",
            "cqrsx::postgres",
            "cqrsx::mongodb",
            "cqrsx::task::adapter",
            "cqrsx::task::dispatcher",
            "cqrsx::task::runtime",
            "cqrsx::host",
            "cqrsx::logging",
            "sqlx",
            "mongodb",
            "axum",
        ],
        test_only: &["cqrsx::cqrs::testing"],
        cqrsx_allowed: None,
    },
    Layer {
        dir: "presentation",
        forbidden: &["crate::infrastructure", "cqrsx", "sqlx", "mongodb"],
        test_only: &[],
        cqrsx_allowed: None,
    },
    Layer {
        dir: "infrastructure",
        forbidden: &["crate::presentation", "crate::bootstrap"],
        test_only: &[],
        cqrsx_allowed: None,
    },
];

/// Blanks comments and string/char literals, keeping line breaks so line numbers stay valid.
fn strip(source: &str) -> String {
    let chars: Vec<char> = source.chars().collect();
    let mut out = String::with_capacity(source.len());
    let blank = |c: char| if c == '\n' { '\n' } else { ' ' };
    let mut i = 0;
    while i < chars.len() {
        let rest = &chars[i..];
        if rest.starts_with(&['/', '/']) {
            while i < chars.len() && chars[i] != '\n' {
                out.push(' ');
                i += 1;
            }
        } else if rest.starts_with(&['/', '*']) {
            let mut depth = 0;
            while i < chars.len() {
                if chars[i..].starts_with(&['/', '*']) {
                    depth += 1;
                    out.push_str("  ");
                    i += 2;
                } else if chars[i..].starts_with(&['*', '/']) {
                    depth -= 1;
                    out.push_str("  ");
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    out.push(blank(chars[i]));
                    i += 1;
                }
            }
        } else if chars[i] == 'r'
            && (rest.starts_with(&['r', '"']) || rest.starts_with(&['r', '#']))
            && !is_ident(chars.get(i.wrapping_sub(1)))
        {
            let hashes = rest[1..].iter().take_while(|c| **c == '#').count();
            if rest.get(1 + hashes) != Some(&'"') {
                out.push(chars[i]);
                i += 1;
                continue;
            }
            let close: Vec<char> = std::iter::once('"').chain(std::iter::repeat_n('#', hashes)).collect();
            i += 2 + hashes;
            out.push_str(&" ".repeat(2 + hashes));
            while i < chars.len() && !chars[i..].starts_with(&close) {
                out.push(blank(chars[i]));
                i += 1;
            }
            out.push_str(&" ".repeat(close.len()));
            i += close.len();
        } else if chars[i] == '"' {
            out.push(' ');
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    out.push(' ');
                    i += 1;
                }
                if i < chars.len() {
                    out.push(blank(chars[i]));
                    i += 1;
                }
            }
            out.push(' ');
            i += 1;
        } else if chars[i] == '\'' && char_literal_len(rest).is_some() {
            let len = char_literal_len(rest).unwrap_or(1);
            out.push_str(&" ".repeat(len));
            i += len;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn is_ident(c: Option<&char>) -> bool {
    c.is_some_and(|c| c.is_alphanumeric() || *c == '_')
}

/// `'x'`, `'\n'`, `'\u{1F600}'`; a lifetime (`'a`) is not a literal.
fn char_literal_len(rest: &[char]) -> Option<usize> {
    match rest {
        ['\'', '\\', ..] => rest.iter().skip(2).position(|c| *c == '\'').map(|p| p + 3),
        ['\'', _, '\'', ..] => Some(3),
        _ => None,
    }
}

/// Removes `#[cfg(test)] mod name { … }` blocks from stripped code.
fn without_test_modules(code: &str) -> String {
    let mut code = code.to_owned();
    while let Some(start) = code.find("#[cfg(test)]") {
        let after = &code[start + "#[cfg(test)]".len()..];
        let trimmed = after.trim_start();
        let is_inline_mod =
            (trimmed.starts_with("mod ") || trimmed.starts_with("pub mod ") || trimmed.starts_with("pub(crate) mod "))
                && trimmed
                    .find('{')
                    .is_some_and(|brace| trimmed.find(';').is_none_or(|semi| brace < semi));
        if !is_inline_mod {
            code.replace_range(start..start + "#[cfg(test)]".len(), &" ".repeat("#[cfg(test)]".len()));
            continue;
        }
        let open = start + "#[cfg(test)]".len() + (after.len() - trimmed.len()) + trimmed.find('{').unwrap_or(0);
        let mut depth = 0;
        let mut end = code.len();
        for (offset, c) in code[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + offset + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        let blanked: String = code[start..end].chars().map(|c| if c == '\n' { '\n' } else { ' ' }).collect();
        code.replace_range(start..end, &blanked);
    }
    code
}

/// Names of child modules declared as `#[cfg(test)] mod name;`: whole files of test code.
fn test_only_children(code: &str) -> Vec<String> {
    let mut children = Vec::new();
    let mut rest = code;
    while let Some(start) = rest.find("#[cfg(test)]") {
        let after = rest[start + "#[cfg(test)]".len()..].trim_start();
        let declaration = after.trim_start_matches("pub ").trim_start_matches("pub(crate) ");
        if let Some(name) = declaration.strip_prefix("mod ")
            && let Some(name) = name.split_whitespace().next().and_then(|n| n.strip_suffix(';'))
        {
            children.push(name.to_owned());
        }
        rest = &rest[start + 1..];
    }
    children
}

/// Expands a `use` tree into full paths: `a::{b, c::{d as e, self}}` → `a::b`, `a::c::d`, `a::c`.
fn expand(prefix: &str, tree: &str, out: &mut Vec<String>) {
    let tree = tree.trim();
    let join = |head: &str| match (prefix.is_empty(), head.is_empty()) {
        (true, _) => head.to_owned(),
        (false, true) => prefix.to_owned(),
        (false, false) => format!("{prefix}::{head}"),
    };
    if let Some(open) = tree.find('{') {
        let head = tree[..open].trim().trim_end_matches("::").trim();
        let inner = &tree[open + 1..tree.rfind('}').unwrap_or(tree.len())];
        let (mut depth, mut from) = (0, 0);
        for (index, c) in inner.char_indices() {
            match c {
                '{' => depth += 1,
                '}' => depth -= 1,
                ',' if depth == 0 => {
                    expand(&join(head), &inner[from..index], out);
                    from = index + 1;
                }
                _ => {}
            }
        }
        expand(&join(head), &inner[from..], out);
    } else if !tree.is_empty() {
        let path = tree.split(" as ").next().unwrap_or(tree).trim();
        out.push(if path == "self" { prefix.to_owned() } else { join(path) });
    }
}

/// Every path the code uses, with its line: expanded `use` trees and inline qualified paths.
fn paths(code: &str) -> Vec<(usize, String)> {
    let line_of = |offset: usize| code[..offset].matches('\n').count() + 1;
    let mut found = Vec::new();
    let mut search = 0;
    while let Some(position) = code[search..].find("use ") {
        let start = search + position;
        let boundary = start == 0 || !is_ident(code[..start].chars().last().as_ref());
        let end = code[start..].find(';').map_or(code.len(), |e| start + e);
        if boundary {
            let mut expanded = Vec::new();
            expand("", &code[start + 4..end], &mut expanded);
            found.extend(
                expanded
                    .into_iter()
                    .map(|path| (line_of(start), path.split_whitespace().collect::<String>())),
            );
            search = end;
        } else {
            search = start + 4;
        }
    }
    for root in [
        "crate::",
        "cqrsx::",
        "sqlx::",
        "mongodb::",
        "axum::",
        "tokio::",
        "serde_json::",
    ] {
        let mut search = 0;
        while let Some(position) = code[search..].find(root) {
            let start = search + position;
            search = start + root.len();
            if start > 0 && (is_ident(code[..start].chars().last().as_ref()) || code[..start].ends_with("::")) {
                continue;
            }
            let path: String = code[start..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                .collect();
            // `root::…::{` is the head of a grouped `use`, already expanded above.
            if path.ends_with("::") && code[start + path.len()..].starts_with('{') {
                continue;
            }
            found.push((line_of(start), path.trim_end_matches(':').to_owned()));
        }
    }
    found
}

fn starts_with(path: &str, prefix: &str) -> bool {
    path == prefix || path.starts_with(&format!("{prefix}::"))
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .flatten()
    {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn every_layer_imports_only_what_it_may() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    for layer in LAYERS {
        rust_files(&src.join(layer.dir), &mut files);
        files.push(src.join(format!("{}.rs", layer.dir)));
    }
    let stripped: Vec<(PathBuf, String)> = files
        .iter()
        .map(|file| {
            (
                file.clone(),
                strip(&fs::read_to_string(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()))),
            )
        })
        .collect();
    let test_files: BTreeSet<PathBuf> = stripped
        .iter()
        .flat_map(|(file, code)| {
            let dir = file.with_extension("");
            test_only_children(code)
                .into_iter()
                .map(move |child| dir.join(format!("{child}.rs")))
        })
        .collect();
    let mut violations = Vec::new();
    let mut checked = 0;
    for (file, code) in &stripped {
        let relative = file.strip_prefix(&src).unwrap_or(file);
        let Some(layer) = LAYERS
            .iter()
            .find(|l| relative.starts_with(l.dir) || relative == Path::new(&format!("{}.rs", l.dir)))
        else {
            continue;
        };
        checked += 1;
        let production: BTreeSet<(usize, String)> = if test_files.contains(file) {
            BTreeSet::new()
        } else {
            paths(&without_test_modules(code)).into_iter().collect()
        };
        for (line, path) in paths(code) {
            let mut broken: Vec<String> = layer
                .forbidden
                .iter()
                .filter(|prefix| starts_with(&path, prefix))
                .map(|prefix| format!("uses `{prefix}`"))
                .collect();
            if production.contains(&(line, path.clone())) {
                broken.extend(
                    layer
                        .test_only
                        .iter()
                        .filter(|prefix| starts_with(&path, prefix))
                        .map(|prefix| format!("uses `{prefix}` outside tests")),
                );
            }
            if let Some(allowed) = layer.cqrsx_allowed
                && starts_with(&path, "cqrsx")
                && path != "cqrsx"
                && !allowed.iter().any(|prefix| starts_with(&path, prefix))
            {
                broken.push("uses a cqrsx module outside its allowlist".to_owned());
            }
            violations.extend(
                broken
                    .into_iter()
                    .map(|why| format!("{}:{line}: `{path}` {why}", relative.display())),
            );
        }
    }
    assert!(
        checked > 50,
        "only {checked} files checked: is the layout still src/<layer>/…?"
    );
    assert!(violations.is_empty(), "layer rules broken:\n{}", violations.join("\n"));
}

#[test]
fn the_checker_sees_grouped_imports_inline_paths_and_ignores_comments_and_strings() {
    let code = strip(
        "use crate::{application::x, infrastructure::{mongo::Repo as R, self}};\n\
         // use crate::presentation::http;\n\
         let s = \"sqlx::query\"; let r = r#\"mongodb::x\"#; let c = '\"';\n\
         fn f<'a>() { cqrsx::postgres::connect(); }\n\
         #[cfg(test)]\nmod tests { use serde_json::json; }\n",
    );
    let found: Vec<String> = paths(&code).into_iter().map(|(_, p)| p).collect();
    assert!(found.contains(&"crate::application::x".to_owned()), "{found:?}");
    assert!(found.contains(&"crate::infrastructure::mongo::Repo".to_owned()), "{found:?}");
    assert!(found.contains(&"crate::infrastructure".to_owned()), "{found:?}");
    assert!(found.contains(&"cqrsx::postgres::connect".to_owned()), "{found:?}");
    assert!(found.contains(&"serde_json::json".to_owned()), "{found:?}");
    assert!(
        !found
            .iter()
            .any(|p| p.contains("presentation") || p.contains("sqlx") || p.contains("mongodb")),
        "{found:?}"
    );
    let production: Vec<String> = paths(&without_test_modules(&code)).into_iter().map(|(_, p)| p).collect();
    assert!(!production.contains(&"serde_json::json".to_owned()), "{production:?}");
    assert_eq!(
        test_only_children("#[cfg(test)]\nmod test_support;\npub mod x;"),
        ["test_support"]
    );
}

/// G-11 (backlog 05/02) until the engine refuses a second registration of an aggregate (05/11): the process builds one
/// command executor per aggregate type, in that aggregate's module of the composition root, and the root builds each
/// once. The HTTP commands and the task handlers' commands to one aggregate share it, and with it the locks; a second
/// executor of one type would be another process to them, meeting the first only at the optimistic check.
#[test]
fn the_process_builds_one_command_executor_per_aggregate_type() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    let stripped: Vec<(PathBuf, String)> = files
        .iter()
        .map(|file| {
            let code = fs::read_to_string(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
            (file.clone(), strip(&code))
        })
        .collect();
    let test_files: BTreeSet<PathBuf> = stripped
        .iter()
        .flat_map(|(file, code)| {
            // The crate root's children live next to it, every other module's in its own directory.
            let dir = if file.parent() == Some(src.as_path()) && (file.ends_with("lib.rs") || file.ends_with("main.rs")) {
                src.clone()
            } else {
                file.with_extension("")
            };
            test_only_children(code)
                .into_iter()
                .map(move |child| dir.join(format!("{child}.rs")))
        })
        .collect();
    let mut built = Vec::new();
    let mut test_executors = Vec::new();
    for (file, code) in stripped.iter().filter(|(file, _)| !test_files.contains(file)) {
        let relative = file.strip_prefix(&src).unwrap_or(file).display().to_string();
        let production = without_test_modules(code);
        let count = production.matches("PostgresCommandExecutor::new(").count();
        if count > 0 {
            built.push((relative.clone(), count));
        }
        if production.contains("TestCommandExecutor") {
            test_executors.push(relative);
        }
    }
    built.sort();
    assert_eq!(
        built,
        [
            ("bootstrap/aggregates/concert.rs".to_owned(), 1),
            ("bootstrap/aggregates/fan.rs".to_owned(), 1)
        ],
        "one executor per aggregate type, in its module of the composition root"
    );
    assert!(
        test_executors.is_empty(),
        "test executors in production code: {test_executors:?}"
    );
    let root = src.join("bootstrap.rs");
    let root = without_test_modules(&strip(
        &fs::read_to_string(&root).unwrap_or_else(|e| panic!("{}: {e}", root.display())),
    ));
    for aggregate in ["concert", "fan"] {
        assert_eq!(
            root.matches(&format!("aggregates::{aggregate}::wire(")).count(),
            1,
            "bootstrap.rs builds the {aggregate} executor once"
        );
    }
}
