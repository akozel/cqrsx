//! Module and layer rules of the clean architecture ("who sees whom", backlog 05, design 01), checked on the source
//! text.
//!
//! The crate is a set of modules, each a directory with its layers inside and its root file next to it: `concert` and
//! `fan` (an aggregate each: `domain`, `application`, `presentation`), `operations` (no aggregate: `application`,
//! `presentation`) and `shared`, the kernel several modules need (`domain`, `application`, `presentation`,
//! `infrastructure`). Inside a module a file or directory is named by its role, never by a name the path already says:
//! an aggregate's state is `concert/domain/aggregate.rs`, not `concert/domain/concert.rs`. Every `use` tree is
//! expanded to full paths and every other qualified path is collected, with comments and string literals removed
//! first (docs link across modules). Inside a module a layer sees the layers
//! below it and the same layers of `shared`; across modules only the box office's application names the contracts of
//! the ledger's tasks. Of the framework (the crates `cqrsx`, `cqrsx_engine`, `cqrsx_postgres`, `cqrsx_mongodb` and
//! `cqrsx_testing`, see `FRAMEWORK`) a domain sees the contracts of the core and their derive macros, a presentation the engine
//! (`cqrsx_engine::Cqrs`: its commands, its views, the dependencies of the modules), the outcomes of a command, the
//! rejection trait, the documents of the read models and the wrong documents of a verification. `main.rs` and `lib.rs` compose the modules, `testing.rs` and `composition.rs` with
//! their directories test them as composed: they are exempt. A few rules apply to production code only: test code may use test
//! tooling. Relative paths (`super::`) are not resolved: one module reaches another only through `crate::`.
//!
//! The layout of a presentation is rule 10 of `.claude/rules/cqrsx-application.md` (transport, kind of engine entry,
//! action), and the names of its input and answer rule 11. What the checks look at: a file in a kind directory names
//! only the engine entry of its kind, a file outside the kind directories (the URL table `http.rs`, the shared helpers,
//! the `operations` endpoints of the process, the settings and the activity feed) names none of the four. The entries are
//! method calls, found textually in the code without comments, string literals and test modules (see `engine_entries` for
//! what that cannot see). An endpoint that reaches an entry through an application function counts as naming it:
//! `ENTRY_VIA_APPLICATION` lists those items, and a test keeps the list equal to the application files that name an
//! entry. Naming: a public type of a presentation that derives `Deserialize` ends in `Request`, one that derives
//! `Serialize` in `Response` (`NOT_ENDPOINT_TYPES` names the exceptions), and no presentation holds a `json!` but the
//! error shape (`JSON_BUILDERS`).
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Layer {
    Domain,
    Application,
    Presentation,
    Infrastructure,
}

impl Layer {
    const ALL: [Layer; 4] = [Layer::Domain, Layer::Application, Layer::Presentation, Layer::Infrastructure];

    fn name(self) -> &'static str {
        match self {
            Layer::Domain => "domain",
            Layer::Application => "application",
            Layer::Presentation => "presentation",
            Layer::Infrastructure => "infrastructure",
        }
    }
}

/// The modules of the crate and their layers.
const MODULES: &[(&str, &[Layer])] = &[
    ("concert", &[Layer::Domain, Layer::Application, Layer::Presentation]),
    ("fan", &[Layer::Domain, Layer::Application, Layer::Presentation]),
    ("operations", &[Layer::Application, Layer::Presentation]),
    (
        "shared",
        &[Layer::Domain, Layer::Application, Layer::Presentation, Layer::Infrastructure],
    ),
];

/// The composition (`main.rs` lists the modules for the engine, `lib.rs` its defaults) and the tests of the modules as
/// composed: they see everything and are not checked.
const EXEMPT: &[&str] = &["main", "lib", "testing", "composition"];

/// What one module may name of another: the producer of a fact builds the consumer's task, so the box office's
/// application names the contracts of the ledger's tasks and nothing else of the ledger. The ledger never names the
/// box office: its contracts are built from `shared` types only.
const ACROSS: &[(&str, Layer, &[&str])] = &[(
    "concert",
    Layer::Application,
    &[
        "crate::fan::application::record_ticket",
        "crate::fan::application::record_cancellation",
    ],
)];

/// Test tooling of the crate: test code of every layer may use it.
const CRATE_TEST_TOOLS: &str = "crate::testing";

/// Paths a place may name although its rules do not allow them, each with its reason and the task that removes it.
/// An exception nobody needs any more fails the check: the task that removes the reason removes the line.
struct Exception {
    id: &'static str,
    /// A file (`….rs`) or a directory prefix; `*` matches any one module (`*/presentation`).
    place: &'static str,
    paths: &'static [&'static str],
    /// The paths are allowed in test code only.
    tests_only: bool,
    why: &'static str,
}

/// The engine serves the modules (backlog 05/12): I-1 (`AppState` in the presentation), I-2 (`Config` in `/api/config`)
/// and I-3 (the rebuild of one hall by the operations' service) are gone with the composition root.
const EXCEPTIONS: &[Exception] = &[];

/// Where a file lives: a layer of a module, a module's root file (`layer: None`), the exempt composition, or nowhere.
#[derive(Debug, PartialEq, Eq)]
enum Located {
    Exempt,
    Module { module: &'static str, layer: Option<Layer> },
    Nowhere(String),
}

fn locate(relative: &Path) -> Located {
    let parts: Vec<String> = relative
        .iter()
        .map(|part| part.to_string_lossy().trim_end_matches(".rs").to_owned())
        .collect();
    let Some(first) = parts.first() else {
        return Located::Nowhere("an empty path".to_owned());
    };
    if EXEMPT.contains(&first.as_str()) {
        return Located::Exempt;
    }
    let Some((module, layers)) = MODULES.iter().find(|(module, _)| module == first) else {
        return Located::Nowhere("is in no module".to_owned());
    };
    let Some(second) = parts.get(1) else {
        return Located::Module { module, layer: None };
    };
    let Some(layer) = layers.iter().find(|layer| layer.name() == second) else {
        return Located::Nowhere(format!("is in no layer of `{module}`"));
    };
    if let Some(inner) = parts[2..]
        .iter()
        .find(|part| Layer::ALL.iter().any(|layer| layer.name() == *part))
    {
        return Located::Nowhere(format!("puts the layer `{inner}` inside the layer `{}`", layer.name()));
    }
    if let Some(repeated) = parts
        .iter()
        .enumerate()
        .skip(2)
        .find_map(|(at, part)| parts[..at].contains(part).then_some(part))
    {
        return Located::Nowhere(format!(
            "repeats `{repeated}`, a name the path already says: name it by its role (an aggregate's state is \
             `domain/aggregate.rs`)"
        ));
    }
    Located::Module {
        module,
        layer: Some(*layer),
    }
}

/// The prefixes of the crate a place may name: its own module's layers up to its own and the same layers of `shared`;
/// a module's root file sees its module and `shared`.
fn crate_allowed(module: &str, layer: Option<Layer>) -> Vec<String> {
    let Some(layer) = layer else {
        return vec![format!("crate::{module}"), "crate::shared".to_owned()];
    };
    let below: &[Layer] = match layer {
        Layer::Domain => &[Layer::Domain],
        Layer::Application => &[Layer::Domain, Layer::Application],
        Layer::Presentation => &[Layer::Domain, Layer::Application, Layer::Presentation],
        Layer::Infrastructure => &[Layer::Domain, Layer::Application, Layer::Infrastructure],
    };
    let mut allowed: Vec<String> = [module, "shared"]
        .iter()
        .flat_map(|owner| below.iter().map(move |layer| format!("crate::{owner}::{}", layer.name())))
        .collect();
    allowed.extend(
        ACROSS
            .iter()
            .filter(|(from, from_layer, _)| *from == module && *from_layer == layer)
            .flat_map(|(_, _, paths)| paths.iter().map(|path| (*path).to_owned())),
    );
    allowed
}

/// What a layer may use outside the crate.
struct Externals {
    /// Path prefixes the layer must not use anywhere, tests included.
    forbidden: &'static [&'static str],
    /// Path prefixes only test code of the layer may use.
    test_only: &'static [&'static str],
    /// When set, every path of a crate of the framework ([`FRAMEWORK`]) must start with one of these prefixes.
    cqrsx_allowed: Option<&'static [&'static str]>,
}

fn externals(layer: Option<Layer>) -> Externals {
    match layer {
        Some(Layer::Domain) => Externals {
            forbidden: &["sqlx", "mongodb", "axum", "tokio"],
            test_only: &["serde_json", "cqrsx_testing"],
            // The contracts of the domain, each with its derive macro under the same path (backlog 07/05): what is
            // known of a command (`Command`), the decision of a command (`Decide`), an event's name and version and
            // the enum of events (`domain::event`), the code and params of a rejection (`domain::rejection`).
            cqrsx_allowed: Some(&[
                "cqrsx::domain::aggregate",
                "cqrsx::domain::command::Command",
                "cqrsx::domain::command::Decide",
                "cqrsx::domain::event",
                "cqrsx::domain::rejection",
                "cqrsx_testing",
            ]),
        },
        Some(Layer::Application) => Externals {
            forbidden: &[
                "cqrsx::worker::adapter",
                "cqrsx::worker::dispatcher",
                "cqrsx::worker::runtime",
                "cqrsx_postgres",
                "cqrsx_mongodb",
                "cqrsx_engine::serve",
                "sqlx",
                "mongodb",
                "axum",
            ],
            test_only: &["cqrsx_testing", "cqrsx::memory"],
            cqrsx_allowed: None,
        },
        Some(Layer::Presentation) => Externals {
            forbidden: &["sqlx", "mongodb"],
            test_only: &[],
            // The handler takes the engine as its state and hands it a command from the source "ui" (backlog 05/12,
            // design 04 and 07), answers its outcome (the error of a command) and, for a refused one, its code and
            // params (05/04): the rejection trait. A read model is read through the engine's view, a stored document is
            // what it answers (05/10, design 06); a wrong document of a verification is answered as the engine reports it
            // (10/05). Nothing else. The path `domain::rejection::Rejection` names the trait
            // and its derive alike (backlog 07/05, decision 2), so a presentation may name both.
            cqrsx_allowed: Some(&[
                "cqrsx_engine::Cqrs",
                "cqrsx::command::CommandError",
                "cqrsx::domain::rejection",
                "cqrsx::view::repository::StoredView",
                "cqrsx::view::verify::Difference",
                "cqrsx::view::verify::DifferenceKind",
            ]),
        },
        Some(Layer::Infrastructure) | None => Externals {
            forbidden: &[],
            test_only: &[],
            cqrsx_allowed: None,
        },
    }
}

/// Whether an exception's place covers a file: `*` matches any one path component, a place without `.rs` is a
/// directory and covers the layer's root file next to it too (`concert/presentation` covers `concert/presentation.rs`).
fn covers(place: &str, relative: &Path) -> bool {
    let file: Vec<String> = relative
        .iter()
        .map(|part| part.to_string_lossy().trim_end_matches(".rs").to_owned())
        .collect();
    let pattern: Vec<&str> = place.trim_end_matches(".rs").split('/').collect();
    let matches = |count: usize| {
        pattern
            .iter()
            .zip(&file)
            .take(count)
            .all(|(want, got)| *want == "*" || want == got)
    };
    if place.ends_with(".rs") {
        file.len() == pattern.len() && matches(pattern.len())
    } else {
        file.len() >= pattern.len() && matches(pattern.len())
    }
}

/// The rules broken by one file. `code` is stripped; `whole_file_is_test` marks a file declared `#[cfg(test)] mod …;`.
/// Every exception that allowed a path is recorded in `used` as (exception, path) indices.
fn violations(
    relative: &Path,
    code: &str,
    whole_file_is_test: bool,
    exceptions: &[Exception],
    used: &mut BTreeSet<(usize, usize)>,
) -> Vec<String> {
    let (module, layer) = match locate(relative) {
        Located::Exempt => return Vec::new(),
        Located::Nowhere(why) => return vec![format!("{}: {why}", relative.display())],
        Located::Module { module, layer } => (module, layer),
    };
    let place = match layer {
        Some(layer) => format!("{module}/{}", layer.name()),
        None => format!("{module}.rs"),
    };
    let allowed = crate_allowed(module, layer);
    let outside = externals(layer);
    let production: BTreeSet<(usize, String)> = if whole_file_is_test {
        BTreeSet::new()
    } else {
        paths(&without_test_modules(code)).into_iter().collect()
    };
    let mut broken = Vec::new();
    // A plain `use a::b;` is found both as a `use` tree and as an inline path: one report each.
    for (line, path) in paths(code).into_iter().collect::<BTreeSet<_>>() {
        let in_production = production.contains(&(line, path.clone()));
        let mut why: Vec<String> = Vec::new();
        if starts_with(&path, "crate") && path != "crate" {
            let visible = allowed.iter().any(|prefix| starts_with(&path, prefix))
                || (!in_production && starts_with(&path, CRATE_TEST_TOOLS));
            let excepted = || {
                exceptions.iter().enumerate().find_map(|(index, exception)| {
                    let covered = covers(exception.place, relative) && (!exception.tests_only || !in_production);
                    let at = exception.paths.iter().position(|prefix| starts_with(&path, prefix))?;
                    covered.then_some((index, at))
                })
            };
            if !visible {
                match excepted() {
                    Some(exception) => {
                        used.insert(exception);
                    }
                    None => why.push(format!("is not visible from `{place}`")),
                }
            }
        }
        why.extend(
            outside
                .forbidden
                .iter()
                .filter(|prefix| starts_with(&path, prefix))
                .map(|prefix| format!("uses `{prefix}`")),
        );
        if in_production {
            why.extend(
                outside
                    .test_only
                    .iter()
                    .filter(|prefix| starts_with(&path, prefix))
                    .map(|prefix| format!("uses `{prefix}` outside tests")),
            );
        }
        if let Some(allowed) = outside.cqrsx_allowed
            && is_framework_path(&path)
            && !allowed.iter().any(|prefix| starts_with(&path, prefix))
        {
            why.push("uses a cqrsx module outside its allowlist".to_owned());
        }
        broken.extend(
            why.into_iter()
                .map(|why| format!("{}:{line}: `{path}` {why}", relative.display())),
        );
    }
    broken
}

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
    let roots = ["crate", "sqlx", "mongodb", "axum", "tokio", "serde_json"]
        .iter()
        .chain(&FRAMEWORK)
        .map(|root| format!("{root}::"));
    for root in roots {
        let root = root.as_str();
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

/// The crates of the framework: the core and every crate built on it. A path into any of them is a path into
/// "cqrsx" for the allowlists, so that a crate split off the core (the engine, the adapters, the testing tools) does not
/// slip out of the rules.
const FRAMEWORK: [&str; 5] = ["cqrsx", "cqrsx_engine", "cqrsx_postgres", "cqrsx_mongodb", "cqrsx_testing"];

/// Whether `path` goes into a crate of the framework (a bare crate name is not a path into it).
fn is_framework_path(path: &str) -> bool {
    FRAMEWORK.iter().any(|name| starts_with(path, name) && path != *name)
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

/// Files declared `#[cfg(test)] mod name;`: whole files of test code. The crate root's children live next to it,
/// every other module's in its own directory.
fn test_files(src: &Path, stripped: &[(PathBuf, String)]) -> BTreeSet<PathBuf> {
    stripped
        .iter()
        .flat_map(|(file, code)| {
            let dir = if file.parent() == Some(src) && (file.ends_with("lib.rs") || file.ends_with("main.rs")) {
                src.to_path_buf()
            } else {
                file.with_extension("")
            };
            test_only_children(code)
                .into_iter()
                .map(move |child| dir.join(format!("{child}.rs")))
        })
        .collect()
}

#[test]
fn every_layer_imports_only_what_it_may() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    let stripped: Vec<(PathBuf, String)> = files
        .iter()
        .map(|file| {
            (
                file.clone(),
                strip(&fs::read_to_string(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()))),
            )
        })
        .collect();
    let test_files = test_files(&src, &stripped);
    let mut broken = Vec::new();
    let mut used = BTreeSet::new();
    let mut checked: BTreeMap<(&str, Option<Layer>), usize> = BTreeMap::new();
    for (file, code) in &stripped {
        let relative = file.strip_prefix(&src).unwrap_or(file);
        if let Located::Module { module, layer } = locate(relative) {
            *checked.entry((module, layer)).or_default() += 1;
        }
        broken.extend(violations(relative, code, test_files.contains(file), EXCEPTIONS, &mut used));
    }
    for (index, exception) in EXCEPTIONS.iter().enumerate() {
        for (at, path) in exception.paths.iter().enumerate() {
            if !used.contains(&(index, at)) {
                broken.push(format!(
                    "exception {} ({}: `{path}`; {}) is not needed any more: remove it",
                    exception.id, exception.place, exception.why
                ));
            }
        }
    }
    let total: usize = checked.values().sum();
    assert!(
        total > 60,
        "only {total} files checked: is the layout still src/<module>/<layer>/…?"
    );
    for (module, layers) in MODULES {
        for layer in *layers {
            assert!(
                checked.get(&(module, Some(*layer))).is_some_and(|count| *count > 0),
                "no file checked in {module}/{}",
                layer.name()
            );
        }
    }
    assert!(broken.is_empty(), "module rules broken:\n{}", broken.join("\n"));
}

/// The rules on examples (backlog 05/03): the box office's application naming the ledger's domain is caught, and so
/// is every other reach across the modules that design 01 rules out.
#[test]
fn the_module_rules_catch_a_reach_into_another_module() {
    let check = |file: &str, code: &str| violations(Path::new(file), &strip(code), false, &[], &mut BTreeSet::new());
    let broken = check("concert/application/fan_ledger.rs", "use crate::fan::domain::Fan;\n");
    assert_eq!(broken.len(), 1, "{broken:?}");
    assert!(
        broken[0].contains("`crate::fan::domain::Fan` is not visible from `concert/application`"),
        "{broken:?}"
    );
    // The contracts of the ledger's tasks are what the box office's application may name of the ledger, nothing more.
    let contract = "use crate::fan::application::record_ticket::RecordTicketV1;\n";
    assert!(check("concert/application/fan_ledger.rs", contract).is_empty());
    for (file, code) in [
        ("concert/presentation/http/queries/get.rs", contract),
        ("concert/application/x.rs", "use crate::fan::application::app::FanApp;\n"),
        ("fan/application/x.rs", "use crate::concert::domain::Concert;\n"),
        ("fan/domain/x.rs", "use crate::concert::domain::events::SeatBooked;\n"),
        (
            "operations/application/x.rs",
            "fn f() { crate::fan::domain::Fan::default(); }\n",
        ),
        (
            "operations/presentation/x.rs",
            "use crate::concert::application::views::concert_hall::ConcertHallView;\n",
        ),
        ("shared/application/x.rs", "use crate::concert;\n"),
        (
            "shared/infrastructure/x.rs",
            "use crate::operations::application::journal::JournalStats;\n",
        ),
        ("concert/domain/x.rs", "use crate::concert::application::app::ConcertApp;\n"),
        ("concert/application/x.rs", "use crate::bootstrap::AppState;\n"),
        ("concert/domain/x.rs", "use cqrsx_postgres::pool::connect_pool;\n"),
        ("concert/presentation/x.rs", "use cqrsx::command::planner::TaskPlanner;\n"),
        ("fan/application/x.rs", "use cqrsx_testing::memory_store::MemoryStore;\n"),
        ("concert/domain/x.rs", "use cqrsx_testing::framework::TestFramework;\n"),
        (
            "concert/domain/x.rs",
            "use cqrsx::memory::event_repository::MemoryEventRepository;\n",
        ),
        (
            "fan/application/x.rs",
            "use cqrsx::memory::view_repository::MemoryViewRepository;\n",
        ),
        ("concert.rs", "use crate::fan::domain::Fan;\n"),
    ] {
        assert_eq!(check(file, code).len(), 1, "{file}: {code}");
    }
    assert!(
        check(
            "fan/application/x.rs",
            "#[cfg(test)]\nmod tests { use cqrsx_testing::memory_store::MemoryStore; use crate::testing; }\n"
        )
        .is_empty(),
        "test tooling in test code"
    );
    assert!(
        check(
            "concert/domain/x.rs",
            "#[cfg(test)]\nmod tests { use cqrsx_testing::framework::TestFramework; }\n"
        )
        .is_empty(),
        "test tooling in the test code of a domain"
    );
    assert!(check("concert.rs", "pub mod domain; use crate::shared::domain::ConcertId;\n").is_empty());
    // Every file has its place: the old layout, a missing layer, a layer inside a layer and a name the path already
    // says are reported.
    for file in [
        "domain/concert.rs",
        "operations/domain.rs",
        "concert/application/domain/x.rs",
        "concert/domain/concert.rs",
        "fan/application/views/views.rs",
        "operations/presentation/http/maintenance/maintenance.rs",
    ] {
        assert_eq!(check(file, "").len(), 1, "{file}");
    }
    assert!(
        check("main.rs", "use crate::fan::domain::Fan;\n").is_empty(),
        "the composition is exempt"
    );
    // An exception allows its paths in its place only, and in test code only when it says so.
    let exceptions = [Exception {
        id: "T",
        place: "*/presentation",
        paths: &["crate::bootstrap::AppState"],
        tests_only: true,
        why: "",
    }];
    let mut used = BTreeSet::new();
    let state = "use crate::bootstrap::AppState;\n";
    let in_tests = "#[cfg(test)]\nmod tests { use crate::bootstrap::AppState; }\n";
    assert!(
        violations(
            Path::new("fan/presentation/http/queries/wallet.rs"),
            &strip(in_tests),
            false,
            &exceptions,
            &mut used
        )
        .is_empty()
    );
    assert!(
        violations(
            Path::new("fan/presentation.rs"),
            &strip(in_tests),
            false,
            &exceptions,
            &mut used
        )
        .is_empty()
    );
    assert_eq!(used, BTreeSet::from([(0, 0)]));
    assert_eq!(
        violations(
            Path::new("fan/presentation/http/queries/wallet.rs"),
            &strip(state),
            false,
            &exceptions,
            &mut used
        )
        .len(),
        1
    );
    assert_eq!(
        violations(
            Path::new("fan/application/wallet.rs"),
            &strip(in_tests),
            false,
            &exceptions,
            &mut used
        )
        .len(),
        1
    );
}

/// The derive macros and the rejection trait on examples (backlog 05/04): a domain derives its events and rejections,
/// an application its tasks, a presentation names the rejection trait; nothing else of cqrsx reaches them.
#[test]
fn the_derive_macros_and_the_rejection_trait_are_visible_where_they_belong() {
    let check = |file: &str, code: &str| violations(Path::new(file), &strip(code), false, &[], &mut BTreeSet::new());
    for (file, code) in [
        (
            "concert/domain/x.rs",
            "use cqrsx::domain::{event::{Event, Events}, rejection::Rejection};\n",
        ),
        (
            "concert/domain/x.rs",
            "#[cfg(test)]\nmod tests { use cqrsx::domain::rejection::Rejection; }\n",
        ),
        (
            "fan/application/x.rs",
            "use cqrsx::{task::Task, domain::rejection::Rejection};\n",
        ),
        ("concert/presentation/x.rs", "use cqrsx::domain::rejection::Rejection;\n"),
    ] {
        assert!(check(file, code).is_empty(), "{file}: {code}");
    }
    for (file, code) in [
        ("concert/domain/x.rs", "use cqrsx::task::Task;\n"),
        ("concert/presentation/x.rs", "use cqrsx::domain::event::Event;\n"),
        ("fan/presentation/x.rs", "use cqrsx::domain::event::Events;\n"),
    ] {
        let broken = check(file, code);
        assert_eq!(broken.len(), 1, "{file}: {code}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
}

/// Backlog 05/08: the HTTP handler hands a command to the engine and answers its outcome; nothing else of the command
/// path reaches a presentation. Backlog 05/12: the engine is the port, the metadata of the command its own.
#[test]
fn the_command_port_is_visible_in_the_presentation_and_nothing_more() {
    let check = |file: &str, code: &str| violations(Path::new(file), &strip(code), false, &[], &mut BTreeSet::new());
    for code in [
        "use cqrsx::command::CommandError;\nuse cqrsx_engine::Cqrs;\n",
        "fn f(e: cqrsx::command::CommandError<()>) {}\n",
        "fn f(cqrs: &cqrsx_engine::Cqrs) {}\n",
    ] {
        assert!(check("concert/presentation/x.rs", code).is_empty(), "{code}");
    }
    for code in [
        "use cqrsx::command::CommandExecutor;\n",
        "use cqrsx::command::metadata::CommandMetadata;\n",
        "use cqrsx::domain::command::Command;\n",
        "use cqrsx::command::observer::CommandObserver;\n",
        "use cqrsx::command::CommitReport;\n",
        "use cqrsx_testing::command_executor::TestCommandExecutor;\n",
        "use cqrsx::command::executor::StoreCommandExecutor;\n",
        "use cqrsx_engine::build::Builder;\n",
    ] {
        let broken = check("fan/presentation/x.rs", code);
        assert_eq!(broken.len(), 1, "{code}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
}

/// The wrong documents a verification found are answered as the engine reports them: the two types that describe one,
/// nothing else of `view::verify` (its functions and counters stay in the application).
#[test]
fn a_difference_of_a_verification_is_visible_in_the_presentation_and_nothing_more_of_verify() {
    let check = |file: &str, code: &str| violations(Path::new(file), &strip(code), false, &[], &mut BTreeSet::new());
    for code in [
        "use cqrsx::view::verify::Difference;\n",
        "use cqrsx::view::verify::DifferenceKind;\n",
        "fn f(d: Vec<cqrsx::view::verify::Difference>) {}\n",
    ] {
        assert!(check("operations/presentation/x.rs", code).is_empty(), "{code}");
    }
    for code in [
        "use cqrsx::view::verify::Verification;\n",
        "use cqrsx::view::verify::verify;\n",
    ] {
        let broken = check("operations/presentation/x.rs", code);
        assert_eq!(broken.len(), 1, "{code}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
    assert!(check("operations/application/x.rs", "use cqrsx::view::verify::Verification;\n").is_empty());
}

/// Backlog 07/07: the engine is a crate of its own, `cqrsx_engine`; every crate of the framework falls under the
/// allowlists, not only the path segment `cqrsx`.
#[test]
fn every_crate_of_the_framework_is_under_the_rules() {
    let check = |file: &str, code: &str| violations(Path::new(file), &strip(code), false, &[], &mut BTreeSet::new());
    // A domain names no part of the engine, in a `use` tree or inline.
    for (code, reports) in [
        ("use cqrsx_engine::Cqrs;\n", 1),
        ("use cqrsx_engine::{Cqrs, build::Builder};\n", 2),
        ("fn f(cqrs: &cqrsx_engine::Cqrs) {}\n", 1),
    ] {
        let broken = check("concert/domain/x.rs", code);
        assert_eq!(broken.len(), reports, "{code}: {broken:?}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
    // A presentation names the engine itself and nothing else of it.
    assert!(check("concert/presentation/x.rs", "use cqrsx_engine::Cqrs;\n").is_empty());
    for code in [
        "use cqrsx_engine::settings::Settings;\n",
        "fn f() { cqrsx_engine::build::Builder::default(); }\n",
        "use cqrsx_postgres::pool::connect_pool;\n",
        "use cqrsx_mongodb::view_repository::MongoViewRepository;\n",
    ] {
        let broken = check("fan/presentation/x.rs", code);
        assert_eq!(broken.len(), 1, "{code}: {broken:?}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
    // An application assembles nothing of the process and opens no store, but may use the engine's settings.
    assert!(check("operations/application/x.rs", "use cqrsx_engine::settings::Settings;\n").is_empty());
    for (code, prefix) in [
        ("use cqrsx_engine::serve::ServeError;\n", "cqrsx_engine::serve"),
        ("use cqrsx_postgres::pool::connect_pool;\n", "cqrsx_postgres"),
        (
            "fn f() { cqrsx_mongodb::checkpoint_store::MongoCheckpointStore::new(); }\n",
            "cqrsx_mongodb",
        ),
    ] {
        let broken = check("operations/application/x.rs", code);
        assert_eq!(broken.len(), 1, "{code}: {broken:?}");
        assert!(broken[0].contains(&format!("uses `{prefix}`")), "{broken:?}");
    }
    // A bare crate name is no path into it; an inline path of any crate of the framework is collected.
    assert!(!is_framework_path("cqrsx_engine") && is_framework_path("cqrsx_engine::Cqrs"));
    assert!(!is_framework_path("cqrsx_other::Cqrs"));
    let found: Vec<String> = paths("fn f() { cqrsx_testing::framework::TestFramework::default(); }\n")
        .into_iter()
        .map(|(_, path)| path)
        .collect();
    assert_eq!(found, ["cqrsx_testing::framework::TestFramework::default"]);
}

/// Backlog 05/10: a presentation reads a read model through the engine and answers its stored documents; nothing else
/// of the read side reaches it. Backlog 05/12: the engine's view (`cqrs.view::<V>()`) instead of a repository.
#[test]
fn the_read_model_queries_are_visible_in_the_presentation_and_nothing_more_of_the_read_side() {
    let check = |file: &str, code: &str| violations(Path::new(file), &strip(code), false, &[], &mut BTreeSet::new());
    for code in [
        "use cqrsx::view::repository::StoredView;\nuse cqrsx_engine::Cqrs;\n",
        "fn f(d: cqrsx::view::repository::StoredView<()>) {}\n",
    ] {
        assert!(check("concert/presentation/x.rs", code).is_empty(), "{code}");
    }
    for code in [
        "use cqrsx::view::repository::ViewRepository;\n",
        "use cqrsx_engine::views::Views;\n",
        "use cqrsx::view::projection::ViewProjection;\n",
        "use cqrsx::view::repository::ViewMeta;\n",
        "use cqrsx::view::verify::verify;\n",
        "use cqrsx_mongodb::view_repository::MongoViewRepository;\n",
        "use cqrsx::memory::view_repository::MemoryViewRepository;\n",
    ] {
        let broken = check("fan/presentation/x.rs", code);
        assert_eq!(broken.len(), 1, "{code}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
}

/// Backlog 05/05: a command decides itself in the domain (`Decide`) and derives what is known of it; the domain sees
/// nothing else of the command path, and the event sink is gone.
#[test]
fn the_command_derives_and_the_decide_contract_are_visible_in_the_domain() {
    let check = |file: &str, code: &str| violations(Path::new(file), &strip(code), false, &[], &mut BTreeSet::new());
    for code in [
        "use cqrsx::domain::command::{Command, Decide};\n",
        "#[cfg(test)]\nmod tests { use cqrsx::domain::command::Command; }\n",
    ] {
        assert!(check("concert/domain/x.rs", code).is_empty(), "{code}");
    }
    for code in [
        "use cqrsx::projection::EventHandler;\n",
        "use cqrsx::command::CommandExecutor;\n",
        "use cqrsx::command::planner::TaskPlanner;\n",
        "use cqrsx::domain::command::Label;\n",
    ] {
        let broken = check("fan/domain/x.rs", code);
        assert_eq!(broken.len(), 1, "{code}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
}

/// The four entries of the engine's `Cqrs`, which are also the kinds of presentation: what a file under
/// `presentation/<transport>/` does is told by the directory it lies in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Entry {
    Commands,
    Queries,
    History,
    Maintenance,
}

impl Entry {
    const ALL: [Entry; 4] = [Entry::Commands, Entry::Queries, Entry::History, Entry::Maintenance];

    /// The name of the kind's directory.
    fn dir(self) -> &'static str {
        match self {
            Entry::Commands => "commands",
            Entry::Queries => "queries",
            Entry::History => "history",
            Entry::Maintenance => "maintenance",
        }
    }

    /// The calls of the entry, for a message.
    fn call(self) -> &'static str {
        match self {
            Entry::Commands => "`source(..)`",
            Entry::Queries => "`view::<V>()`",
            Entry::History => "`history::<A>(..)`",
            Entry::Maintenance => "`views()` or `view::<V>().rebuild(..)`",
        }
    }
}

/// The transports of a presentation; a new one (a queue, a scheduler) is a sibling directory of `http`.
const TRANSPORTS: &[&str] = &["http"];

/// Application items that reach an engine entry for a presentation that names them, each with the application file that
/// calls the entry. A path is an item (never a whole module that also holds items unrelated to the entry). The next
/// test keeps the files equal to the application files that name an entry.
const ENTRY_VIA_APPLICATION: &[(&str, &str, Entry)] = &[
    (
        "crate::concert::application::time_travel",
        "concert/application/time_travel.rs",
        Entry::History,
    ),
    (
        "crate::concert::application::rebuild::rebuild_hall",
        "concert/application/rebuild.rs",
        Entry::Maintenance,
    ),
    (
        "crate::operations::application::rebuild::ReadSideService",
        "operations/application/engine.rs",
        Entry::Maintenance,
    ),
    (
        "crate::operations::application::verify::VerifyService",
        "operations/application/engine.rs",
        Entry::Maintenance,
    ),
];

/// The engine entry a call names, if `name_start` (in stripped code) is the name of one. The entries are
/// `source(`, `history(` or `history::<`, `views(` and `view::<V>(..)`, which is the maintenance entry when `.rebuild`
/// follows it and a query otherwise; whitespace and line breaks between the parts do not matter (rustfmt breaks
/// chains). Limits of a textual check: a method named like an entry on another type (`Error::source()`) is reported,
/// and a view kept in a variable and rebuilt later reads as a query.
fn entry_at(code: &str, name_start: usize) -> Option<Entry> {
    let rest = &code[name_start..];
    let name: String = rest.chars().take_while(|c| is_ident(Some(c))).collect();
    let after = rest[name.len()..].trim_start();
    match name.as_str() {
        "source" => after.starts_with('(').then_some(Entry::Commands),
        "history" => (after.starts_with('(') || after.starts_with("::")).then_some(Entry::History),
        "views" => after.starts_with('(').then_some(Entry::Maintenance),
        "view" => {
            let mut after = after;
            if after.starts_with("::<") {
                let mut depth = 0;
                let end = after.char_indices().find_map(|(index, c)| {
                    match c {
                        '<' => depth += 1,
                        '>' => depth -= 1,
                        _ => {}
                    }
                    (depth == 0 && c == '>').then_some(index + 1)
                })?;
                after = after[end..].trim_start();
            }
            if !after.starts_with('(') {
                return None;
            }
            let mut depth = 0;
            let end = after.char_indices().find_map(|(index, c)| {
                match c {
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    _ => {}
                }
                (depth == 0 && c == ')').then_some(index + 1)
            })?;
            let rebuilds = after[end..]
                .trim_start()
                .strip_prefix(".rebuild")
                .is_some_and(|tail| !is_ident(tail.chars().next().as_ref()));
            Some(if rebuilds { Entry::Maintenance } else { Entry::Queries })
        }
        _ => None,
    }
}

/// The engine entries a stripped code calls directly, with their lines: `cqrs.source(..)`, `Cqrs::source(..)` and the like.
fn engine_entries(code: &str) -> Vec<(usize, Entry)> {
    let line_of = |offset: usize| code[..offset].matches('\n').count() + 1;
    let dotted = code.match_indices('.').map(|(at, _)| at + 1);
    let qualified = code
        .match_indices("Cqrs::")
        .filter(|(at, _)| !is_ident(code[..*at].chars().last().as_ref()))
        .map(|(at, _)| at + "Cqrs::".len());
    let mut found: Vec<(usize, Entry)> = dotted
        .chain(qualified)
        .filter_map(|start| entry_at(code, start).map(|entry| (line_of(start), entry)))
        .collect();
    found.sort();
    found
}

/// The path of a presentation file as components without `.rs`, or `None` for any other file.
fn presentation_parts(relative: &Path) -> Option<Vec<String>> {
    let parts: Vec<String> = relative
        .iter()
        .map(|part| part.to_string_lossy().trim_end_matches(".rs").to_owned())
        .collect();
    let in_module = parts
        .first()
        .is_some_and(|first| MODULES.iter().any(|(module, _)| module == first));
    (in_module && parts.get(1).is_some_and(|layer| layer == "presentation")).then_some(parts)
}

/// The engine entries a stripped file of production code uses: the calls it makes and the entries of the application
/// items it names (`ENTRY_VIA_APPLICATION`).
fn entries_named(code: &str) -> Vec<(usize, Entry)> {
    let production = without_test_modules(code);
    let mut found = engine_entries(&production);
    for (line, path) in paths(&production) {
        found.extend(
            ENTRY_VIA_APPLICATION
                .iter()
                .filter(|(item, _, _)| starts_with(&path, item))
                .map(|(_, _, entry)| (line, *entry)),
        );
    }
    found.sort();
    found.dedup();
    found
}

/// The kind rule on one file with stripped `code`: a file under `presentation/<transport>/<kind>/` uses only the entry of its
/// kind; every other file of a presentation uses none of the four; the transport and the kind are among the known ones.
fn kind_violations(relative: &Path, code: &str) -> Vec<String> {
    let Some(parts) = presentation_parts(relative) else {
        return Vec::new();
    };
    let shown = relative.display();
    if let Some(transport) = parts.get(2)
        && !TRANSPORTS.contains(&transport.as_str())
    {
        return vec![format!(
            "{shown}: `{transport}` is not a transport ({}): a presentation file lies under `presentation/<transport>/`",
            TRANSPORTS.join(", ")
        )];
    }
    let own = if parts.len() >= 5 {
        match Entry::ALL.iter().find(|entry| entry.dir() == parts[3]) {
            Some(entry) => Some(*entry),
            None => {
                return vec![format!(
                    "{shown}: `{}` is not a kind: a directory of a transport is `commands`, `queries`, `history` or \
                     `maintenance`",
                    parts[3]
                )];
            }
        }
    } else {
        None
    };
    entries_named(code)
        .into_iter()
        .filter(|(_, entry)| Some(*entry) != own)
        .map(|(line, entry)| match own {
            Some(own) => format!(
                "{shown}:{line}: uses the {} entry ({}), but the file is in `{}/`",
                entry.dir(),
                entry.call(),
                own.dir()
            ),
            None => format!(
                "{shown}:{line}: uses the {} entry ({}), but a file outside the kind directories uses none of the four",
                entry.dir(),
                entry.call()
            ),
        })
        .collect()
}

/// The stripped source of every file of the crate that is not a test file, with its path relative to `src`.
fn production_sources() -> Vec<(PathBuf, String)> {
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
    let test_files = test_files(&src, &stripped);
    stripped
        .into_iter()
        .filter(|(file, _)| !test_files.contains(file))
        .map(|(file, code)| (file.strip_prefix(&src).map(Path::to_path_buf).unwrap_or(file), code))
        .collect()
}

/// A file of a presentation names the engine entry of its kind and no other (a presentation file outside the kind
/// directories none of the four), the transports and the kinds are the known ones, and the production code of the tree
/// obeys it. Every kind is exercised, so the rule cannot pass for lack of files.
#[test]
fn a_presentation_file_names_only_the_engine_entry_of_its_kind() {
    let mut broken = Vec::new();
    let (mut checked, mut outside) = (0, 0);
    let mut exercised = BTreeSet::new();
    for (relative, code) in production_sources() {
        let Some(parts) = presentation_parts(&relative) else {
            continue;
        };
        checked += 1;
        broken.extend(kind_violations(&relative, &code));
        match parts
            .get(3)
            .and_then(|kind| Entry::ALL.iter().find(|entry| entry.dir() == kind))
        {
            Some(entry) if parts.len() >= 5 => {
                if !entries_named(&code).is_empty() {
                    exercised.insert(*entry);
                }
            }
            Some(_) | None => outside += 1,
        }
    }
    assert!(broken.is_empty(), "kind rule broken:\n{}", broken.join("\n"));
    assert!(
        checked >= 12,
        "only {checked} presentation files: is the layout still presentation/<transport>/<kind>/?"
    );
    assert!(outside > 0, "no presentation file outside the kind directories");
    assert_eq!(
        exercised,
        BTreeSet::from(Entry::ALL),
        "a kind with no file that names its entry"
    );
}

/// The kind rule on examples: every wrong entry in every kind, an entry in a file outside the kind directories, an
/// entry reached through an application item, a stray file and a stray directory are refused; the right entries,
/// formatted chains, the free calls of the process, comments, strings and test modules are not.
#[test]
fn the_kind_rule_refuses_every_wrong_entry() {
    let check = |file: &str, code: &str| kind_violations(Path::new(file), &strip(code));
    let (commands, queries) = (
        "concert/presentation/http/commands/x.rs",
        "concert/presentation/http/queries/x.rs",
    );
    let (history, maintenance) = (
        "concert/presentation/http/history/x.rs",
        "concert/presentation/http/maintenance/x.rs",
    );
    let free = "operations/presentation/http/health.rs";
    let source = "fn f(cqrs: &Cqrs) { cqrs.source(\"ui\").execute::<A>(&id, c); }\n";
    let get = "fn f(cqrs: &Cqrs) { cqrs.view::<V>().get(&id); }\n";
    let list = "fn f(cqrs: &Cqrs) { cqrs.view::<V>()\n    .list(); }\n";
    let rebuild = "fn f(cqrs: &Cqrs) { cqrs.view::<V>()\n    .rebuild(&id); }\n";
    let views = "fn f(cqrs: &Cqrs) { cqrs.views().status(); }\n";
    let replay = "fn f(cqrs: &Cqrs) { cqrs.history::<A>(&id); }\n";
    for (file, code) in [
        (queries, source),
        (queries, views),
        (queries, replay),
        (queries, rebuild),
        (queries, "fn f(cqrs: &Cqrs) { Cqrs::source(cqrs, \"ui\"); }\n"),
        (commands, get),
        (commands, views),
        ("concert/presentation/http/commands/execute/response.rs", get),
        (history, source),
        (history, list),
        (maintenance, get),
        (maintenance, source),
        (maintenance, replay),
        (free, source),
        (free, get),
        (free, replay),
        (free, views),
        ("shared/presentation/http.rs", source),
        ("concert/presentation/http.rs", views),
        ("concert/presentation.rs", get),
        // An application item that reaches an entry counts as naming it.
        (queries, "use crate::concert::application::time_travel;\n"),
        (commands, "use crate::concert::application::rebuild::rebuild_hall;\n"),
        (history, "use crate::operations::application::rebuild::ReadSideService;\n"),
        (free, "use crate::operations::application::verify::{VerifyService, X};\n"),
        // Where a file lies.
        ("concert/presentation/get.rs", ""),
        ("operations/presentation/http/activity/stream.rs", ""),
    ] {
        assert_eq!(check(file, code).len(), 1, "{file}: {code}");
    }
    let broken = check(queries, "fn f() {}\nfn g(cqrs: &Cqrs) { cqrs.view::<V>().rebuild(&id); }\n");
    assert_eq!(
        broken,
        [
            "concert/presentation/http/queries/x.rs:2: uses the maintenance entry (`views()` or `view::<V>().rebuild(..)`), \
          but the file is in `queries/`"
        ]
    );
    assert!(check("operations/presentation/http/activity/stream.rs", "")[0].contains("`activity` is not a kind"));
    assert!(check("concert/presentation/get.rs", "")[0].contains("`get` is not a transport"));
    for (file, code) in [
        (
            commands,
            "fn f(cqrs: &Cqrs) {\n    cqrs\n        .source(\"ui\")\n        .execute_with_id::<A>(id, &key, c);\n}\n",
        ),
        (commands, source),
        (queries, get),
        (queries, list),
        (queries, "fn f(cqrs: &Cqrs) { wallet_of(&cqrs.view::<Wrapper<V>>(), &id); }\n"),
        (history, replay),
        (history, "use crate::concert::application::time_travel;\n"),
        // A helper next to the endpoints of its kind, and the request and response of a directory endpoint, name no
        // entry or the entry of their kind.
        ("concert/presentation/http/history/summary.rs", "fn summary() {}\n"),
        (
            "concert/presentation/http/commands/execute/request.rs",
            "fn into_command() {}\n",
        ),
        (
            "concert/presentation/http/history/state_at/response.rs",
            "use crate::concert::application::time_travel::ConcertAt;\n",
        ),
        (
            "operations/presentation/http/maintenance/rebuild/response.rs",
            "use crate::operations::application::rebuild::{RebuildRefused, RebuildStarted};\n",
        ),
        (maintenance, views),
        (maintenance, rebuild),
        (
            maintenance,
            "use crate::operations::application::rebuild::{ReadSideService, RebuildRefused};\n",
        ),
        (maintenance, "use crate::concert::application::rebuild::rebuild_hall;\n"),
        (
            free,
            "fn f(cqrs: &Cqrs) { cqrs.settings().snapshot_every(); cqrs.stopping(); dependency::<T>(cqrs)?; }\n",
        ),
        (free, "use crate::operations::application::rebuild::RebuildPace;\n"),
        (
            "concert/presentation/http.rs",
            "fn routes() { Router::new().route(\"/x\", get(queries::get::handle)); }\n",
        ),
        // Comments, strings, test modules, fields and bindings are no calls.
        (queries, "// cqrs.source(\"ui\")\nfn f() { let s = \"cqrs.views()\"; }\n"),
        (
            queries,
            "#[cfg(test)]\nmod tests { fn f(cqrs: &Cqrs) { cqrs.source(\"ui\"); } }\n",
        ),
        (
            free,
            "fn f(stored: S) { let StoredView { id, view, .. } = stored; stored.view.sold; }\n",
        ),
    ] {
        assert!(check(file, code).is_empty(), "{file}: {code}: {:?}", check(file, code));
    }
}

/// Public types of a presentation that carry serde derives but are neither the input nor the answer of an endpoint:
/// the data of a server-sent event.
const NOT_ENDPOINT_TYPES: &[&str] = &["ActivityHello"];

/// The one file of a presentation that builds JSON by hand: the shape every error answer shares.
const JSON_BUILDERS: &[&str] = &["shared/presentation/http/error.rs"];

/// A public struct or enum of stripped code with the traits it derives.
#[derive(Debug, PartialEq, Eq)]
struct PublicType {
    line: usize,
    name: String,
    derives: Vec<String>,
}

/// The index of the bracket that opens the one at `close` (`)`, `]` or `}`), in stripped code.
fn opening(code: &str, close: usize) -> Option<usize> {
    let mut depth = 0;
    for (index, c) in code[..=close].char_indices().rev() {
        match c {
            ')' | ']' | '}' => depth += 1,
            '(' | '[' | '{' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

/// The traits named in the `#[derive(..)]` attributes right before the item that starts at `item`, by their last path
/// segment (`serde::Serialize` is `Serialize`); other attributes between them are skipped.
fn derives_before(code: &str, item: usize) -> Vec<String> {
    let mut derives = Vec::new();
    let mut end = item;
    loop {
        let before = code[..end].trim_end();
        if !before.ends_with(']') {
            return derives;
        }
        let Some(open) = opening(before, before.len() - 1) else {
            return derives;
        };
        let Some(attribute) = before[..open].strip_suffix('#').map(|_| &before[open..]) else {
            return derives;
        };
        if let Some(list) = attribute.strip_prefix("[derive(").and_then(|rest| rest.strip_suffix(")]")) {
            derives.extend(
                list.split(',')
                    .filter_map(|name| name.rsplit("::").next())
                    .map(|name| name.trim().to_owned())
                    .filter(|name| !name.is_empty()),
            );
        }
        end = open - 1;
    }
}

/// The public structs and enums of stripped code, `pub` with or without a restriction (`pub(crate)`).
fn public_types(code: &str) -> Vec<PublicType> {
    let mut found = Vec::new();
    for keyword in ["struct", "enum"] {
        for (at, _) in code.match_indices(keyword) {
            let rest = &code[at + keyword.len()..];
            let is_word = !is_ident(code[..at].chars().last().as_ref()) && rest.starts_with(char::is_whitespace);
            if !is_word {
                continue;
            }
            let mut before = code[..at].trim_end();
            if before.ends_with(')')
                && let Some(open) = opening(before, before.len() - 1)
            {
                before = before[..open].trim_end();
            }
            let Some(item) = before
                .strip_suffix("pub")
                .filter(|rest| !is_ident(rest.chars().last().as_ref()))
            else {
                continue;
            };
            let name: String = rest.trim_start().chars().take_while(|c| is_ident(Some(c))).collect();
            found.push(PublicType {
                line: code[..at].matches('\n').count() + 1,
                name,
                derives: derives_before(code, item.len()),
            });
        }
    }
    found.sort_by_key(|item| item.line);
    found
}

/// The naming rule on one file with stripped `code`: the input of an endpoint is a `…Request`, its answer a
/// `…Response`, and no presentation builds JSON by hand outside the error shape. A public type that derives
/// `Deserialize` ends in `Request`, one that derives `Serialize` in `Response`; the parts of a body are private types
/// and have any name. Limits of a textual check: a hand-written `impl Serialize` is not seen, and a type that
/// derives both is held to `Request` first.
fn wire_violations(relative: &Path, code: &str) -> Vec<String> {
    if presentation_parts(relative).is_none() {
        return Vec::new();
    }
    let production = without_test_modules(code);
    let shown = relative.display();
    let mut broken = Vec::new();
    for item in public_types(&production) {
        let has = |name: &str| item.derives.iter().any(|derive| derive == name);
        let rule = if has("Deserialize") {
            Some(("Deserialize", "Request"))
        } else if has("Serialize") {
            Some(("Serialize", "Response"))
        } else {
            None
        };
        if let Some((derive, suffix)) = rule
            && !item.name.ends_with(suffix)
            && !NOT_ENDPOINT_TYPES.contains(&item.name.as_str())
        {
            broken.push(format!(
                "{shown}:{}: `{}` derives `{derive}`: a public type of a presentation that does is named `<Subject><Action>{suffix}` \
                 (the parts of a body are private)",
                item.line, item.name
            ));
        }
    }
    if !JSON_BUILDERS.iter().any(|file| Path::new(file) == relative) {
        for (at, _) in production.match_indices("json!") {
            if !is_ident(production[..at].chars().last().as_ref()) {
                broken.push(format!(
                    "{shown}:{}: `json!` builds an answer by hand: a presentation answers with a `…Response` type",
                    production[..at].matches('\n').count() + 1
                ));
            }
        }
    }
    broken
}

/// Every public type of a presentation that is serialized or deserialized is named for its role, and no presentation
/// holds a `json!` outside the error shape. Both kinds of type are exercised, so the rule cannot pass for lack of them.
#[test]
fn a_presentation_names_its_requests_and_responses_and_builds_no_json_by_hand() {
    let mut broken = Vec::new();
    let (mut requests, mut responses) = (0, 0);
    for (relative, code) in production_sources() {
        broken.extend(wire_violations(&relative, &code));
        if presentation_parts(&relative).is_some() {
            for item in public_types(&without_test_modules(&code)) {
                requests += usize::from(item.name.ends_with("Request") && item.derives.iter().any(|d| d == "Deserialize"));
                responses += usize::from(item.name.ends_with("Response") && item.derives.iter().any(|d| d == "Serialize"));
            }
        }
    }
    assert!(broken.is_empty(), "naming rule broken:\n{}", broken.join("\n"));
    assert!(
        requests >= 2,
        "only {requests} request types: is the rule still looking at the presentations?"
    );
    assert!(
        responses >= 10,
        "only {responses} response types: is the rule still looking at the presentations?"
    );
}

/// The naming rule on examples: a wrong name for a request or a response, in any spelling of the derive and the
/// visibility, and a hand-built answer are refused; the right names, private parts, the event data, derives that carry
/// no serde, `json!` in tests and in the error shape, other layers, comments and strings are not.
#[test]
fn the_naming_rule_refuses_a_misnamed_wire_type_and_a_hand_built_answer() {
    let check = |file: &str, code: &str| wire_violations(Path::new(file), &strip(code));
    let handler = "concert/presentation/http/commands/x.rs";
    for code in [
        "#[derive(Debug, Deserialize)]\npub struct CommandDto { a: u8 }\n",
        "#[derive(Debug, Deserialize)]\npub enum CommandDto { A }\n",
        "#[derive(Debug, Serialize)]\npub struct Answer { a: u8 }\n",
        "#[derive(Debug, serde::Serialize)]\npub struct Answer;\n",
        "#[derive(\n    Debug,\n    Serialize,\n)]\npub struct Answer;\n",
        "#[derive(Debug)]\n#[derive(Serialize)]\n#[serde(transparent)]\npub struct Answer(Vec<u8>);\n",
        "#[derive(Serialize)]\npub(crate) struct Answer;\n",
        "#[derive(Serialize)]\npub struct ResponseOfThings;\n",
        "#[derive(Deserialize)]\npub struct Request2;\n",
    ] {
        let broken = check(handler, code);
        assert_eq!(broken.len(), 1, "{code}: {broken:?}");
        assert!(broken[0].contains("derives"), "{broken:?}");
    }
    for code in [
        "fn f() -> Json<Value> { Json(json!({ \"a\": 1 })) }\n",
        "fn f() { serde_json::json!(1); }\n",
    ] {
        let broken = check(handler, code);
        assert_eq!(broken.len(), 1, "{code}: {broken:?}");
        assert!(broken[0].contains("`json!`"), "{broken:?}");
    }
    for code in [
        "#[derive(Debug, Deserialize)]\npub struct ConcertCommandRequest { a: u8 }\n",
        "#[derive(Debug, Serialize)]\n#[serde(transparent)]\npub struct ConcertHallListResponse(Vec<Entry>);\n",
        "#[derive(Debug, Serialize)]\nstruct Entry { a: u8 }\n",
        "#[derive(Debug, Serialize)]\npub(super) struct ConcertHallResponse { a: u8 }\n",
        "#[derive(Debug, Serialize)]\npub struct ActivityHello { a: u8 }\n",
        "#[derive(Debug, Clone)]\npub struct Anything { a: u8 }\n",
        "pub struct Plain;\npub enum Plainer {}\n",
        "#[derive(Debug, Serialize, Deserialize)]\nstruct Both;\n",
        "// #[derive(Serialize)] pub struct Answer;\nlet s = \"json!(1)\";\n",
        "#[cfg(test)]\nmod tests { #[derive(Serialize)] pub struct Answer; fn f() { json!(1); } }\n",
        "fn f() { to_json!(1); my_json!(1); }\n",
    ] {
        assert!(check(handler, code).is_empty(), "{code}");
    }
    assert!(check("shared/presentation/http/error.rs", "fn f() { json!({}); }\n").is_empty());
    assert_eq!(check("shared/presentation/http.rs", "fn f() { json!({}); }\n").len(), 1);
    // Only a presentation is held to the names.
    let misnamed = "#[derive(Serialize)]\npub struct Answer;\nfn f() { json!(1); }\n";
    for file in [
        "concert/application/x.rs",
        "shared/infrastructure/x.rs",
        "concert/domain/x.rs",
        "concert.rs",
    ] {
        assert!(check(file, misnamed).is_empty(), "{file}");
    }
}

/// The application items that reach an engine entry are listed (`ENTRY_VIA_APPLICATION`): the production code of the
/// application files that call an entry is exactly the files the list names, with the entries it names. A new
/// application function over the engine, or one that stops using it, fails here until the list says so.
#[test]
fn the_application_items_that_reach_an_engine_entry_are_listed() {
    let calling: BTreeSet<(String, Entry)> = production_sources()
        .into_iter()
        .filter(|(relative, _)| {
            let parts = relative
                .iter()
                .map(|part| part.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            parts.get(1).is_some_and(|layer| layer == "application")
        })
        .flat_map(|(relative, code)| {
            let entries: BTreeSet<Entry> = engine_entries(&without_test_modules(&code))
                .into_iter()
                .map(|(_, entry)| entry)
                .collect();
            let file = relative.to_string_lossy().into_owned();
            entries.into_iter().map(move |entry| (file.clone(), entry))
        })
        .collect();
    let listed: BTreeSet<(String, Entry)> = ENTRY_VIA_APPLICATION
        .iter()
        .map(|(_, file, entry)| ((*file).to_owned(), *entry))
        .collect();
    assert_eq!(
        calling, listed,
        "application files that call an engine entry and ENTRY_VIA_APPLICATION differ: list the item a presentation names \
         to reach it, or remove the row"
    );
}

#[test]
fn the_checker_sees_grouped_imports_inline_paths_and_ignores_comments_and_strings() {
    let code = strip(
        "use crate::{application::x, infrastructure::{mongo::Repo as R, self}};\n\
         // use crate::presentation::http;\n\
         let s = \"sqlx::query\"; let r = r#\"mongodb::x\"#; let c = '\"';\n\
         fn f<'a>() { cqrsx_postgres::connect(); }\n\
         #[cfg(test)]\nmod tests { use serde_json::json; }\n",
    );
    let found: Vec<String> = paths(&code).into_iter().map(|(_, p)| p).collect();
    assert!(found.contains(&"crate::application::x".to_owned()), "{found:?}");
    assert!(found.contains(&"crate::infrastructure::mongo::Repo".to_owned()), "{found:?}");
    assert!(found.contains(&"crate::infrastructure".to_owned()), "{found:?}");
    assert!(found.contains(&"cqrsx_postgres::connect".to_owned()), "{found:?}");
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

/// G-11 (backlog 05/02; since 05/11 the engine refuses a second registration of an aggregate type, since 05/12 it
/// builds the executors of the process): one command executor per aggregate type and process. The demo builds none of
/// its own: the engine builds one per aggregate type a module registers, and `main.rs` builds the engine once, with
/// every module once. The HTTP commands and the task handlers' commands to one aggregate share it, and with it the
/// locks; a second executor of one type would be another process to them, meeting the first only at the optimistic
/// check.
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
    let test_files = test_files(&src, &stripped);
    let mut built = Vec::new();
    for (file, code) in stripped.iter().filter(|(file, _)| !test_files.contains(file)) {
        let relative = file.strip_prefix(&src).unwrap_or(file).display().to_string();
        let production = without_test_modules(code);
        for executor in ["StoreCommandExecutor", "TestCommandExecutor"] {
            if production.contains(executor) {
                built.push((relative.clone(), executor));
            }
        }
    }
    assert!(
        built.is_empty(),
        "command executors in production code: the engine builds them: {built:?}"
    );
    let main = src.join("main.rs");
    let main = strip(&fs::read_to_string(&main).unwrap_or_else(|e| panic!("{}: {e}", main.display())));
    assert_eq!(main.matches("Cqrs::from_env(").count(), 1, "main.rs builds the engine once");
    for module in ["concert", "fan", "operations"] {
        assert_eq!(
            main.matches(&format!(".module({module}::module)")).count(),
            1,
            "main.rs registers the {module} module once"
        );
    }
}
