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
//! the ledger's tasks. Of cqrsx a domain sees its contracts and their derive macros, a presentation the engine (its
//! commands, its views, the dependencies of the modules), the outcomes of a command, the rejection trait and the
//! documents of the read models. `main.rs` and `lib.rs` compose the modules, `testing.rs` and `composition.rs` with
//! their directories test them as composed: they are exempt. A few rules apply to production code only: test code may use test
//! tooling. Relative paths (`super::`) are not resolved: one module reaches another only through `crate::`.
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
    /// When set, every `cqrsx` path must start with one of these prefixes.
    cqrsx_allowed: Option<&'static [&'static str]>,
}

fn externals(layer: Option<Layer>) -> Externals {
    match layer {
        Some(Layer::Domain) => Externals {
            forbidden: &["sqlx", "mongodb", "axum", "tokio"],
            test_only: &["serde_json", "cqrsx::cqrs::testing"],
            // The derive macros of the domain's contracts (what is known of a command, the enum of commands, an
            // event's name and version, the enum of events, the code and params of a rejection), the decision of a
            // command, and the traits of the commands and the rejections, which the domain's tests call.
            cqrsx_allowed: Some(&[
                "cqrsx::Command",
                "cqrsx::Commands",
                "cqrsx::Event",
                "cqrsx::Events",
                "cqrsx::Rejection",
                "cqrsx::cqrs::aggregate",
                "cqrsx::cqrs::command::CommandInfo",
                "cqrsx::cqrs::command::decide",
                "cqrsx::cqrs::event",
                "cqrsx::cqrs::rejection",
                "cqrsx::cqrs::testing",
            ]),
        },
        Some(Layer::Application) => Externals {
            forbidden: &[
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
        Some(Layer::Presentation) => Externals {
            forbidden: &["sqlx", "mongodb"],
            test_only: &[],
            // The handler takes the engine as its state and hands it a command from the source "ui" (backlog 05/12,
            // design 04 and 07), answers its outcome (the error of a command) and, for a refused one, its code and
            // params (05/04): the rejection trait. A read model is read through the engine's view, a stored document is
            // what it answers (05/10, design 06). Nothing else.
            cqrsx_allowed: Some(&[
                "cqrsx::engine::Cqrs",
                "cqrsx::cqrs::command::CommandGatewayError",
                "cqrsx::cqrs::rejection",
                "cqrsx::cqrs::projection::repository::StoredView",
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
            && starts_with(&path, "cqrsx")
            && path != "cqrsx"
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
        ("concert/presentation/get.rs", contract),
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
            "use crate::operations::application::ledger::EventLedgerStats;\n",
        ),
        ("concert/domain/x.rs", "use crate::concert::application::app::ConcertApp;\n"),
        ("concert/application/x.rs", "use crate::bootstrap::AppState;\n"),
        ("concert/domain/x.rs", "use cqrsx::postgres::pool::connect_pool;\n"),
        (
            "concert/presentation/x.rs",
            "use cqrsx::cqrs::command::planner::TaskPlanner;\n",
        ),
        ("fan/application/x.rs", "use cqrsx::cqrs::testing::mem_store::MemStore;\n"),
        ("concert.rs", "use crate::fan::domain::Fan;\n"),
    ] {
        assert_eq!(check(file, code).len(), 1, "{file}: {code}");
    }
    assert!(
        check(
            "fan/application/x.rs",
            "#[cfg(test)]\nmod tests { use cqrsx::cqrs::testing::mem_store::MemStore; use crate::testing; }\n"
        )
        .is_empty(),
        "test tooling in test code"
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
        "operations/presentation/activity/activity/stream.rs",
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
            Path::new("fan/presentation/wallet.rs"),
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
            Path::new("fan/presentation/wallet.rs"),
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
        ("concert/domain/x.rs", "use cqrsx::{Event, Events, Rejection};\n"),
        (
            "concert/domain/x.rs",
            "#[cfg(test)]\nmod tests { use cqrsx::cqrs::rejection::Rejection; }\n",
        ),
        ("fan/application/x.rs", "use cqrsx::{Task, cqrs::rejection::Rejection};\n"),
        ("concert/presentation/x.rs", "use cqrsx::cqrs::rejection::Rejection;\n"),
    ] {
        assert!(check(file, code).is_empty(), "{file}: {code}");
    }
    for (file, code) in [
        ("concert/domain/x.rs", "use cqrsx::Task;\n"),
        ("concert/presentation/x.rs", "use cqrsx::Event;\n"),
        ("fan/presentation/x.rs", "use cqrsx::cqrs::event::DomainEvent;\n"),
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
        "use cqrsx::{cqrs::command::CommandGatewayError, engine::Cqrs};\n",
        "fn f(e: cqrsx::cqrs::command::CommandGatewayError<()>) {}\n",
        "fn f(cqrs: &cqrsx::engine::Cqrs) {}\n",
    ] {
        assert!(check("concert/presentation/x.rs", code).is_empty(), "{code}");
    }
    for code in [
        "use cqrsx::cqrs::command::CommandExecutor;\n",
        "use cqrsx::cqrs::command::metadata::CommandMetadata;\n",
        "use cqrsx::cqrs::command::CommandInfo;\n",
        "use cqrsx::cqrs::command::observer::CommandObserver;\n",
        "use cqrsx::cqrs::CommitReport;\n",
        "use cqrsx::cqrs::testing::command::TestCommandExecutor;\n",
        "use cqrsx::postgres::command::PostgresCommandExecutor;\n",
        "use cqrsx::engine::build::Builder;\n",
    ] {
        let broken = check("fan/presentation/x.rs", code);
        assert_eq!(broken.len(), 1, "{code}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
}

/// Backlog 05/10: a presentation reads a read model through the engine and answers its stored documents; nothing else
/// of the read side reaches it. Backlog 05/12: the engine's view (`cqrs.view::<V>()`) instead of a repository.
#[test]
fn the_read_model_queries_are_visible_in_the_presentation_and_nothing_more_of_the_read_side() {
    let check = |file: &str, code: &str| violations(Path::new(file), &strip(code), false, &[], &mut BTreeSet::new());
    for code in [
        "use cqrsx::{cqrs::projection::repository::StoredView, engine::Cqrs};\n",
        "fn f(d: cqrsx::cqrs::projection::repository::StoredView<()>) {}\n",
    ] {
        assert!(check("concert/presentation/x.rs", code).is_empty(), "{code}");
    }
    for code in [
        "use cqrsx::cqrs::projection::repository::ViewRepository;\n",
        "use cqrsx::engine::views::Views;\n",
        "use cqrsx::cqrs::projection::ViewProjection;\n",
        "use cqrsx::cqrs::projection::repository::ViewContext;\n",
        "use cqrsx::cqrs::projection::verify::verify;\n",
        "use cqrsx::mongodb::view_repository::MongoViewRepository;\n",
        "use cqrsx::cqrs::testing::mem_view_repository::MemViewRepository;\n",
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
        "use cqrsx::{Command, Commands, cqrs::command::decide::Decide};\n",
        "#[cfg(test)]\nmod tests { use cqrsx::cqrs::command::CommandInfo; }\n",
    ] {
        assert!(check("concert/domain/x.rs", code).is_empty(), "{code}");
    }
    for code in [
        "use cqrsx::cqrs::event_sink::EventSink;\n",
        "use cqrsx::cqrs::command::CommandExecutor;\n",
        "use cqrsx::cqrs::command::planner::TaskPlanner;\n",
    ] {
        let broken = check("fan/domain/x.rs", code);
        assert_eq!(broken.len(), 1, "{code}");
        assert!(broken[0].contains("uses a cqrsx module outside its allowlist"), "{broken:?}");
    }
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
        for executor in ["PostgresCommandExecutor", "CommandExecutorCore", "TestCommandExecutor"] {
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
