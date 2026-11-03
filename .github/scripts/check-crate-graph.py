#!/usr/bin/env python3
"""Checks the crate graph of `doc/crates.md` against `cargo metadata` of both workspaces.

For every package of the root workspace and of the demo workspace and every dependency of it whose name starts with
`cqrsx`, one edge line is computed: `<dependency> -> <package>`, followed by a bracket with `dev` for a dev-dependency,
`no-default` when the dependant turns the default features off and `feature <f1>, <f2>` for the features it asks for,
separated by `; `. A build dependency is not expected and is reported as an error. The lines of the first fenced code
block after the heading `## Dependency graph` of `doc/crates.md` must be exactly these lines.

Needs `cargo` on the path. Run from anywhere inside the repository: `python3 .github/scripts/check-crate-graph.py`.
Nothing is printed when the document is right; exit code 1 lists the edges the document lacks and the edges it has in
excess.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

DOCUMENT = "doc/crates.md"
HEADING = "## Dependency graph"
MANIFESTS = ("Cargo.toml", "demo/Cargo.toml")
CRATE_PREFIX = "cqrsx"
FENCE = re.compile(r"^\s{0,3}(```|~~~)")


def repository_root() -> Path:
    output = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True)
    return Path(output.stdout.strip())


def metadata_of(manifest: Path) -> dict:
    command = ["cargo", "metadata", "--format-version", "1", "--no-deps", "--manifest-path", str(manifest)]
    output = subprocess.run(command, capture_output=True, text=True, check=True)
    return json.loads(output.stdout)


def edge_line(package: str, dependency: dict, errors: list[str]) -> str:
    kind = dependency["kind"]
    notes = []
    if kind == "dev":
        notes.append("dev")
    elif kind is not None:
        errors.append(f"{package}: {dependency['name']} is a {kind} dependency, which the graph does not describe")
    if not dependency["uses_default_features"]:
        notes.append("no-default")
    if dependency["features"]:
        notes.append("feature " + ", ".join(dependency["features"]))
    bracket = f" [{'; '.join(notes)}]" if notes else ""
    return f"{dependency['name']} -> {package}{bracket}"


def computed_edges(root: Path, errors: list[str]) -> set[str]:
    edges = set()
    for manifest in MANIFESTS:
        for package in metadata_of(root / manifest)["packages"]:
            for dependency in package["dependencies"]:
                if dependency["name"].startswith(CRATE_PREFIX):
                    edges.add(edge_line(package["name"], dependency, errors))
    return edges


def documented_edges(root: Path) -> set[str] | None:
    """The non-empty lines of the first fenced block after the heading, or None when the document has none."""
    lines = (root / DOCUMENT).read_text(encoding="utf-8").splitlines()
    if HEADING not in (line.strip() for line in lines):
        return None
    start = next(index for index, line in enumerate(lines) if line.strip() == HEADING)
    block, fenced = [], False
    for line in lines[start + 1 :]:
        if FENCE.match(line):
            if fenced:
                return {entry.strip() for entry in block if entry.strip()}
            fenced = True
        elif fenced:
            block.append(line)
    return None


def main() -> int:
    root = repository_root()
    errors: list[str] = []
    computed = computed_edges(root, errors)
    documented = documented_edges(root)
    if documented is None:
        errors.append(f"{DOCUMENT}: no fenced block after the heading `{HEADING}`")
    else:
        errors += [f"missing from {DOCUMENT}: {line}" for line in sorted(computed - documented)]
        errors += [f"not in the cargo metadata: {line}" for line in sorted(documented - computed)]
    for line in errors:
        print(line)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
