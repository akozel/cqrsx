#!/usr/bin/env python3
"""Checks the relative links of the documentation tracked by git.

Every relative link in a tracked `.md` or `.html` file must lead to a tracked file or directory, and an anchor to a
heading (Markdown) or an `id` (HTML) of its target. A link into an untracked path, such as the ignored `doc/backlog/`,
is broken: a clone of the repository has no such path. External links (`https:`, `mailto:` and the like) are not
checked.

Locally a new file not yet added counts as tracked and a deleted one as gone, so the check sees the next commit. Run
from anywhere inside the repository: `python3 .github/scripts/check-links.py`. Exit code 1 lists the broken links.
"""

import html
import posixpath
import re
import subprocess
import sys
from pathlib import Path

# Application sources that happen to be HTML, not documentation: their links are web paths of the application.
NOT_DOCUMENTATION = {"demo/front-end/index.html"}

MARKDOWN_LINK = re.compile(r"!?\[(?:[^\[\]]|\[[^\]]*\])*\]\(\s*<?([^()\s>]+(?:\([^()\s]*\))?[^()\s>]*)>?(?:\s+\"[^\"]*\")?\s*\)")
MARKDOWN_REFERENCE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+.*)?$", re.MULTILINE)
HTML_LINK = re.compile(r"""\b(?:href|src)\s*=\s*(?:"([^"]*)"|'([^']*)')""", re.IGNORECASE)
HTML_ID = re.compile(r"""\b(?:id|name)\s*=\s*(?:"([^"]*)"|'([^']*)')""", re.IGNORECASE)
SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")
FENCE = re.compile(r"^\s{0,3}(```|~~~)")
HEADING = re.compile(r"^\s{0,3}#{1,6}\s+(.*?)\s*#*\s*$")
INLINE_CODE = re.compile(r"(`+)(.+?)\1")


def repository_root() -> Path:
    output = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True)
    return Path(output.stdout.strip())


def tracked_files(root: Path) -> set[str]:
    """The files of the next commit: tracked or new and not ignored, and present on disk."""
    command = ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"]
    output = subprocess.run(command, cwd=root, capture_output=True, text=True, check=True)
    return {path for path in output.stdout.split("\0") if path and (root / path).is_file()}


def tracked_directories(files: set[str]) -> set[str]:
    directories = {""}
    for path in files:
        parent = posixpath.dirname(path)
        while parent and parent not in directories:
            directories.add(parent)
            parent = posixpath.dirname(parent)
    return directories


def markdown_prose(text: str) -> str:
    """The text without fenced code blocks and inline code: links there are examples, not links."""
    lines, fenced = [], False
    for line in text.splitlines():
        if FENCE.match(line):
            fenced = not fenced
            lines.append("")
        else:
            lines.append("" if fenced else INLINE_CODE.sub("", line))
    return "\n".join(lines)


def github_slug(heading: str) -> str:
    text = INLINE_CODE.sub(lambda match: match.group(2), heading)
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text).lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def markdown_anchors(text: str) -> set[str]:
    anchors, seen, fenced = set(), {}, False
    for line in text.splitlines():
        if FENCE.match(line):
            fenced = not fenced
            continue
        heading = None if fenced else HEADING.match(line)
        if heading:
            slug = github_slug(heading.group(1))
            count = seen.get(slug, 0)
            seen[slug] = count + 1
            anchors.add(slug if count == 0 else f"{slug}-{count}")
    anchors.update(first or second for first, second in HTML_ID.findall(text))
    return anchors


def links_of(path: str, text: str) -> list[str]:
    if path.endswith(".md"):
        prose = markdown_prose(text)
        found = MARKDOWN_LINK.findall(prose) + MARKDOWN_REFERENCE.findall(prose)
        found += [first or second for first, second in HTML_LINK.findall(prose)]
    else:
        found = [html.unescape(first or second) for first, second in HTML_LINK.findall(text)]
    return [link.strip() for link in found if link.strip()]


def anchors_of(root: Path, path: str, cache: dict[str, set[str]]) -> set[str] | None:
    if not (path.endswith(".md") or path.endswith(".html")):
        return None
    if path not in cache:
        text = (root / path).read_text(encoding="utf-8")
        cache[path] = markdown_anchors(text) if path.endswith(".md") else {
            first or second for first, second in HTML_ID.findall(text)
        }
    return cache[path]


def main() -> int:
    root = repository_root()
    files = tracked_files(root)
    directories = tracked_directories(files)
    documents = sorted(path for path in files if path.endswith((".md", ".html")) and path not in NOT_DOCUMENTATION)
    anchors: dict[str, set[str]] = {}
    broken = []
    for document in documents:
        for link in links_of(document, (root / document).read_text(encoding="utf-8")):
            if SCHEME.match(link) or link.startswith("//"):
                continue
            target, _, anchor = link.partition("#")
            target = target.split("?", 1)[0]
            if target.startswith("/"):
                resolved = posixpath.normpath(target.lstrip("/")) if target.strip("/") else ""
            elif target:
                resolved = posixpath.normpath(posixpath.join(posixpath.dirname(document), target))
            else:
                resolved = document
            resolved = "" if resolved == "." else resolved
            if resolved.startswith("../") or (resolved not in files and resolved not in directories):
                broken.append(f"{document}: {link} (no such tracked path)")
                continue
            if anchor and resolved in files:
                known = anchors_of(root, resolved, anchors)
                if known is not None and anchor not in known:
                    broken.append(f"{document}: {link} (no such anchor)")
    for line in broken:
        print(line)
    print(f"{len(documents)} documents checked, {len(broken)} broken links", file=sys.stderr)
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
