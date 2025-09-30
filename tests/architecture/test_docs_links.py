from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _iter_markdown_files(root: Path) -> Iterable[Path]:
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.lower().endswith(".md"):
                yield Path(dirpath) / filename


_LINK_PATTERN = re.compile(r"!{0,1}\[[^\]]*\]\(([^)]+)\)")


def _extract_links(md_text: str) -> List[str]:
    return [m.group(1).strip() for m in _LINK_PATTERN.finditer(md_text)]


def _is_external(href: str) -> bool:
    lower = href.lower()
    return (
        lower.startswith("http://")
        or lower.startswith("https://")
        or lower.startswith("mailto:")
        or lower.startswith("tel:")
        or lower.startswith("javascript:")
        or lower.startswith("ftp://")
        or lower.startswith("data:")
    )


def _normalize_target(base_file: Path, href: str) -> Path:
    # Strip URL fragment/query parts
    path_only = href.split("#", 1)[0].split("?", 1)[0]
    # Skip anchors-only and empty
    if not path_only or path_only.startswith("#"):
        return base_file  # sentinel to mark as existing
    # Absolute path (repo-root relative) — treat as from PROJECT_ROOT
    if path_only.startswith("/"):
        return (PROJECT_ROOT / path_only.lstrip("/"))
    return (base_file.parent / path_only)


def test_docs_relative_links_exist() -> None:
    docs_root = PROJECT_ROOT / "docs"

    markdown_files: List[Path] = []
    if docs_root.exists():
        markdown_files.extend(_iter_markdown_files(docs_root))
    # Also include top-level README.md if present
    top_level_readme = PROJECT_ROOT / "README.md"
    if top_level_readme.exists():
        markdown_files.append(top_level_readme)

    broken: List[Tuple[Path, str]] = []

    for md_file in markdown_files:
        try:
            text = md_file.read_text(encoding="utf-8")
        except Exception:
            # Skip unreadable file
            continue

        for href in _extract_links(text):
            if _is_external(href):
                continue
            target = _normalize_target(md_file, href)
            # If target is the sentinel (same file), treat as existing
            if target == md_file:
                continue
            if target.is_dir():
                # Allow directory links if they contain a README.md (typical docs convention)
                if (target / "README.md").exists():
                    continue
            if not target.exists():
                broken.append((md_file, href))

    if broken:
        details = [
            f"- {src.relative_to(PROJECT_ROOT)} -> '{href}'"
            for src, href in broken
        ]
        raise AssertionError(
            "Broken relative links found in docs:\n" + "\n".join(details)
        )


