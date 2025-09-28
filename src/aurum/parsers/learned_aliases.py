"""Learned schema alias mapping support for schema inference.

This module optionally loads additional alias candidates from a JSON file to
augment the built-in alias heuristics. It allows gradual learning from
historical vendor ingestions without hard-coding into the library.

Configuration:
- Environment variable `AURUM_LEARNED_ALIASES` can point to a JSON file.
- Otherwise, we look for `config/learned_aliases.json` relative to the repo.

File format example (keys are canonical column names):
{
  "tenor_label": ["period", "tenor", "mth"],
  "mid": ["price", "mtm", "settle"],
  "price_type": ["type", "quote_type"]
}
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Mapping, MutableMapping, Sequence, Optional, Dict


def _default_path() -> Path:
    env = os.getenv("AURUM_LEARNED_ALIASES")
    if env:
        return Path(env)
    return Path("config/learned_aliases.json")


def load(path: Optional[str | os.PathLike[str]] = None) -> Mapping[str, Sequence[str]]:
    p = Path(path) if path is not None else _default_path()
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        # Ensure values are sequences of strings
        result: Dict[str, Sequence[str]] = {
            str(k): tuple(str(v) for v in (vals or ())) for k, vals in (data or {}).items()
        }
        return result
    except Exception:
        # Be resilient to malformed files
        return {}


def merge(
    base: Optional[Mapping[str, Sequence[str]]],
    learned: Optional[Mapping[str, Sequence[str]]],
) -> Mapping[str, Sequence[str]]:
    """Merge two alias maps, concatenating values and de-duplicating.

    - ``base``: usually overrides provided via runtime options
    - ``learned``: additional synonyms discovered from history
    """
    merged: MutableMapping[str, Sequence[str]] = {}
    for source in (learned or {}, base or {}):  # learned first, then base overrides
        for canonical, aliases in source.items():
            existing = list(merged.get(canonical, ()))
            existing.extend(list(aliases or ()))
            # de-duplicate while preserving order
            seen = set()
            deduped = [a for a in existing if not (a in seen or seen.add(a))]
            merged[canonical] = tuple(deduped)
    return merged


__all__ = ["load", "merge"]

