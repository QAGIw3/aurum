#!/usr/bin/env python3
"""Run a Great Expectations suite against lakeFS objects for the current action event."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from aurum.dq.validator import enforce_expectation_suite


def _load_event() -> dict:
    try:
        payload = sys.stdin.read()
        return json.loads(payload) if payload else {}
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse lakeFS action event JSON: {exc}") from exc


def _collect_paths(event: dict, prefix: str) -> List[str]:
    paths: List[str] = []
    raw_paths = event.get("paths") or []
    for entry in raw_paths:
        if isinstance(entry, dict):
            path = entry.get("path")
        else:
            path = entry
        if not path:
            continue
        if prefix and not path.startswith(prefix):
            continue
        paths.append(path)
    return paths


def _download_objects(lakectl: str, repo: str, ref: str, paths: Iterable[str]) -> List[Path]:
    downloaded: List[Path] = []
    temp_root = Path(tempfile.mkdtemp(prefix="lakefs_ge_"))
    for path in paths:
        target = temp_root / Path(path).name
        target.parent.mkdir(parents=True, exist_ok=True)
        uri = f"lakefs://{repo}/{ref}/{path}"
        cmd = [lakectl, "fs", "download", uri, str(target)]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError as exc:
            raise SystemExit(
                f"lakectl CLI not found when running {' '.join(cmd)}. "
                "Set LAKECTL_CLI or make sure lakectl is on PATH."
            ) from exc
        except subprocess.CalledProcessError as exc:
            raise SystemExit(
                f"Failed to download lakeFS object '{uri}': {exc.stderr.decode() or exc}"
            ) from exc
        downloaded.append(target)
    return downloaded


def _load_dataframe(files: Iterable[Path]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for file_path in files:
        suffix = file_path.suffix.lower()
        if suffix in {".parquet", ".pq"}:
            frames.append(pd.read_parquet(file_path))
        elif suffix == ".csv":
            frames.append(pd.read_csv(file_path))
        elif suffix in {".json", ".ndjson"}:
            frames.append(pd.read_json(file_path, lines=suffix == ".ndjson"))
        else:
            raise SystemExit(f"Unsupported file type '{suffix}' for expectation validation")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", required=True, help="Object key prefix to validate (e.g. raw/curve_landing)")
    parser.add_argument("--suite", required=True, help="Path to the Great Expectations suite JSON file")
    parser.add_argument(
        "--suite-name",
        default=None,
        help="Optional friendly name for logging (defaults to suite file stem)",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Succeed even if no matching files were present in the event",
    )
    args = parser.parse_args()

    event = _load_event()
    repo = event.get("repository_id") or event.get("repository")
    ref = event.get("source_ref") or event.get("ref")
    if not repo or not ref:
        raise SystemExit("lakeFS event did not include repository/ref metadata")

    paths = _collect_paths(event, args.prefix)
    if not paths:
        if args.allow_empty:
            print(f"No objects matching prefix '{args.prefix}' detected; skipping validation.")
            return
        raise SystemExit(f"No objects with prefix '{args.prefix}' found in event payload")

    lakectl = os.getenv("LAKECTL_CLI", "lakectl")
    downloaded = _download_objects(lakectl, repo, ref, paths)
    try:
        df = _load_dataframe(downloaded)
        if df.empty and not args.allow_empty:
            raise SystemExit(
                f"Downloaded data for prefix '{args.prefix}' is empty; failing validation"
            )
        enforce_expectation_suite(df, args.suite, suite_name=args.suite_name)
        print(
            f"Great Expectations suite '{args.suite_name or Path(args.suite).stem}' passed for prefix '{args.prefix}'"
        )
    finally:
        for file_path in downloaded:
            try:
                file_path.unlink(missing_ok=True)
            except Exception:
                pass
        # Remove temp dir root
        if downloaded:
            temp_root = downloaded[0].parent
            try:
                temp_root.rmdir()
            except OSError:
                pass


if __name__ == "__main__":
    main()
