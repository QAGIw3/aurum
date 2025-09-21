#!/usr/bin/env python3
"""Print or apply Airflow variable settings for catalog-driven EIA datasets."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / 'config' / 'eia_ingest_datasets.json'
BULK_CONFIG_PATH = BASE_DIR / 'config' / 'eia_bulk_datasets.json'


def load_config(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding='utf-8'))
    datasets = data.get('datasets', [])
    if not isinstance(datasets, list):
        raise ValueError("Expected 'datasets' array in config")
    return datasets


def build_variable_map(datasets: list[dict]) -> dict[str, str]:
    variables: dict[str, str] = {}
    for dataset in datasets:
        topic_var = dataset.get('topic_var')
        units_var = dataset.get('units_var')
        default_topic = dataset.get('default_topic')
        default_units = dataset.get('default_units')
        if topic_var and default_topic:
            variables[topic_var] = default_topic
        if units_var and default_units:
            variables[units_var] = default_units
    return variables


def build_bulk_variable_map(datasets: list[dict]) -> dict[str, str]:
    variables: dict[str, str] = {}
    for dataset in datasets:
        topic_var = dataset.get('topic_var')
        default_topic = dataset.get('default_topic')
        if topic_var and default_topic:
            variables[topic_var] = default_topic
        units_var = dataset.get('units_var')
        default_units = dataset.get('default_units')
        if units_var and default_units:
            variables[units_var] = default_units
    return variables


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Call `airflow variables set` for each variable')
    parser.add_argument('--config', default=str(CONFIG_PATH), help='Path to eia_ingest_datasets.json')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found at {config_path}", file=sys.stderr)
        return 1
    datasets = load_config(config_path)
    variables = build_variable_map(datasets)
    if BULK_CONFIG_PATH.exists():
        bulk_datasets = load_config(BULK_CONFIG_PATH)
        variables.update(build_bulk_variable_map(bulk_datasets))
    if not variables:
        print("No variables detected in config", file=sys.stderr)
        return 0

    if args.apply:
        for key, value in variables.items():
            cmd = ['airflow', 'variables', 'set', key, value]
            print(' '.join(cmd))
            subprocess.run(cmd, check=True)
    else:
        for key, value in variables.items():
            print(f"airflow variables set {key} {value}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
