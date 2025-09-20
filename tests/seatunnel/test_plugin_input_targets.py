from __future__ import annotations

import re
from pathlib import Path


TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "seatunnel" / "jobs"


def test_sink_plugin_input_matches_transform_output() -> None:
    pattern_sink = re.compile(r"sink\s*\{[\s\S]*?\}\s*\}")
    pattern_plugin_input = re.compile(r"plugin_input\s*=\s*\"([A-Za-z0-9_]+)\"")
    pattern_result_table = re.compile(r"result_table_name\s*=\s*\"([A-Za-z0-9_]+)\"")

    for tmpl in TEMPLATES_DIR.glob("*.conf.tmpl"):
        text = tmpl.read_text(encoding="utf-8")
        # Capture all transform outputs
        outputs = set(pattern_result_table.findall(text))
        # Look only inside sink blocks for plugin_input(s)
        for sink_block in pattern_sink.findall(text):
            for match in pattern_plugin_input.finditer(sink_block):
                target = match.group(1)
                assert (
                    target in outputs
                ), f"plugin_input '{target}' not produced by any transform in {tmpl.name}"

