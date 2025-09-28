"""Example vendor parser exposed via entry points for discovery.

This plugin is intentionally simple and returns a minimal canonical frame.
"""
from __future__ import annotations

from datetime import date
import pandas as pd


def parse_demo(path: str, asof: date) -> pd.DataFrame:
    # Ignore path; emit a single demo row
    return pd.DataFrame(
        {
            "tenor_label": ["2025-01"],
            "mid": [42.0],
            "curve_key": ["demo_ep|example"],
            "asof_date": [asof],
            "sheet_name": ["demo"],
        }
    )


__all__ = ["parse_demo"]

