from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


def parse_files(paths: Iterable[Path], *, as_of: Optional[date] = None) -> pd.DataFrame:  # pragma: no cover - stub
    return pd.DataFrame()


def write_output(
    df: pd.DataFrame,
    output_dir: Path | str,
    *,
    as_of: Optional[date],
    fmt: str,
) -> None:  # pragma: no cover - stub
    return None


