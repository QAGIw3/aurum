"""Advanced vendor curve parsing framework with plugin architecture."""
from __future__ import annotations

from datetime import date
from typing import Callable, Mapping

import pandas as pd

from .formats import FileFormat
from .plugin import (
    ParseDiagnostics,
    ParseResult,
    VendorParserPlugin,
    parse_vendor,
    register as register_plugin,
    registry,
)

PARSERS = registry  # Backwards compatibility alias


def register(name: str, parser: VendorParserPlugin | Callable[[str, date], pd.DataFrame]) -> None:
    """Register a vendor parser implementation."""
    register_plugin(name, parser)  # type: ignore[arg-type]


def parse(vendor: str, path: str, asof: date, *, options: Mapping[str, object] | None = None) -> pd.DataFrame:
    """Parse the vendor document and return a canonical dataframe."""
    result = parse_vendor(vendor, path, asof, return_result=False, options=options)
    if isinstance(result, pd.DataFrame):  # Legacy behaviour
        return result
    return result.dataframe


def parse_with_diagnostics(
    vendor: str,
    path: str,
    asof: date,
    *,
    options: Mapping[str, object] | None = None,
) -> ParseResult:
    """Parse the vendor document returning diagnostics and dataframe."""
    result = parse_vendor(vendor, path, asof, return_result=True, options=options)
    if isinstance(result, ParseResult):
        return result
    # In practice ``parse_vendor`` returns ParseResult when ``return_result`` is True
    # but keep a safeguard for backwards compatibility.
    return ParseResult(dataframe=result, diagnostics=_empty_diagnostics())


def _empty_diagnostics() -> ParseDiagnostics:
    return ParseDiagnostics(
        vendor="",
        format=FileFormat.UNKNOWN,
        schema_confidence=0.0,
        validation_confidence=0.0,
        anomaly_confidence=0.0,
        issues=tuple(),
        warnings=tuple(),
        anomalies=pd.DataFrame(),
        recovery_corrections={},
        schema_mapping={},
        missing_columns=tuple(),
        unexpected_columns=tuple(),
        validation_issues=tuple(),
        performance={},
        rows=0,
    )


# Register built-in parsers
from . import parse_pw  # noqa: F401
from . import parse_eugp  # noqa: F401
from . import parse_rp  # noqa: F401
from . import parse_simple  # noqa: F401
from . import generic  # noqa: F401

# Optional: discover third-party plugins via entry points
try:  # pragma: no cover - discovery tested in integration contexts
    registry.discover_entrypoint_plugins()
except Exception:
    pass

__all__ = [
    "register",
    "parse",
    "parse_with_diagnostics",
    "ParseResult",
    "ParseDiagnostics",
    "VendorParserPlugin",
]
