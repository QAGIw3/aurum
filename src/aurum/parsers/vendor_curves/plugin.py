"""Plugin architecture for vendor curve parsers."""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict

import logging

import pandas as pd

from .formats import FileFormat, LoadedDocument, detect_format, load_document
from .performance import PerformanceTracker
from .error_recovery import ErrorRecoveryEngine
from ..ml_parser import CurveAnomalyDetector
from ..schema_inference import SchemaInferenceEngine
from ..validation_engine import ValidationEngine
from .schema import CANONICAL_COLUMNS
from .. import learned_aliases

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParserContext:
    """Context provided to parser plugins."""

    vendor: str
    asof: date
    path: str
    format: FileFormat
    options: Mapping[str, Any]


@dataclass(frozen=True)
class ParseDiagnostics:
    """Diagnostics captured during parsing."""

    vendor: str
    format: FileFormat
    schema_confidence: float
    validation_confidence: float
    anomaly_confidence: float
    issues: tuple[str, ...]
    warnings: tuple[str, ...]
    anomalies: pd.DataFrame
    recovery_corrections: Mapping[str, int]
    schema_mapping: Mapping[str, str]
    missing_columns: tuple[str, ...]
    unexpected_columns: tuple[str, ...]
    validation_issues: tuple[str, ...]
    performance: Mapping[str, float]
    rows: int


@dataclass(frozen=True)
class ParseResult:
    """Wraps the parsed dataframe with diagnostics."""

    dataframe: pd.DataFrame
    diagnostics: ParseDiagnostics


class VendorParserPlugin:
    """Base class for vendor-specific parser implementations."""

    name: str
    supported_formats: tuple[FileFormat, ...] | None = None

    def parse(self, context: ParserContext, document: LoadedDocument) -> pd.DataFrame:
        raise NotImplementedError


class FunctionVendorPlugin(VendorParserPlugin):
    """Adapter bridging legacy function-style parsers into plugin registry."""

    def __init__(self, name: str, func: Callable[[str, date], pd.DataFrame]) -> None:
        self.name = name
        self._func = func
        self.supported_formats = None

    def parse(self, context: ParserContext, document: LoadedDocument) -> pd.DataFrame:  # noqa: ARG002
        return self._func(context.path, context.asof)


class VendorParserRegistry:
    """Registry orchestrating parser plugins and advanced capabilities."""

    def __init__(self) -> None:
        self._plugins: Dict[str, VendorParserPlugin] = {}
        self.schema_engine = SchemaInferenceEngine()
        self.validation_engine = ValidationEngine()
        self.anomaly_detector = CurveAnomalyDetector()
        self.error_recovery = ErrorRecoveryEngine()

    def register(self, name: str, parser: VendorParserPlugin | Callable[[str, date], pd.DataFrame]) -> None:
        if isinstance(parser, VendorParserPlugin):
            plugin = parser
        else:
            plugin = FunctionVendorPlugin(name, parser)
        LOGGER.debug("Registering vendor parser '%s' with formats %s", name, plugin.supported_formats)
        self._plugins[name] = plugin

    # Mapping-like helpers for backwards compatibility
    def __contains__(self, name: str) -> bool:  # pragma: no cover - trivial
        return name in self._plugins

    def __getitem__(self, name: str) -> VendorParserPlugin:  # pragma: no cover - trivial
        return self._plugins[name]

    def __iter__(self):  # pragma: no cover - trivial
        return iter(self._plugins)

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._plugins)

    def pop(self, name: str, default: Any | None = None) -> Any:  # pragma: no cover - trivial
        return self._plugins.pop(name, default)

    def discover_entrypoint_plugins(self, group: str = "aurum.vendor_parsers") -> int:
        """Discover and register parser plugins via entry points.

        External packages can expose vendor parsers by defining entry points in
        the ``group``. Each entry point should resolve to either:
        - a ``VendorParserPlugin`` instance, or
        - a callable ``(path: str, asof: date) -> pd.DataFrame``

        The entry point name is used as the vendor key unless the object has a
        ``name`` attribute, in which case that value is preferred.

        Returns the number of successfully registered plugins.
        """
        try:  # pragma: no cover - importlib.metadata availability depends on runtime
            from importlib.metadata import entry_points
        except Exception:  # pragma: no cover - extremely unlikely
            return 0

        count = 0
        try:
            eps = entry_points()
            if hasattr(eps, "select"):
                candidates = eps.select(group=group)  # type: ignore[attr-defined]
            elif isinstance(eps, dict):  # Python <3.10 back-compat
                candidates = eps.get(group, [])
            else:
                candidates = [ep for ep in eps if getattr(ep, "group", None) == group]
        except Exception:
            return 0

        for ep in candidates:
            try:
                obj = ep.load()
                name = getattr(obj, "name", ep.name)
                self.register(name, obj)
                count += 1
                LOGGER.info("Discovered and registered vendor parser '%s' from entry point '%s'", name, ep.name)
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.warning("Failed to load vendor parser entry point '%s': %s", getattr(ep, "name", "?"), exc)
        return count

    def parse(
        self,
        vendor: str,
        path: str,
        asof: date,
        *,
        return_result: bool = False,
        options: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame | ParseResult:
        try:
            plugin = self._plugins[vendor]
        except KeyError as exc:  # pragma: no cover - defensive
            raise ValueError(f"No parser registered for vendor '{vendor}'") from exc

        tracker = PerformanceTracker()
        if isinstance(plugin, FunctionVendorPlugin):
            with tracker.track("detect_format"):
                fmt = FileFormat.UNKNOWN
                if path:
                    fmt = detect_format(path)
            document = LoadedDocument(format=fmt, payload=None)
        else:
            with tracker.track("load_document"):
                document = load_document(path)

        context = ParserContext(vendor=vendor, asof=asof, path=path, format=document.format, options=options or {})

        # Build configurable engines per-call to respect options without mutating registry-wide defaults
        anomaly_cfg = (options or {}).get("anomaly", {}) if options else {}
        validation_cfg = (options or {}).get("validation", {}) if options else {}
        schema_cfg = (options or {}).get("schema", {}) if options else {}

        anomaly_detector = CurveAnomalyDetector(
            value_columns=anomaly_cfg.get("value_columns", self.anomaly_detector.value_columns),
            group_column=anomaly_cfg.get("group_column", self.anomaly_detector.group_column),
            zscore_threshold=float(anomaly_cfg.get("zscore_threshold", self.anomaly_detector.zscore_threshold)),
            min_points=int(anomaly_cfg.get("min_points", self.anomaly_detector.min_points)),
        )

        validation_engine = ValidationEngine(
            required_columns=validation_cfg.get("required_columns", self.validation_engine.required_columns),
            min_confidence=float(validation_cfg.get("min_confidence", self.validation_engine.min_confidence)),
        )

        # Merge learned aliases with any overrides provided via options
        learned = learned_aliases.load()
        alias_candidates = schema_cfg.get("alias_candidates")
        merged_aliases = (
            learned_aliases.merge(alias_candidates, learned)
            if (alias_candidates or learned)
            else None
        )
        schema_engine = SchemaInferenceEngine(
            required_columns=schema_cfg.get("required_columns", None),
            alias_candidates=merged_aliases,
        )

        with tracker.track("plugin_parse"):
            frame = plugin.parse(context, document)

        if frame is None:
            frame = pd.DataFrame(columns=CANONICAL_COLUMNS)

        candidate = document.payload if isinstance(document.payload, (pd.DataFrame, Mapping)) else frame

        with tracker.track("schema_inference"):
            schema_result = schema_engine.infer(candidate)
            if schema_result.column_mapping:
                frame = schema_result.rename(frame)

        frame = self._ensure_canonical_columns(frame)

        # Stabilise the index to avoid downstream edge-cases in pandas ops
        frame = frame.reset_index(drop=True)

        with tracker.track("error_recovery"):
            frame, recovery_report = self.error_recovery.apply(frame)

        with tracker.track("validation"):
            validation_result = validation_engine.validate(frame)

        with tracker.track("anomaly_detection"):
            anomaly_result = anomaly_detector.detect(frame)

        tracker.metrics.mark_rows(len(frame))

        # Include human-friendly notes about inferred column mappings as additional validation messages
        mapping_messages = tuple(
            f"Mapped alias column '{original}' -> '{canonical}'"
            for original, canonical in schema_result.column_mapping.items()
            if str(original) != str(canonical)
        )

        diagnostics = ParseDiagnostics(
            vendor=vendor,
            format=context.format,
            schema_confidence=schema_result.confidence,
            validation_confidence=validation_result.confidence,
            anomaly_confidence=anomaly_result.confidence_score,
            issues=tuple(issue.message for issue in validation_result.issues if issue.severity == "error"),
            warnings=tuple(issue.message for issue in validation_result.issues if issue.severity == "warning"),
            anomalies=anomaly_result.anomalies,
            recovery_corrections=recovery_report.corrections,
            schema_mapping=schema_result.column_mapping,
            missing_columns=schema_result.missing_columns,
            unexpected_columns=schema_result.unexpected_columns,
            validation_issues=tuple(issue.message for issue in validation_result.issues) + mapping_messages,
            performance=dict(tracker.metrics.durations),
            rows=len(frame),
        )

        if return_result:
            return ParseResult(dataframe=frame, diagnostics=diagnostics)
        return frame

    def _ensure_canonical_columns(self, frame: pd.DataFrame) -> pd.DataFrame:
        # Construct a fresh DataFrame in canonical order to avoid pandas internal edge cases
        length = len(frame)
        data: dict[str, pd.Series] = {}
        for column in CANONICAL_COLUMNS:
            if column in frame.columns:
                # Use the existing series (copy to avoid chained assignment surprises)
                data[column] = frame[column].copy()
            else:
                data[column] = pd.Series([pd.NA] * length)
        return pd.DataFrame(data)


registry = VendorParserRegistry()
register = registry.register
parse_vendor = registry.parse


__all__ = [
    "VendorParserRegistry",
    "VendorParserPlugin",
    "FunctionVendorPlugin",
    "ParseDiagnostics",
    "ParseResult",
    "ParserContext",
    "register",
    "parse_vendor",
]
