"""Advanced curve analytics engine orchestration module."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Iterable, Literal, Mapping, Sequence

import numpy as np
import pandas as pd

from . import interpolation, pattern_recognition

CurveScalar = float
CurveArrayLike = Sequence[CurveScalar] | np.ndarray | pd.Series


@dataclass
class CurveMetadata:
    """Describes identifying information and conventions for a curve."""

    curve_key: str
    as_of: pd.Timestamp
    currency: str | None = None
    tenor_type: str | None = None
    price_type: str | None = None
    day_count: str | None = None
    calendar: str | None = None
    asset_class: str | None = None
    extra: dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Mapping[str, object]) -> "CurveMetadata":
        """Create metadata from a canonical curve row."""

        instance = cls(
            curve_key=str(row.get("curve_key", "")),
            as_of=pd.to_datetime(row.get("asof_date", row.get("as_of", datetime.utcnow()))),
            currency=cls._optional_str(row.get("currency")),
            tenor_type=cls._optional_str(row.get("tenor_type")),
            price_type=cls._optional_str(row.get("price_type")),
            day_count=cls._optional_str(row.get("day_count")),
            calendar=cls._optional_str(row.get("calendar")),
            asset_class=cls._optional_str(row.get("asset_class")),
            extra={k: v for k, v in row.items() if k not in {
                "curve_key",
                "asof_date",
                "as_of",
                "currency",
                "tenor_type",
                "price_type",
                "day_count",
                "calendar",
                "asset_class",
            }},
        )
        return instance

    @staticmethod
    def _optional_str(value: object) -> str | None:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        return str(value)


@dataclass
class Curve:
    """Numeric representation of a single financial curve."""

    tenors: np.ndarray
    values: np.ndarray
    metadata: CurveMetadata
    measure: str = "value"
    timestamps: np.ndarray | None = None
    quality_flags: np.ndarray | None = None

    def __post_init__(self) -> None:
        if self.tenors.shape != self.values.shape:
            raise ValueError("tenors and values must have identical shape")
        if self.timestamps is not None and self.timestamps.shape != self.values.shape:
            raise ValueError("timestamps must match the number of curve points")
        if self.quality_flags is not None and self.quality_flags.shape != self.values.shape:
            raise ValueError("quality_flags must match the number of curve points")

    @property
    def size(self) -> int:
        return int(self.tenors.size)

    def to_dataframe(self) -> pd.DataFrame:
        """Return the curve as a tidy DataFrame."""

        df = pd.DataFrame({"tenor": self.tenors, self.measure: self.values})
        if self.timestamps is not None:
            df["timestamp"] = self.timestamps
        if self.quality_flags is not None:
            df["quality_flag"] = self.quality_flags
        for key, value in self.metadata.extra.items():
            df[key] = value
        df.attrs["metadata"] = self.metadata
        return df

    def copy_with(self, *, values: np.ndarray | None = None, measure: str | None = None) -> "Curve":
        """Return a shallow copy with updated arrays/metadata."""

        return Curve(
            tenors=self.tenors.copy(),
            values=self.values.copy() if values is None else np.asarray(values, dtype=float),
            metadata=replace(self.metadata),
            measure=measure or self.measure,
            timestamps=None if self.timestamps is None else self.timestamps.copy(),
            quality_flags=None if self.quality_flags is None else self.quality_flags.copy(),
        )

    def sort(self) -> "Curve":
        """Return a version sorted by tenor ascending."""

        order = np.argsort(self.tenors)
        return Curve(
            tenors=self.tenors[order],
            values=self.values[order],
            metadata=replace(self.metadata),
            measure=self.measure,
            timestamps=None if self.timestamps is None else self.timestamps[order],
            quality_flags=None if self.quality_flags is None else self.quality_flags[order],
        )


@dataclass(slots=True)
class CurveAnalyticsConfig:
    """Configuration for analytics orchestration."""

    default_interpolation: str = "cspline"
    default_extrapolation: str = "flat_forward"
    default_smoother: str = "tikhonov"
    default_cluster_method: str = "kmeans"
    default_similarity_metric: str = "euclidean"
    enforce_monotonicity: bool = False
    tenor_parser: Literal["auto", "label", "numeric"] = "auto"
    smoothing_strength: float = 1.0


def parse_tenor_label(label: str | float | int) -> float:
    """Convert a tenor label into a year fraction."""

    if isinstance(label, (int, float)):
        return float(label)
    label = str(label).strip().upper()
    if not label:
        raise ValueError("Empty tenor label encountered")
    magnitude_str = ""
    unit = ""
    for char in label:
        if char.isdigit() or char == ".":
            magnitude_str += char
        else:
            unit += char
    magnitude = float(magnitude_str or 0.0)
    unit = unit or "Y"
    if unit == "D":
        return magnitude / 365.0
    if unit == "W":
        return magnitude / 52.0
    if unit == "M":
        return magnitude / 12.0
    if unit == "Q":
        return magnitude / 4.0
    if unit == "S":  # season assumed half-year
        return magnitude * 0.5
    if unit == "Y":
        return magnitude
    raise ValueError(f"Unsupported tenor unit: {unit}")


def _coerce_tenors(tenors: CurveArrayLike, parser: Literal["auto", "label", "numeric"] = "auto") -> np.ndarray:
    series = pd.Series(tenors)
    if parser == "numeric":
        values = pd.to_numeric(series, errors="raise")
        return values.to_numpy(dtype=float)
    if parser == "label" or (parser == "auto" and series.dtype == object):
        return series.apply(parse_tenor_label).to_numpy(dtype=float)
    return pd.to_numeric(series, errors="coerce").fillna(method="ffill").fillna(method="bfill").to_numpy(dtype=float)


def curve_from_dataframe(
    df: pd.DataFrame,
    *,
    value_column: str,
    tenor_column: str,
    metadata_fields: Sequence[str] | None = None,
    parser: Literal["auto", "label", "numeric"] = "auto",
    measure: str | None = None,
) -> Curve:
    """Create a curve from a tidy DataFrame."""

    default_fields = [
        "curve_key",
        "asof_date",
        "as_of",
        "currency",
        "tenor_type",
        "price_type",
        "day_count",
        "calendar",
        "asset_class",
    ]
    if not metadata_fields:
        metadata_fields = [field for field in default_fields if field in df.columns]
    else:
        metadata_fields = list(dict.fromkeys(metadata_fields))
    if value_column not in df:
        raise KeyError(f"Missing value column '{value_column}'")
    if tenor_column not in df:
        raise KeyError(f"Missing tenor column '{tenor_column}'")
    tenors = _coerce_tenors(df[tenor_column], parser=parser)
    values = pd.to_numeric(df[value_column], errors="coerce").to_numpy(dtype=float)
    meta_dict: dict[str, object] = {}
    for field in metadata_fields:
        if field in df:
            unique_values = df[field].dropna().unique()
            meta_dict[field] = unique_values[0] if unique_values.size else None
    if "curve_key" not in meta_dict:
        meta_dict["curve_key"] = df.attrs.get("curve_key", "")
    if "asof_date" not in meta_dict and "as_of" not in meta_dict:
        meta_dict["asof_date"] = df.attrs.get("as_of") or df.attrs.get("asof_date")
    metadata = CurveMetadata.from_row(meta_dict)
    timestamps = df["timestamp"].to_numpy() if "timestamp" in df else None
    quality_flags = df["quality_flag"].to_numpy() if "quality_flag" in df else None
    return Curve(
        tenors=tenors,
        values=values,
        metadata=metadata,
        measure=measure or value_column,
        timestamps=timestamps,
        quality_flags=quality_flags,
    ).sort()


def curves_from_dataframe(
    df: pd.DataFrame,
    *,
    value_column: str,
    tenor_column: str,
    metadata_fields: Sequence[str] | None = None,
    group_keys: Sequence[str] | str | None = None,
    parser: Literal["auto", "label", "numeric"] = "auto",
) -> list[Curve]:
    """Split a DataFrame into multiple curves keyed by metadata columns."""

    if group_keys is None:
        group_keys = [key for key in ("curve_key", "asof_date", "as_of") if key in df.columns]
        if not group_keys:
            raise ValueError("group_keys required when curve_key/asof_date columns are missing")
    if isinstance(group_keys, str):
        group_keys = [group_keys]

    curves: list[Curve] = []
    for key_values, group in df.groupby(group_keys, dropna=False, sort=True):
        if not isinstance(key_values, tuple):
            key_values = (key_values,)
        metadata_overrides = {column: value for column, value in zip(group_keys, key_values)}
        merged_metadata_fields = list(dict.fromkeys((metadata_fields or []) + list(group_keys)))
        curve_df = group.copy()
        for column, value in metadata_overrides.items():
            curve_df[column] = value
        curve = curve_from_dataframe(
            curve_df,
            value_column=value_column,
            tenor_column=tenor_column,
            metadata_fields=merged_metadata_fields,
            parser=parser,
        )
        curves.append(curve)
    return curves


@dataclass
class CurveCollection:
    """Container for a sequence of curves keyed by timestamp."""

    curves: list[Curve]

    def to_dataframe(self) -> pd.DataFrame:
        frames = []
        for curve in self.curves:
            df = curve.to_dataframe()
            df["curve_key"] = curve.metadata.curve_key
            df["as_of"] = curve.metadata.as_of
            frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class CurveAnalyticsEngine:
    """Facade that coordinates curve analytics workflows."""

    def __init__(self, config: CurveAnalyticsConfig | None = None):
        self.config = config or CurveAnalyticsConfig()

    # --- Core curve operations -------------------------------------------------

    def interpolate(
        self,
        curve: Curve,
        *,
        method: str | None = None,
        targets: CurveArrayLike,
        constraints: interpolation.InterpolationConstraints | None = None,
    ) -> np.ndarray:
        chosen_method = method or self.config.default_interpolation
        return interpolation.interpolate(
            curve.tenors,
            curve.values,
            targets=_coerce_tenors(targets, parser="numeric"),
            method=chosen_method,
            constraints=constraints,
        )

    def extrapolate(
        self,
        curve: Curve,
        *,
        method: str | None = None,
        horizon: CurveArrayLike,
        constraints: interpolation.ExtrapolationConstraints | None = None,
    ) -> np.ndarray:
        chosen_method = method or self.config.default_extrapolation
        return interpolation.extrapolate(
            curve.tenors,
            curve.values,
            horizon=_coerce_tenors(horizon, parser="numeric"),
            method=chosen_method,
            constraints=constraints,
        )

    def smooth(
        self,
        curve: Curve,
        *,
        method: str | None = None,
        strength: float | None = None,
        preserve_monotonicity: bool | None = None,
    ) -> Curve:
        chosen_method = method or self.config.default_smoother
        params = interpolation.SmootherParams(
            strength=strength if strength is not None else self.config.smoothing_strength,
            enforce_monotonicity=preserve_monotonicity if preserve_monotonicity is not None else self.config.enforce_monotonicity,
        )
        smoothed_values, diagnostics = interpolation.smooth(curve.tenors, curve.values, method=chosen_method, params=params)
        result = curve.copy_with(values=smoothed_values, measure=f"{curve.measure}_smoothed")
        result.metadata.extra = {
            **(curve.metadata.extra or {}),
            "smoothing_method": chosen_method,
            "smoothing_diagnostics": diagnostics,
        }
        return result

    # --- Feature extraction ----------------------------------------------------

    def extract_features(self, curve: Curve) -> pattern_recognition.CurveFeatures:
        return pattern_recognition.extract_features(curve)

    def cluster(
        self,
        curves: Sequence[Curve],
        *,
        method: str | None = None,
        n_clusters: int | None = None,
    ) -> pattern_recognition.ClusterResult:
        chosen_method = method or self.config.default_cluster_method
        return pattern_recognition.cluster_curves(curves, method=chosen_method, n_clusters=n_clusters)

    def find_similar(
        self,
        curve: Curve,
        others: Sequence[Curve],
        *,
        metric: str | None = None,
        k: int = 5,
    ) -> pattern_recognition.SimilarityResult:
        chosen_metric = metric or self.config.default_similarity_metric
        return pattern_recognition.find_similar_curves(curve, others, metric=chosen_metric, k=k)

    def classify_shape(self, curve: Curve) -> pattern_recognition.ShapeClassification:
        return pattern_recognition.classify_curve_shape(curve)

    # --- Historical analytics --------------------------------------------------

    def historical_trends(
        self,
        curves: Sequence[Curve],
        *,
        window: int = 60,
    ) -> pattern_recognition.HistoricalTrendResult:
        return pattern_recognition.compute_historical_trends(curves, window=window)

    # --- Scenario analysis -----------------------------------------------------

    def generate_scenarios(
        self,
        curve: Curve,
        templates: Sequence[pattern_recognition.ScenarioTemplate],
    ) -> list[pattern_recognition.CurveScenario]:
        return pattern_recognition.generate_curve_scenarios(curve, templates)

    def apply_scenario(
        self,
        curve: Curve,
        scenario: pattern_recognition.CurveScenario,
    ) -> Curve:
        new_values = pattern_recognition.apply_curve_scenario(curve, scenario)
        applied = curve.copy_with(values=new_values, measure=f"{curve.measure}_{scenario.name}")
        applied.metadata.extra = {
            **(curve.metadata.extra or {}),
            "scenario_name": scenario.name,
            "scenario_payload": scenario.payload,
        }
        return applied

    # --- Visualization ---------------------------------------------------------

    def to_plot_payload(
        self,
        curve: Curve,
        *,
        overlays: Sequence[Curve] | None = None,
        title: str | None = None,
    ) -> dict[str, object]:
        return pattern_recognition.build_plot_payload(curve, overlays=overlays, title=title)

    # --- Performance benchmarking ---------------------------------------------

    def benchmark(
        self,
        curves: Sequence[Curve],
        *,
        repeat: int = 3,
    ) -> pattern_recognition.BenchmarkResult:
        return pattern_recognition.run_benchmarks(curves, repeat=repeat)


__all__ = [
    "Curve",
    "CurveAnalyticsConfig",
    "CurveAnalyticsEngine",
    "CurveCollection",
    "CurveMetadata",
    "curve_from_dataframe",
    "curves_from_dataframe",
    "parse_tenor_label",
]
