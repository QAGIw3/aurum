"""Pattern recognition, clustering, and analytics for curves."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Sequence

import time

import numpy as np
import pandas as pd

from .analytics_engine import Curve
from . import interpolation

try:  # Optional scikit-learn components for clustering and similarity
    from sklearn.cluster import AgglomerativeClustering, DBSCAN, KMeans
    from sklearn.metrics import silhouette_score
except ImportError:  # pragma: no cover - allow the package to load without sklearn
    AgglomerativeClustering = None  # type: ignore[assignment]
    DBSCAN = None  # type: ignore[assignment]
    KMeans = None  # type: ignore[assignment]
    silhouette_score = None  # type: ignore[assignment]
    HAS_SKLEARN = False
else:  # pragma: no cover
    HAS_SKLEARN = True


@dataclass(slots=True)
class CurveFeatures:
    """Numeric feature representation of a curve."""

    curve_key: str
    as_of: pd.Timestamp | None
    measure: str
    vector: np.ndarray
    components: dict[str, float]
    grid: np.ndarray
    grid_values: np.ndarray
    normalized_grid: np.ndarray


@dataclass(slots=True)
class ClusterResult:
    """Output of a clustering pass across curves."""

    method: str
    labels: np.ndarray
    centers: np.ndarray
    metrics: dict[str, float | int | None]
    features: list[CurveFeatures]


@dataclass(slots=True)
class SimilarNeighbor:
    """Single neighbor entry in a similarity search."""

    curve_key: str
    as_of: pd.Timestamp | None
    distance: float
    rank: int


@dataclass(slots=True)
class SimilarityResult:
    """K-nearest neighbors result for a target curve."""

    metric: str
    target: CurveFeatures
    neighbors: list[SimilarNeighbor]


@dataclass(slots=True)
class PatternDetection:
    """Detailed shape diagnostics for a curve."""

    num_peaks: int
    num_troughs: int
    peak_positions: list[float]
    trough_positions: list[float]
    monotonic: bool
    comments: str | None


@dataclass(slots=True)
class ShapeClassification:
    """Classification output with confidence and supporting evidence."""

    label: str
    confidence: float
    evidence: dict[str, float]
    alternatives: list[tuple[str, float]]


@dataclass(slots=True)
class HistoricalTrendResult:
    """Historical curve analytics including PCA factors and rolling trends."""

    grid: np.ndarray
    values: pd.DataFrame
    components: np.ndarray
    factor_scores: pd.DataFrame
    explained_variance: np.ndarray
    rolling_slopes: pd.DataFrame
    rolling_zscores: pd.DataFrame
    metadata: dict[str, object]


@dataclass(slots=True)
class ScenarioTemplate:
    """Blueprint for generating curve stress scenarios."""

    name: str
    scenario_type: Literal["parallel", "twist", "butterfly", "local", "custom"]
    magnitude: float
    pivot: float | None = None
    width: float | None = None
    units: Literal["absolute", "relative"] = "absolute"
    payload: dict[str, object] | None = None
    custom_fn: Callable[[Curve], np.ndarray] | None = None


@dataclass(slots=True)
class CurveScenario:
    """Concrete scenario with tenor-aligned shock values."""

    name: str
    tenors: np.ndarray
    shock: np.ndarray
    units: Literal["absolute", "relative"]
    payload: dict[str, object]


@dataclass(slots=True)
class BenchmarkResult:
    """Performance measurements across core analytics operations."""

    timings: dict[str, float]
    per_curve: list[dict[str, float]]
    repeat: int
    metadata: dict[str, object]


def extract_features(
    curve: Curve,
    *,
    grid_size: int = 21,
    normalized: bool = True,
) -> CurveFeatures:
    """Compute descriptive statistics and embeddings for downstream analytics."""

    if curve.size < 2:
        raise ValueError("Curve must contain at least two points for feature extraction")

    tenors = curve.tenors
    values = curve.values
    tenor_span = float(tenors[-1] - tenors[0])
    if tenor_span <= 0:
        tenor_span = float(np.maximum(tenors.max() - tenors.min(), 1e-6))

    grid = np.linspace(tenors[0], tenors[-1], grid_size)
    grid_values = interpolation.interpolate(tenors, values, targets=grid, method="linear_zero")

    level = float(np.mean(values))
    slope = float((values[-1] - values[0]) / tenor_span)
    curvature = float(_estimate_curvature(tenors, values))
    convexity = float(_estimate_convexity(tenors, values))
    volatility = float(np.std(np.diff(values))) if values.size > 2 else 0.0
    piecewise_slopes = _piecewise_slopes(grid, grid_values, segments=4)
    dv01_distribution = _dv01_distribution(tenors, values, buckets=4)

    normalized_grid = grid_values.copy()
    if normalized:
        std = float(np.std(normalized_grid))
        if std == 0:
            std = 1.0
        normalized_grid = (normalized_grid - float(level)) / std

    vector = np.concatenate([
        np.array([level, slope, curvature, convexity, volatility], dtype=float),
        piecewise_slopes,
        dv01_distribution,
        normalized_grid,
    ])

    components: dict[str, float] = {
        "level": level,
        "slope": slope,
        "curvature": curvature,
        "convexity": convexity,
        "volatility": volatility,
    }
    for idx, value in enumerate(piecewise_slopes, start=1):
        components[f"slope_q{idx}"] = float(value)
    for idx, value in enumerate(dv01_distribution, start=1):
        components[f"dv01_q{idx}"] = float(value)

    return CurveFeatures(
        curve_key=curve.metadata.curve_key,
        as_of=curve.metadata.as_of,
        measure=curve.measure,
        vector=vector.astype(float),
        components=components,
        grid=grid,
        grid_values=grid_values.astype(float),
        normalized_grid=normalized_grid.astype(float),
    )


def _estimate_curvature(tenors: np.ndarray, values: np.ndarray) -> float:
    if tenors.size < 3:
        return 0.0
    second_diff = np.gradient(np.gradient(values, tenors), tenors)
    return float(np.mean(second_diff))


def _estimate_convexity(tenors: np.ndarray, values: np.ndarray) -> float:
    if tenors.size < 3:
        return 0.0
    second_diff = np.gradient(np.gradient(values, tenors), tenors)
    return float(np.mean(np.abs(second_diff)))


def _piecewise_slopes(grid: np.ndarray, grid_values: np.ndarray, *, segments: int) -> np.ndarray:
    segments = max(segments, 1)
    bucket_size = max(int(np.floor(grid.size / segments)), 1)
    slopes = []
    for idx in range(segments):
        start = idx * bucket_size
        end = min((idx + 1) * bucket_size, grid.size - 1)
        if end <= start:
            slopes.append(0.0)
            continue
        delta_v = grid_values[end] - grid_values[start]
        delta_t = grid[end] - grid[start]
        slopes.append(float(delta_v / delta_t) if delta_t != 0 else 0.0)
    return np.asarray(slopes, dtype=float)


def _dv01_distribution(tenors: np.ndarray, values: np.ndarray, *, buckets: int) -> np.ndarray:
    if tenors.size < 2:
        return np.zeros(buckets, dtype=float)
    normalized_tenors = (tenors - tenors.min()) / max(tenors.max() - tenors.min(), 1e-6)
    bucket_edges = np.linspace(0.0, 1.0, buckets + 1)
    bucket_values = np.zeros(buckets, dtype=float)
    for idx in range(buckets):
        mask = (normalized_tenors >= bucket_edges[idx]) & (normalized_tenors < bucket_edges[idx + 1])
        if not np.any(mask):
            continue
        avg_tenor = np.mean(tenors[mask])
        avg_value = np.mean(values[mask])
        bucket_values[idx] = abs(avg_value) * max(avg_tenor, 1e-6)
    total = bucket_values.sum()
    if total == 0:
        return bucket_values
    return bucket_values / total


def detect_patterns(curve: Curve) -> PatternDetection:
    """Detect key shape patterns such as peaks, troughs, and monotonicity."""

    tenors = curve.tenors
    values = curve.values
    if values.size < 3:
        return PatternDetection(0, 0, [], [], True, "Insufficient points for pattern detection")

    diffs = np.diff(values)
    sign = np.sign(diffs)
    for idx in range(1, sign.size):
        if sign[idx] == 0:
            sign[idx] = sign[idx - 1]
    if sign.size > 0 and sign[0] == 0:
        sign[0] = 1
    sign_changes = np.diff(sign)

    peaks_idx = [idx + 1 for idx, change in enumerate(sign_changes) if change < 0]
    troughs_idx = [idx + 1 for idx, change in enumerate(sign_changes) if change > 0]

    peaks = [float(tenors[idx]) for idx in peaks_idx if 0 <= idx < tenors.size]
    troughs = [float(tenors[idx]) for idx in troughs_idx if 0 <= idx < tenors.size]
    monotonic = not peaks and not troughs

    comments = None
    if monotonic:
        comments = "Monotonic curve"
    elif len(peaks) > len(troughs):
        comments = "Dominated by peaks"
    elif len(troughs) > len(peaks):
        comments = "Dominated by troughs"

    return PatternDetection(
        num_peaks=len(peaks),
        num_troughs=len(troughs),
        peak_positions=peaks,
        trough_positions=troughs,
        monotonic=monotonic,
        comments=comments,
    )


def _feature_matrix(curves: Sequence[Curve]) -> tuple[np.ndarray, list[CurveFeatures]]:
    features = [extract_features(curve) for curve in curves]
    matrix = np.vstack([feature.vector for feature in features]) if features else np.empty((0, 0))
    return matrix, features


def _standardize(matrix: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if matrix.size == 0:
        return matrix, np.zeros((1, 0)), np.ones((1, 0))
    mean = np.mean(matrix, axis=0, keepdims=True)
    std = np.std(matrix, axis=0, keepdims=True)
    std[std == 0] = 1.0
    standardized = (matrix - mean) / std
    return standardized, mean, std


def _cluster_centers(matrix: np.ndarray, labels: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return np.empty((0, 0))
    unique_labels = np.unique(labels)
    centers = []
    for label in unique_labels:
        mask = labels == label
        if not np.any(mask):
            continue
        centers.append(np.mean(matrix[mask], axis=0))
    return np.vstack(centers) if centers else np.empty((0, matrix.shape[1]))


def cluster_curves(
    curves: Sequence[Curve],
    *,
    method: Literal["kmeans", "hierarchical", "dbscan"] = "kmeans",
    n_clusters: int | None = None,
    eps: float | None = None,
    min_samples: int = 3,
    random_state: int = 42,
) -> ClusterResult:
    """Cluster curves using the requested algorithm."""

    if not curves:
        raise ValueError("At least one curve required for clustering")
    matrix, features = _feature_matrix(curves)
    standardized, mean, std = _standardize(matrix)
    method = method.lower()

    if method == "kmeans":
        if not HAS_SKLEARN or KMeans is None:
            raise ImportError("scikit-learn is required for k-means clustering")
        k = n_clusters or max(2, min(5, standardized.shape[0]))
        model = KMeans(n_clusters=k, random_state=random_state, n_init=10)
        labels = model.fit_predict(standardized)
        centers = model.cluster_centers_ * std + mean
        inertia = float(model.inertia_)
        silhouette = float(silhouette_score(standardized, labels)) if silhouette_score and len(np.unique(labels)) > 1 else None
        metrics = {
            "n_clusters": int(k),
            "inertia": inertia,
            "silhouette": silhouette,
        }
    elif method == "hierarchical":
        if not HAS_SKLEARN or AgglomerativeClustering is None:
            raise ImportError("scikit-learn is required for hierarchical clustering")
        k = n_clusters or max(2, min(5, standardized.shape[0]))
        model = AgglomerativeClustering(n_clusters=k, linkage="ward")
        labels = model.fit_predict(standardized)
        centers = _cluster_centers(matrix, labels)
        silhouette = float(silhouette_score(standardized, labels)) if silhouette_score and len(np.unique(labels)) > 1 else None
        metrics = {
            "n_clusters": int(len(np.unique(labels))),
            "silhouette": silhouette,
        }
    elif method == "dbscan":
        if not HAS_SKLEARN or DBSCAN is None:
            raise ImportError("scikit-learn is required for DBSCAN clustering")
        epsilon = eps if eps is not None else float(np.sqrt(matrix.shape[1]))
        model = DBSCAN(eps=epsilon, min_samples=max(min_samples, 2))
        labels = model.fit_predict(standardized)
        mask = labels != -1
        centers = _cluster_centers(matrix[mask], labels[mask]) if np.any(mask) else np.empty((0, matrix.shape[1]))
        noise_fraction = float(np.mean(labels == -1))
        valid_labels = labels[labels != -1]
        silhouette = (
            float(silhouette_score(standardized[labels != -1], valid_labels))
            if silhouette_score and np.unique(valid_labels).size > 1
            else None
        )
        metrics = {
            "n_clusters": int(np.unique(labels[labels != -1]).size),
            "noise_fraction": noise_fraction,
            "silhouette": silhouette,
        }
    else:
        raise ValueError(f"Unsupported clustering method: {method}")

    return ClusterResult(
        method=method,
        labels=labels,
        centers=centers,
        metrics=metrics,
        features=features,
    )


def find_similar_curves(
    target: Curve,
    others: Sequence[Curve],
    *,
    metric: Literal["euclidean", "cosine", "dtw"] = "euclidean",
    k: int = 5,
) -> SimilarityResult:
    if k <= 0:
        raise ValueError("k must be positive for similarity search")
    target_features = extract_features(target)
    other_features = [extract_features(curve) for curve in others]
    distances = [
        (features, _distance(target_features, features, metric))
        for features in other_features
    ]
    distances.sort(key=lambda item: item[1])
    neighbors = [
        SimilarNeighbor(
            curve_key=features.curve_key,
            as_of=features.as_of,
            distance=float(distance),
            rank=idx + 1,
        )
        for idx, (features, distance) in enumerate(distances[:k])
    ]
    return SimilarityResult(metric=metric, target=target_features, neighbors=neighbors)


def _distance(
    reference: CurveFeatures,
    candidate: CurveFeatures,
    metric: Literal["euclidean", "cosine", "dtw"],
) -> float:
    if metric == "euclidean":
        return float(np.linalg.norm(reference.vector - candidate.vector))
    if metric == "cosine":
        denom = np.linalg.norm(reference.vector) * np.linalg.norm(candidate.vector)
        if denom == 0:
            return 1.0
        cosine_similarity = np.dot(reference.vector, candidate.vector) / denom
        return float(1.0 - cosine_similarity)
    if metric == "dtw":
        return float(_dtw_distance(reference.normalized_grid, candidate.normalized_grid))
    raise ValueError(f"Unsupported similarity metric: {metric}")


def _dtw_distance(series_a: np.ndarray, series_b: np.ndarray) -> float:
    n, m = series_a.size, series_b.size
    cost_matrix = np.full((n + 1, m + 1), np.inf)
    cost_matrix[0, 0] = 0.0
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = abs(series_a[i - 1] - series_b[j - 1])
            cost_matrix[i, j] = cost + min(
                cost_matrix[i - 1, j],
                cost_matrix[i, j - 1],
                cost_matrix[i - 1, j - 1],
            )
    return cost_matrix[n, m] / (n + m)


def classify_curve_shape(curve: Curve) -> ShapeClassification:
    """Classify curve shape into canonical regimes with confidence scoring."""

    features = extract_features(curve)
    patterns = detect_patterns(curve)
    tenors = curve.tenors
    values = curve.values
    tenor_span = float(tenors[-1] - tenors[0]) if tenors.size > 1 else 1.0
    value_scale = float(np.std(values)) if values.size > 1 else float(abs(values[0]) if values.size else 1.0)
    value_scale = max(value_scale, 1e-6)
    slope_scale = value_scale / max(tenor_span, 1e-6)

    norm_slope = features.components.get("slope", 0.0) / slope_scale
    norm_curvature = features.components.get("curvature", 0.0) * tenor_span**2 / value_scale
    slopes = [features.components.get(f"slope_q{i}", 0.0) for i in range(1, 5)]
    norm_slopes = [s / slope_scale for s in slopes]

    steep_delta = norm_slopes[-1] - norm_slopes[0]
    flat_delta = norm_slopes[0] - norm_slopes[-1]

    scores: dict[str, float] = {
        "normal": max(0.0, norm_slope) + max(0.0, 1.5 - abs(steep_delta)) * 0.1,
        "inverted": max(0.0, -norm_slope),
        "steepener": max(0.0, steep_delta),
        "flattener": max(0.0, 1.0 - abs(norm_slope)) + max(0.0, flat_delta),
        "humped": float(patterns.num_peaks == 1 and patterns.num_troughs <= 1) * (1.0 + abs(norm_curvature)),
        "double_humped": float(patterns.num_peaks >= 2) * patterns.num_peaks,
        "s_shaped": float(patterns.num_peaks >= 1 and patterns.num_troughs >= 1) * (patterns.num_peaks + patterns.num_troughs),
    }

    # Ensure no zero-sum scenario
    total_score = sum(scores.values())
    if total_score == 0:
        scores = {key: 1.0 for key in scores}
        total_score = float(len(scores))

    label = max(scores, key=scores.get)
    confidence = float(scores[label] / total_score)

    alternatives = [
        (key, float(scores[key] / total_score))
        for key in sorted(scores, key=scores.get, reverse=True)
        if key != label
    ][:3]

    evidence = {
        "norm_slope": float(norm_slope),
        "norm_curvature": float(norm_curvature),
        "steep_delta": float(steep_delta),
        "flat_delta": float(flat_delta),
        "num_peaks": float(patterns.num_peaks),
        "num_troughs": float(patterns.num_troughs),
    }

    return ShapeClassification(
        label=label,
        confidence=min(confidence, 1.0),
        evidence=evidence,
        alternatives=alternatives,
    )


def compute_historical_trends(
    curves: Sequence[Curve],
    *,
    window: int = 60,
    n_components: int = 3,
) -> HistoricalTrendResult:
    """Perform PCA factor analysis and rolling trend diagnostics on curves."""

    if len(curves) < 2:
        raise ValueError("At least two curves required for historical analysis")

    sorted_curves = sorted(
        curves,
        key=lambda curve: curve.metadata.as_of or pd.Timestamp.min,
    )
    min_tenor = float(min(curve.tenors[0] for curve in sorted_curves))
    max_tenor = float(max(curve.tenors[-1] for curve in sorted_curves))
    grid = np.linspace(min_tenor, max_tenor, 21)

    records: list[dict[str, object]] = []
    matrix_rows: list[np.ndarray] = []
    for curve in sorted_curves:
        as_of = curve.metadata.as_of or pd.Timestamp.utcnow()
        grid_values = interpolation.interpolate(curve.tenors, curve.values, targets=grid, method="linear_zero")
        record = {
            "as_of": as_of,
            "curve_key": curve.metadata.curve_key,
        }
        record.update({f"tenor_{idx}": float(value) for idx, value in enumerate(grid_values)})
        records.append(record)
        matrix_rows.append(grid_values.astype(float))

    values_df = pd.DataFrame(records).sort_values("as_of").reset_index(drop=True)
    grid_columns = [column for column in values_df.columns if column.startswith("tenor_")]
    matrix = values_df[grid_columns].to_numpy(dtype=float)

    centered = matrix - np.mean(matrix, axis=0, keepdims=True)
    sample_count = centered.shape[0]
    n_components = min(n_components, centered.shape[0], centered.shape[1])
    if n_components == 0:
        raise ValueError("Unable to compute PCA with zero components")

    U, singular_values, VT = np.linalg.svd(centered, full_matrices=False)
    components = VT[:n_components]
    scores = centered @ components.T
    explained_variance = (singular_values**2) / max(sample_count - 1, 1)
    explained_ratio = explained_variance[:n_components] / max(explained_variance.sum(), 1e-8)

    factor_df = pd.DataFrame(
        scores[:, :n_components],
        columns=[f"factor_{index + 1}" for index in range(n_components)],
    )
    factor_df.insert(0, "curve_key", values_df["curve_key"])
    factor_df.insert(0, "as_of", values_df["as_of"])

    slopes_df = _rolling_slopes(
        values_df["as_of"],
        values_df["curve_key"],
        matrix,
        window,
        grid_columns,
    )
    zscores_df = _rolling_zscores(
        values_df["as_of"],
        values_df["curve_key"],
        matrix,
        window,
        grid_columns,
    )

    metadata = {
        "window": window,
        "curve_keys": list(values_df["curve_key"].unique()),
        "explained_variance_ratio": explained_ratio,
    }

    return HistoricalTrendResult(
        grid=grid,
        values=values_df,
        components=components,
        factor_scores=factor_df,
        explained_variance=explained_ratio,
        rolling_slopes=slopes_df,
        rolling_zscores=zscores_df,
        metadata=metadata,
    )


def _rolling_slopes(
    timestamps: pd.Series,
    curve_keys: pd.Series,
    matrix: np.ndarray,
    window: int,
    columns: Sequence[str],
) -> pd.DataFrame:
    numeric_time = pd.to_datetime(timestamps).view("int64") / 86_400_000_000_000
    numeric_time = numeric_time.to_numpy(dtype=float)
    slopes = np.full_like(matrix, np.nan)
    for column_idx in range(matrix.shape[1]):
        slopes[:, column_idx] = _rolling_linear_regression(numeric_time, matrix[:, column_idx], window)
    slopes_df = pd.DataFrame(slopes, columns=columns)
    slopes_df.insert(0, "curve_key", curve_keys.to_numpy())
    slopes_df.insert(0, "as_of", pd.to_datetime(timestamps).to_numpy())
    return slopes_df


def _rolling_zscores(
    timestamps: pd.Series,
    curve_keys: pd.Series,
    matrix: np.ndarray,
    window: int,
    columns: Sequence[str],
) -> pd.DataFrame:
    zscores = np.full_like(matrix, np.nan)
    for row in range(matrix.shape[0]):
        start = max(0, row - window + 1)
        window_slice = matrix[start : row + 1]
        mean = window_slice.mean(axis=0)
        std = window_slice.std(axis=0)
        std[std == 0] = 1.0
        zscores[row] = (matrix[row] - mean) / std
    zscores_df = pd.DataFrame(zscores, columns=columns)
    zscores_df.insert(0, "curve_key", curve_keys.to_numpy())
    zscores_df.insert(0, "as_of", pd.to_datetime(timestamps).to_numpy())
    return zscores_df


def _rolling_linear_regression(time_axis: np.ndarray, values: np.ndarray, window: int) -> np.ndarray:
    n = values.size
    slopes = np.full(n, np.nan)
    if window <= 1:
        return slopes
    for idx in range(window - 1, n):
        start = idx - window + 1
        x = time_axis[start : idx + 1]
        y = values[start : idx + 1]
        x_centered = x - np.mean(x)
        denom = np.dot(x_centered, x_centered)
        if denom == 0:
            continue
        y_centered = y - np.mean(y)
        slopes[idx] = np.dot(x_centered, y_centered) / denom
    return slopes


def generate_curve_scenarios(
    curve: Curve,
    templates: Sequence[ScenarioTemplate],
) -> list[CurveScenario]:
    """Generate tenor-aligned stress scenarios from templates."""

    scenarios: list[CurveScenario] = []
    for template in templates:
        tenors = curve.tenors.copy()
        shock = _build_scenario_shock(curve, template)
        payload = {
            "scenario_type": template.scenario_type,
            "magnitude": template.magnitude,
            "pivot": template.pivot,
            "width": template.width,
            **(template.payload or {}),
        }
        scenarios.append(
            CurveScenario(
                name=template.name,
                tenors=tenors,
                shock=shock,
                units=template.units,
                payload=payload,
            )
        )
    return scenarios


def apply_curve_scenario(curve: Curve, scenario: CurveScenario) -> np.ndarray:
    """Apply a precomputed scenario to a curve and return shocked values."""

    if scenario.tenors.shape != curve.tenors.shape or not np.allclose(scenario.tenors, curve.tenors):
        base_values = interpolation.interpolate(curve.tenors, curve.values, targets=scenario.tenors, method="linear_zero")
    else:
        base_values = curve.values

    if scenario.units == "absolute":
        shocked = base_values + scenario.shock
    else:
        shocked = base_values * (1.0 + scenario.shock)

    if scenario.payload.get("enforce_monotonicity"):
        increasing = shocked[-1] >= shocked[0]
        shocked = _enforce_monotone(np.asarray(shocked, dtype=float), increasing=increasing)

    return shocked


def _build_scenario_shock(curve: Curve, template: ScenarioTemplate) -> np.ndarray:
    tenors = curve.tenors
    if template.custom_fn is not None:
        shock_values = template.custom_fn(curve)
        return np.asarray(shock_values, dtype=float)

    magnitude = float(template.magnitude)
    span = max(tenors[-1] - tenors[0], 1e-6)
    normalized = (tenors - tenors[0]) / span

    if template.scenario_type == "parallel":
        return np.full_like(tenors, magnitude, dtype=float)
    if template.scenario_type == "twist":
        pivot = template.pivot if template.pivot is not None else tenors[0]
        pivot_norm = (pivot - tenors[0]) / span
        return magnitude * (normalized - pivot_norm)
    if template.scenario_type == "butterfly":
        return magnitude * (4.0 * (normalized - 0.5) ** 2 - 1.0)
    if template.scenario_type == "local":
        pivot = template.pivot if template.pivot is not None else tenors.mean()
        width = template.width if template.width is not None else span / 6.0
        return magnitude * np.exp(-0.5 * ((tenors - pivot) / max(width, 1e-6)) ** 2)
    if template.scenario_type == "custom":
        if template.payload and "shock" in template.payload:
            return np.asarray(template.payload["shock"], dtype=float)
        raise ValueError("Custom scenario requires custom_fn or payload['shock']")
    raise ValueError(f"Unsupported scenario type: {template.scenario_type}")


def _enforce_monotone(values: np.ndarray, *, increasing: bool) -> np.ndarray:
    output = values.astype(float, copy=True)
    if increasing:
        for idx in range(1, output.size):
            if output[idx] < output[idx - 1]:
                output[idx] = output[idx - 1]
    else:
        for idx in range(1, output.size):
            if output[idx] > output[idx - 1]:
                output[idx] = output[idx - 1]
    return output


def build_plot_payload(
    curve: Curve,
    *,
    overlays: Sequence[Curve] | None = None,
    title: str | None = None,
) -> dict[str, object]:
    """Produce a Plotly-compatible payload for advanced curve visualization."""

    overlays = overlays or []
    traces: list[dict[str, object]] = []
    base_trace = {
        "type": "scatter",
        "mode": "lines+markers",
        "name": curve.metadata.curve_key or curve.measure,
        "x": curve.tenors.tolist(),
        "y": curve.values.tolist(),
        "line": {"width": 2.0, "color": "#1f77b4"},
        "marker": {"size": 6},
        "hovertemplate": "Tenor=%{x:.2f}<br>Value=%{y:.5f}<extra>{}</extra>".format(curve.measure),
    }


def run_benchmarks(curves: Sequence[Curve], repeat: int = 3) -> BenchmarkResult:
    """Benchmark key analytics operations across the supplied curves."""

    if not curves:
        raise ValueError("At least one curve required for benchmarking")

    per_curve: list[dict[str, float]] = []
    interpolation_samples: list[float] = []
    smoothing_samples: list[float] = []
    feature_samples: list[float] = []

    for curve in curves:
        targets = np.linspace(curve.tenors[0], curve.tenors[-1], min(100, curve.size * 5))
        interp_time = _time_operation(
            lambda: interpolation.interpolate(curve.tenors, curve.values, targets=targets, method="linear_zero"),
            repeat,
        )
        smoothing_time = _time_operation(
            lambda: interpolation.smooth(curve.tenors, curve.values, method="tikhonov"),
            repeat,
        )
        feature_time = _time_operation(lambda: extract_features(curve), repeat)
        interpolation_samples.append(interp_time)
        smoothing_samples.append(smoothing_time)
        feature_samples.append(feature_time)
        per_curve.append(
            {
                "curve_key": curve.metadata.curve_key,
                "points": float(curve.size),
                "interpolation_ms": interp_time,
                "smoothing_ms": smoothing_time,
                "feature_ms": feature_time,
            }
        )

    timings: dict[str, float] = {
        "interp_avg_ms": float(np.mean(interpolation_samples)),
        "smooth_avg_ms": float(np.mean(smoothing_samples)),
        "feature_avg_ms": float(np.mean(feature_samples)),
    }

    clustering_time = None
    if len(curves) >= 3:
        clustering_time = _time_operation(
            lambda: cluster_curves(curves, method="kmeans", n_clusters=min(3, len(curves))),
            1,
        )
        timings["cluster_ms"] = clustering_time

    similarity_time = None
    if len(curves) >= 2:
        similarity_time = _time_operation(
            lambda: find_similar_curves(curves[0], curves[1:]),
            1,
        )
        timings["similarity_ms"] = similarity_time

    pca_time = None
    if len(curves) >= min(5, repeat + 1):
        pca_time = _time_operation(
            lambda: compute_historical_trends(curves, window=min(10, len(curves))),
            1,
        )
        timings["historical_ms"] = pca_time

    metadata = {
        "curve_count": len(curves),
        "average_points": float(np.mean([curve.size for curve in curves])),
        "repeat": repeat,
    }

    return BenchmarkResult(
        timings=timings,
        per_curve=per_curve,
        repeat=repeat,
        metadata=metadata,
    )


def _time_operation(func: Callable[[], object], repeat: int) -> float:
    total = 0.0
    for _ in range(max(repeat, 1)):
        start = time.perf_counter()
        func()
        total += time.perf_counter() - start
    return (total / max(repeat, 1)) * 1000.0
    traces.append(base_trace)

    palette = ["#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]
    for idx, overlay in enumerate(overlays):
        traces.append(
            {
                "type": "scatter",
                "mode": "lines+markers",
                "name": overlay.metadata.curve_key or f"overlay_{idx+1}",
                "x": overlay.tenors.tolist(),
                "y": overlay.values.tolist(),
                "line": {
                    "width": 1.5,
                    "dash": "dash",
                    "color": palette[idx % len(palette)],
                },
                "marker": {"size": 5},
                "hovertemplate": "Tenor=%{x:.2f}<br>Value=%{y:.5f}<extra>{}</extra>".format(overlay.measure),
            }
        )

    layout = {
        "title": title or f"Curve: {curve.metadata.curve_key}",
        "xaxis": {
            "title": "Tenor (years)",
            "gridcolor": "#f0f0f0",
            "zeroline": False,
        },
        "yaxis": {
            "title": curve.measure,
            "gridcolor": "#f0f0f0",
            "zeroline": False,
        },
        "hovermode": "x unified",
        "legend": {"orientation": "h", "x": 0.0, "y": -0.2},
        "margin": {"l": 60, "r": 30, "t": 50, "b": 60},
    }

    controls = {
        "interpolation_methods": [
            "linear",
            "cspline",
            "monotone_cubic",
            "pchip",
            "akima",
            "log_discount",
        ],
        "smoothing_methods": ["tikhonov", "savitzky_golay", "loess", "kalman"],
        "scenario_templates": [
            "parallel",
            "twist",
            "butterfly",
            "local",
        ],
    }

    return {
        "data": traces,
        "layout": layout,
        "controls": controls,
        "metadata": {
            "curve_key": curve.metadata.curve_key,
            "measure": curve.measure,
            "as_of": str(curve.metadata.as_of) if curve.metadata.as_of is not None else None,
        },
    }


__all__ = [
    "CurveFeatures",
    "extract_features",
    "PatternDetection",
    "ShapeClassification",
    "detect_patterns",
    "classify_curve_shape",
    "ScenarioTemplate",
    "CurveScenario",
    "generate_curve_scenarios",
    "apply_curve_scenario",
    "build_plot_payload",
    "BenchmarkResult",
    "run_benchmarks",
    "ClusterResult",
    "SimilarityResult",
    "SimilarNeighbor",
    "cluster_curves",
    "find_similar_curves",
]
