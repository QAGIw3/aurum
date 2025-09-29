"""Numerical interpolation, extrapolation, and smoothing helpers for curves."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Literal, Sequence

import numpy as np

try:  # Optional SciPy-based implementations for higher fidelity methods
    from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator
    from scipy.optimize import curve_fit
    from scipy.signal import savgol_filter
except ImportError:  # pragma: no cover - SciPy may not be installed in minimal envs
    Akima1DInterpolator = None  # type: ignore[assignment]
    CubicSpline = None  # type: ignore[assignment]
    PchipInterpolator = None  # type: ignore[assignment]
    curve_fit = None  # type: ignore[assignment]
    savgol_filter = None  # type: ignore[assignment]
    HAS_SCIPY = False
else:  # pragma: no cover - exercised in environments with SciPy available
    HAS_SCIPY = True

InterpolationMethod = Literal[
    "linear",
    "cspline",
    "monotone_cubic",
    "pchip",
    "akima",
    "tension",
    "log_discount",
    "linear_zero",
    "forward_flat",
    "piecewise_constant_forward",
]

ExtrapolationMethod = Literal["flat_forward", "linear", "linear_decay", "parametric_tail"]


@dataclass
class InterpolationConstraints:
    """Constraints applied to base curve nodes and interpolation outputs."""

    enforce_monotonicity: bool = False
    monotonic_direction: Literal["increasing", "decreasing"] = "increasing"
    enforce_positivity: bool = False
    lower_bound: float | None = None
    upper_bound: float | None = None
    regularization_lambda: float = 0.0


@dataclass
class ExtrapolationConstraints:
    """Constraints applied during extrapolation."""

    min_value: float | None = None
    max_value: float | None = None
    slope_limit: float | None = None
    decay: float = 5.0
    tail_value: float | None = None


@dataclass
class SmootherParams:
    """Configuration for smoothing algorithms."""

    strength: float = 1.0
    enforce_monotonicity: bool = False
    method_params: dict[str, float] | None = None


@dataclass
class SmoothDiagnostics:
    """Metadata returned by smoothing routines for transparency."""

    method: str
    effective_strength: float
    residual_norm: float
    iterations: int


def _prepare_series(tenors: Sequence[float], values: Sequence[float]) -> tuple[np.ndarray, np.ndarray]:
    tenors_array = np.asarray(tenors, dtype=float)
    values_array = np.asarray(values, dtype=float)
    if tenors_array.size != values_array.size:
        raise ValueError("tenors and values must have identical length")
    mask = np.isfinite(tenors_array) & np.isfinite(values_array)
    if not np.all(mask):
        tenors_array = tenors_array[mask]
        values_array = values_array[mask]
    if tenors_array.size < 2:
        raise ValueError("at least two valid curve points required")
    order = np.argsort(tenors_array)
    tenors_array = tenors_array[order]
    values_array = values_array[order]
    unique_tenors, inverse = np.unique(tenors_array, return_inverse=True)
    if unique_tenors.size != tenors_array.size:
        aggregated = np.zeros(unique_tenors.size)
        counts = np.zeros(unique_tenors.size)
        for idx, inv in enumerate(inverse):
            aggregated[inv] += values_array[idx]
            counts[inv] += 1
        values_array = aggregated / np.maximum(counts, 1.0)
        tenors_array = unique_tenors
    return tenors_array, values_array


def _apply_constraints_to_nodes(
    values: np.ndarray,
    constraints: InterpolationConstraints,
) -> np.ndarray:
    constrained = values.astype(float, copy=True)
    if constraints.regularization_lambda > 0 and constrained.size > 2:
        constrained = _tikhonov_regularize(constrained, constraints.regularization_lambda)
    if constraints.enforce_monotonicity:
        constrained = _enforce_monotonicity(constrained, constraints.monotonic_direction)
    if constraints.enforce_positivity:
        constrained = np.maximum(constrained, 0.0)
    if constraints.lower_bound is not None or constraints.upper_bound is not None:
        constrained = np.clip(
            constrained,
            constraints.lower_bound if constraints.lower_bound is not None else -np.inf,
            constraints.upper_bound if constraints.upper_bound is not None else np.inf,
        )
    return constrained


def _apply_constraints_to_array(
    array: np.ndarray,
    constraints: InterpolationConstraints | ExtrapolationConstraints | None,
) -> np.ndarray:
    if constraints is None:
        return array
    constrained = array.astype(float, copy=True)
    lower_bound = getattr(constraints, "lower_bound", getattr(constraints, "min_value", None))
    upper_bound = getattr(constraints, "upper_bound", getattr(constraints, "max_value", None))
    if lower_bound is not None or upper_bound is not None:
        constrained = np.clip(
            constrained,
            lower_bound if lower_bound is not None else -np.inf,
            upper_bound if upper_bound is not None else np.inf,
        )
    if getattr(constraints, "enforce_positivity", False):
        constrained = np.maximum(constrained, 0.0)
    return constrained


def _tikhonov_regularize(values: np.ndarray, lam: float) -> np.ndarray:
    lam = max(float(lam), 0.0)
    if lam <= 0.0 or values.size < 3:
        return values
    n = values.size
    diff = np.zeros((n, n))
    for i in range(1, n - 1):
        diff[i, i - 1] = 1.0
        diff[i, i] = -2.0
        diff[i, i + 1] = 1.0
    penalty = diff.T @ diff
    system = np.eye(n) + lam * penalty
    try:
        return np.linalg.solve(system, values)
    except np.linalg.LinAlgError:  # pragma: no cover - defensive fallback
        return values


def _enforce_monotonicity(values: np.ndarray, direction: Literal["increasing", "decreasing"]) -> np.ndarray:
    monotone = values.astype(float, copy=True)
    if monotone.size <= 1:
        return monotone
    if direction == "increasing":
        for idx in range(1, monotone.size):
            if monotone[idx] < monotone[idx - 1]:
                monotone[idx] = monotone[idx - 1]
    else:
        for idx in range(1, monotone.size):
            if monotone[idx] > monotone[idx - 1]:
                monotone[idx] = monotone[idx - 1]
    return monotone


def _build_core_interpolator(
    tenors: np.ndarray,
    values: np.ndarray,
    method: InterpolationMethod,
) -> Callable[[np.ndarray], np.ndarray]:
    if method == "linear" or method == "linear_zero":
        return lambda target: np.interp(target, tenors, values)
    if method == "forward_flat" or method == "piecewise_constant_forward":
        def _forward_flat(target: np.ndarray) -> np.ndarray:
            idx = np.searchsorted(tenors, target, side="right") - 1
            idx = np.clip(idx, 0, len(values) - 1)
            return values[idx]

        return _forward_flat
    if method == "cspline" and HAS_SCIPY and CubicSpline is not None:
        spline = CubicSpline(tenors, values, bc_type="natural", extrapolate=True)
        return lambda target: spline(target)
    if method in {"monotone_cubic", "pchip"} and HAS_SCIPY and PchipInterpolator is not None:
        interpolator = PchipInterpolator(tenors, values, extrapolate=True)
        return lambda target: interpolator(target)
    if method == "akima" and HAS_SCIPY and Akima1DInterpolator is not None:
        interpolator = Akima1DInterpolator(tenors, values)
        return lambda target: interpolator(target)
    if method == "tension" and HAS_SCIPY and CubicSpline is not None:
        # Approximate tensioned spline by blending natural and clamped boundary conditions.
        spline_natural = CubicSpline(tenors, values, bc_type="natural", extrapolate=True)
        spline_clamped = CubicSpline(
            tenors,
            values,
            bc_type=((1, (values[1] - values[0]) / (tenors[1] - tenors[0])), (1, (values[-1] - values[-2]) / (tenors[-1] - tenors[-2]))),
            extrapolate=True,
        )
        def _tensioned(target: np.ndarray) -> np.ndarray:
            weight = 0.5
            return weight * spline_natural(target) + (1.0 - weight) * spline_clamped(target)

        return _tensioned
    return lambda target: np.interp(target, tenors, values)


def _finance_interpolation(
    tenors: np.ndarray,
    values: np.ndarray,
    targets: np.ndarray,
    method: InterpolationMethod,
) -> np.ndarray:
    eps = 1e-8
    if method == "log_discount":
        tenor_safe = np.maximum(tenors, eps)
        discount = np.exp(-values * tenor_safe)
        log_discount = np.log(discount)
        interpolated = np.interp(targets, tenors, log_discount)
        return -interpolated / np.maximum(targets, eps)
    if method == "forward_flat":  # Already handled by `_build_core_interpolator`, keep for clarity
        idx = np.searchsorted(tenors, targets, side="right") - 1
        idx = np.clip(idx, 0, len(values) - 1)
        return values[idx]
    if method == "piecewise_constant_forward":
        idx = np.searchsorted(tenors, targets, side="right") - 1
        idx = np.clip(idx, 0, len(values) - 1)
        return values[idx]
    if method == "linear_zero":
        return np.interp(targets, tenors, values)
    raise ValueError(f"Unsupported finance interpolation method: {method}")


def interpolate(
    tenors: Sequence[float],
    values: Sequence[float],
    *,
    targets: Sequence[float] | np.ndarray,
    method: InterpolationMethod = "cspline",
    constraints: InterpolationConstraints | None = None,
) -> np.ndarray:
    """Interpolate a curve at the specified target tenors."""

    constraints = constraints or InterpolationConstraints()
    base_tenors, base_values = _prepare_series(tenors, values)
    constrained_values = _apply_constraints_to_nodes(base_values, constraints)
    result_targets = np.asarray(targets, dtype=float)
    if method in {"log_discount", "forward_flat", "piecewise_constant_forward", "linear_zero"}:
        interpolated = _finance_interpolation(base_tenors, constrained_values, result_targets, method)
    else:
        interpolator = _build_core_interpolator(base_tenors, constrained_values, method)
        interpolated = np.asarray(interpolator(result_targets), dtype=float)
    return _apply_constraints_to_array(interpolated, constraints)


def extrapolate(
    tenors: Sequence[float],
    values: Sequence[float],
    *,
    horizon: Sequence[float] | np.ndarray,
    method: ExtrapolationMethod = "flat_forward",
    constraints: ExtrapolationConstraints | None = None,
    base_interpolation: InterpolationMethod = "linear",
) -> np.ndarray:
    """Extrapolate curves beyond the calibrated tenor grid."""

    constraints = constraints or ExtrapolationConstraints()
    base_tenors, base_values = _prepare_series(tenors, values)
    interpolator = _build_core_interpolator(base_tenors, base_values, base_interpolation)
    result_horizon = np.asarray(horizon, dtype=float)
    output = np.zeros_like(result_horizon, dtype=float)
    min_t, max_t = base_tenors[0], base_tenors[-1]
    for idx, target in enumerate(result_horizon):
        if min_t <= target <= max_t:
            output[idx] = interpolator(np.asarray([target]))[0]
        elif target > max_t:
            output[idx] = _extrapolate_right(base_tenors, base_values, target, method, constraints)
        else:
            output[idx] = _extrapolate_left(base_tenors, base_values, target, method, constraints)
    return _apply_constraints_to_array(output, constraints)


def _bounded_slope(candidate: float, constraints: ExtrapolationConstraints) -> float:
    if constraints.slope_limit is None:
        return candidate
    return float(np.clip(candidate, -abs(constraints.slope_limit), abs(constraints.slope_limit)))


def _extrapolate_right(
    tenors: np.ndarray,
    values: np.ndarray,
    target: float,
    method: ExtrapolationMethod,
    constraints: ExtrapolationConstraints,
) -> float:
    if method == "flat_forward":
        return float(values[-1])
    if method == "linear":
        slope = (values[-1] - values[-2]) / (tenors[-1] - tenors[-2])
        slope = _bounded_slope(slope, constraints)
        return float(values[-1] + slope * (target - tenors[-1]))
    if method == "linear_decay":
        slope = (values[-1] - values[-2]) / (tenors[-1] - tenors[-2])
        slope = _bounded_slope(slope, constraints)
        decay = constraints.decay if constraints.decay > 0 else 5.0
        factor = np.exp(-(target - tenors[-1]) / decay)
        return float(values[-1] + slope * (target - tenors[-1]) * factor)
    if method == "parametric_tail":
        return float(_parametric_tail(tenors, values, target, side="right", constraints=constraints))
    raise ValueError(f"Unsupported extrapolation method: {method}")


def _extrapolate_left(
    tenors: np.ndarray,
    values: np.ndarray,
    target: float,
    method: ExtrapolationMethod,
    constraints: ExtrapolationConstraints,
) -> float:
    if method == "flat_forward":
        return float(values[0])
    if method == "linear":
        slope = (values[1] - values[0]) / (tenors[1] - tenors[0])
        slope = _bounded_slope(slope, constraints)
        return float(values[0] + slope * (target - tenors[0]))
    if method == "linear_decay":
        slope = (values[1] - values[0]) / (tenors[1] - tenors[0])
        slope = _bounded_slope(slope, constraints)
        decay = constraints.decay if constraints.decay > 0 else 5.0
        factor = np.exp(-(tenors[0] - target) / decay)
        return float(values[0] + slope * (target - tenors[0]) * factor)
    if method == "parametric_tail":
        return float(_parametric_tail(tenors, values, target, side="left", constraints=constraints))
    raise ValueError(f"Unsupported extrapolation method: {method}")


def _parametric_tail(
    tenors: np.ndarray,
    values: np.ndarray,
    target: float,
    *,
    side: Literal["left", "right"],
    constraints: ExtrapolationConstraints,
) -> float:
    if not HAS_SCIPY or curve_fit is None or tenors.size < 4:
        if side == "right":
            return values[-1]
        return values[0]

    if side == "right":
        sample_t = tenors[-4:]
        sample_v = values[-4:]
        shift = sample_t[0]
        x = sample_t - shift
        x_target = target - shift
    else:
        sample_t = tenors[:4]
        sample_v = values[:4]
        shift = sample_t[-1]
        x = shift - sample_t
        x_target = shift - target

    def model(x_val: np.ndarray, a: float, b: float, c: float) -> np.ndarray:
        return a + b * np.exp(-c * x_val)

    guess_a = constraints.tail_value if constraints.tail_value is not None else sample_v[-1]
    guess_b = sample_v[0] - guess_a
    guess_c = 1.0 / max(np.mean(np.diff(x)), 1e-6)
    try:
        params, _ = curve_fit(
            model,
            x,
            sample_v,
            p0=(guess_a, guess_b, abs(guess_c)),
            maxfev=10000,
        )
    except Exception:  # pragma: no cover - conservative fallback when fit fails
        if side == "right":
            return sample_v[-1]
        return sample_v[0]
    a, b, c = params
    estimate = float(model(np.array([max(x_target, 0.0)]), a, b, abs(c))[0])
    return estimate


def smooth(
    tenors: Sequence[float],
    values: Sequence[float],
    *,
    method: Literal["tikhonov", "savitzky_golay", "loess", "kalman"] = "tikhonov",
    params: SmootherParams | None = None,
) -> tuple[np.ndarray, SmoothDiagnostics]:
    """Apply smoothing or de-noising to the supplied curve values."""

    params = params or SmootherParams()
    method_params = params.method_params or {}
    base_tenors, base_values = _prepare_series(tenors, values)
    method = method.lower()

    if method == "tikhonov":
        lam = float(method_params.get("lambda", params.strength))
        smoothed = _tikhonov_regularize(base_values, lam)
        iterations = 1
        effective_strength = lam
    elif method == "savitzky_golay":
        window = int(method_params.get("window", max(5, int(round(params.strength * 5)) | 1)))
        if window % 2 == 0:
            window += 1
        window = min(window, base_values.size - 1 if base_values.size % 2 == 0 else base_values.size)
        window = max(window, 3)
        polyorder = int(method_params.get("polyorder", min(3, window - 1)))
        if HAS_SCIPY and savgol_filter is not None and window <= base_values.size:
            smoothed = savgol_filter(base_values, window_length=window, polyorder=polyorder, mode="interp")
        else:
            smoothed = _moving_average(base_values, window)
        iterations = 1
        effective_strength = window
    elif method == "loess":
        frac = float(method_params.get("frac", min(0.4 + params.strength * 0.1, 1.0)))
        iterations = int(method_params.get("iterations", 2))
        smoothed = _loess_smooth(base_tenors, base_values, frac=frac, iterations=iterations)
        effective_strength = frac
    elif method == "kalman":
        process_var = float(method_params.get("process_var", max(params.strength * 1e-4, 1e-6)))
        measurement_var = float(method_params.get("measurement_var", 1e-3))
        smoothed = _kalman_smooth(base_values, process_var=process_var, measurement_var=measurement_var)
        iterations = 1
        effective_strength = process_var
    else:
        raise ValueError(f"Unsupported smoothing method: {method}")

    if params.enforce_monotonicity:
        direction = "increasing" if base_values[-1] >= base_values[0] else "decreasing"
        smoothed = _enforce_monotonicity(smoothed, direction)

    residual_norm = float(np.linalg.norm(smoothed - base_values))
    diagnostics = SmoothDiagnostics(
        method=method,
        effective_strength=float(effective_strength),
        residual_norm=residual_norm,
        iterations=iterations,
    )
    return smoothed, diagnostics


def _moving_average(values: np.ndarray, window: int) -> np.ndarray:
    window = max(int(window), 1)
    if window <= 1:
        return values
    kernel = np.ones(window) / window
    padding = window // 2
    padded = np.pad(values, (padding, padding), mode="edge")
    convolved = np.convolve(padded, kernel, mode="valid")
    return convolved[: values.size]


def _loess_smooth(
    tenors: np.ndarray,
    values: np.ndarray,
    *,
    frac: float,
    iterations: int,
) -> np.ndarray:
    n = values.size
    frac = float(np.clip(frac, 0.05, 1.0))
    span = max(int(np.ceil(frac * n)), 2)
    result = values.astype(float, copy=True)
    for _ in range(max(iterations, 1)):
        for idx in range(n):
            distances = np.abs(tenors - tenors[idx])
            order = np.argsort(distances)
            neighbors = order[:span]
            max_distance = distances[neighbors[-1]]
            if max_distance == 0:
                result[idx] = values[idx]
                continue
            weights = (1 - (distances[neighbors] / max_distance) ** 3) ** 3
            X = np.column_stack((np.ones(neighbors.size), tenors[neighbors] - tenors[idx]))
            W = np.diag(weights)
            try:
                beta = np.linalg.pinv(X.T @ W @ X) @ (X.T @ W @ values[neighbors])
                result[idx] = beta[0]
            except np.linalg.LinAlgError:  # pragma: no cover - fallback when matrix ill-conditioned
                result[idx] = values[neighbors].mean()
    return result


def _kalman_smooth(
    values: np.ndarray,
    *,
    process_var: float,
    measurement_var: float,
) -> np.ndarray:
    estimate = values.astype(float, copy=True)
    if estimate.size == 0:
        return estimate
    state_estimate = estimate[0]
    covariance = 1.0
    smoothed = np.zeros_like(estimate)
    for idx, measurement in enumerate(estimate):
        # Predict step
        covariance += process_var
        # Update step
        kalman_gain = covariance / (covariance + measurement_var)
        state_estimate = state_estimate + kalman_gain * (measurement - state_estimate)
        covariance = (1 - kalman_gain) * covariance
        smoothed[idx] = state_estimate
    return smoothed


__all__ = [
    "InterpolationConstraints",
    "ExtrapolationConstraints",
    "SmootherParams",
    "SmoothDiagnostics",
    "ExtrapolationMethod",
    "InterpolationMethod",
    "interpolate",
    "extrapolate",
    "smooth",
]
