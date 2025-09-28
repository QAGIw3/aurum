"""Command-line interface for ML workflows (training, forecasting, anomalies).

Usage examples:
  aurum-ml train-forecast --csv data.csv --column price --name energy_price
  aurum-ml forecast --name energy_price --version latest --steps 24
  aurum-ml anomalies --csv data.csv --column price --window 24 --z 3.0
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from .anomaly_detection import AdaptiveAnomalyDetector, detect_anomalies
from .feature_engineering import FeaturePipelineConfig
from .forecasting.shape import NearestNeighborShapeForecaster
from .registry import ModelRegistry
from .volatility import VolatilityEngine
from .retraining import retrain_best_forecaster


def _load_series_from_csv(path: str | Path, column: str, parse_dates: Optional[str] = None) -> pd.Series:
    df = pd.read_csv(path, parse_dates=[parse_dates] if parse_dates else None)
    if column not in df.columns:
        raise SystemExit(f"Column '{column}' not found in {path}")
    s = df[column]
    if parse_dates and parse_dates in df.columns:
        s.index = pd.to_datetime(df[parse_dates])
    return s


def _parse_int_list(value: Optional[str]) -> tuple[int, ...]:
    if not value:
        return ()
    parts = [p.strip() for p in value.split(",") if p.strip()]
    try:
        return tuple(sorted({int(p) for p in parts}))
    except ValueError as exc:
        raise SystemExit(f"Invalid integer list: {value}") from exc


def cmd_train_forecast(args: argparse.Namespace) -> None:
    s = _load_series_from_csv(args.csv, args.column, args.time_index)
    config = FeaturePipelineConfig(
        rolling_windows=_parse_int_list(args.feature_windows) or (3, 12, 24),
        lags=_parse_int_list(args.feature_lags) or (1, 2, 24),
        include_calendar=not args.no_feature_calendar,
        include_volatility=not args.no_feature_volatility,
    )
    outcome = retrain_best_forecaster(
        lambda: s,
        model_name=args.name,
        version=args.version,
        horizon=args.horizon,
        initial_train_size=args.initial,
        step=args.step,
        freq_hint=args.freq,
        feature_config=config,
        enable_ab_testing=not args.no_ab_test,
    )
    print(json.dumps(outcome.__dict__, indent=2))


def cmd_forecast(args: argparse.Namespace) -> None:
    reg = ModelRegistry()
    version = args.version
    if args.version == "latest":
        latest = reg.latest(args.name)
        if latest is None:
            raise SystemExit(f"No versions found for model '{args.name}'")
        version = latest[0]
    model = reg.load(args.name, version)
    fc = model.forecast(args.steps)
    df = pd.DataFrame({
        "timestamp": fc.predictions.index if hasattr(fc.predictions.index, "to_numpy") else range(len(fc.predictions)),
        "prediction": fc.predictions.to_numpy(),
    })
    if fc.lower is not None and fc.upper is not None:
        df["lower"] = fc.lower.to_numpy()
        df["upper"] = fc.upper.to_numpy()
    print(df.to_csv(index=False))


def cmd_anomalies(args: argparse.Namespace) -> None:
    s = _load_series_from_csv(args.csv, args.column, args.time_index)
    if args.mad_threshold is not None or args.cooldown > 0 or args.mode == "adaptive":
        detector = AdaptiveAnomalyDetector(
            window=args.window,
            z_threshold=args.z,
            mad_threshold=args.mad_threshold or 3.5,
            cooldown=args.cooldown,
        )
        df = detector.detect(s)
    else:
        df = detect_anomalies(s, window=args.window, z_threshold=args.z)
    print(df.to_csv(index=False))


def cmd_shape_forecast(args: argparse.Namespace) -> None:
    s = _load_series_from_csv(args.csv, args.column, args.time_index)
    model = NearestNeighborShapeForecaster(window_size=args.window, horizon=args.horizon)
    model.fit(s)
    res = model.forecast()
    preds = [
        {"timestamp": str(idx), "prediction": float(val)}
        for idx, val in zip(res.predictions.index, res.predictions.values)
    ]
    out = {
        "match_start": res.match_start,
        "match_end": res.match_end,
        "match_distance": res.match_distance,
        "predictions": preds,
    }
    print(json.dumps(out, indent=2))


def cmd_volatility(args: argparse.Namespace) -> None:
    close = _load_series_from_csv(args.csv, args.column, args.time_index)
    engine = VolatilityEngine(window=args.window)
    engine.fit(close)
    summary = engine.diagnostics()
    frame = engine.to_frame()
    print(json.dumps({"summary": summary, "series": frame.reset_index().to_dict(orient="list")}, default=str, indent=2))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Aurum ML CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    # train-forecast
    t = sub.add_parser("train-forecast", help="Retrain and register the best forecaster")
    t.add_argument("--csv", required=True)
    t.add_argument("--column", required=True)
    t.add_argument("--time-index", dest="time_index")
    t.add_argument("--name", default="energy_price_forecaster")
    t.add_argument("--version")
    t.add_argument("--horizon", type=int, default=6)
    t.add_argument("--initial", type=int, default=100)
    t.add_argument("--step", type=int, default=1)
    t.add_argument("--freq", help="frequency hint, e.g., 'H' for hourly")
    t.add_argument("--feature-windows", help="comma separated rolling windows, e.g., 6,24,48")
    t.add_argument("--feature-lags", help="comma separated lag features, e.g., 1,24,48")
    t.add_argument("--no-feature-calendar", action="store_true")
    t.add_argument("--no-feature-volatility", action="store_true")
    t.add_argument("--no-ab-test", action="store_true")
    t.set_defaults(func=cmd_train_forecast)

    # forecast
    f = sub.add_parser("forecast", help="Run forecast using a saved model")
    f.add_argument("--name", default="energy_price_forecaster")
    f.add_argument("--version", default="latest")
    f.add_argument("--steps", type=int, default=24)
    f.set_defaults(func=cmd_forecast)

    # anomalies
    a = sub.add_parser("anomalies", help="Detect anomalies in a CSV column")
    a.add_argument("--csv", required=True)
    a.add_argument("--column", required=True)
    a.add_argument("--time-index", dest="time_index")
    a.add_argument("--window", type=int, default=24)
    a.add_argument("--z", type=float, default=3.0)
    a.add_argument("--mad-threshold", type=float)
    a.add_argument("--cooldown", type=int, default=0)
    a.add_argument("--mode", choices=["standard", "adaptive"], default="standard")
    a.set_defaults(func=cmd_anomalies)

    # shape-forecast
    s = sub.add_parser("shape-forecast", help="Pattern-based curve shape forecasting")
    s.add_argument("--csv", required=True)
    s.add_argument("--column", required=True)
    s.add_argument("--time-index", dest="time_index")
    s.add_argument("--window", type=int, default=24)
    s.add_argument("--horizon", type=int, default=6)
    s.set_defaults(func=cmd_shape_forecast)

    # volatility
    v = sub.add_parser("volatility", help="Compute volatility metrics and classify regime")
    v.add_argument("--csv", required=True)
    v.add_argument("--column", required=True)
    v.add_argument("--time-index", dest="time_index")
    v.add_argument("--window", type=int, default=24)
    v.set_defaults(func=cmd_volatility)

    return p


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])

