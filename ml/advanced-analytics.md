Advanced Analytics and ML Platform (Updated)
===========================================

Overview
- Objective: predictive analytics, curve forecasting, anomaly detection, and volatility analysis backed by automated MLOps.
- Scope: modular components in `src/aurum/ml` with CLI tooling (`aurum-ml`) and registry integration.

Key Enhancements
- Forecasting: `ForecastingPipeline` orchestrates candidate models (naive, Holt-Winters, damped trend, regression, ensembles) with rolling-origin evaluation and automated selection.
- Feature engineering: `FeaturePipeline` configurable lags, rolling stats, EWMA, calendar features, realized volatility, and exogenous joins with future feature generation support.
- Anomalies: `AdaptiveAnomalyDetector` hybrid z-score/MAD detector with cooldown control and streaming-friendly `update()` API.
- Volatility: `VolatilityEngine` aggregates realized, EWMA, Parkinson, and Garman–Klass estimators plus regime diagnostics.
- Experimentation: `ABTestingService` logs sequential experiments and provides leaderboards; retraining integrates automatic challenger vs. champion comparison.
- MLOps: `retrain_best_forecaster` now leverages the forecasting pipeline, persists feature config/columns, evaluation reports, and A/B experiment metadata to the model registry.

Module Reference
- `src/aurum/ml/feature_engineering.py`
  - `FeaturePipelineConfig`, `FeaturePipeline`, `build_features`, `compute_realized_volatility`
  - Future feature generation via `FeaturePipeline.make_future_features()`
- `src/aurum/ml/forecasting/`
  - `models.py`: Holt-Winters, damped trend, regression, ensemble forecasters
  - `pipeline.py`: candidate evaluation, best-model retrieval, reporting
  - `shape.py`: curve-shape nearest-neighbour forecaster
- `src/aurum/ml/anomaly_detection.py`
  - `AdaptiveAnomalyDetector` streaming interface, `detect_anomalies()` convenience wrapper
- `src/aurum/ml/volatility.py`
  - `VolatilityEngine`, `VolatilityRegime`, individual estimators
- `src/aurum/ml/retraining.py`
  - `retrain_best_forecaster` integrates feature pipeline, forecasting pipeline, registry, and A/B logging
- `src/aurum/ml/ab_testing.py`
  - `run_ab_test`, `ABTestingService` for sequential experiment tracking

CLI Highlights (`aurum-ml`)
- `train-forecast`: new flags for feature control (`--feature-windows`, `--feature-lags`, `--no-feature-calendar`, `--no-feature-volatility`, `--no-ab-test`). Output JSON includes evaluation report and optional A/B experiments.
- `forecast`: unchanged usage; works with registered advanced models.
- `anomalies`: optional adaptive detector via `--mode adaptive`, `--mad-threshold`, `--cooldown`.
- `volatility`: powered by `VolatilityEngine`, returning multi-metric series + summary diagnostics.
- `shape-forecast`: unchanged pattern-matching predictions.

Integration Points
- Registry: metadata now captures feature config, columns, evaluation leaderboard, and AB experiments in `metadata.json`.
- Airflow/cron: continue invoking `aurum-ml train-forecast` with new flags as needed; retraining outcome structure is backward compatible with additional fields.
- Streaming: `AdaptiveAnomalyDetector.update()` slot into `KafkaProcessor` handlers for real-time anomaly publication.

Testing & Validation
- Unit tests (see `tests/ml/`) cover forecasting pipeline selection, anomaly detector scoring, and volatility diagnostics.
- CLI smoke tests recommended: run `aurum-ml train-forecast ...` on sample CSV, inspect registry output under `artifacts/models/`.

