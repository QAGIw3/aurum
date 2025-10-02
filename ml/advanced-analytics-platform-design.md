Advanced Analytics & ML Platform Architecture
============================================

Vision
- Deliver modular analytics capabilities for forecasting, curve shape prediction, volatility insight, anomaly detection, and lifecycle automation tailored to energy markets.
- Ensure pandas/numpy-only core dependencies with optional hooks for external schedulers and streaming infrastructure already present in Aurum.

Component Overview
1. Data & Feature Layer
   - FeaturePipeline: configurable pipeline built from FeatureBlocks (lags, rolling stats, calendar embeddings, external regressors).
   - Context-aware transformers that respect DatetimeIndex frequency and emit feature metadata for downstream models.
   - Caches intermediate computations (rolling windows) to avoid recomputation across models in the same run.

2. Forecasting Layer
   - BaseForecaster protocol with `fit(series, features=None)` and `forecast(steps, future_features=None)`.
   - Model set:
     - HoltWintersForecaster: additive triple exponential smoothing with level/trend/season.
     - DampedTrendForecaster: Holt’s method with damped trend for smoother long horizons.
     - RegressionForecaster: ordinary least squares on engineered features (supports autoregressive lags + exogenous signals).
     - EnsembleForecaster: blends member forecasts using weighted averaging or median.
   - ForecastingPipeline orchestrates feature building, model training, backtesting and ensembling. Exposes evaluation reports and calibrated prediction intervals.

3. Curve Shape Intelligence
   - NearestNeighborShapeForecaster (existing) kept; wrap inside ShapeForecastService that applies detrending, normalization, and optional ensemble of multiple windows/horizons.
   - Adds shape similarity diagnostics (match distance z-score, pattern coverage statistics).

4. Volatility & Risk
   - VolatilityEngine: aggregates realized volatility, Parkinson, Garman-Klass, and EWMA metrics.
   - RegimeClassifier uses VolatilityEngine outputs to label low/normal/high/extreme with hysteresis and quantile trends.
   - Exposes `to_frame()` for downstream storage and `diagnostics()` summary for dashboards.

5. Anomaly Detection
   - AdaptiveAnomalyDetector with hybrid z-score + Median Absolute Deviation thresholds and optional volatility scaling.
   - Supports online updates (`update(value, timestamp) -> Optional[AnomalyEvent]`) and batch detection (`detect(series)`).
   - Outputs standardized payloads for streaming (`dict` with anomaly metadata and score components).

6. Evaluation & Experimentation
   - Extends evaluation metrics with coverage (prediction interval hit rate) and directional accuracy.
   - ABTestingService: wraps existing `run_ab_test` and adds sequential logging of experiments + uplift calculations.

7. MLOps & Automation
   - RetrainingOrchestrator builds ForecastingPipeline from configuration, runs rolling-origin backtests, registers best model via ModelRegistry.
   - Stores evaluation artifacts (feature config, candidate metrics, A/B outcomes) in metadata JSON.
   - CLI integration: `aurum-ml train-forecast` uses orchestrator; new commands for `diagnostics` and `volatility report`.

8. Integration Points
   - Streaming: AdaptiveAnomalyDetector integrates with `KafkaProcessor` for online detection.
   - Scheduling: Airflow DAGs call the orchestrator with YAML/JSON configs stored in `conf/ml/` (optional future work).
   - Observability: All major services emit structured logs via structlog (leverage existing logging stack).

Planned Deliverables
- Code updates in `src/aurum/ml/feature_engineering.py`, `forecasting/`, `anomaly_detection.py`, `volatility.py`, `retraining.py`, `ab_testing.py`, and `cli.py`.
- New modules: `src/aurum/ml/forecasting/pipeline.py`, `src/aurum/ml/forecasting/models.py`, `src/aurum/ml/volatility_engine.py` (if separation aids clarity).
- Unit tests covering pipeline assembly, anomaly detection scoring, and volatility regime classification.

Testing Strategy
- Focused unit tests in `tests/ml/` verifying:
  - ForecastingPipeline selects best candidate via RMSE on synthetic data.
  - AdaptiveAnomalyDetector flags injected anomalies with expected sides.
  - VolatilityEngine computes consistent metrics across simple OHLC series.

Risks & Mitigations
- Holt-Winters numeric stability: enforce smoothing coefficients within bounds, add fallback if variance zero.
- Regression forecaster collinearity: default ridge regularization parameter with closed-form solution.
- Performance: caching and vectorized numpy/pandas operations to support ~50k point series within seconds.

Timeline (for reference)
- Phase 1: Feature & forecasting enhancements (days 1-3).
- Phase 2: Detection, volatility, and evaluation upgrades (days 3-5).
- Phase 3: MLOps orchestration + docs/tests (days 5-7).

