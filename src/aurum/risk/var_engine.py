"""Value-at-Risk engine and portfolio risk analytics.

Features
- VaR/CVaR via Historical, Parametric (variance-covariance), and Monte Carlo
- Portfolio aggregation, factor exposure handling, and risk attribution
- Counterparty risk assessment (rating-weighted exposures)
- Risk limit monitoring with optional alert dispatch
- Scenario analysis utilities and dashboard assembly
- Optional integration with external data providers (EIA/FRED/ISO)

Design
- Pure-Python + NumPy/Pandas implementation to keep runtime light
- Optional dependencies and integrations are guarded and fail-safe
- Pydantic models define IO contracts and ease API wiring
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from datetime import datetime

import math
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, validator

try:  # Optional, used for alerting if available
    from aurum.observability.telemetry_facade import get_telemetry_facade
except Exception:  # pragma: no cover - optional runtime
    get_telemetry_facade = None  # type: ignore[assignment]

try:  # Optional provider access
    from aurum.data.external_provider_adapters import ExternalProviderAdapter, EiaProviderAdapter, FredProviderAdapter
except Exception:  # pragma: no cover - optional runtime
    ExternalProviderAdapter = None  # type: ignore[assignment]
    EiaProviderAdapter = None  # type: ignore[assignment]
    FredProviderAdapter = None  # type: ignore[assignment]


class VaRMethod(str, Enum):
    HISTORICAL = "historical"
    PARAMETRIC = "parametric"
    MONTE_CARLO = "monte_carlo"


class VaRConfig(BaseModel):
    method: VaRMethod = VaRMethod.MONTE_CARLO
    confidence_level: float = Field(0.95, ge=0.5, le=0.999)
    horizon_days: int = Field(1, ge=1, description="Time horizon for VaR in trading days")
    # Historical
    lookback_days: int = Field(252, ge=30)
    # Parametric
    covariance_method: str = Field("sample", description="sample|ewma(|lambda)|ledoit_wolf")
    distribution: str = Field("normal", description="normal|t|cornish_fisher")
    # Monte Carlo
    num_simulations: int = Field(10000, ge=1000, le=1000000)
    random_seed: Optional[int] = None

    @validator("covariance_method")
    def _validate_cov(cls, v: str) -> str:
        v = (v or "sample").lower()
        if not (v.startswith("sample") or v.startswith("ewma") or v.startswith("ledoit_wolf")):
            raise ValueError("covariance_method must be sample|ewma(|lambda)|ledoit_wolf")
        return v


class PositionInput(BaseModel):
    asset_id: str
    notional_value: float
    position_type: str = Field("long", description="long|short|hedge")
    currency: str = "USD"
    risk_factors: Dict[str, float] = Field(default_factory=dict, description="factor -> sensitivity")
    # Counterparty metadata (optional)
    counterparty: Optional[str] = None
    credit_rating: Optional[str] = Field(None, description="e.g., AAA, AA, A, BBB, BB, B, CCC")


class PortfolioInput(BaseModel):
    portfolio_id: str
    positions: List[PositionInput]


class RiskLimitBreach(BaseModel):
    limit_name: str
    value: float
    threshold: float
    severity: str = Field("warning", description="info|warning|critical")
    detail: Optional[str] = None


class RiskLimitsConfig(BaseModel):
    max_var: Optional[float] = Field(None, description="Absolute VaR threshold")
    max_cvar: Optional[float] = None
    max_position_var: Optional[float] = None
    max_concentration: Optional[float] = Field(
        None, description="Max single-position notional weight (0-1)"
    )
    max_counterparty_exposure: Optional[float] = None
    alert_channel: Optional[str] = Field(
        None, description="Optional channel/topic name for alerts"
    )


class CounterpartyExposure(BaseModel):
    counterparty: str
    exposure: float
    risk_weight: float


class CounterpartyRiskResult(BaseModel):
    total_exposure: float
    exposures: List[CounterpartyExposure]
    top_counterparties: List[CounterpartyExposure]


class VaRResult(BaseModel):
    portfolio_id: str
    method: VaRMethod
    confidence_level: float
    horizon_days: int
    var: float
    cvar: float
    volatility: float
    mean_pnl: float
    max_drawdown: float
    position_var: Dict[str, float]
    factor_contributions: Dict[str, float]
    breaches: List[RiskLimitBreach] = Field(default_factory=list)
    counterparty_risk: Optional[CounterpartyRiskResult] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RiskDashboard(BaseModel):
    portfolio_id: str
    as_of: datetime
    metrics: Dict[str, Any]
    var: VaRResult
    scenarios: Dict[str, Dict[str, Any]]
    alerts: List[RiskLimitBreach]


# --------- Helpers


def _direction_multiplier(position_type: str) -> float:
    t = (position_type or "long").lower()
    if t == "short":
        return -1.0
    if t == "hedge":
        return -0.5
    return 1.0


def _zscore(confidence: float) -> float:
    # Inverse CDF for normal via approximation
    # For 0.90->1.2816, 0.95->1.64485, 0.975->1.95996, 0.99->2.3263
    from math import sqrt, log

    if confidence <= 0 or confidence >= 1:
        return 0.0
    p = confidence
    # Beasley-Springer/Moro approximation
    a = [2.50662823884, -18.61500062529, 41.39119773534, -25.44106049637]
    b = [-8.47351093090, 23.08336743743, -21.06224101826, 3.13082909833]
    c = [0.3374754822726147, 0.9761690190917186, 0.1607979714918209,
         0.0276438810333863, 0.0038405729373609, 0.0003951896511919,
         0.0000321767881768, 0.0000002888167364, 0.0000003960315187]
    y = p - 0.5
    if abs(y) < 0.42:
        r = y * y
        num = y * (((a[3]*r + a[2])*r + a[1]) * r + a[0])
        den = (((b[3]*r + b[2])*r + b[1]) * r + b[0]) + 1.0
        x = num / den
        return x
    r = p
    if y > 0:
        r = 1 - p
    s = log(-log(r))
    x = c[0] + s * (c[1] + s * (c[2] + s * (c[3] + s * (c[4] + s * (c[5] + s * (c[6] + s * (c[7] + s * c[8])))))))
    return -x if y < 0 else x


def _max_drawdown(series: np.ndarray) -> float:
    if series.size == 0:
        return 0.0
    cum = np.cumsum(series)
    peak = np.maximum.accumulate(cum)
    drawdown = peak - cum
    return float(np.max(drawdown))


def _ewma_cov(returns: np.ndarray, lam: float = 0.94) -> np.ndarray:
    # returns: T x N
    t, n = returns.shape
    cov = np.cov(returns, rowvar=False, ddof=0)
    weights = np.array([lam ** (t - 1 - i) for i in range(t)])
    weights /= weights.sum() if weights.sum() else 1.0
    mean = np.average(returns, axis=0, weights=weights)
    demeaned = returns - mean
    # Weighted covariance
    cov = (demeaned.T * weights) @ demeaned
    return cov


# --------- External data access (optional)


class ExternalRiskDataClient:
    """Minimal adapter around ExternalProviderAdapter for factor series.

    This fetcher is optional and returns empty data if providers are not available.
    """

    def __init__(self, provider: str = "eia") -> None:
        self._provider = provider
        self._adapter = None
        if ExternalProviderAdapter is not None:
            try:
                if provider.lower() == "eia" and EiaProviderAdapter is not None:
                    self._adapter = EiaProviderAdapter()
                elif provider.lower() == "fred" and FredProviderAdapter is not None:
                    self._adapter = FredProviderAdapter()
                else:
                    self._adapter = ExternalProviderAdapter(provider)
            except Exception:
                self._adapter = None

    async def fetch_returns(
        self,
        series_map: Mapping[str, str],
        *,
        lookback_days: int = 252,
    ) -> Dict[str, np.ndarray]:
        """Fetch factor series by id and convert to log returns.

        Args:
            series_map: factor -> external series id
            lookback_days: limit lookback window
        Returns: factor -> returns array (np.ndarray)
        """
        if self._adapter is None:
            return {}
        try:
            results: Dict[str, np.ndarray] = {}
            for factor, sid in series_map.items():
                records = await self._adapter.fetch_observations(sid, limit=lookback_days)
                # Expect records with fields {timestamp/date, value}
                values: List[float] = []
                for r in records:
                    v = r.get("value")
                    try:
                        values.append(float(v))
                    except Exception:
                        continue
                if len(values) > 2:
                    arr = np.array(values, dtype=float)
                    ret = np.diff(np.log(np.clip(arr, 1e-12, None)))
                    results[factor] = ret
            return results
        except Exception:
            return {}


# --------- VaR Engine


class VaREngine:
    def __init__(self, *, telemetry_source: str = "risk.var_engine") -> None:
        self._rng = np.random.default_rng()
        self._telemetry = None
        if get_telemetry_facade:
            try:
                self._telemetry = get_telemetry_facade()
            except Exception:
                self._telemetry = None
        self._telemetry_source = telemetry_source

    def _emit(self, level: str, message: str, **fields: Any) -> None:
        t = self._telemetry
        if not t:
            return
        try:
            if level == "info":
                t.info(message, source=self._telemetry_source, **fields)
            elif level == "error":
                t.error(message, source=self._telemetry_source, **fields)
            else:
                t.info(message, source=self._telemetry_source, **fields)
        except Exception:
            pass

    def _collect_factors(self, positions: Sequence[PositionInput]) -> List[str]:
        return sorted({f for p in positions for f in p.risk_factors.keys()})

    def _build_exposure_matrix(
        self, positions: Sequence[PositionInput], factor_names: Sequence[str]
    ) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        npos = len(positions)
        nf = len(factor_names)
        exposures = np.zeros((npos, nf))
        scalars = np.zeros(npos)
        names: List[str] = []
        for i, p in enumerate(positions):
            names.append(p.asset_id)
            scalars[i] = _direction_multiplier(p.position_type) * float(p.notional_value)
            for j, f in enumerate(factor_names):
                exposures[i, j] = float(p.risk_factors.get(f, 0.0))
        return exposures, scalars, names

    def _estimate_covariance(self, returns: np.ndarray, method: str) -> np.ndarray:
        if returns.size == 0:
            return np.eye(1)
        m = (method or "sample").lower()
        if m.startswith("sample"):
            cov = np.cov(returns, rowvar=False, ddof=0)
        elif m.startswith("ewma"):
            # ewma or ewma:<lambda>
            lam = 0.94
            parts = m.split(":", 1)
            if len(parts) == 2:
                try:
                    lam = float(parts[1])
                except Exception:
                    lam = 0.94
            cov = _ewma_cov(returns, lam=lam)
        else:
            # Try Ledoit-Wolf if available, fallback to sample
            cov = None
            try:
                from sklearn.covariance import LedoitWolf  # type: ignore

                lw = LedoitWolf(store_precision=False, assume_centered=False)
                lw.fit(returns)
                cov = lw.covariance_
            except Exception:
                cov = np.cov(returns, rowvar=False, ddof=0)
        # Make symmetric positive semi-definite
        cov = np.nan_to_num(cov, nan=0.0, posinf=0.0, neginf=0.0)
        try:
            # Clip small negatives from numerical noise
            eigvals, eigvecs = np.linalg.eigh(cov)
            eigvals = np.clip(eigvals, 0.0, None)
            cov = (eigvecs * eigvals) @ eigvecs.T
        except Exception:
            pass
        return cov

    def _parametric_var(
        self,
        exposures: np.ndarray,
        scalars: np.ndarray,
        factor_cov: np.ndarray,
        confidence_level: float,
    ) -> Tuple[float, float, float, Dict[str, float]]:
        # Portfolio weights by notional scaling of factor exposures
        # Portfolio PnL variance = (E*scalars)^T * Cov * (E*scalars)
        # Compute effective portfolio factor weights: w_f = sum_i (scalar_i * exposure_i,f)
        eff = exposures.T @ scalars  # N_factors
        var_port = float(eff.T @ factor_cov @ eff)
        std_port = math.sqrt(max(var_port, 0.0))
        z = abs(_zscore(confidence_level))
        var_val = max(z * std_port, 0.0)
        # CVaR under normal ~ var * (pdf(z)/(1-cl))
        # pdf(z) = 1/sqrt(2*pi)*exp(-z^2/2)
        cvar = var_val
        try:
            pdf = (1.0 / math.sqrt(2 * math.pi)) * math.exp(-(z ** 2) / 2)
            tail = max(1.0 - confidence_level, 1e-9)
            cvar = var_val + (pdf / tail - z) * std_port
            cvar = max(cvar, var_val)
        except Exception:
            pass
        # Position-level VaR via variance share approximation
        # Contribution_i ~ sqrt(Var_i) scaled to sum to portfolio VaR
        pos_var = np.sum((exposures @ factor_cov) * exposures, axis=1) * (scalars ** 2)
        pos_var = np.clip(pos_var, 0.0, None)
        sum_sqrt = np.sum(np.sqrt(pos_var)) or 1.0
        position_var = {
            str(i): float(var_val * (math.sqrt(v) / sum_sqrt)) for i, v in enumerate(pos_var)
        }
        return var_val, cvar, std_port, position_var

    def _historical_var(
        self,
        factor_returns: np.ndarray,
        exposures: np.ndarray,
        scalars: np.ndarray,
        confidence_level: float,
    ) -> Tuple[float, float, float, Dict[str, float], np.ndarray]:
        # factor_returns: T x Nfactors
        if factor_returns.size == 0:
            return 0.0, 0.0, 0.0, {}, np.zeros(0)
        pnl_positions = (factor_returns @ exposures.T) * scalars  # T x Npos
        pnl_portfolio = np.sum(pnl_positions, axis=1)  # T
        losses = -np.sort(pnl_portfolio)
        pct = confidence_level * 100
        var_val = float(np.percentile(losses, pct))
        tail = losses[losses >= var_val]
        cvar = float(np.mean(tail)) if tail.size else var_val
        volatility = float(np.std(pnl_portfolio))
        # Position VaR as quantile of position PnLs
        pos_losses = -np.sort(pnl_positions, axis=0)
        pct_idx = int(min(max(round((pct / 100.0) * (pos_losses.shape[0] - 1)), 0), pos_losses.shape[0] - 1))
        position_var = {str(i): float(pos_losses[pct_idx, i]) for i in range(pos_losses.shape[1])}
        return var_val, max(cvar, var_val), volatility, position_var, pnl_portfolio

    def _monte_carlo_var(
        self,
        exposures: np.ndarray,
        scalars: np.ndarray,
        factor_cov: np.ndarray,
        confidence_level: float,
        num_simulations: int,
        seed: Optional[int] = None,
    ) -> Tuple[float, float, float, Dict[str, float], np.ndarray]:
        rng = np.random.default_rng(seed) if seed is not None else self._rng
        nf = factor_cov.shape[0]
        mean = np.zeros(nf)
        # Regularize covariance for stability
        safe_cov = factor_cov + np.eye(nf) * 1e-9
        base = rng.multivariate_normal(mean, safe_cov, size=num_simulations, method="eigh")
        pnl_positions = (base @ exposures.T) * scalars  # sims x Npos
        portfolio = np.sum(pnl_positions, axis=1)
        losses = -np.sort(portfolio)
        pct = confidence_level * 100
        var_val = float(np.percentile(losses, pct))
        tail = losses[losses >= var_val]
        cvar = float(np.mean(tail)) if tail.size else var_val
        volatility = float(np.std(portfolio))
        pos_losses = -np.sort(pnl_positions, axis=0)
        pct_idx = int(min(max(round((pct / 100.0) * (pos_losses.shape[0] - 1)), 0), pos_losses.shape[0] - 1))
        position_var = {str(i): float(pos_losses[pct_idx, i]) for i in range(pos_losses.shape[1])}
        return var_val, max(cvar, var_val), volatility, position_var, portfolio

    def _counterparty_risk(self, positions: Sequence[PositionInput]) -> CounterpartyRiskResult:
        # Simple Basel-like risk weights mapping
        weights = {
            "aaa": 0.01,
            "aa": 0.02,
            "a": 0.03,
            "bbb": 0.06,
            "bb": 0.12,
            "b": 0.20,
            "ccc": 0.50,
        }
        expo: Dict[str, Tuple[float, float]] = {}
        total = 0.0
        for p in positions:
            cp = (p.counterparty or "UNKNOWN").upper()
            rw = weights.get((p.credit_rating or "bbb").lower(), 0.06)
            eff = max(float(p.notional_value), 0.0) * abs(_direction_multiplier(p.position_type))
            total += eff
            e, _ = expo.get(cp, (0.0, rw))
            expo[cp] = (e + eff, rw)
        exposures = [CounterpartyExposure(counterparty=k, exposure=v[0], risk_weight=v[1]) for k, v in expo.items()]
        exposures.sort(key=lambda x: x.exposure, reverse=True)
        top = exposures[: min(5, len(exposures))]
        return CounterpartyRiskResult(total_exposure=total, exposures=exposures, top_counterparties=top)

    def _check_limits(
        self,
        result: VaRResult,
        positions: Sequence[PositionInput],
        limits: Optional[RiskLimitsConfig],
    ) -> List[RiskLimitBreach]:
        if not limits:
            return []
        breaches: List[RiskLimitBreach] = []
        if limits.max_var is not None and result.var > limits.max_var:
            breaches.append(
                RiskLimitBreach(
                    limit_name="max_var", value=result.var, threshold=limits.max_var, severity="critical"
                )
            )
        if limits.max_cvar is not None and result.cvar > limits.max_cvar:
            breaches.append(
                RiskLimitBreach(
                    limit_name="max_cvar", value=result.cvar, threshold=limits.max_cvar, severity="critical"
                )
            )
        if limits.max_position_var is not None and result.position_var:
            max_pos = max(result.position_var.values())
            if max_pos > limits.max_position_var:
                breaches.append(
                    RiskLimitBreach(
                        limit_name="max_position_var", value=max_pos, threshold=limits.max_position_var, severity="warning"
                    )
                )
        if limits.max_concentration is not None and positions:
            notionals = [abs(p.notional_value) for p in positions]
            total = sum(notionals) or 1.0
            concentration = max(notionals) / total
            if concentration > limits.max_concentration:
                breaches.append(
                    RiskLimitBreach(
                        limit_name="max_concentration", value=concentration, threshold=limits.max_concentration, severity="warning"
                    )
                )
        if limits.max_counterparty_exposure is not None and result.counterparty_risk:
            top = result.counterparty_risk.top_counterparties[0].exposure if result.counterparty_risk.top_counterparties else 0.0
            if top > limits.max_counterparty_exposure:
                breaches.append(
                    RiskLimitBreach(
                        limit_name="max_counterparty_exposure",
                        value=top,
                        threshold=limits.max_counterparty_exposure,
                        severity="critical",
                    )
                )
        return breaches

    def _maybe_alert(self, portfolio_id: str, breaches: Sequence[RiskLimitBreach], channel: Optional[str]) -> None:
        if not breaches:
            return
        # Structured log for observability
        payload = [b.dict() for b in breaches]
        self._emit(
            "info",
            "Risk limit breaches detected",
            portfolio_id=portfolio_id,
            breaches=payload,
            channel=channel,
        )
        # Emit alerts via telemetry facade (routes to AlertManager/Kafka)
        try:
            if self._telemetry and hasattr(self._telemetry, "create_alert"):
                for breach in breaches:
                    sev = "CRITICAL" if (breach.severity or "").lower() == "critical" else (
                        "HIGH" if (breach.severity or "").lower() == "high" else "MEDIUM"
                    )
                    self._telemetry.create_alert(
                        title=f"Risk limit breach: {breach.limit_name}",
                        message=f"Portfolio {portfolio_id} breached {breach.limit_name}: {breach.value:.4f} > {breach.threshold:.4f}",
                        severity=sev,
                        portfolio_id=portfolio_id,
                        limit_name=breach.limit_name,
                        value=breach.value,
                        threshold=breach.threshold,
                        channel=channel,
                        component="risk.var_engine",
                    )
        except Exception:
            # Non-fatal
            pass

    def _factor_contributions(
        self, factor_cov: np.ndarray, exposures: np.ndarray, scalars: np.ndarray, factor_names: Sequence[str]
    ) -> Dict[str, float]:
        eff = exposures.T @ scalars
        total_var = float(eff.T @ factor_cov @ eff)
        if total_var <= 0:
            return {f: 0.0 for f in factor_names}
        contrib = (eff * (factor_cov @ eff)) / total_var
        return {f: float(max(c, 0.0)) for f, c in zip(factor_names, contrib)}

    def _build_result(
        self,
        portfolio: PortfolioInput,
        method: VaRMethod,
        config: VaRConfig,
        var_val: float,
        cvar_val: float,
        vol: float,
        position_var: Dict[str, float],
        factor_contrib: Dict[str, float],
        portfolio_series: Optional[np.ndarray],
        limits: Optional[RiskLimitsConfig],
        positions: Sequence[PositionInput],
        extra_meta: Optional[Dict[str, Any]] = None,
    ) -> VaRResult:
        cp_risk = self._counterparty_risk(positions)
        max_dd = _max_drawdown(portfolio_series if portfolio_series is not None else np.array([]))
        result = VaRResult(
            portfolio_id=portfolio.portfolio_id,
            method=method,
            confidence_level=config.confidence_level,
            horizon_days=config.horizon_days,
            var=max(var_val, 0.0),
            cvar=max(cvar_val, var_val),
            volatility=max(vol, 0.0),
            mean_pnl=float(np.mean(portfolio_series)) if portfolio_series is not None and portfolio_series.size else 0.0,
            max_drawdown=max_dd,
            position_var=position_var,
            factor_contributions=factor_contrib,
            counterparty_risk=cp_risk,
            metadata=extra_meta or {},
        )
        breaches = self._check_limits(result, positions, limits)
        result.breaches = breaches
        if breaches:
            self._maybe_alert(portfolio.portfolio_id, breaches, limits.alert_channel if limits else None)
        return result

    def calculate_var(
        self,
        portfolio: PortfolioInput,
        config: Optional[VaRConfig] = None,
        *,
        factor_returns: Optional[Dict[str, np.ndarray]] = None,
        limits: Optional[RiskLimitsConfig] = None,
    ) -> VaRResult:
        cfg = config or VaRConfig()
        positions = tuple(portfolio.positions)
        factor_names = self._collect_factors(positions)
        exposures, scalars, _ = self._build_exposure_matrix(positions, factor_names)

        factor_cov: Optional[np.ndarray] = None
        portfolio_series: Optional[np.ndarray] = None
        var_val = cvar_val = vol = 0.0
        position_var: Dict[str, float] = {}

        if cfg.method == VaRMethod.PARAMETRIC:
            # Estimate covariance from historical factor returns if available; otherwise identity
            if factor_returns and factor_names:
                # Build T x N matrix
                cols = [np.asarray(factor_returns.get(f, np.zeros(0)), dtype=float) for f in factor_names]
                # Align to same length
                min_len = min((len(c) for c in cols if len(c) > 0), default=0)
                arr = np.vstack([c[-min_len:] for c in cols]).T if min_len > 0 else np.zeros((0, len(cols)))
            else:
                arr = np.zeros((0, len(factor_names)))
            factor_cov = self._estimate_covariance(arr, cfg.covariance_method) if arr.size else np.eye(len(factor_names) or 1)
            var_val, cvar_val, vol, position_var = self._parametric_var(exposures, scalars, factor_cov, cfg.confidence_level)
        elif cfg.method == VaRMethod.HISTORICAL:
            if not factor_returns:
                factor_returns = {f: np.zeros(0) for f in factor_names}
            cols = [np.asarray(factor_returns.get(f, np.zeros(0)), dtype=float) for f in factor_names]
            min_len = min((len(c) for c in cols if len(c) > 0), default=0)
            arr = np.vstack([c[-min_len:] for c in cols]).T if min_len > 0 else np.zeros((0, len(cols)))
            var_val, cvar_val, vol, position_var, portfolio_series = self._historical_var(
                arr, exposures, scalars, cfg.confidence_level
            )
            factor_cov = np.cov(arr, rowvar=False, ddof=0) if arr.size else np.eye(len(factor_names) or 1)
        else:  # Monte Carlo
            if factor_returns and factor_names:
                cols = [np.asarray(factor_returns.get(f, np.zeros(0)), dtype=float) for f in factor_names]
                min_len = min((len(c) for c in cols if len(c) > 0), default=0)
                arr = np.vstack([c[-min_len:] for c in cols]).T if min_len > 0 else np.zeros((0, len(cols)))
                factor_cov = self._estimate_covariance(arr, cfg.covariance_method) if arr.size else np.eye(len(factor_names) or 1)
            else:
                factor_cov = np.eye(len(factor_names) or 1)
            var_val, cvar_val, vol, position_var, portfolio_series = self._monte_carlo_var(
                exposures,
                scalars,
                factor_cov,
                cfg.confidence_level,
                cfg.num_simulations,
                cfg.random_seed,
            )

        factor_contrib = self._factor_contributions(factor_cov if factor_cov is not None else np.eye(len(factor_names) or 1), exposures, scalars, factor_names)
        meta = {
            "factors": factor_names,
            "num_positions": len(positions),
            "method": cfg.method.value,
        }
        return self._build_result(
            portfolio,
            cfg.method,
            cfg,
            var_val,
            cvar_val,
            vol,
            position_var,
            factor_contrib,
            portfolio_series,
            limits,
            positions,
            extra_meta=meta,
        )

    def scenario_pnl(
        self,
        portfolio: PortfolioInput,
        factor_shocks: Mapping[str, float],
    ) -> float:
        """Apply simple factor shocks to compute instantaneous PnL impact.

        Positive shock is interpreted as factor increase; exposure sign + direction
        determine PnL sign.
        """
        positions = portfolio.positions
        if not positions:
            return 0.0
        factor_names = self._collect_factors(positions)
        exposures, scalars, _ = self._build_exposure_matrix(positions, factor_names)
        shock_vec = np.array([float(factor_shocks.get(f, 0.0)) for f in factor_names])  # Nf
        pnl_positions = (exposures @ shock_vec) * scalars  # Npos
        return float(np.sum(pnl_positions))

    def build_dashboard(
        self,
        portfolio: PortfolioInput,
        var_result: VaRResult,
        scenarios: Mapping[str, Mapping[str, float]] | None = None,
    ) -> RiskDashboard:
        scen_results: Dict[str, Dict[str, Any]] = {}
        for name, shocks in (scenarios or {}).items():
            try:
                pnl = self.scenario_pnl(portfolio, shocks)
                scen_results[name] = {"pnl_impact": pnl, "shocks": dict(shocks)}
            except Exception as exc:  # pragma: no cover - defensive
                scen_results[name] = {"error": str(exc), "shocks": dict(shocks)}
        metrics = {
            "var": var_result.var,
            "cvar": var_result.cvar,
            "volatility": var_result.volatility,
            "max_drawdown": var_result.max_drawdown,
            "top_counterparties": [c.dict() for c in (var_result.counterparty_risk.top_counterparties if var_result.counterparty_risk else [])],
        }
        alerts = list(var_result.breaches)
        return RiskDashboard(
            portfolio_id=portfolio.portfolio_id,
            as_of=datetime.utcnow(),
            metrics=metrics,
            var=var_result,
            scenarios=scen_results,
            alerts=alerts,
        )
