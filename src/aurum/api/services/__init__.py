"""Domain service façade modules with fault-tolerant imports.

The legacy router layer expects to import dozens of service classes from this
package.  Some of those modules depend on optional infrastructure (e.g.
FastAPI routers or external SDKs) that might not be available in lightweight
test environments.  To keep the package importable we lazily import each
service and quietly skip the ones that fail, rather than failing the entire
package import.
"""

from __future__ import annotations

import logging
from importlib import import_module
from typing import Iterable, Tuple

_logger = logging.getLogger(__name__)


_EXPORTS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    (".base_service", (
        "ServiceInterface",
        "QueryableServiceInterface",
        "DimensionalServiceInterface",
        "ExportableServiceInterface",
    )),
    (".curves_service", ("CurvesService",)),
    (".metadata_service", ("MetadataService",)),
    (".ppa_service", ("PpaService",)),
    (".drought_service", ("DroughtService",)),
    (".iso_service", ("IsoService",)),
    (".eia_service", ("EiaService",)),
    (".scenario_service", ("ScenarioService",)),
    (".feature_store_service", (
        "FeatureStoreService",
        "FeatureConfig",
        "FeatureDefinition",
        "CrossAssetFeature",
        "get_feature_store_service",
        "get_features_for_scenario",
        "get_training_features",
        "create_time_window_features",
        "create_lag_features",
        "create_seasonal_features",
    )),
    (".auto_reforecast_service", (
        "AutoReforecastService",
        "ForecastTrigger",
        "TriggerCondition",
        "TriggerEvent",
        "ReforcastJob",
        "DebounceConfig",
        "BackpressureConfig",
        "get_auto_reforecast_service",
        "trigger_forecast_rerun",
        "create_weather_trigger",
        "create_price_trigger",
    )),
    (".renewables_ingestion_service", (
        "RenewablesIngestionService",
        "DataSourceConfig",
        "IngestionJob",
        "DataQualityCheck",
        "RenewablesDataPoint",
        "RenewablesDataset",
        "RenewablesIngestionDAO",
        "get_renewables_ingestion_service",
        "ingest_satellite_data",
        "ingest_weather_station_data",
    )),
    (".model_registry_service", (
        "ModelRegistryService",
        "ModelConfig",
        "ModelVersion",
        "TrainingJob",
        "ModelComparison",
        "RetrainSchedule",
        "ModelRegistryDAO",
        "get_model_registry_service",
        "train_load_forecasting_model",
        "train_price_forecasting_model",
        "get_current_champion_model",
    )),
    (".bidding_rl_service", (
        "BiddingRLService",
        "AuctionEnvironment",
        "BiddingPolicy",
        "RLTrainingSession",
        "PolicyEvaluation",
        "AuctionResult",
        "get_bidding_rl_service",
        "simulate_auction_scenario",
        "evaluate_bidding_policy",
    )),
    (".carbon_rec_service", (
        "CarbonRecService",
        "CarbonInstrument",
        "CarbonPricing",
        "PortfolioCarbonExposure",
        "RECTrading",
        "CarbonInstrumentType",
        "CarbonMarket",
        "get_carbon_rec_service",
        "calculate_asset_carbon_impact",
        "analyze_portfolio_carbon_risk",
    )),
    (".risk_engine_service", (
        "RiskEngineService",
        "PortfolioPosition",
        "RiskDistributionConfig",
        "RiskScenario",
        "RiskCalculationResult",
        "PortfolioAggregation",
        "RiskDistributionType",
        "CorrelationModel",
        "RiskMetricType",
        "get_risk_engine_service",
        "calculate_portfolio_risk_metrics",
        "run_stress_test",
    )),
    (".regulatory_tracker_service", (
        "RegulatoryTrackerService",
        "RegulatoryArtifact",
        "RegulatoryAlert",
        "PolicyTagging",
        "RegulatorySource",
        "PolicyImpactLevel",
        "get_regulatory_tracker_service",
        "ingest_regulatory_updates",
        "get_regulatory_impact_for_portfolio",
        "get_market_regulatory_summary",
    )),
    (".plugin_system_service", (
        "PluginSystemService",
        "PluginContract",
        "PluginInstance",
        "PluginSecurityLevel",
        "PluginStatus",
        "get_plugin_system_service",
        "discover_and_load_plugins",
    )),
    (".developer_workspace_service", (
        "DeveloperWorkspaceService",
        "NotebookEnvironment",
        "NotebookSession",
        "NotebookTemplate",
        "get_developer_workspace_service",
        "create_notebook_session",
        "get_api_documentation",
    )),
    (".dbt_management_service", (
        "DBTManagementService",
        "DBTModel",
        "DataMart",
        "TestFixture",
        "LineageNode",
        "FreshnessCheck",
        "get_dbt_management_service",
        "run_model_tests",
        "generate_development_fixtures",
        "analyze_model_impact",
    )),
    (".performance_monitoring_service", (
        "PerformanceMonitoringService",
        "PerformanceBudget",
        "LoadTestScenario",
        "PerformanceTestResult",
        "PerformanceComparison",
        "get_performance_monitoring_service",
        "run_performance_regression_check",
    )),
    (".esg_risk_service", (
        "ESGRiskService",
        "ESGScore",
        "ESGRiskCategory",
        "ESGRiskMetric",
        "ESGPortfolioAnalysis",
        "ESGAdjustedRiskResult",
        "get_esg_risk_service",
        "get_portfolio_esg_dashboard",
        "calculate_esg_adjusted_risk",
    )),
)


def _safe_import(module_name: str, names: Iterable[str]) -> None:
    try:
        module = import_module(module_name, package=__name__)
    except Exception:  # pragma: no cover - defensive guard
        _logger.debug("Skipping import for %%s due to error", module_name, exc_info=True)
        return

    for name in names:
        try:
            globals()[name] = getattr(module, name)
        except AttributeError:
            _logger.debug(
                "Module %%s does not expose expected attribute %%s", module_name, name,
                exc_info=True,
            )
            continue
        __all__.append(name)


__all__: list[str] = []

for module_name, exports in _EXPORTS:
    _safe_import(module_name, exports)

__all__ = sorted(set(__all__))


def __getattr__(name: str) -> object:
    """Provide a helpful error when a requested export is unavailable."""
    if name in {item for _, items in _EXPORTS for item in items}:
        raise AttributeError(
            f"{name} is not available because its module failed to import."
        ) from None
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
