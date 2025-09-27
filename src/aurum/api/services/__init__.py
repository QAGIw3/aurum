"""Domain service façade modules.

These modules provide a stable, typed surface for routers while the legacy
`aurum.api.service` is gradually decomposed. Import services from here instead
of the legacy module to ease future refactors.

Phase 1.3 Service Layer Decomposition:
- Each domain has dedicated service classes
- Services implement standard interfaces for consistency
- DAO pattern separates data access from business logic
- >85% test coverage requirement for service layer
"""

from .base_service import (
    ServiceInterface,
    QueryableServiceInterface,
    DimensionalServiceInterface,
    ExportableServiceInterface,
)
from .curves_service import CurvesService
from .metadata_service import MetadataService
from .ppa_service import PpaService
from .drought_service import DroughtService
from .iso_service import IsoService
from .eia_service import EiaService
from .scenario_service import ScenarioService
from .feature_store_service import (
    FeatureStoreService,
    FeatureConfig,
    FeatureDefinition,
    CrossAssetFeature,
    get_feature_store_service,
    get_features_for_scenario,
    get_training_features,
    create_time_window_features,
    create_lag_features,
    create_seasonal_features
)
from .auto_reforecast_service import (
    AutoReforecastService,
    ForecastTrigger,
    TriggerCondition,
    TriggerEvent,
    ReforcastJob,
    DebounceConfig,
    BackpressureConfig,
    get_auto_reforecast_service,
    trigger_forecast_rerun,
    create_weather_trigger,
    create_price_trigger
)
from .renewables_ingestion_service import (
    RenewablesIngestionService,
    DataSourceConfig,
    IngestionJob,
    DataQualityCheck,
    RenewablesDataPoint,
    RenewablesDataset,
    RenewablesIngestionDAO,
    get_renewables_ingestion_service,
    ingest_satellite_data,
    ingest_weather_station_data
)
from .model_registry_service import (
    ModelRegistryService,
    ModelConfig,
    ModelVersion,
    TrainingJob,
    ModelComparison,
    RetrainSchedule,
    ModelRegistryDAO,
    get_model_registry_service,
    train_load_forecasting_model,
    train_price_forecasting_model,
    get_current_champion_model
)
from .bidding_rl_service import (
    BiddingRLService,
    AuctionEnvironment,
    BiddingPolicy,
    RLTrainingSession,
    PolicyEvaluation,
    AuctionResult,
    get_bidding_rl_service,
    simulate_auction_scenario,
    evaluate_bidding_policy
)
from .carbon_rec_service import (
    CarbonRecService,
    CarbonInstrument,
    CarbonPricing,
    PortfolioCarbonExposure,
    RECTrading,
    CarbonInstrumentType,
    CarbonMarket,
    get_carbon_rec_service,
    calculate_asset_carbon_impact,
    analyze_portfolio_carbon_risk
)
from .risk_engine_service import (
    RiskEngineService,
    PortfolioPosition,
    RiskDistributionConfig,
    RiskScenario,
    RiskCalculationResult,
    PortfolioAggregation,
    RiskDistributionType,
    CorrelationModel,
    RiskMetricType,
    get_risk_engine_service,
    calculate_portfolio_risk_metrics,
    run_stress_test
)
from .regulatory_tracker_service import (
    RegulatoryTrackerService,
    RegulatoryArtifact,
    RegulatoryAlert,
    PolicyTagging,
    RegulatorySource,
    PolicyImpactLevel,
    get_regulatory_tracker_service,
    ingest_regulatory_updates,
    get_regulatory_impact_for_portfolio,
    get_market_regulatory_summary
)
from .plugin_system_service import (
    PluginSystemService,
    PluginContract,
    PluginInstance,
    PluginSecurityLevel,
    PluginStatus,
    get_plugin_system_service,
    discover_and_load_plugins
)
from .developer_workspace_service import (
    DeveloperWorkspaceService,
    NotebookEnvironment,
    NotebookSession,
    NotebookTemplate,
    get_developer_workspace_service,
    create_notebook_session,
    get_api_documentation
)
from .dbt_management_service import (
    DBTManagementService,
    DBTModel,
    DataMart,
    TestFixture,
    LineageNode,
    FreshnessCheck,
    get_dbt_management_service,
    run_model_tests,
    generate_development_fixtures,
    analyze_model_impact
)
from .performance_monitoring_service import (
    PerformanceMonitoringService,
    PerformanceBudget,
    LoadTestScenario,
    PerformanceTestResult,
    PerformanceComparison,
    get_performance_monitoring_service,
    run_performance_regression_check
)

__all__ = [
    # Base interfaces
    "ServiceInterface",
    "QueryableServiceInterface",
    "DimensionalServiceInterface",
    "ExportableServiceInterface",
    # Domain services
    "CurvesService",
    "MetadataService",
    "PpaService",
    "DroughtService",
    "IsoService",
    "EiaService",
    "ScenarioService",
    "FeatureStoreService",
    # Feature store components
    "FeatureConfig",
    "FeatureDefinition",
    "CrossAssetFeature",
    "get_feature_store_service",
    "get_features_for_scenario",
    "get_training_features",
    "create_time_window_features",
    "create_lag_features",
    "create_seasonal_features",
    # Auto-reforecast components
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
    # Renewables ingestion components
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
    # Model registry components
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
    # Bidding RL components
    "BiddingRLService",
    "AuctionEnvironment",
    "BiddingPolicy",
    "RLTrainingSession",
    "PolicyEvaluation",
    "AuctionResult",
    "get_bidding_rl_service",
    "simulate_auction_scenario",
    "evaluate_bidding_policy",
    # Carbon REC components
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
    # Risk engine components
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
    # Regulatory tracker components
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
    # Plugin system components
    "PluginSystemService",
    "PluginContract",
    "PluginInstance",
    "PluginSecurityLevel",
    "PluginStatus",
    "get_plugin_system_service",
    "discover_and_load_plugins",
    # Developer workspace components
    "DeveloperWorkspaceService",
    "NotebookEnvironment",
    "NotebookSession",
    "NotebookTemplate",
    "get_developer_workspace_service",
    "create_notebook_session",
    "get_api_documentation",
    # Performance monitoring components
    "PerformanceMonitoringService",
    "PerformanceBudget",
    "LoadTestScenario",
    "PerformanceTestResult",
    "PerformanceComparison",
    "get_performance_monitoring_service",
    "run_performance_regression_check",
    # DBT management components
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
]
