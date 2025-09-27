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
    get_training_features
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
]
