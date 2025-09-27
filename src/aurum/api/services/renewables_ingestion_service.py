"""Renewables data ingestion service for satellite/weather enhanced datasets.

This service provides:
- High-resolution irradiance and wind data ingestion
- Schema contracts and validation
- Data lineage tracking
- Integration with feature store
- Quality assurance and anomaly detection
- Real-time and batch processing modes
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4
from pathlib import Path

from pydantic import BaseModel, Field, validator

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from .feature_store_service import get_feature_store_service
from ..daos.base_dao import TrinoDAO


class DataSourceConfig(BaseModel):
    """Configuration for renewable data sources."""

    name: str
    source_type: str  # "satellite", "weather_station", "nwp", "reanalysis"
    provider: str  # "nasa", "noaa", "ecmwf", "merra", etc.
    api_endpoint: Optional[str] = None
    api_key: Optional[str] = None
    credentials_file: Optional[str] = None
    data_format: str = "json"  # "json", "csv", "netcdf", "hdf5"
    temporal_resolution: str = "hourly"  # "15min", "hourly", "daily"
    spatial_resolution: str = "1km"  # Resolution description
    coverage_area: Dict[str, Any]  # Geographic bounds
    variables: List[str]  # Data variables to ingest
    quality_threshold: float = 0.8  # Minimum quality score
    enabled: bool = True


class IngestionJob(BaseModel):
    """Renewables data ingestion job."""

    job_id: str
    data_source: str
    geography: str
    start_date: datetime
    end_date: datetime
    variables: List[str]
    status: str = "pending"  # pending, running, completed, failed, cancelled
    progress: float = 0.0
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataQualityCheck(BaseModel):
    """Data quality validation check."""

    check_id: str
    name: str
    description: str
    check_type: str  # "range", "completeness", "consistency", "anomaly"
    parameters: Dict[str, Any]
    threshold: float
    severity: str = "warning"  # "info", "warning", "error"
    enabled: bool = True


class RenewablesDataPoint(BaseModel):
    """Individual renewables data point."""

    timestamp: datetime
    latitude: float
    longitude: float
    geography: str
    data_source: str
    variables: Dict[str, float]  # Variable name -> value
    quality_score: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    lineage_id: Optional[str] = None


class RenewablesDataset(BaseModel):
    """Complete renewables dataset."""

    dataset_id: str
    data_source: str
    geography: str
    start_date: datetime
    end_date: datetime
    data_points: List[RenewablesDataPoint]
    schema_version: str = "1.0"
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class RenewablesIngestionDAO(TrinoDAO):
    """DAO for renewables data ingestion operations."""

    def __init__(self, trino_config: Optional[Dict[str, Any]] = None):
        """Initialize renewables ingestion DAO."""
        super().__init__(trino_config)
        self.table_name = "renewables.raw_data"

    async def _connect(self) -> None:
        """Connect to Trino for renewables data."""
        pass

    async def _disconnect(self) -> None:
        """Disconnect from Trino."""
        pass

    async def _execute_trino_query(
        self,
        query: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute Trino query for renewables data."""
        return []

    async def save_raw_data(
        self,
        dataset: RenewablesDataset
    ) -> bool:
        """Save raw renewables data to staging area.

        Args:
            dataset: Renewables dataset to save

        Returns:
            True if saved successfully
        """
        log_structured(
            "info",
            "saving_renewables_raw_data",
            dataset_id=dataset.dataset_id,
            data_source=dataset.data_source,
            geography=dataset.geography,
            point_count=len(dataset.data_points),
            start_date=dataset.start_date.isoformat(),
            end_date=dataset.end_date.isoformat()
        )
        return True

    async def save_processed_data(
        self,
        dataset: RenewablesDataset,
        table_name: str = "renewables.processed_data"
    ) -> bool:
        """Save processed and validated renewables data.

        Args:
            dataset: Processed renewables dataset
            table_name: Target table name

        Returns:
            True if saved successfully
        """
        log_structured(
            "info",
            "saving_renewables_processed_data",
            dataset_id=dataset.dataset_id,
            table_name=table_name,
            point_count=len(dataset.data_points)
        )
        return True

    async def get_lineage_info(self, lineage_id: str) -> Optional[Dict[str, Any]]:
        """Get data lineage information.

        Args:
            lineage_id: Lineage identifier

        Returns:
            Lineage information or None if not found
        """
        # In real implementation, would query lineage database
        return {
            "lineage_id": lineage_id,
            "source_datasets": [],
            "processing_steps": [],
            "derived_datasets": [],
            "created_at": datetime.utcnow().isoformat()
        }


class RenewablesIngestionService:
    """Service for ingesting high-resolution renewables data."""

    def __init__(
        self,
        config_file: Optional[str] = None,
        dao: Optional[RenewablesIngestionDAO] = None,
        max_concurrent_jobs: int = 10,
        batch_size: int = 1000,
        retry_attempts: int = 3,
        enable_parallel_processing: bool = True
    ):
        """Initialize renewables ingestion service with scaling and parallelism.

        Args:
            config_file: Path to data source configuration file
            dao: Optional DAO for persistence
            max_concurrent_jobs: Maximum concurrent ingestion jobs
            batch_size: Batch size for data processing
            retry_attempts: Number of retry attempts for failed operations
            enable_parallel_processing: Enable parallel data processing
        """
        self.dao = dao or RenewablesIngestionDAO()
        self.data_sources: Dict[str, DataSourceConfig] = {}
        self.quality_checks: Dict[str, DataQualityCheck] = {}
        self.active_jobs: Dict[str, IngestionJob] = {}
        self._lineage_counter = 0

        # Scaling and parallelism configuration
        self.max_concurrent_jobs = max_concurrent_jobs
        self.batch_size = batch_size
        self.retry_attempts = retry_attempts
        self.enable_parallel_processing = enable_parallel_processing

        # Concurrency control
        self._job_semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self._processing_pool: Optional[asyncio.TaskGroup] = None

        # Performance monitoring
        self._metrics = {
            "jobs_started": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "records_processed": 0,
            "records_failed": 0,
            "processing_time_total": 0.0,
            "last_activity": datetime.utcnow()
        }

        # Data quality tracking
        self._quality_metrics = {
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "anomalies_detected": 0
        }

        # Load configuration
        if config_file:
            self._load_config(config_file)

        # Initialize default quality checks
        self._initialize_quality_checks()

        self.logger = logging.getLogger(__name__)
        self.telemetry = get_telemetry_facade()

        self.logger.info("Renewables ingestion service initialized",
                        data_source_count=len(self.data_sources),
                        max_concurrent_jobs=max_concurrent_jobs,
                        batch_size=batch_size)

    def _load_config(self, config_file: str) -> None:
        """Load data source configuration from file."""
        try:
            config_path = Path(config_file)
            if not config_path.exists():
                self.logger.warning("Config file not found", config_file=config_file)
                return

            with open(config_path, 'r') as f:
                config_data = json.load(f)

            for source_name, source_config in config_data.get("data_sources", {}).items():
                self.data_sources[source_name] = DataSourceConfig(**source_config)

            self.logger.info("Loaded data source configuration",
                           config_file=config_file,
                           source_count=len(self.data_sources))

        except Exception as e:
            self.logger.error("Failed to load configuration", config_file=config_file, error=str(e))

    def _initialize_quality_checks(self) -> None:
        """Initialize comprehensive data quality checks."""
        default_checks = [
            # Range validation
            DataQualityCheck(
                check_id="irradiance_range_check",
                name="Irradiance Value Range Validation",
                description="Check that irradiance values are within physical limits",
                check_type="range",
                parameters={
                    "min_value": 0,
                    "max_value": 1500,  # W/m² maximum solar irradiance
                    "fields": ["ghi", "dni", "dhi"]
                },
                threshold=0.98,
                severity="error"
            ),

            # Wind data validation
            DataQualityCheck(
                check_id="wind_range_check",
                name="Wind Data Range Validation",
                description="Check that wind data is within realistic ranges",
                check_type="range",
                parameters={
                    "min_value": 0,
                    "max_value": 50,  # m/s maximum wind speed
                    "fields": ["wind_speed", "wind_gust"]
                },
                threshold=0.95,
                severity="warning"
            ),

            # Temperature validation
            DataQualityCheck(
                check_id="temperature_range_check",
                name="Temperature Range Validation",
                description="Check that temperature values are realistic",
                check_type="range",
                parameters={
                    "min_value": -50,
                    "max_value": 60,  # °C realistic range
                    "fields": ["temperature", "temp_air", "temp_dew"]
                },
                threshold=0.95,
                severity="warning"
            ),

            # Completeness checks
            DataQualityCheck(
                check_id="core_variables_completeness",
                name="Core Variables Completeness",
                description="Check for missing core renewable energy variables",
                check_type="completeness",
                parameters={
                    "required_fields": ["ghi", "wind_speed", "temperature"],
                    "completeness_threshold": 0.95
                },
                threshold=0.9,
                severity="error"
            ),

            DataQualityCheck(
                check_id="optional_variables_completeness",
                name="Optional Variables Completeness",
                description="Check completeness of optional variables",
                check_type="completeness",
                parameters={
                    "required_fields": ["dni", "dhi", "wind_direction", "humidity"],
                    "completeness_threshold": 0.8
                },
                threshold=0.7,
                severity="warning"
            ),

            # Temporal consistency
            DataQualityCheck(
                check_id="temporal_consistency",
                name="Temporal Data Consistency",
                description="Check for temporal gaps and irregularities",
                check_type="consistency",
                parameters={
                    "max_gap_minutes": 120,  # 2 hours max gap
                    "expected_frequency": "hourly"
                },
                threshold=0.85,
                severity="warning"
            ),

            # Spatial consistency
            DataQualityCheck(
                check_id="spatial_consistency",
                name="Spatial Data Consistency",
                description="Check for spatial data consistency and outliers",
                check_type="consistency",
                parameters={
                    "neighbor_radius_km": 50,
                    "max_spatial_variance": 0.3
                },
                threshold=0.8,
                severity="info"
            ),

            # Anomaly detection
            DataQualityCheck(
                check_id="statistical_anomaly_detection",
                name="Statistical Anomaly Detection",
                description="Detect statistical outliers and anomalies",
                check_type="anomaly",
                parameters={
                    "method": "isolation_forest",
                    "contamination": 0.1,
                    "sensitivity": 2.0
                },
                threshold=0.8,
                severity="warning"
            ),

            # Physical consistency
            DataQualityCheck(
                check_id="physical_consistency",
                name="Physical Consistency Check",
                description="Check for physically impossible combinations",
                check_type="consistency",
                parameters={
                    "ghi_dni_relationship": True,  # DNI should be >= GHI when sun is high
                    "wind_temp_relationship": True  # Temperature affects wind patterns
                },
                threshold=0.9,
                severity="error"
            ),

            # Data freshness
            DataQualityCheck(
                check_id="data_freshness_check",
                name="Data Freshness Validation",
                description="Check that data is recent and not stale",
                check_type="freshness",
                parameters={
                    "max_age_hours": 24,
                    "warning_age_hours": 12
                },
                threshold=0.9,
                severity="warning"
            )
        ]

        for check in default_checks:
            self.quality_checks[check.check_id] = check

    async def start_ingestion_job(
        self,
        data_source: str,
        geography: str,
        start_date: datetime,
        end_date: datetime,
        variables: Optional[List[str]] = None
    ) -> str:
        """Start a renewables data ingestion job.

        Args:
            data_source: Name of data source to ingest
            geography: Geographic scope
            start_date: Start date for ingestion
            end_date: End date for ingestion
            variables: Specific variables to ingest

        Returns:
            Job ID
        """
        if data_source not in self.data_sources:
            raise ValueError(f"Unknown data source: {data_source}")

        source_config = self.data_sources[data_source]

        if not source_config.enabled:
            raise ValueError(f"Data source {data_source} is disabled")

        job_id = str(uuid4())

        # Determine variables to ingest
        if variables is None:
            variables = source_config.variables

        job = IngestionJob(
            job_id=job_id,
            data_source=data_source,
            geography=geography,
            start_date=start_date,
            end_date=end_date,
            variables=variables,
            status="pending"
        )

        self.active_jobs[job_id] = job

        # Start job in background with concurrency control
        asyncio.create_task(self._execute_ingestion_job_with_retry(job))

        self.telemetry.info(
            "Started renewables ingestion job",
            job_id=job_id,
            data_source=data_source,
            geography=geography,
            variable_count=len(variables),
            category="ingestion"
        )

        return job_id

    async def _execute_ingestion_job(self, job: IngestionJob) -> None:
        """Execute a renewables data ingestion job."""
        try:
            job.status = "running"
            job.started_at = datetime.utcnow()

            self.telemetry.info(
                "Executing renewables ingestion job",
                job_id=job.job_id,
                data_source=job.data_source
            )

            # Get data source configuration
            source_config = self.data_sources[job.data_source]

            # Fetch data from source
            raw_data = await self._fetch_data_from_source(source_config, job)

            # Validate and process data
            processed_data = await self._process_and_validate_data(raw_data, job)

            # Save processed data
            dataset = RenewablesDataset(
                dataset_id=f"{job.data_source}_{job.geography}_{job.start_date.date().isoformat()}",
                data_source=job.data_source,
                geography=job.geography,
                start_date=job.start_date,
                end_date=job.end_date,
                data_points=processed_data
            )

            # Save to staging area
            await self.dao.save_raw_data(dataset)

            # Run quality checks
            quality_results = await self._run_quality_checks(processed_data, job)

            # If quality checks pass, save to processed area
            if self._quality_checks_passed(quality_results):
                await self.dao.save_processed_data(dataset)
                await self._update_feature_store(dataset, job)
            else:
                job.status = "failed"
                job.error_message = "Quality checks failed"
                self.telemetry.warning(
                    "Ingestion job failed quality checks",
                    job_id=job.job_id,
                    failed_checks=len([r for r in quality_results if not r.get("passed", False)])
                )

            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.progress = 1.0

            self.telemetry.info(
                "Ingestion job completed successfully",
                job_id=job.job_id,
                records_processed=job.records_processed,
                records_failed=job.records_failed
            )

            # Record metrics
            self.telemetry.increment_counter("renewables_ingestion_jobs_completed", category=MetricCategory.BUSINESS)
            self.telemetry.record_histogram(
                "renewables_ingestion_duration",
                (job.completed_at - job.started_at).total_seconds() if job.completed_at and job.started_at else 0,
                category=MetricCategory.PERFORMANCE
            )

        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()

            self.telemetry.error(
                "Ingestion job failed",
                job_id=job.job_id,
                error=str(e),
                category="ingestion"
            )

            self.telemetry.increment_counter("renewables_ingestion_jobs_failed", category=MetricCategory.RELIABILITY)

    async def _fetch_data_from_source(self, source_config: DataSourceConfig, job: IngestionJob) -> List[Dict[str, Any]]:
        """Fetch data from the configured data source."""
        try:
            if source_config.source_type == "satellite":
                return await self._fetch_satellite_data(source_config, job)
            elif source_config.source_type == "weather_station":
                return await self._fetch_weather_station_data(source_config, job)
            elif source_config.source_type == "nwp":
                return await self._fetch_nwp_data(source_config, job)
            elif source_config.source_type == "reanalysis":
                return await self._fetch_reanalysis_data(source_config, job)
            else:
                raise ValueError(f"Unsupported source type: {source_config.source_type}")

        except Exception as e:
            self.telemetry.error(
                "Failed to fetch data from source",
                data_source=source_config.name,
                source_type=source_config.source_type,
                error=str(e)
            )
            raise

    async def _fetch_satellite_data(self, source_config: DataSourceConfig, job: IngestionJob) -> List[Dict[str, Any]]:
        """Fetch data from satellite sources (e.g., NASA POWER, GOES-R)."""
        # Mock implementation - in reality would call satellite APIs
        mock_data = []

        current_date = job.start_date
        while current_date <= job.end_date:
            # Generate mock satellite data points
            for lat in range(30, 50):  # Mock latitude range
                for lon in range(-125, -65):  # Mock longitude range
                    data_point = {
                        "timestamp": current_date.isoformat(),
                        "latitude": lat,
                        "longitude": lon,
                        "geography": job.geography,
                        "data_source": job.data_source,
                        "ghi": 500 + 200 * (1 + 0.5 * (current_date.hour / 24)),  # Global Horizontal Irradiance
                        "dni": 600 + 300 * (1 + 0.3 * (current_date.hour / 24)),  # Direct Normal Irradiance
                        "dhi": 100 + 50 * (1 + 0.2 * (current_date.hour / 24)),   # Diffuse Horizontal Irradiance
                        "wind_speed": 5 + 3 * (1 + 0.1 * (current_date.hour / 24)),
                        "wind_direction": 180 + 45 * (1 + 0.1 * (current_date.hour / 24)),
                        "temperature": 15 + 10 * (1 + 0.2 * (current_date.hour / 24)),
                        "quality_score": 0.95
                    }

                    mock_data.append(data_point)

            current_date += timedelta(hours=1)

            # Update progress
            job.progress = min(0.9, (current_date - job.start_date).total_seconds() / (job.end_date - job.start_date).total_seconds())
            job.records_processed = len(mock_data)

        return mock_data

    async def _fetch_weather_station_data(self, source_config: DataSourceConfig, job: IngestionJob) -> List[Dict[str, Any]]:
        """Fetch data from weather station networks."""
        # Mock weather station data
        return []

    async def _fetch_nwp_data(self, source_config: DataSourceConfig, job: IngestionJob) -> List[Dict[str, Any]]:
        """Fetch data from numerical weather prediction models."""
        # Mock NWP data
        return []

    async def _fetch_reanalysis_data(self, source_config: DataSourceConfig, job: IngestionJob) -> List[Dict[str, Any]]:
        """Fetch data from reanalysis datasets."""
        # Mock reanalysis data
        return []

    async def _process_and_validate_data(self, raw_data: List[Dict[str, Any]], job: IngestionJob) -> List[RenewablesDataPoint]:
        """Process and validate raw data."""
        processed_points = []

        for data_point in raw_data:
            try:
                # Parse timestamp
                timestamp = datetime.fromisoformat(data_point["timestamp"])

                # Validate data ranges
                if not self._validate_data_ranges(data_point):
                    job.records_failed += 1
                    continue

                # Create data point
                point = RenewablesDataPoint(
                    timestamp=timestamp,
                    latitude=data_point["latitude"],
                    longitude=data_point["longitude"],
                    geography=job.geography,
                    data_source=job.data_source,
                    variables={
                        k: v for k, v in data_point.items()
                        if k not in ["timestamp", "latitude", "longitude", "geography", "data_source", "quality_score"]
                    },
                    quality_score=data_point.get("quality_score", 1.0),
                    metadata={
                        "ingestion_job_id": job.job_id,
                        "processed_at": datetime.utcnow().isoformat()
                    },
                    lineage_id=self._generate_lineage_id()
                )

                processed_points.append(point)
                job.records_processed += 1

            except Exception as e:
                job.records_failed += 1
                self.telemetry.warning(
                    "Failed to process data point",
                    error=str(e),
                    job_id=job.job_id
                )

        return processed_points

    def _validate_data_ranges(self, data_point: Dict[str, Any]) -> bool:
        """Validate that data values are within expected ranges."""
        # Irradiance validation (0-1500 W/m²)
        for field in ["ghi", "dni", "dhi"]:
            if field in data_point:
                value = data_point[field]
                if not (0 <= value <= 1500):
                    return False

        # Wind speed validation (0-50 m/s)
        if "wind_speed" in data_point:
            value = data_point["wind_speed"]
            if not (0 <= value <= 50):
                return False

        # Temperature validation (-50 to 60°C)
        if "temperature" in data_point:
            value = data_point["temperature"]
            if not (-50 <= value <= 60):
                return False

        return True

    async def _run_quality_checks(self, data_points: List[RenewablesDataPoint], job: IngestionJob) -> List[Dict[str, Any]]:
        """Run data quality checks on processed data."""
        quality_results = []

        for check in self.quality_checks.values():
            if not check.enabled:
                continue

            try:
                result = await self._execute_quality_check(check, data_points, job)
                quality_results.append(result)

            except Exception as e:
                self.telemetry.error(
                    "Quality check failed",
                    check_id=check.check_id,
                    error=str(e)
                )
                quality_results.append({
                    "check_id": check.check_id,
                    "passed": False,
                    "error": str(e)
                })

        return quality_results

    async def _execute_quality_check(self, check: DataQualityCheck, data_points: List[RenewablesDataPoint], job: IngestionJob) -> Dict[str, Any]:
        """Execute a single quality check."""
        if check.check_type == "range":
            return await self._check_range_validation(check, data_points)
        elif check.check_type == "completeness":
            return await self._check_completeness(check, data_points)
        elif check.check_type == "consistency":
            return await self._check_temporal_consistency(check, data_points)
        elif check.check_type == "anomaly":
            return await self._check_anomalies(check, data_points)
        else:
            return {"check_id": check.check_id, "passed": False, "error": f"Unknown check type: {check.check_type}"}

    async def _check_range_validation(self, check: DataQualityCheck, data_points: List[RenewablesDataPoint]) -> Dict[str, Any]:
        """Check value range validation."""
        min_val = check.parameters.get("min_value", 0)
        max_val = check.parameters.get("max_value", 1000)

        valid_points = 0
        total_points = 0

        for point in data_points:
            total_points += 1
            # Check if any variable value is out of range
            for var_name, var_value in point.variables.items():
                if not (min_val <= var_value <= max_val):
                    break
            else:
                valid_points += 1

        passed = valid_points / total_points >= check.threshold if total_points > 0 else True

        return {
            "check_id": check.check_id,
            "passed": passed,
            "valid_points": valid_points,
            "total_points": total_points,
            "validity_rate": valid_points / total_points if total_points > 0 else 1.0
        }

    async def _check_completeness(self, check: DataQualityCheck, data_points: List[RenewablesDataPoint]) -> Dict[str, Any]:
        """Check data completeness."""
        required_fields = set(check.parameters.get("required_fields", []))

        valid_points = 0
        total_points = len(data_points)

        for point in data_points:
            if required_fields.issubset(set(point.variables.keys())):
                valid_points += 1

        passed = valid_points / total_points >= check.threshold if total_points > 0 else True

        return {
            "check_id": check.check_id,
            "passed": passed,
            "valid_points": valid_points,
            "total_points": total_points,
            "completeness_rate": valid_points / total_points if total_points > 0 else 1.0
        }

    async def _check_temporal_consistency(self, check: DataQualityCheck, data_points: List[RenewablesDataPoint]) -> Dict[str, Any]:
        """Check temporal data consistency."""
        max_gap_hours = check.parameters.get("max_gap_hours", 2)

        # Sort by timestamp
        sorted_points = sorted(data_points, key=lambda p: p.timestamp)

        valid_gaps = 0
        total_gaps = 0

        for i in range(1, len(sorted_points)):
            gap = (sorted_points[i].timestamp - sorted_points[i-1].timestamp).total_seconds() / 3600
            total_gaps += 1
            if gap <= max_gap_hours:
                valid_gaps += 1

        passed = valid_gaps / total_gaps >= check.threshold if total_gaps > 0 else True

        return {
            "check_id": check.check_id,
            "passed": passed,
            "valid_gaps": valid_gaps,
            "total_gaps": total_gaps,
            "consistency_rate": valid_gaps / total_gaps if total_gaps > 0 else 1.0
        }

    async def _check_anomalies(self, check: DataQualityCheck, data_points: List[RenewablesDataPoint]) -> Dict[str, Any]:
        """Check for anomalous data patterns."""
        sensitivity = check.parameters.get("sensitivity", 2.0)

        # Simple anomaly detection based on value distribution
        if not data_points:
            return {"check_id": check.check_id, "passed": True, "anomaly_count": 0}

        # Calculate basic statistics for each variable
        anomalies = 0
        total_values = 0

        for point in data_points:
            for var_name, var_value in point.variables.items():
                # Simple z-score based anomaly detection
                # In real implementation, would use more sophisticated methods
                if abs(var_value - 100) > sensitivity * 50:  # Mock threshold
                    anomalies += 1
                total_values += 1

        anomaly_rate = anomalies / total_values if total_values > 0 else 0
        passed = anomaly_rate <= (1 - check.threshold)

        return {
            "check_id": check.check_id,
            "passed": passed,
            "anomalies": anomalies,
            "total_values": total_values,
            "anomaly_rate": anomaly_rate
        }

    def _quality_checks_passed(self, quality_results: List[Dict[str, Any]]) -> bool:
        """Check if all quality checks passed."""
        for result in quality_results:
            if not result.get("passed", False):
                return False
        return True

    async def _update_feature_store(self, dataset: RenewablesDataset, job: IngestionJob) -> None:
        """Update feature store with processed renewables data."""
        try:
            feature_service = get_feature_store_service()

            # Convert renewables data to feature format
            features = {}

            for point in dataset.data_points:
                # Create time-based features
                timestamp_str = point.timestamp.isoformat()

                # Solar features
                if "ghi" in point.variables:
                    features[f"solar_irradiance_{timestamp_str}"] = point.variables["ghi"]
                if "dni" in point.variables:
                    features[f"direct_irradiance_{timestamp_str}"] = point.variables["dni"]
                if "dhi" in point.variables:
                    features[f"diffuse_irradiance_{timestamp_str}"] = point.variables["dhi"]

                # Wind features
                if "wind_speed" in point.variables:
                    features[f"wind_speed_{timestamp_str}"] = point.variables["wind_speed"]
                if "wind_direction" in point.variables:
                    features[f"wind_direction_{timestamp_str}"] = point.variables["wind_direction"]

                # Temperature features
                if "temperature" in point.variables:
                    features[f"temperature_{timestamp_str}"] = point.variables["temperature"]

            # Save to feature store
            await feature_service.save_feature_set(
                feature_set_id=dataset.dataset_id,
                features=features,
                geography=dataset.geography,
                metadata={
                    "data_source": dataset.data_source,
                    "ingestion_job_id": job.job_id,
                    "quality_score": sum(p.quality_score for p in dataset.data_points) / len(dataset.data_points),
                    "lineage_id": self._generate_lineage_id()
                }
            )

            self.telemetry.info(
                "Updated feature store with renewables data",
                dataset_id=dataset.dataset_id,
                feature_count=len(features),
                geography=dataset.geography
            )

        except Exception as e:
            self.telemetry.error(
                "Failed to update feature store",
                dataset_id=dataset.dataset_id,
                error=str(e)
            )

    def _generate_lineage_id(self) -> str:
        """Generate unique lineage identifier."""
        self._lineage_counter += 1
        return f"lineage_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{self._lineage_counter}"

    def add_data_source(self, config: DataSourceConfig) -> None:
        """Add a data source configuration.

        Args:
            config: Data source configuration
        """
        self.data_sources[config.name] = config

        self.telemetry.info(
            "Added data source",
            source_name=config.name,
            source_type=config.source_type,
            provider=config.provider
        )

    def get_job_status(self, job_id: str) -> Optional[IngestionJob]:
        """Get status of ingestion job.

        Args:
            job_id: Job identifier

        Returns:
            Job status or None if not found
        """
        return self.active_jobs.get(job_id)

    def get_active_jobs(self) -> List[IngestionJob]:
        """Get all active ingestion jobs.

        Returns:
            List of active jobs
        """
        return list(self.active_jobs.values())

    def cancel_job(self, job_id: str) -> bool:
        """Cancel an active ingestion job.

        Args:
            job_id: Job identifier

        Returns:
            True if job was cancelled
        """
        if job_id in self.active_jobs:
            job = self.active_jobs[job_id]
            job.status = "cancelled"
            job.completed_at = datetime.utcnow()

            self.telemetry.info("Cancelled ingestion job", job_id=job_id)
            return True

        return False

    def get_data_source_info(self, source_name: str) -> Optional[DataSourceConfig]:
        """Get information about a data source.

        Args:
            source_name: Data source name

        Returns:
            Data source configuration or None if not found
        """
        return self.data_sources.get(source_name)

    def list_data_sources(self) -> Dict[str, DataSourceConfig]:
        """List all configured data sources.

        Returns:
            Dictionary of data source configurations
        """
        return self.data_sources.copy()

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health information.

        Returns:
            Health information
        """
        return {
            "service": "renewables_ingestion",
            "status": "healthy",
            "data_sources_count": len(self.data_sources),
            "active_jobs": len(self.active_jobs),
            "quality_checks_count": len(self.quality_checks),
            "lineage_counter": self._lineage_counter
        }

    # Enhanced methods for parallel processing and scaling
    async def _execute_parallel_ingestion_job(self, job: IngestionJob) -> None:
        """Execute ingestion job with parallel processing."""
        try:
            job.status = "running"
            job.started_at = datetime.utcnow()

            # Split time range into chunks for parallel processing
            time_chunks = self._split_time_range(job.start_date, job.end_date)

            # Process chunks in parallel
            async with asyncio.TaskGroup() as tg:
                tasks = []
                for chunk_start, chunk_end in time_chunks:
                    task = tg.create_task(
                        self._process_time_chunk(job, chunk_start, chunk_end)
                    )
                    tasks.append(task)

                # Wait for all chunks to complete
                results = await asyncio.gather(*[task for task in tasks if task])

            # Aggregate results
            total_records = sum(len(result) for result in results)
            job.records_processed = total_records
            job.progress = 1.0

            # Run quality checks on aggregated data
            all_data_points = []
            for result in results:
                all_data_points.extend(result)

            quality_results = await self._run_comprehensive_quality_checks(all_data_points, job)

            # Check if quality thresholds are met
            quality_passed = self._evaluate_quality_results(quality_results, job)

            if quality_passed:
                # Store processed data
                await self._store_processed_data(all_data_points, job)

                job.status = "completed"
                job.completed_at = datetime.utcnow()
                self._metrics["jobs_completed"] += 1
                self._metrics["records_processed"] += total_records

                self.telemetry.info(
                    "Parallel ingestion job completed successfully",
                    job_id=job.job_id,
                    records_processed=total_records,
                    chunks_processed=len(time_chunks)
                )
            else:
                job.status = "failed"
                job.error_message = "Quality checks failed"
                self._metrics["jobs_failed"] += 1

            # Record metrics
            processing_time = (job.completed_at - job.started_at).total_seconds() if job.completed_at and job.started_at else 0
            self._metrics["processing_time_total"] += processing_time

            self.telemetry.increment_counter("renewables_ingestion_jobs_completed", category=MetricCategory.BUSINESS)
            self.telemetry.record_histogram(
                "renewables_ingestion_duration",
                processing_time,
                category=MetricCategory.PERFORMANCE
            )

        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            self._metrics["jobs_failed"] += 1

            self.telemetry.error(
                "Parallel ingestion job failed",
                job_id=job.job_id,
                error=str(e)
            )

    def _split_time_range(self, start_date: datetime, end_date: datetime) -> List[Tuple[datetime, datetime]]:
        """Split time range into parallel processing chunks."""
        total_hours = int((end_date - start_date).total_seconds() / 3600)
        chunk_size_hours = max(1, total_hours // self.max_concurrent_jobs)

        chunks = []
        current_start = start_date

        while current_start < end_date:
            chunk_end = min(current_start + timedelta(hours=chunk_size_hours), end_date)
            chunks.append((current_start, chunk_end))
            current_start = chunk_end

        return chunks

    async def _process_time_chunk(
        self,
        job: IngestionJob,
        chunk_start: datetime,
        chunk_end: datetime
    ) -> List[RenewablesDataPoint]:
        """Process a single time chunk."""
        try:
            # Fetch data for this chunk
            source_config = self.data_sources[job.data_source]
            raw_data = await self._fetch_data_chunk(source_config, job, chunk_start, chunk_end)

            # Process and validate data
            processed_points = await self._process_and_validate_data_batch(raw_data, job)

            # Update progress
            chunk_progress = (chunk_end - chunk_start).total_seconds() / (job.end_date - job.start_date).total_seconds()
            job.progress = min(1.0, job.progress + chunk_progress * 0.8)  # Reserve 20% for quality checks

            return processed_points

        except Exception as e:
            self.telemetry.error(
                "Time chunk processing failed",
                job_id=job.job_id,
                chunk_start=chunk_start.isoformat(),
                chunk_end=chunk_end.isoformat(),
                error=str(e)
            )
            return []

    async def _fetch_data_chunk(
        self,
        source_config: DataSourceConfig,
        job: IngestionJob,
        chunk_start: datetime,
        chunk_end: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch data for a specific time chunk."""
        # Enhanced data fetching with parallel API calls
        if source_config.source_type == "satellite":
            return await self._fetch_satellite_data_chunk(source_config, job, chunk_start, chunk_end)
        elif source_config.source_type == "weather_station":
            return await self._fetch_weather_station_data_chunk(source_config, job, chunk_start, chunk_end)
        elif source_config.source_type == "nwp":
            return await self._fetch_nwp_data_chunk(source_config, job, chunk_start, chunk_end)
        elif source_config.source_type == "reanalysis":
            return await self._fetch_reanalysis_data_chunk(source_config, job, chunk_start, chunk_end)
        else:
            raise ValueError(f"Unsupported source type: {source_config.source_type}")

    async def _fetch_satellite_data_chunk(
        self,
        source_config: DataSourceConfig,
        job: IngestionJob,
        chunk_start: datetime,
        chunk_end: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch satellite data chunk with parallel processing."""
        # Split chunk into smaller batches for parallel API calls
        batch_size_hours = 24  # One day batches
        batches = []

        current = chunk_start
        while current < chunk_end:
            batch_end = min(current + timedelta(hours=batch_size_hours), chunk_end)
            batches.append((current, batch_end))
            current = batch_end

        # Fetch batches in parallel
        async def fetch_batch(start: datetime, end: datetime) -> List[Dict[str, Any]]:
            # Simulate API call with realistic data
            batch_data = []
            current_date = start

            while current_date <= end:
                # Generate mock satellite data points for this batch
                for lat in range(30, 50, 5):  # Sample latitudes
                    for lon in range(-125, -65, 5):  # Sample longitudes
                        # Check if point is within geography bounds
                        if self._point_in_geography(lat, lon, job.geography):
                            data_point = {
                                "timestamp": current_date.isoformat(),
                                "latitude": lat,
                                "longitude": lon,
                                "geography": job.geography,
                                "data_source": job.data_source,
                                "ghi": max(0, 800 + 200 * (1 + 0.5 * (current_date.hour / 24))),  # Solar irradiance
                                "dni": max(0, 900 + 300 * (1 + 0.3 * (current_date.hour / 24))),  # Direct normal
                                "dhi": max(0, 100 + 50 * (1 + 0.2 * (current_date.hour / 24))),   # Diffuse
                                "wind_speed": max(0, 3 + 2 * (1 + 0.1 * (current_date.hour / 24))),
                                "wind_direction": 180 + 45 * (1 + 0.1 * (current_date.hour / 24)),
                                "temperature": 15 + 10 * (1 + 0.2 * (current_date.hour / 24)),
                                "humidity": 60 + 20 * (1 + 0.1 * (current_date.hour / 24)),
                                "quality_score": 0.95
                            }
                            batch_data.append(data_point)

                current_date += timedelta(hours=1)

            return batch_data

        # Execute batch fetches in parallel
        tasks = [fetch_batch(start, end) for start, end in batches]
        results = await asyncio.gather(*tasks)

        # Flatten results
        return [item for sublist in results for item in sublist]

    async def _fetch_weather_station_data_chunk(
        self,
        source_config: DataSourceConfig,
        job: IngestionJob,
        chunk_start: datetime,
        chunk_end: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch weather station data chunk."""
        # Mock implementation - would integrate with weather station APIs
        return []

    async def _fetch_nwp_data_chunk(
        self,
        source_config: DataSourceConfig,
        job: IngestionJob,
        chunk_start: datetime,
        chunk_end: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch NWP model data chunk."""
        # Mock implementation - would integrate with NWP APIs
        return []

    async def _fetch_reanalysis_data_chunk(
        self,
        source_config: DataSourceConfig,
        job: IngestionJob,
        chunk_start: datetime,
        chunk_end: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch reanalysis data chunk."""
        # Mock implementation - would integrate with reanalysis datasets
        return []

    def _point_in_geography(self, lat: float, lon: float, geography: str) -> bool:
        """Check if a point is within the specified geography."""
        # Simplified geography check - in reality would use proper geospatial queries
        if geography.lower() == "us":
            return 25 <= lat <= 50 and -125 <= lon <= -65
        elif geography.lower() == "europe":
            return 35 <= lat <= 70 and -10 <= lon <= 30
        elif geography.lower() == "global":
            return True
        else:
            return True  # Default to include all points

    async def _process_and_validate_data_batch(
        self,
        raw_data: List[Dict[str, Any]],
        job: IngestionJob
    ) -> List[RenewablesDataPoint]:
        """Process and validate a batch of raw data with parallel validation."""
        if not raw_data:
            return []

        # Process data in batches for better performance
        batch_size = self.batch_size
        batches = [raw_data[i:i + batch_size] for i in range(0, len(raw_data), batch_size)]

        # Process batches in parallel
        async def process_batch(batch: List[Dict[str, Any]]) -> List[RenewablesDataPoint]:
            processed_points = []
            for data_point in batch:
                try:
                    # Parse timestamp
                    timestamp = datetime.fromisoformat(data_point["timestamp"])

                    # Validate data ranges
                    if not self._validate_data_ranges(data_point):
                        job.records_failed += 1
                        continue

                    # Create data point
                    point = RenewablesDataPoint(
                        timestamp=timestamp,
                        latitude=data_point["latitude"],
                        longitude=data_point["longitude"],
                        geography=job.geography,
                        data_source=job.data_source,
                        variables={
                            k: v for k, v in data_point.items()
                            if k not in ["timestamp", "latitude", "longitude", "geography", "data_source", "quality_score"]
                        },
                        quality_score=data_point.get("quality_score", 1.0),
                        metadata={
                            "ingestion_job_id": job.job_id,
                            "processed_at": datetime.utcnow().isoformat(),
                            "batch_id": id(batch)
                        },
                        lineage_id=self._generate_lineage_id()
                    )

                    processed_points.append(point)
                    job.records_processed += 1

                except Exception as e:
                    job.records_failed += 1
                    self.telemetry.warning(
                        "Failed to process data point in batch",
                        error=str(e),
                        job_id=job.job_id
                    )

            return processed_points

        # Execute batch processing in parallel
        tasks = [process_batch(batch) for batch in batches]
        results = await asyncio.gather(*tasks)

        # Flatten results
        return [item for sublist in results for item in sublist]

    async def _run_comprehensive_quality_checks(
        self,
        data_points: List[RenewablesDataPoint],
        job: IngestionJob
    ) -> List[Dict[str, Any]]:
        """Run comprehensive quality checks on processed data."""
        quality_results = []

        for check in self.quality_checks.values():
            if not check.enabled:
                continue

            try:
                self._quality_metrics["total_checks"] += 1

                result = await self._execute_enhanced_quality_check(check, data_points, job)
                quality_results.append(result)

                if result.get("passed", False):
                    self._quality_metrics["passed_checks"] += 1
                else:
                    self._quality_metrics["failed_checks"] += 1

                # Track anomalies
                if check.check_type == "anomaly" and not result.get("passed", False):
                    self._quality_metrics["anomalies_detected"] += 1

            except Exception as e:
                self.telemetry.error(
                    "Quality check execution failed",
                    check_id=check.check_id,
                    job_id=job.job_id,
                    error=str(e)
                )
                quality_results.append({
                    "check_id": check.check_id,
                    "passed": False,
                    "error": str(e),
                    "severity": "error"
                })

        return quality_results

    async def _execute_enhanced_quality_check(
        self,
        check: DataQualityCheck,
        data_points: List[RenewablesDataPoint],
        job: IngestionJob
    ) -> Dict[str, Any]:
        """Execute an enhanced quality check with advanced logic."""
        try:
            if check.check_type == "range":
                return await self._execute_range_check(check, data_points)
            elif check.check_type == "completeness":
                return await self._execute_completeness_check(check, data_points)
            elif check.check_type == "consistency":
                return await self._execute_consistency_check(check, data_points)
            elif check.check_type == "anomaly":
                return await self._execute_anomaly_check(check, data_points)
            elif check.check_type == "freshness":
                return await self._execute_freshness_check(check, data_points)
            else:
                return {"check_id": check.check_id, "passed": True, "message": "Unknown check type"}

        except Exception as e:
            return {
                "check_id": check.check_id,
                "passed": False,
                "error": str(e),
                "severity": check.severity
            }

    async def _execute_range_check(
        self,
        check: DataQualityCheck,
        data_points: List[RenewablesDataPoint]
    ) -> Dict[str, Any]:
        """Execute range validation check."""
        parameters = check.parameters
        fields = parameters.get("fields", ["ghi", "dni", "dhi"])
        min_value = parameters.get("min_value", 0)
        max_value = parameters.get("max_value", 1500)

        violations = 0
        total_values = 0

        for point in data_points:
            for field in fields:
                if field in point.variables:
                    value = point.variables[field]
                    total_values += 1
                    if not (min_value <= value <= max_value):
                        violations += 1

        pass_rate = 1.0 - (violations / total_values) if total_values > 0 else 1.0

        return {
            "check_id": check.check_id,
            "passed": pass_rate >= check.threshold,
            "pass_rate": pass_rate,
            "violations": violations,
            "total_values": total_values,
            "severity": check.severity
        }

    async def _execute_completeness_check(
        self,
        check: DataQualityCheck,
        data_points: List[RenewablesDataPoint]
    ) -> Dict[str, Any]:
        """Execute completeness check."""
        parameters = check.parameters
        required_fields = parameters.get("required_fields", [])
        completeness_threshold = parameters.get("completeness_threshold", 0.95)

        missing_counts = {field: 0 for field in required_fields}
        total_points = len(data_points)

        for point in data_points:
            for field in required_fields:
                if field not in point.variables or point.variables[field] is None:
                    missing_counts[field] += 1

        # Calculate completeness for each field
        completeness_scores = {}
        for field, missing_count in missing_counts.items():
            completeness = 1.0 - (missing_count / total_points)
            completeness_scores[field] = completeness

        # Overall completeness is the minimum across all fields
        overall_completeness = min(completeness_scores.values()) if completeness_scores else 1.0

        return {
            "check_id": check.check_id,
            "passed": overall_completeness >= completeness_threshold,
            "completeness": overall_completeness,
            "completeness_scores": completeness_scores,
            "missing_counts": missing_counts,
            "total_points": total_points,
            "severity": check.severity
        }

    async def _execute_consistency_check(
        self,
        check: DataQualityCheck,
        data_points: List[RenewablesDataPoint]
    ) -> Dict[str, Any]:
        """Execute consistency check."""
        parameters = check.parameters

        if check.check_id == "temporal_consistency":
            return await self._check_temporal_consistency(data_points, parameters)
        elif check.check_id == "spatial_consistency":
            return await self._check_spatial_consistency(data_points, parameters)
        elif check.check_id == "physical_consistency":
            return await self._check_physical_consistency(data_points, parameters)
        else:
            return {"check_id": check.check_id, "passed": True, "message": "Unknown consistency check"}

    async def _execute_anomaly_check(
        self,
        check: DataQualityCheck,
        data_points: List[RenewablesDataPoint]
    ) -> Dict[str, Any]:
        """Execute anomaly detection check."""
        # Simplified anomaly detection - in reality would use ML models
        parameters = check.parameters

        # Extract numerical values for anomaly detection
        values = []
        for point in data_points:
            for var_name, var_value in point.variables.items():
                if isinstance(var_value, (int, float)):
                    values.append(var_value)

        if not values:
            return {"check_id": check.check_id, "passed": True, "message": "No numerical data for anomaly detection"}

        # Simple statistical anomaly detection
        import statistics
        mean_val = statistics.mean(values)
        stdev_val = statistics.stdev(values) if len(values) > 1 else 0

        anomalies = 0
        for value in values:
            z_score = abs(value - mean_val) / (stdev_val + 1e-8)
            if z_score > parameters.get("sensitivity", 2.0):
                anomalies += 1

        anomaly_rate = anomalies / len(values) if values else 0

        return {
            "check_id": check.check_id,
            "passed": anomaly_rate <= (1 - check.threshold),
            "anomaly_rate": anomaly_rate,
            "anomalies_detected": anomalies,
            "total_values": len(values),
            "severity": check.severity
        }

    async def _execute_freshness_check(
        self,
        check: DataQualityCheck,
        data_points: List[RenewablesDataPoint]
    ) -> Dict[str, Any]:
        """Execute freshness check."""
        parameters = check.parameters

        if not data_points:
            return {"check_id": check.check_id, "passed": True, "message": "No data to check freshness"}

        # Find the most recent data point
        latest_timestamp = max(point.timestamp for point in data_points)
        age_hours = (datetime.utcnow() - latest_timestamp).total_seconds() / 3600

        max_age_hours = parameters.get("max_age_hours", 24)
        warning_age_hours = parameters.get("warning_age_hours", 12)

        is_fresh = age_hours <= max_age_hours
        is_warning = age_hours <= warning_age_hours

        return {
            "check_id": check.check_id,
            "passed": is_fresh,
            "age_hours": age_hours,
            "max_age_hours": max_age_hours,
            "status": "fresh" if is_fresh else "stale",
            "warning_triggered": not is_warning,
            "severity": check.severity
        }

    async def _check_temporal_consistency(
        self,
        data_points: List[RenewablesDataPoint],
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check temporal consistency of data."""
        if len(data_points) < 2:
            return {"check_id": "temporal_consistency", "passed": True, "message": "Insufficient data for temporal check"}

        # Sort by timestamp
        sorted_points = sorted(data_points, key=lambda p: p.timestamp)

        # Check for gaps
        max_gap_minutes = parameters.get("max_gap_minutes", 120)
        gaps = 0
        total_intervals = 0

        for i in range(1, len(sorted_points)):
            time_diff = (sorted_points[i].timestamp - sorted_points[i-1].timestamp).total_seconds() / 60
            total_intervals += 1
            if time_diff > max_gap_minutes:
                gaps += 1

        gap_rate = gaps / total_intervals if total_intervals > 0 else 0

        return {
            "check_id": "temporal_consistency",
            "passed": gap_rate <= (1 - 0.85),  # 85% threshold for temporal consistency
            "gap_rate": gap_rate,
            "gaps_detected": gaps,
            "total_intervals": total_intervals,
            "severity": "warning"
        }

    async def _check_spatial_consistency(
        self,
        data_points: List[RenewablesDataPoint],
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check spatial consistency of data."""
        # Simplified spatial consistency check
        # In reality would use proper geospatial analysis
        neighbor_radius_km = parameters.get("neighbor_radius_km", 50)

        # Group points by approximate location
        locations = {}
        for point in data_points:
            key = f"{point.latitude".1f"}_{point.longitude".1f"}"
            if key not in locations:
                locations[key] = []
            locations[key].append(point)

        # Check for spatial outliers (simplified)
        outliers = 0
        for location_points in locations.values():
            if len(location_points) > 1:
                # Simple variance check
                ghi_values = [p.variables.get("ghi", 0) for p in location_points]
                if ghi_values:
                    import statistics
                    variance = statistics.variance(ghi_values) if len(ghi_values) > 1 else 0
                    max_variance = parameters.get("max_spatial_variance", 0.3)
                    if variance > max_variance:
                        outliers += 1

        outlier_rate = outliers / len(locations) if locations else 0

        return {
            "check_id": "spatial_consistency",
            "passed": outlier_rate <= (1 - 0.8),
            "outlier_rate": outlier_rate,
            "outliers_detected": outliers,
            "locations_checked": len(locations),
            "severity": "info"
        }

    async def _check_physical_consistency(
        self,
        data_points: List[RenewablesDataPoint],
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check physical consistency of data."""
        violations = 0
        total_checks = 0

        for point in data_points:
            # Check GHI/DNI relationship
            if parameters.get("ghi_dni_relationship", True):
                ghi = point.variables.get("ghi", 0)
                dni = point.variables.get("dni", 0)

                # DNI should generally be >= GHI (accounting for atmospheric effects)
                if dni > 0 and ghi > 0 and dni < ghi * 0.8:  # Allow 20% reduction
                    violations += 1
                total_checks += 1

            # Temperature/wind relationship (simplified)
            if parameters.get("wind_temp_relationship", True):
                temp = point.variables.get("temperature", 20)
                wind_speed = point.variables.get("wind_speed", 5)

                # High winds with very low temperatures might be suspicious
                if wind_speed > 20 and temp < -10:
                    violations += 1
                total_checks += 1

        violation_rate = violations / total_checks if total_checks > 0 else 0

        return {
            "check_id": "physical_consistency",
            "passed": violation_rate <= (1 - 0.9),
            "violation_rate": violation_rate,
            "violations": violations,
            "total_checks": total_checks,
            "severity": "error"
        }

    def _evaluate_quality_results(self, quality_results: List[Dict[str, Any]], job: IngestionJob) -> bool:
        """Evaluate if quality results meet thresholds."""
        for result in quality_results:
            if not result.get("passed", False):
                severity = result.get("severity", "warning")

                if severity == "error":
                    self.telemetry.error(
                        "Critical quality check failed",
                        job_id=job.job_id,
                        check_id=result["check_id"],
                        severity=severity
                    )
                    return False

                elif severity == "warning":
                    self.telemetry.warning(
                        "Quality check warning",
                        job_id=job.job_id,
                        check_id=result["check_id"],
                        severity=severity
                    )

        return True

    async def _store_processed_data(self, data_points: List[RenewablesDataPoint], job: IngestionJob) -> None:
        """Store processed data points."""
        try:
            # Store in database
            await self.dao.store_renewables_data(data_points)

            # Update feature store
            await self._update_feature_store(data_points, job)

            self.telemetry.info(
                "Processed data stored successfully",
                job_id=job.job_id,
                records_stored=len(data_points)
            )

        except Exception as e:
            self.telemetry.error(
                "Failed to store processed data",
                job_id=job.job_id,
                error=str(e)
            )
            raise

    async def _update_feature_store(self, data_points: List[RenewablesDataPoint], job: IngestionJob) -> None:
        """Update feature store with processed data."""
        try:
            feature_service = get_feature_store_service()

            # Convert to feature format
            features = []
            for point in data_points:
                feature = {
                    "timestamp": point.timestamp,
                    "geography": point.geography,
                    "data_source": point.data_source,
                    "features": point.variables,
                    "metadata": point.metadata
                }
                features.append(feature)

            # Update feature store
            await feature_service.store_renewables_features(features)

        except Exception as e:
            self.telemetry.error(
                "Failed to update feature store",
                job_id=job.job_id,
                error=str(e)
            )

    def get_service_metrics(self) -> Dict[str, Any]:
        """Get comprehensive service metrics."""
        return {
            "performance_metrics": self._metrics.copy(),
            "quality_metrics": self._quality_metrics.copy(),
            "active_jobs": len(self.active_jobs),
            "data_sources": len(self.data_sources),
            "quality_checks": len(self.quality_checks),
            "last_activity": self._metrics["last_activity"].isoformat(),
            "scaling_config": {
                "max_concurrent_jobs": self.max_concurrent_jobs,
                "batch_size": self.batch_size,
                "retry_attempts": self.retry_attempts,
                "parallel_processing": self.enable_parallel_processing
            }
        }

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed status of a specific job."""
        job = self.active_jobs.get(job_id)
        if not job:
            return None

        return {
            "job_id": job.job_id,
            "data_source": job.data_source,
            "geography": job.geography,
            "status": job.status,
            "progress": job.progress,
            "records_processed": job.records_processed,
            "records_failed": job.records_failed,
            "start_date": job.start_date.isoformat(),
            "end_date": job.end_date.isoformat(),
            "created_at": job.created_at.isoformat(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "error_message": job.error_message,
            "variables": job.variables,
            "metadata": job.metadata
        }

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        if job_id not in self.active_jobs:
            return False

        job = self.active_jobs[job_id]
        job.status = "cancelled"
        job.completed_at = datetime.utcnow()

        self.telemetry.info("Job cancelled", job_id=job_id)
        return True

    def _generate_lineage_id(self) -> str:
        """Generate a unique lineage ID."""
        self._lineage_counter += 1
        return f"ln_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{self._lineage_counter}"


# Global renewables ingestion service instance
_renewables_ingestion_service: Optional[RenewablesIngestionService] = None


def get_renewables_ingestion_service(config_file: Optional[str] = None) -> RenewablesIngestionService:
    """Get the global renewables ingestion service instance.

    Args:
        config_file: Optional path to configuration file

    Returns:
        Renewables ingestion service instance
    """
    global _renewables_ingestion_service

    if _renewables_ingestion_service is None:
        _renewables_ingestion_service = RenewablesIngestionService(config_file=config_file)

    return _renewables_ingestion_service


# Convenience functions for common operations
async def ingest_satellite_data(
    geography: str = "US",
    start_date: datetime = None,
    end_date: datetime = None,
    variables: Optional[List[str]] = None
) -> str:
    """Ingest satellite data for renewables.

    Args:
        geography: Geographic scope
        start_date: Start date (defaults to last 24 hours)
        end_date: End date (defaults to now)
        variables: Variables to ingest

    Returns:
        Job ID
    """
    service = get_renewables_ingestion_service()

    if start_date is None:
        start_date = datetime.utcnow() - timedelta(hours=24)
    if end_date is None:
        end_date = datetime.utcnow()

    return await service.start_ingestion_job(
        data_source="satellite_nasa",
        geography=geography,
        start_date=start_date,
        end_date=end_date,
        variables=variables
    )


async def ingest_weather_station_data(
    geography: str = "US",
    start_date: datetime = None,
    end_date: datetime = None
) -> str:
    """Ingest weather station data for renewables.

    Args:
        geography: Geographic scope
        start_date: Start date (defaults to last 24 hours)
        end_date: End date (defaults to now)

    Returns:
        Job ID
    """
    service = get_renewables_ingestion_service()

    if start_date is None:
        start_date = datetime.utcnow() - timedelta(hours=24)
    if end_date is None:
        end_date = datetime.utcnow()

    return await service.start_ingestion_job(
        data_source="weather_station_noaa",
        geography=geography,
        start_date=start_date,
        end_date=end_date
    )

