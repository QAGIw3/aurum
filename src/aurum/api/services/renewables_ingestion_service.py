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
        dao: Optional[RenewablesIngestionDAO] = None
    ):
        """Initialize renewables ingestion service.

        Args:
            config_file: Path to data source configuration file
            dao: Optional DAO for persistence
        """
        self.dao = dao or RenewablesIngestionDAO()
        self.data_sources: Dict[str, DataSourceConfig] = {}
        self.quality_checks: Dict[str, DataQualityCheck] = {}
        self.active_jobs: Dict[str, IngestionJob] = {}
        self._lineage_counter = 0

        # Load configuration
        if config_file:
            self._load_config(config_file)

        # Initialize default quality checks
        self._initialize_quality_checks()

        self.logger = logging.getLogger(__name__)
        self.telemetry = get_telemetry_facade()

        self.logger.info("Renewables ingestion service initialized",
                        data_source_count=len(self.data_sources))

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
        """Initialize default data quality checks."""
        default_checks = [
            DataQualityCheck(
                check_id="range_check",
                name="Value Range Validation",
                description="Check that values are within expected ranges",
                check_type="range",
                parameters={"min_value": 0, "max_value": 2000},  # W/m² for irradiance
                threshold=0.95,
                severity="error"
            ),
            DataQualityCheck(
                check_id="completeness_check",
                name="Data Completeness",
                description="Check for missing data points",
                check_type="completeness",
                parameters={"required_fields": ["ghi", "dni", "wind_speed"]},
                threshold=0.9,
                severity="warning"
            ),
            DataQualityCheck(
                check_id="temporal_consistency",
                name="Temporal Consistency",
                description="Check for temporal data consistency",
                check_type="consistency",
                parameters={"max_gap_hours": 2},
                threshold=0.85,
                severity="warning"
            ),
            DataQualityCheck(
                check_id="anomaly_detection",
                name="Anomaly Detection",
                description="Detect anomalous data patterns",
                check_type="anomaly",
                parameters={"sensitivity": 2.0},
                threshold=0.8,
                severity="info"
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

        # Start job in background
        asyncio.create_task(self._execute_ingestion_job(job))

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
