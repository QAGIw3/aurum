from __future__ import annotations

"""Airflow operators for vendor curve parsing tasks.

Phase 2: Vendor Parser-Airflow Integration
Custom operators to handle vendor curve file parsing and validation.
"""

import os
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

try:
    from aurum.parsers.vendor_curves import parse
    from aurum.parsers.runner import parse_files
except ImportError:
    parse = None
    parse_files = None

LOGGER = logging.getLogger(__name__)


class VendorCurveParsingOperator(BaseOperator):
    """Operator for parsing vendor curve files using the unified parser framework."""

    template_fields = ['vendor', 'file_pattern', 'output_path', 'asof_date']

    @apply_defaults
    def __init__(
        self,
        vendor: str,
        file_pattern: str = "EOD_{vendor}_*.xlsx",
        drop_dir: Optional[str] = None,
        output_path: Optional[str] = None,
        output_format: str = "parquet",
        asof_date: Optional[str] = None,
        quarantine_on_error: bool = True,
        max_retries: int = 3,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.vendor = vendor
        self.file_pattern = file_pattern
        self.drop_dir = drop_dir or os.environ.get("AURUM_VENDOR_DROP_DIR", "/opt/airflow/data/vendor")
        self.output_path = output_path or os.environ.get("AURUM_PARSED_OUTPUT_DIR", "/opt/airflow/data/processed")
        self.output_format = output_format
        self.asof_date = asof_date
        self.quarantine_on_error = quarantine_on_error
        self.max_retries = max_retries

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute vendor curve parsing."""
        if parse_files is None:
            raise ImportError("aurum.parsers.runner.parse_files not available")

        # Get execution date
        execution_date = context.get('ds')
        if self.asof_date:
            asof = date.fromisoformat(self.asof_date)
        elif execution_date:
            asof = date.fromisoformat(execution_date)
        else:
            asof = date.today()

        # Find files to process
        drop_dir = Path(self.drop_dir)
        pattern = self.file_pattern.format(vendor=self.vendor.upper())
        files = sorted(drop_dir.glob(pattern))

        if not files:
            LOGGER.info(f"No files found for vendor={self.vendor} in {drop_dir} with pattern {pattern}")
            return {
                'vendor': self.vendor,
                'files_processed': 0,
                'rows_parsed': 0,
                'rows_quarantined': 0,
                'status': 'no_files'
            }

        LOGGER.info(f"Found {len(files)} files for vendor={self.vendor}: {[str(f) for f in files]}")

        try:
            # Parse files using the unified parser
            df = parse_files(files, as_of=asof)
            
            if df.empty:
                LOGGER.warning(f"Parsed zero rows for vendor={self.vendor}")
                return {
                    'vendor': self.vendor,
                    'files_processed': len(files),
                    'rows_parsed': 0,
                    'rows_quarantined': 0,
                    'status': 'empty_result'
                }

            # Write output
            output_path = Path(self.output_path) / f"{self.vendor.lower()}_{asof.isoformat()}.{self.output_format}"
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if self.output_format == "parquet":
                df.to_parquet(output_path, index=False)
            elif self.output_format == "csv":
                df.to_csv(output_path, index=False)
            else:
                raise ValueError(f"Unsupported output format: {self.output_format}")

            LOGGER.info(f"Successfully parsed {len(df)} rows to {output_path}")

            # Push results to XCom
            result = {
                'vendor': self.vendor,
                'files_processed': len(files),
                'rows_parsed': len(df),
                'rows_quarantined': 0,
                'output_path': str(output_path),
                'status': 'success'
            }

            # Store file list in XCom
            context['ti'].xcom_push(key='input_files', value=[str(f) for f in files])
            context['ti'].xcom_push(key='rows', value=len(df))
            context['ti'].xcom_push(key='rows_quarantine', value=0)

            return result

        except Exception as exc:
            LOGGER.error(f"Failed to parse vendor files for {self.vendor}: {exc}", exc_info=True)
            
            if self.quarantine_on_error:
                # Move files to quarantine
                quarantine_dir = Path(self.drop_dir) / "quarantine"
                quarantine_dir.mkdir(exist_ok=True)
                
                quarantined_files = []
                for file_path in files:
                    quarantine_path = quarantine_dir / f"{file_path.stem}_{asof.isoformat()}_ERROR{file_path.suffix}"
                    file_path.rename(quarantine_path)
                    quarantined_files.append(str(quarantine_path))
                
                LOGGER.info(f"Quarantined {len(quarantined_files)} files due to parsing error")
                
                return {
                    'vendor': self.vendor,
                    'files_processed': 0,
                    'rows_parsed': 0,
                    'rows_quarantined': len(files),
                    'quarantined_files': quarantined_files,
                    'status': 'quarantined',
                    'error': str(exc)
                }
            else:
                raise


class VendorCurveValidationOperator(BaseOperator):
    """Operator for validating parsed vendor curve data."""

    template_fields = ['vendor', 'input_path']

    @apply_defaults
    def __init__(
        self,
        vendor: str,
        input_path: str,
        validation_rules: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.vendor = vendor
        self.input_path = input_path
        self.validation_rules = validation_rules or {}

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute validation of parsed vendor curve data."""
        import pandas as pd
        
        try:
            # Load the parsed data
            input_path = Path(self.input_path)
            if not input_path.exists():
                raise FileNotFoundError(f"Input file not found: {input_path}")

            if input_path.suffix == '.parquet':
                df = pd.read_parquet(input_path)
            elif input_path.suffix == '.csv':
                df = pd.read_csv(input_path)
            else:
                raise ValueError(f"Unsupported file format: {input_path.suffix}")

            LOGGER.info(f"Loaded {len(df)} rows from {input_path}")

            # Basic validation
            validation_results = {
                'vendor': self.vendor,
                'input_path': str(input_path),
                'total_rows': len(df),
                'validation_passed': True,
                'validation_errors': [],
                'validation_warnings': []
            }

            # Check for empty dataset
            if df.empty:
                validation_results['validation_errors'].append("Dataset is empty")
                validation_results['validation_passed'] = False

            # Check for required columns (basic validation)
            expected_columns = {'asof', 'tenor_label', 'mid'}
            missing_columns = expected_columns - set(df.columns)
            if missing_columns:
                validation_results['validation_errors'].append(f"Missing required columns: {missing_columns}")
                validation_results['validation_passed'] = False

            # Check for null values in critical columns
            if 'mid' in df.columns:
                null_mid_count = df['mid'].isnull().sum()
                if null_mid_count > 0:
                    validation_results['validation_warnings'].append(f"Found {null_mid_count} null values in 'mid' column")

            # Check for duplicate records
            if not df.empty and len(df.columns) > 0:
                duplicate_count = df.duplicated().sum()
                if duplicate_count > 0:
                    validation_results['validation_warnings'].append(f"Found {duplicate_count} duplicate rows")

            # Apply custom validation rules
            for rule_name, rule_config in self.validation_rules.items():
                try:
                    # Simple rule evaluation (can be extended)
                    if rule_name == "min_rows" and len(df) < rule_config.get("threshold", 1):
                        validation_results['validation_errors'].append(f"Dataset has fewer than {rule_config['threshold']} rows")
                        validation_results['validation_passed'] = False
                    elif rule_name == "max_null_percentage":
                        null_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
                        if null_percentage > rule_config.get("threshold", 10):
                            validation_results['validation_warnings'].append(f"Null percentage ({null_percentage:.2f}%) exceeds threshold")
                except Exception as rule_exc:
                    validation_results['validation_warnings'].append(f"Validation rule '{rule_name}' failed: {rule_exc}")

            LOGGER.info(f"Validation completed for {self.vendor}: passed={validation_results['validation_passed']}")

            return validation_results

        except Exception as exc:
            LOGGER.error(f"Validation failed for {self.vendor}: {exc}", exc_info=True)
            return {
                'vendor': self.vendor,
                'input_path': str(self.input_path),
                'validation_passed': False,
                'validation_errors': [f"Validation execution failed: {exc}"],
                'validation_warnings': []
            }


class VendorCurveMonitoringOperator(BaseOperator):
    """Operator for monitoring vendor curve parsing performance and alerting."""

    template_fields = ['vendor']

    @apply_defaults
    def __init__(
        self,
        vendor: str,
        performance_thresholds: Optional[Dict[str, Any]] = None,
        alert_channels: Optional[List[str]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.vendor = vendor
        self.performance_thresholds = performance_thresholds or {
            'max_processing_time_minutes': 10,
            'min_success_rate_percentage': 95,
            'max_quarantine_rate_percentage': 5
        }
        self.alert_channels = alert_channels or ['log']

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute monitoring and alerting for vendor curve parsing."""
        # Get results from upstream tasks
        parsing_result = context['ti'].xcom_pull(task_ids=f'parse_{self.vendor.lower()}_curves')
        validation_result = context['ti'].xcom_pull(task_ids=f'validate_{self.vendor.lower()}_curves')

        monitoring_result = {
            'vendor': self.vendor,
            'timestamp': datetime.now().isoformat(),
            'alerts': [],
            'metrics': {},
            'status': 'healthy'
        }

        if parsing_result:
            # Check processing performance
            processing_time = context.get('task_instance', {}).get('duration', 0) / 60  # Convert to minutes
            monitoring_result['metrics']['processing_time_minutes'] = processing_time

            if processing_time > self.performance_thresholds['max_processing_time_minutes']:
                alert = f"Processing time ({processing_time:.2f} min) exceeded threshold ({self.performance_thresholds['max_processing_time_minutes']} min)"
                monitoring_result['alerts'].append(alert)
                monitoring_result['status'] = 'warning'

            # Check success metrics
            rows_parsed = parsing_result.get('rows_parsed', 0)
            rows_quarantined = parsing_result.get('rows_quarantined', 0)
            total_rows = rows_parsed + rows_quarantined

            if total_rows > 0:
                success_rate = (rows_parsed / total_rows) * 100
                quarantine_rate = (rows_quarantined / total_rows) * 100
                
                monitoring_result['metrics']['success_rate_percentage'] = success_rate
                monitoring_result['metrics']['quarantine_rate_percentage'] = quarantine_rate

                if success_rate < self.performance_thresholds['min_success_rate_percentage']:
                    alert = f"Success rate ({success_rate:.2f}%) below threshold ({self.performance_thresholds['min_success_rate_percentage']}%)"
                    monitoring_result['alerts'].append(alert)
                    monitoring_result['status'] = 'critical'

                if quarantine_rate > self.performance_thresholds['max_quarantine_rate_percentage']:
                    alert = f"Quarantine rate ({quarantine_rate:.2f}%) above threshold ({self.performance_thresholds['max_quarantine_rate_percentage']}%)"
                    monitoring_result['alerts'].append(alert)
                    monitoring_result['status'] = 'warning'

        if validation_result and not validation_result.get('validation_passed', True):
            monitoring_result['alerts'].extend(validation_result.get('validation_errors', []))
            monitoring_result['status'] = 'critical'

        # Send alerts if any
        if monitoring_result['alerts']:
            self._send_alerts(monitoring_result)

        LOGGER.info(f"Monitoring completed for {self.vendor}: status={monitoring_result['status']}, alerts={len(monitoring_result['alerts'])}")

        return monitoring_result

    def _send_alerts(self, monitoring_result: Dict[str, Any]) -> None:
        """Send alerts through configured channels."""
        for channel in self.alert_channels:
            if channel == 'log':
                for alert in monitoring_result['alerts']:
                    LOGGER.warning(f"ALERT [{self.vendor}]: {alert}")
            # Add other alert channels (email, slack, etc.) as needed
            elif channel == 'metrics':
                # Push to metrics system
                try:
                    from prometheus_client import Counter
                    alert_counter = Counter('aurum_vendor_parsing_alerts_total', 'Vendor parsing alerts', ['vendor', 'status'])
                    alert_counter.labels(vendor=self.vendor, status=monitoring_result['status']).inc(len(monitoring_result['alerts']))
                except ImportError:
                    pass