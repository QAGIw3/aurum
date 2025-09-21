"""
Airflow DAG for monitoring dataset staleness.

This DAG:
- Monitors data freshness across all datasets
- Checks watermarks against expected frequencies
- Generates alerts for stale data
- Provides monitoring dashboards and reports
- Integrates with SLA monitoring system
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Import staleness monitoring components
try:
    from aurum.staleness import (
        StalenessDetector,
        StalenessConfig,
        StalenessCheck,
        StalenessMonitor,
        WatermarkTracker,
        StalenessAlertManager
    )
    STALENESS_MONITORING_AVAILABLE = True
except ImportError:
    STALENESS_MONITORING_AVAILABLE = False

# Configuration
STALENESS_CONFIG = StalenessConfig(
    warning_threshold_hours=2,
    critical_threshold_hours=6,
    grace_period_hours=1,
    check_interval_minutes=5,
    alert_cooldown_minutes=15,
    dataset_thresholds={
        "eia": {"warning": 1, "critical": 3},  # More frequent for EIA data
        "fred": {"warning": 4, "critical": 12},  # Less frequent for FRED
        "cpi": {"warning": 4, "critical": 12},   # Monthly CPI data
        "iso": {"warning": 1, "critical": 4},    # ISO data needs freshness
        "noaa": {"warning": 6, "critical": 24},  # Weather data less critical
    }
)

# Default DAG arguments
DEFAULT_ARGS = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

def create_staleness_monitor_dag(
    dag_id: str = "dataset_staleness_monitor",
    schedule_interval: str = "*/5 * * * *",  # Every 5 minutes
    start_date: datetime = None,
    **kwargs
):
    """Create staleness monitoring DAG.

    Args:
        dag_id: DAG identifier
        schedule_interval: Cron schedule
        start_date: DAG start date
        **kwargs: Additional DAG arguments
    """
    if start_date is None:
        start_date = datetime(2024, 1, 1)

    dag = DAG(
        dag_id=dag_id,
        description="Monitor dataset staleness and alert on data freshness issues",
        default_args=DEFAULT_ARGS,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,
        tags=["aurum", "monitoring", "staleness", "data-quality"],
        **kwargs
    )

    # Start task
    start_task = EmptyOperator(
        task_id="start_staleness_monitoring",
        task_group="staleness_monitoring",
        dag=dag
    )

    # Initialize staleness detector
    def initialize_staleness_detector(**context):
        """Initialize the staleness detector with configured datasets."""
        if not STALENESS_MONITORING_AVAILABLE:
            raise RuntimeError("Staleness monitoring components not available")

        detector = StalenessDetector(STALENESS_CONFIG)

        # Register datasets for monitoring
        datasets_to_monitor = [
            # EIA datasets
            ("eia_electricity_prices", 1),   # Hourly
            ("eia_electricity_demand", 1),   # Hourly
            ("eia_natural_gas_prices", 24),  # Daily

            # FRED datasets
            ("fred_unemployment", 4 * 7 * 24),  # Weekly
            ("fred_gdp", 30 * 24),             # Monthly
            ("fred_inflation", 30 * 24),       # Monthly

            # CPI datasets
            ("cpi_all_items", 30 * 24),         # Monthly
            ("cpi_energy", 30 * 24),           # Monthly
            ("cpi_food", 30 * 24),             # Monthly

            # ISO datasets
            ("iso_nyiso_lmp_dam", 1),          # Hourly
            ("iso_nyiso_load", 1),             # Hourly
            ("iso_pjm_lmp_rtm", 1),            # Hourly

            # NOAA datasets
            ("noaa_ghcnd_daily", 24),          # Daily
            ("noaa_ghcnd_hourly", 1),          # Hourly
        ]

        for dataset, frequency_hours in datasets_to_monitor:
            detector.add_staleness_check(StalenessCheck(
                dataset=dataset,
                check_interval_minutes=5,
                enabled=True,
                alert_on_stale=True,
                alert_on_critical=True
            ))

        # Store detector in XCom for use by other tasks
        context['task_instance'].xcom_push(key='staleness_detector', value=detector)

        return f"Initialized staleness monitoring for {len(datasets_to_monitor)} datasets"

    init_task = PythonOperator(
        task_id="initialize_staleness_detector",
        python_callable=initialize_staleness_detector,
        task_group="staleness_monitoring",
        dag=dag
    )

    # Run staleness checks
    def run_staleness_checks(**context):
        """Run staleness checks for all datasets."""
        if not STALENESS_MONITORING_AVAILABLE:
            raise RuntimeError("Staleness monitoring components not available")

        # Get detector from XCom
        detector = context['task_instance'].xcom_pull(
            key='staleness_detector',
            task_ids='initialize_staleness_detector'
        )

        if detector is None:
            raise RuntimeError("Staleness detector not found in XCom")

        # Run checks
        results = detector.run_staleness_checks()

        # Log results
        stale_count = results.get('stale_datasets', 0)
        critical_count = results.get('critical_datasets', 0)

        if stale_count > 0 or critical_count > 0:
            print(f"⚠️  Found {stale_count} stale and {critical_count} critical datasets")
        else:
            print("✅ All datasets are fresh")

        # Store results in XCom for downstream tasks
        context['task_instance'].xcom_push(key='staleness_results', value=results)

        return results

    check_task = PythonOperator(
        task_id="run_staleness_checks",
        python_callable=run_staleness_checks,
        task_group="staleness_monitoring",
        dag=dag
    )

    # Process stale datasets
    def process_stale_datasets(**context):
        """Process stale datasets and generate alerts."""
        if not STALENESS_MONITORING_AVAILABLE:
            raise RuntimeError("Staleness monitoring components not available")

        # Get detector from XCom
        detector = context['task_instance'].xcom_pull(
            key='staleness_detector',
            task_ids='initialize_staleness_detector'
        )

        if detector is None:
            raise RuntimeError("Staleness detector not found in XCom")

        # Get stale datasets
        stale_datasets = detector.get_stale_datasets()

        if not stale_datasets:
            print("✅ No stale datasets found")
            return {"processed_datasets": 0}

        print(f"🔍 Processing {len(stale_datasets)} stale datasets")

        # Process each stale dataset
        processed_count = 0
        for staleness_info in stale_datasets:
            try:
                # Process stale dataset
                print(f"  📊 Dataset {staleness_info.dataset} is "
                      f"{staleness_info.staleness_level.value} "
                      f"({staleness_info.hours_since_update".2f"}h old)")

                processed_count += 1

            except Exception as e:
                print(f"  ❌ Error processing {staleness_info.dataset}: {e}")

        return {"processed_datasets": processed_count}

    process_task = PythonOperator(
        task_id="process_stale_datasets",
        python_callable=process_stale_datasets,
        task_group="staleness_monitoring",
        dag=dag
    )

    # Generate staleness report
    def generate_staleness_report(**context):
        """Generate comprehensive staleness monitoring report."""
        if not STALENESS_MONITORING_AVAILABLE:
            raise RuntimeError("Staleness monitoring components not available")

        # Get detector from XCom
        detector = context['task_instance'].xcom_pull(
            key='staleness_detector',
            task_ids='initialize_staleness_detector'
        )

        if detector is None:
            raise RuntimeError("Staleness detector not found in XCom")

        # Generate report
        report = {
            "timestamp": datetime.now().isoformat(),
            "detector_status": detector.get_detector_status(),
            "summary": {
                "total_checks": len(detector.checks),
                "enabled_checks": len([c for c in detector.checks.values() if c.enabled]),
                "consecutive_failures": detector.consecutive_failures.copy(),
            }
        }

        # Store report for downstream tasks
        context['task_instance'].xcom_push(key='staleness_report', value=report)

        # Log report summary
        print(f"📋 Generated staleness report: {report['summary']}")

        return report

    report_task = PythonOperator(
        task_id="generate_staleness_report",
        python_callable=generate_staleness_report,
        task_group="staleness_monitoring",
        dag=dag
    )

    # End task
    end_task = EmptyOperator(
        task_id="end_staleness_monitoring",
        task_group="staleness_monitoring",
        dag=dag
    )

    # Set up task dependencies
    start_task >> init_task >> check_task >> process_task >> report_task >> end_task

    return dag

def create_critical_staleness_monitor_dag(
    dag_id: str = "critical_staleness_monitor",
    schedule_interval: str = "*/15 * * * *",  # Every 15 minutes
    start_date: datetime = None,
    **kwargs
):
    """Create a DAG focused on critical staleness monitoring with faster checks.

    Args:
        dag_id: DAG identifier
        schedule_interval: Cron schedule (more frequent for critical monitoring)
        start_date: DAG start date
        **kwargs: Additional DAG arguments
    """
    if start_date is None:
        start_date = datetime(2024, 1, 1)

    # More aggressive configuration for critical datasets
    critical_config = StalenessConfig(
        warning_threshold_hours=1,
        critical_threshold_hours=2,
        grace_period_hours=0.5,  # Shorter grace period
        check_interval_minutes=15,  # More frequent checks
        alert_cooldown_minutes=5,    # Faster alert cooldown
        dataset_thresholds={
            "eia": {"warning": 0.5, "critical": 1.0},     # Very tight for EIA
            "iso": {"warning": 0.5, "critical": 1.0},     # Critical for ISO data
            "fred": {"warning": 2, "critical": 6},        # Standard for FRED
            "cpi": {"warning": 2, "critical": 6},         # Standard for CPI
            "noaa": {"warning": 4, "critical": 12},       # Relaxed for NOAA
        }
    )

    dag = DAG(
        dag_id=dag_id,
        description="Critical dataset staleness monitoring with fast detection",
        default_args=DEFAULT_ARGS,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,
        tags=["aurum", "monitoring", "staleness", "critical", "data-quality"],
        **kwargs
    )

    # Critical staleness check task
    def check_critical_staleness(**context):
        """Check for critical staleness issues."""
        if not STALENESS_MONITORING_AVAILABLE:
            raise RuntimeError("Staleness monitoring components not available")

        detector = StalenessDetector(critical_config)

        # Focus on critical datasets
        critical_datasets = [
            "eia_electricity_prices",
            "iso_nyiso_lmp_dam",
            "iso_pjm_lmp_rtm",
            "fred_unemployment",  # Important economic indicator
        ]

        for dataset in critical_datasets:
            detector.add_staleness_check(StalenessCheck(
                dataset=dataset,
                check_interval_minutes=15,
                enabled=True,
                alert_on_stale=True,
                alert_on_critical=True
            ))

        # Run checks
        results = detector.run_staleness_checks()

        # Check for critical issues
        critical_datasets = detector.get_critical_datasets()

        if critical_datasets:
            print(f"🚨 CRITICAL: {len(critical_datasets)} datasets are critically stale")
            for dataset in critical_datasets:
                print(f"  - {dataset.dataset}: {dataset.hours_since_update".2f"}h old")
        else:
            print("✅ No critical staleness issues detected")

        return results

    critical_check_task = PythonOperator(
        task_id="check_critical_staleness",
        python_callable=check_critical_staleness,
        dag=dag
    )

    return dag

# Create the main staleness monitoring DAG
main_staleness_dag = create_staleness_monitor_dag()

# Create the critical staleness monitoring DAG
critical_staleness_dag = create_critical_staleness_monitor_dag()

if __name__ == "__main__":
    # Allow running this script to test DAG creation
    print("Created staleness monitoring DAGs:")
    print(f"  - {main_staleness_dag.dag_id}")
    print(f"  - {critical_staleness_dag.dag_id}")

    # Print some configuration info
    print(f"\nMain DAG schedule: {main_staleness_dag.schedule_interval}")
    print(f"Critical DAG schedule: {critical_staleness_dag.schedule_interval}")
