"""
Airflow DAG for running canary dataset checks to detect upstream API breaks.

This DAG:
- Runs canary checks for all registered data sources
- Monitors API health and data quality
- Generates alerts when APIs are broken or returning unexpected data
- Provides early warning system for data pipeline issues
- Integrates with SLA monitoring system
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Import canary monitoring components
try:
    from aurum.canary import (
        CanaryManager,
        CanaryConfig,
        APIHealthChecker,
        CanaryRunner,
        CanaryAlertManager
    )
    CANARY_MONITORING_AVAILABLE = True
except ImportError:
    CANARY_MONITORING_AVAILABLE = False

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

def create_canary_monitor_dag(
    dag_id: str = "canary_dataset_monitor",
    schedule_interval: str = "*/15 * * * *",  # Every 15 minutes
    start_date: datetime = None,
    **kwargs
):
    """Create canary monitoring DAG.

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
        description="Monitor canary datasets to detect upstream API breaks and data quality issues",
        default_args=DEFAULT_ARGS,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,
        tags=["aurum", "monitoring", "canary", "api-health", "data-quality"],
        **kwargs
    )

    # Start task
    start_task = EmptyOperator(
        task_id="start_canary_monitoring",
        task_group="canary_monitoring",
        dag=dag
    )

    # Initialize canary manager
    def initialize_canary_manager(**context):
        """Initialize the canary manager with configured canaries."""
        if not CANARY_MONITORING_AVAILABLE:
            raise RuntimeError("Canary monitoring components not available")

        canary_manager = CanaryManager()
        api_checker = APIHealthChecker()

        # Register canary datasets for different data sources
        canary_configs = [
            # EIA canaries
            CanaryConfig(
                name="eia_electricity_prices_canary",
                source="eia",
                dataset="electricity_prices",
                api_endpoint="https://api.eia.gov/v2/electricity/retail-sales/data/",
                api_params={
                    "api_key": "DEMO_KEY",  # Use demo key for canary
                    "frequency": "monthly",
                    "data[0]": "price",
                    "facets[stateid][]": "CA",
                    "start": "2023-01",
                    "end": "2023-01",
                    "length": "1"
                },
                expected_response_format="json",
                expected_fields=["response", "data"],
                timeout_seconds=30,
                description="EIA electricity prices API health check"
            ),

            CanaryConfig(
                name="eia_petroleum_prices_canary",
                source="eia",
                dataset="petroleum_prices",
                api_endpoint="https://api.eia.gov/v2/petroleum/pri/spt/data/",
                api_params={
                    "api_key": "DEMO_KEY",
                    "frequency": "weekly",
                    "data[0]": "value",
                    "facets[product][]": "EPD2DXL0",
                    "start": "2024-01-01",
                    "end": "2024-01-01",
                    "length": "1"
                },
                expected_response_format="json",
                expected_fields=["response", "data"],
                timeout_seconds=30,
                description="EIA petroleum prices API health check"
            ),

            # FRED canaries
            CanaryConfig(
                name="fred_unemployment_canary",
                source="fred",
                dataset="unemployment",
                api_endpoint="https://api.stlouisfed.org/fred/series/observations",
                api_params={
                    "series_id": "UNRATE",
                    "api_key": "demo",  # FRED demo key
                    "file_type": "json",
                    "observation_start": "2024-01-01",
                    "observation_end": "2024-01-01",
                    "limit": "1"
                },
                expected_response_format="json",
                expected_fields=["realtime_start", "realtime_end", "observations"],
                timeout_seconds=30,
                description="FRED unemployment rate API health check"
            ),

            CanaryConfig(
                name="fred_gdp_canary",
                source="fred",
                dataset="gdp",
                api_endpoint="https://api.stlouisfed.org/fred/series/observations",
                api_params={
                    "series_id": "GDP",
                    "api_key": "demo",
                    "file_type": "json",
                    "observation_start": "2023-01-01",
                    "observation_end": "2023-01-01",
                    "limit": "1"
                },
                expected_response_format="json",
                expected_fields=["realtime_start", "realtime_end", "observations"],
                timeout_seconds=30,
                description="FRED GDP API health check"
            ),

            # CPI canaries
            CanaryConfig(
                name="cpi_all_items_canary",
                source="cpi",
                dataset="cpi_all_items",
                api_endpoint="https://api.stlouisfed.org/fred/series/observations",
                api_params={
                    "series_id": "CPIAUCSL",
                    "api_key": "demo",
                    "file_type": "json",
                    "observation_start": "2024-01-01",
                    "observation_end": "2024-01-01",
                    "limit": "1"
                },
                expected_response_format="json",
                expected_fields=["realtime_start", "realtime_end", "observations"],
                timeout_seconds=30,
                description="CPI All Items API health check"
            ),

            # NOAA canaries
            CanaryConfig(
                name="noaa_weather_canary",
                source="noaa",
                dataset="weather",
                api_endpoint="https://www.ncei.noaa.gov/access/services/data/v1",
                api_params={
                    "dataset": "daily-summaries",
                    "stations": "USW00094728",  # Example station
                    "startDate": "2024-01-01",
                    "endDate": "2024-01-01",
                    "format": "json",
                    "limit": "1"
                },
                expected_response_format="json",
                expected_fields=["results"],
                timeout_seconds=45,  # NOAA can be slower
                description="NOAA weather data API health check"
            ),

            # ISO canaries
            CanaryConfig(
                name="nyiso_lmp_canary",
                source="iso",
                dataset="nyiso_lmp",
                api_endpoint="http://mis.nyiso.com/public/api/v1/lbmp/current",
                api_params={
                    "format": "json",
                    "limit": "1"
                },
                expected_response_format="json",
                expected_fields=["data"],
                timeout_seconds=30,
                description="NYISO LMP data API health check"
            ),

            CanaryConfig(
                name="pjm_lmp_canary",
                source="iso",
                dataset="pjm_lmp",
                api_endpoint="https://api.pjm.com/api/v1/lmp/current",
                api_params={
                    "format": "json",
                    "limit": "1"
                },
                expected_response_format="json",
                expected_fields=["items"],
                timeout_seconds=30,
                description="PJM LMP data API health check"
            ),

            CanaryConfig(
                name="caiso_oasis_canary",
                source="iso",
                dataset="caiso_oasis",
                api_endpoint="http://oasis.caiso.com/oasisapi/SingleZip",
                api_params={
                    "queryname": "SLD_FCST",
                    "startdatetime": "20240101T00:00-0000",
                    "enddatetime": "20240101T23:59-0000",
                    "version": "1",
                    "format": "json"
                },
                expected_response_format="json",
                expected_fields=["ServiceResponse"],
                timeout_seconds=60,  # CAISO can be slower
                description="CAISO OASIS API health check"
            ),
        ]

        # Register all canaries
        for config in canary_configs:
            canary_manager.register_canary(config)

        # Store components in XCom for use by other tasks
        context['task_instance'].xcom_push(key='canary_manager', value=canary_manager)
        context['task_instance'].xcom_push(key='api_checker', value=api_checker)

        return f"Initialized canary monitoring with {len(canary_configs)} canaries"

    init_task = PythonOperator(
        task_id="initialize_canary_manager",
        python_callable=initialize_canary_manager,
        task_group="canary_monitoring",
        dag=dag
    )

    # Run canary checks
    def run_canary_checks(**context):
        """Run canary checks for all registered datasets."""
        if not CANARY_MONITORING_AVAILABLE:
            raise RuntimeError("Canary monitoring components not available")

        # Get components from XCom
        canary_manager = context['task_instance'].xcom_pull(
            key='canary_manager',
            task_ids='initialize_canary_manager'
        )

        api_checker = context['task_instance'].xcom_pull(
            key='api_checker',
            task_ids='initialize_canary_manager'
        )

        if not canary_manager or not api_checker:
            raise RuntimeError("Canary components not found in XCom")

        # Create runner and execute canaries
        runner = CanaryRunner(canary_manager, api_checker)
        results = runner.run_all_canaries()

        # Log summary
        total_canaries = len(results)
        healthy_canaries = len([r for r in results.values() if r.is_success()])
        unhealthy_canaries = len([r for r in results.values() if r.is_failure()])
        warning_canaries = len([r for r in results.values() if r.has_warnings()])

        print(f"🐦 Canary Check Results:")
        print(f"   Total: {total_canaries}")
        print(f"   Healthy: {healthy_canaries}")
        print(f"   Unhealthy: {unhealthy_canaries}")
        print(f"   Warnings: {warning_canaries}")

        if unhealthy_canaries > 0:
            print(f"   🚨 Unhealthy canaries: {[r.canary_name for r in results.values() if r.is_failure()]}")

        # Store results in XCom for downstream tasks
        context['task_instance'].xcom_push(key='canary_results', value=results)

        return {
            "total_canaries": total_canaries,
            "healthy_canaries": healthy_canaries,
            "unhealthy_canaries": unhealthy_canaries,
            "warning_canaries": warning_canaries,
            "results": results
        }

    check_task = PythonOperator(
        task_id="run_canary_checks",
        python_callable=run_canary_checks,
        task_group="canary_monitoring",
        dag=dag
    )

    # Process canary results
    def process_canary_results(**context):
        """Process canary results and generate alerts."""
        if not CANARY_MONITORING_AVAILABLE:
            raise RuntimeError("Canary monitoring components not available")

        # Get components and results
        canary_manager = context['task_instance'].xcom_pull(
            key='canary_manager',
            task_ids='initialize_canary_manager'
        )

        results = context['task_instance'].xcom_pull(
            key='canary_results',
            task_ids='run_canary_checks'
        )

        if not canary_manager or not results:
            raise RuntimeError("Canary data not found in XCom")

        # Process results and generate alerts
        alert_manager = CanaryAlertManager()
        alert_manager._canary_manager = canary_manager  # For accessing canary data

        for canary_name, result in results.items():
            alert_manager.process_canary_result(canary_name, result.to_dict())

        # Get alert summary
        alert_summary = alert_manager.get_canary_alert_summary(hours=1)

        # Log processing summary
        print(f"📊 Canary Processing Summary:")
        print(f"   Active alerts: {len(alert_manager.get_active_canary_alerts())}")
        print(f"   Recent alerts: {alert_summary['total_alerts']}")
        if alert_summary['total_alerts'] > 0:
            print(f"   Alert severities: {alert_summary['by_severity']}")

        # Store alert summary in XCom
        context['task_instance'].xcom_push(key='alert_summary', value=alert_summary)

        return alert_summary

    process_task = PythonOperator(
        task_id="process_canary_results",
        python_callable=process_canary_results,
        task_group="canary_monitoring",
        dag=dag
    )

    # Generate canary report
    def generate_canary_report(**context):
        """Generate comprehensive canary monitoring report."""
        if not CANARY_MONITORING_AVAILABLE:
            raise RuntimeError("Canary monitoring components not available")

        # Get components
        canary_manager = context['task_instance'].xcom_pull(
            key='canary_manager',
            task_ids='initialize_canary_manager'
        )

        results = context['task_instance'].xcom_pull(
            key='canary_results',
            task_ids='run_canary_checks'
        )

        if not canary_manager or not results:
            raise RuntimeError("Canary data not found in XCom")

        # Generate comprehensive report
        report = {
            "timestamp": datetime.now().isoformat(),
            "canary_manager_status": canary_manager.get_canary_health_summary(),
            "execution_results": {
                name: result.to_dict() for name, result in results.items()
            },
            "summary": {
                "total_canaries": len(results),
                "successful_canaries": len([r for r in results.values() if r.is_success()]),
                "failed_canaries": len([r for r in results.values() if r.is_failure()]),
                "canaries_with_warnings": len([r for r in results.values() if r.has_warnings()]),
            }
        }

        # Store report for downstream tasks
        context['task_instance'].xcom_push(key='canary_report', value=report)

        # Log report summary
        summary = report["summary"]
        print(f"📋 Generated canary report: {summary}")

        return report

    report_task = PythonOperator(
        task_id="generate_canary_report",
        python_callable=generate_canary_report,
        task_group="canary_monitoring",
        dag=dag
    )

    # End task
    end_task = EmptyOperator(
        task_id="end_canary_monitoring",
        task_group="canary_monitoring",
        dag=dag
    )

    # Set up task dependencies
    start_task >> init_task >> check_task >> process_task >> report_task >> end_task

    return dag

def create_critical_canary_monitor_dag(
    dag_id: str = "critical_canary_monitor",
    schedule_interval: str = "*/5 * * * *",  # Every 5 minutes for critical monitoring
    start_date: datetime = None,
    **kwargs
):
    """Create a DAG focused on critical canary monitoring with faster checks.

    Args:
        dag_id: DAG identifier
        schedule_interval: Cron schedule (more frequent for critical monitoring)
        start_date: DAG start date
        **kwargs: Additional DAG arguments
    """
    if start_date is None:
        start_date = datetime(2024, 1, 1)

    dag = DAG(
        dag_id=dag_id,
        description="Critical canary monitoring with fast detection of API breaks",
        default_args=DEFAULT_ARGS,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,
        tags=["aurum", "monitoring", "canary", "critical", "api-health"],
        **kwargs
    )

    # Critical canary check task
    def check_critical_canaries(**context):
        """Check critical canaries with high priority."""
        if not CANARY_MONITORING_AVAILABLE:
            raise RuntimeError("Canary monitoring components not available")

        canary_manager = CanaryManager()
        api_checker = APIHealthChecker()

        # Register only critical canaries
        critical_canaries = [
            # Critical financial data
            ("eia_electricity_prices_canary", "eia"),
            ("fred_unemployment_canary", "fred"),

            # Critical infrastructure data
            ("nyiso_lmp_canary", "iso"),
            ("pjm_lmp_canary", "iso"),

            # Critical economic indicators
            ("cpi_all_items_canary", "cpi"),
        ]

        for canary_name, source in critical_canaries:
            canary_manager.register_canary(CanaryConfig(
                name=canary_name,
                source=source,
                dataset=f"{source}_critical",
                api_endpoint="https://api.example.com/health",  # Placeholder
                timeout_seconds=30,
                description=f"Critical {source} API health check"
            ))

        # Create runner and execute critical canaries
        runner = CanaryRunner(canary_manager, api_checker)
        results = runner.run_all_canaries()

        # Check for critical issues
        critical_issues = []
        for canary_name, result in results.items():
            if result.is_failure():
                critical_issues.append({
                    "canary": canary_name,
                    "status": result.status.value,
                    "errors": result.errors[:2],  # First 2 errors
                    "consecutive_failures": getattr(result, 'consecutive_failures', 0)
                })

        if critical_issues:
            print(f"🚨 CRITICAL CANARY ISSUES DETECTED:")
            for issue in critical_issues:
                print(f"  - {issue['canary']}: {issue['status']} ({len(issue['errors'])} errors)")

            # Store critical issues in XCom
            context['task_instance'].xcom_push(key='critical_issues', value=critical_issues)
        else:
            print("✅ All critical canaries healthy")

        return {
            "total_canaries": len(results),
            "critical_issues": len(critical_issues),
            "results": results
        }

    critical_check_task = PythonOperator(
        task_id="check_critical_canaries",
        python_callable=check_critical_canaries,
        dag=dag
    )

    return dag

# Create the main canary monitoring DAG
main_canary_dag = create_canary_monitor_dag()

# Create the critical canary monitoring DAG
critical_canary_dag = create_critical_canary_monitor_dag()

if __name__ == "__main__":
    # Allow running this script to test DAG creation
    print("Created canary monitoring DAGs:")
    print(f"  - {main_canary_dag.dag_id}")
    print(f"  - {critical_canary_dag.dag_id}")

    # Print some configuration info
    print(f"\nMain DAG schedule: {main_canary_dag.schedule_interval}")
    print(f"Critical DAG schedule: {critical_canary_dag.schedule_interval}")
