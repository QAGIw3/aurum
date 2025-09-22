"""Example demonstrating canary deployment for ISO data ingestion.

This example shows how to use the canary deployment system to safely roll out
new versions of ISO data ingestion pipelines with automated health monitoring
and intelligent traffic management.
"""

import asyncio
from datetime import datetime
from typing import Dict, List

from aurum.canary import (
    deploy_caiso_with_canary,
    get_deployment_status,
    get_deployment_health_status,
    TrafficEndpoint,
    CanaryConfig
)
from aurum.external.pipeline.orchestrator import PipelineConfig
from aurum.security.vault_client import patch_environment_with_vault_credentials


async def main():
    """Main example function."""
    print("🚀 Starting ISO Data Ingestion Canary Deployment Example")
    print("=" * 60)

    try:
        # 1. Set up credentials and configuration
        print("📋 Step 1: Setting up credentials and configuration")
        await patch_environment_with_vault_credentials()

        pipeline_config = PipelineConfig(
            kafka_bootstrap_servers="localhost:9092",
            timescale_connection="postgresql://aurum:aurum@localhost:5432/aurum",
            iceberg_catalog="nessie",
            enable_great_expectations=True,
            enable_seatunnel_validation=True
        )
        print("✅ Configuration loaded")

        # 2. Define baseline and canary endpoints
        print("\n🌐 Step 2: Defining service endpoints")

        baseline_endpoints = [
            TrafficEndpoint(
                name="caiso-baseline-v2.0.0",
                url="https://api.caiso-v2-0-0.service.com",
                version="v2.0.0",
                weight=100.0,
                healthy=True,
                metadata={"environment": "production", "region": "us-west"}
            ),
            TrafficEndpoint(
                name="caiso-database-baseline",
                url="postgresql://caiso-prod-v2-0-0:5432/aurum",
                version="v2.0.0",
                healthy=True,
                metadata={"type": "database", "pool": "main"}
            )
        ]

        canary_endpoints = [
            TrafficEndpoint(
                name="caiso-canary-v2.1.0",
                url="https://api.caiso-v2-1-0.service.com",
                version="v2.1.0",
                weight=0.0,
                healthy=True,
                metadata={"environment": "canary", "region": "us-west", "features": "enhanced_validation"}
            ),
            TrafficEndpoint(
                name="caiso-database-canary",
                url="postgresql://caiso-canary-v2-1-0:5432/aurum",
                version="v2.1.0",
                healthy=True,
                metadata={"type": "database", "pool": "canary"}
            )
        ]

        print("✅ Baseline endpoints:")
        for endpoint in baseline_endpoints:
            print(f"   - {endpoint.name} ({endpoint.version})")

        print("✅ Canary endpoints:")
        for endpoint in canary_endpoints:
            print(f"   - {endpoint.name} ({endpoint.version})")

        # 3. Create canary deployment configuration
        print("\n⚙️ Step 3: Creating canary deployment configuration")

        canary_config = CanaryConfig(
            name="caiso-ingestion-v2.1.0",
            version="v2.1.0",
            description="CAISO data ingestion with enhanced validation and performance improvements",
            initial_traffic_percent=5.0,
            traffic_increment_percent=10.0,
            max_traffic_percent=100.0,
            warmup_duration_minutes=5,
            evaluation_duration_minutes=10,
            promotion_duration_minutes=15,
            success_threshold_percent=95.0,
            latency_threshold_ms=1000.0,
            error_rate_threshold_percent=1.0,
            auto_rollback_enabled=True
        )

        print("✅ Canary configuration:")
        print(f"   - Initial traffic: {canary_config.initial_traffic_percent}%")
        print(f"   - Traffic increment: {canary_config.traffic_increment_percent}%")
        print(f"   - Evaluation duration: {canary_config.evaluation_duration_minutes} minutes")
        print(f"   - Auto-rollback: {canary_config.auto_rollback_enabled}")

        # 4. Deploy with canary strategy
        print("\n🚀 Step 4: Starting canary deployment")

        deployment_name = await deploy_caiso_with_canary(
            version="v2.1.0",
            baseline_endpoints=baseline_endpoints,
            canary_endpoints=canary_endpoints,
            description="Enhanced CAISO data ingestion with improved validation"
        )

        print(f"✅ Deployment started: {deployment_name}")

        # 5. Monitor deployment progress
        print("\n📊 Step 5: Monitoring deployment progress")

        max_monitoring_time = 60  # Monitor for up to 60 minutes
        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < max_monitoring_time:
            status = await get_deployment_status(deployment_name)

            if not status:
                print("❌ Deployment not found")
                break

            print(f"\n📈 Deployment Status (Time: {(datetime.now() - start_time).total_seconds() / 60:.".1f"minutes)")
            print(f"   Status: {status['status']}")
            print(f"   Traffic: {status['current_traffic_percent']}%")
            print(f"   Duration: {status['duration_minutes']".1f"} minutes")

            # Show metrics if available
            metrics = status.get('deployment_metrics', {})
            if metrics:
                print("   📊 Metrics:"                print(f"      Success Rate: {metrics.get('success_rate_percent', 0)".1f"}%")
                print(f"      Error Rate: {metrics.get('error_rate_percent', 0)".1f"}%")
                print(f"      Avg Latency: {metrics.get('avg_latency_ms', 0)".1f"}ms")

            # Show health status
            health_status = status.get('health_status', 'unknown')
            health_confidence = status.get('health_confidence', 0.0)
            print(f"   ❤️ Health: {health_status} (confidence: {health_confidence".1f"})")

            # Check if deployment is complete
            if status['status'] == 'completed':
                print("
✅ Deployment completed successfully!"                print(f"   Final traffic: {status['current_traffic_percent']}%")
                break
            elif status['status'] == 'rolled_back':
                print("
⚠️ Deployment was rolled back due to issues"                break
            elif status['status'] == 'failed':
                print("
❌ Deployment failed"                break

            # Wait before next check
            await asyncio.sleep(30)

        # 6. Get final health report
        print("
🏥 Step 6: Final health assessment"        health_report = await get_deployment_health_status(deployment_name)

        if health_report:
            print(f"   Overall Health: {health_report.overall_status.value}")
            print(f"   Confidence Score: {health_report.confidence_score".2f"}")

            print("   📋 Component Health:")
            print(f"      API: {health_report.api_health.value}")
            print(f"      Database: {health_report.database_health.value}")
            print(f"      Kafka: {health_report.kafka_health.value}")

            print(f"   📊 Performance:")
            print(f"      Avg Latency: {health_report.avg_latency_ms".1f"}ms")
            print(f"      Error Rate: {health_report.error_rate_percent".2f"}%")
            print(f"      Throughput: {health_report.throughput_rps".1f"} req/sec")

            if health_report.alerts:
                print(f"   🚨 Alerts: {len(health_report.alerts)}")
                for alert in health_report.alerts[:3]:  # Show first 3 alerts
                    print(f"      - {alert['severity']}: {alert['message']}")

        print("
🎉 Canary deployment example completed!"
    except Exception as e:
        print(f"\n❌ Error in canary deployment example: {e}")
        raise


async def demonstrate_traffic_management():
    """Demonstrate traffic management capabilities."""
    print("\n🌐 Traffic Management Demonstration")
    print("=" * 40)

    from aurum.canary import get_traffic_manager, TrafficEndpoint, RoutingRule, TrafficStrategy

    try:
        traffic_manager = await get_traffic_manager()

        # Create test endpoints
        baseline_ep = TrafficEndpoint(
            name="test-baseline",
            url="https://baseline.example.com",
            version="v1.0.0",
            healthy=True
        )

        canary_ep = TrafficEndpoint(
            name="test-canary",
            url="https://canary.example.com",
            version="v1.1.0",
            healthy=True
        )

        await traffic_manager.add_endpoint(baseline_ep)
        await traffic_manager.add_endpoint(canary_ep)

        # Create routing rule
        rule = RoutingRule(
            name="test-traffic-rule",
            strategy=TrafficStrategy.WEIGHTED,
            baseline_endpoints=[baseline_ep],
            canary_endpoints=[canary_ep],
            canary_traffic_percent=30.0
        )

        await traffic_manager.add_routing_rule(rule)

        # Get traffic statistics
        stats = await traffic_manager.get_traffic_stats("test-traffic-rule")

        if stats:
            print("✅ Traffic stats retrieved:"            print(f"   Total requests: {stats['total_requests']}")
            print(f"   Baseline traffic: {stats['baseline_percent']".1f"}%")
            print(f"   Canary traffic: {stats['canary_percent']".1f"}%")

        print("✅ Traffic management demonstration completed")

    except Exception as e:
        print(f"❌ Traffic management demonstration failed: {e}")


async def demonstrate_health_monitoring():
    """Demonstrate health monitoring capabilities."""
    print("\n❤️ Health Monitoring Demonstration")
    print("=" * 40)

    from aurum.canary import get_health_monitor

    try:
        health_monitor = await get_health_monitor()

        # Set up monitoring for a test deployment
        await health_monitor.add_deployment_monitoring(
            "test-deployment",
            {
                "api": "https://api.example.com/health",
                "database": "postgresql://db.example.com:5432/health",
                "kafka": "kafka://kafka.example.com:9092"
            },
            [
                {
                    "name": "api_connectivity",
                    "provider": "http",
                    "target": "https://api.example.com/health"
                }
            ]
        )

        # Get health status
        health_report = await health_monitor.get_deployment_health("test-deployment")

        if health_report:
            print("✅ Health report generated:"            print(f"   Overall status: {health_report.overall_status.value}")
            print(f"   Confidence: {health_report.confidence_score".2f"}")
            print(f"   Health checks: {len(health_report.health_checks)}")
            print(f"   Failed checks: {len(health_report.failed_checks)}")

        print("✅ Health monitoring demonstration completed")

    except Exception as e:
        print(f"❌ Health monitoring demonstration failed: {e}")


if __name__ == "__main__":
    print("🧪 ISO Data Ingestion Canary Deployment Examples")
    print("=" * 50)

    async def run_all_examples():
        """Run all example demonstrations."""
        try:
            # Main canary deployment example
            await main()

            # Traffic management demo
            await demonstrate_traffic_management()

            # Health monitoring demo
            await demonstrate_health_monitoring()

        except KeyboardInterrupt:
            print("\n⏹️ Examples interrupted by user")
        except Exception as e:
            print(f"\n❌ Examples failed with error: {e}")
            raise

    # Run the examples
    asyncio.run(run_all_examples())
