# Canary Deployment System

## Overview

The Canary Deployment System provides a comprehensive solution for safe production rollouts of ISO data ingestion pipelines. It enables gradual traffic migration, automated health validation, intelligent decision-making, and automatic rollback capabilities.

## 🎯 **Key Features**

### **🔄 Gradual Traffic Migration**
- **Weighted Traffic Splitting**: Route requests based on configurable percentages
- **Header-Based Routing**: Route specific users or sessions to canary versions
- **User ID-Based Routing**: Consistent routing based on user identification
- **Session-Based Routing**: Route based on session affinity
- **Geographic Routing**: Route based on geographic location

### **🏥 Health Monitoring & Validation**
- **Multi-Component Health Checks**: HTTP, Database, Kafka, and Metrics-based checks
- **Performance Monitoring**: Latency, throughput, and error rate tracking
- **Automated Health Assessment**: Continuous evaluation of system health
- **Alert Generation**: Proactive alerting on health degradation

### **🤖 Intelligent Decision Making**
- **Performance-Based Decisions**: Promote/rollback based on latency and error rates
- **Health-Based Decisions**: Make decisions based on component health status
- **Composite Decision Policies**: Combine multiple factors for robust decisions
- **Configurable Thresholds**: Customizable success criteria and thresholds

### **🔄 Automatic Rollback**
- **Threshold-Based Rollback**: Automatic rollback on performance degradation
- **Health-Based Rollback**: Rollback on component failures
- **Manual Override**: Ability to manually trigger rollbacks
- **Graceful Degradation**: Safe traffic reduction strategies

## 🏗️ **Architecture**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Traffic       │    │   Deployment     │    │   Health        │
│   Manager       │◀──▶│   Manager        │◀──▶│   Monitor       │
│                 │    │                  │    │                 │
│ - Route requests│    │ - Manage         │    │ - Health checks │
│ - Split traffic │    │   deployments    │    │ - Performance   │
│ - Load balance  │    │ - Track progress │    │   monitoring    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌──────────────────┐
│   Orchestrator   │    │   Decision       │
│                 │    │   Engine         │
│ - Coordinate    │    │                  │
│   components    │    │ - Evaluate       │
│ - Execute       │    │   metrics        │
│   decisions     │    │ - Make decisions │
└─────────────────┘    └──────────────────┘
```

## 📋 **Components**

### **1. CanaryDeploymentOrchestrator**
Main orchestration layer that coordinates all deployment activities.

```python
from aurum.canary import get_canary_orchestrator, CanaryConfig, TrafficEndpoint

# Create orchestrator
orchestrator = await get_canary_orchestrator()

# Configure canary deployment
config = CanaryConfig(
    name="caiso-v2.1.0",
    version="v2.1.0",
    initial_traffic_percent=5.0,
    traffic_increment_percent=10.0,
    success_threshold_percent=95.0,
    auto_rollback_enabled=True
)

# Deploy with canary strategy
deployment_name = await orchestrator.deploy_with_canary(
    config,
    baseline_endpoints,
    canary_endpoints
)
```

### **2. TrafficManager**
Handles traffic splitting and routing between baseline and canary versions.

```python
from aurum.canary import get_traffic_manager, TrafficStrategy

# Set up traffic routing
baseline_endpoints = [TrafficEndpoint(name="baseline", url="...", version="v1.0")]
canary_endpoints = [TrafficEndpoint(name="canary", url="...", version="v2.0")]

await traffic_manager.add_endpoint(baseline_endpoints[0])
await traffic_manager.add_endpoint(canary_endpoints[0])

# Route 20% of traffic to canary
await traffic_manager.update_traffic_percentage("rule_name", 20.0)
```

### **3. HealthMonitor**
Provides comprehensive health monitoring and alerting.

```python
from aurum.canary import get_health_monitor

# Set up health monitoring
health_monitor = await get_health_monitor()

await health_monitor.add_deployment_monitoring(
    "caiso-deployment",
    {
        "api": "https://api.caiso.com/health",
        "database": "postgresql://caiso-db:5432/health"
    },
    [
        {
            "name": "api_health",
            "provider": "http",
            "target": "https://api.caiso.com/health"
        }
    ]
)

# Get health status
health_report = await health_monitor.get_deployment_health("caiso-deployment")
print(f"Health: {health_report.overall_status.value}")
```

## 🚀 **Usage Examples**

### **Basic Canary Deployment**

```python
import asyncio
from aurum.canary import deploy_caiso_with_canary, get_deployment_status
from aurum.canary import TrafficEndpoint

async def main():
    # Define endpoints
    baseline_endpoints = [
        TrafficEndpoint(
            name="caiso-baseline",
            url="https://api.caiso-v1.example.com",
            version="v1.0.0"
        )
    ]

    canary_endpoints = [
        TrafficEndpoint(
            name="caiso-canary",
            url="https://api.caiso-v2.example.com",
            version="v2.0.0"
        )
    ]

    # Deploy with canary strategy
    deployment_name = await deploy_caiso_with_canary(
        version="v2.0.0",
        baseline_endpoints=baseline_endpoints,
        canary_endpoints=canary_endpoints,
        description="Enhanced CAISO with improved performance"
    )

    # Monitor deployment
    while True:
        status = await get_deployment_status(deployment_name)

        print(f"Status: {status['status']}")
        print(f"Traffic: {status['current_traffic_percent']}%")
        print(f"Health: {status['health_status']}")

        if status['status'] in ['completed', 'rolled_back', 'failed']:
            break

        await asyncio.sleep(30)

asyncio.run(main())
```

### **Multi-ISO Deployment**

```python
from aurum.canary import deploy_multi_iso_with_canary

# Deploy multiple ISOs with coordinated canary
deployment_names = await deploy_multi_iso_with_canary(
    iso_codes=["CAISO", "MISO", "PJM"],
    version="v2.1.0",
    baseline_endpoints_dict={
        "CAISO": [caiso_baseline],
        "MISO": [miso_baseline],
        "PJM": [pjm_baseline]
    },
    canary_endpoints_dict={
        "CAISO": [caiso_canary],
        "MISO": [miso_canary],
        "PJM": [pjm_canary]
    }
)

for iso, deployment_name in deployment_names.items():
    print(f"{iso}: {deployment_name}")
```

### **Custom Decision Policies**

```python
from aurum.canary import get_canary_orchestrator, DeploymentContext, DeploymentDecision

async def custom_decision_policy(context: DeploymentContext) -> DeploymentDecision:
    """Custom decision policy based on business rules."""

    # Get current status
    status = await get_canary_orchestrator().get_deployment_status(context.deployment_name)

    if not status:
        return DeploymentDecision.PAUSE

    # Business hours policy - be more conservative during peak hours
    current_hour = datetime.now().hour
    if 9 <= current_hour <= 17:  # Business hours
        if status['error_rate_percent'] > 0.5:  # Stricter threshold
            return DeploymentDecision.ROLLBACK

    # Standard evaluation
    if (status['success_rate_percent'] < 95.0 or
        status['error_rate_percent'] > 1.0 or
        status['health_status'] == 'critical'):
        return DeploymentDecision.ROLLBACK

    if context.current_traffic_percent >= 100.0:
        return DeploymentDecision.PROMOTE

    return DeploymentDecision.CONTINUE

# Use custom policy
orchestrator = await get_canary_orchestrator()
# Configure orchestrator to use custom policy
```

## ⚙️ **Configuration**

### **CanaryConfig Parameters**

```python
CanaryConfig(
    name="caiso-deployment-v2.0.0",           # Deployment name
    version="v2.0.0",                         # Version identifier
    description="Enhanced CAISO pipeline",    # Description

    # Traffic configuration
    initial_traffic_percent=5.0,              # Starting traffic %
    traffic_increment_percent=10.0,           # Traffic increase per step
    max_traffic_percent=100.0,               # Maximum traffic %

    # Timing configuration
    warmup_duration_minutes=5,               # Warmup time
    evaluation_duration_minutes=10,          # Evaluation time per step
    promotion_duration_minutes=15,           # Time at 100% before completion

    # Success criteria
    success_threshold_percent=95.0,          # Success rate threshold
    latency_threshold_ms=1000.0,             # Latency threshold
    error_rate_threshold_percent=1.0,        # Error rate threshold

    # Safety features
    auto_rollback_enabled=True,              # Enable automatic rollback
    rollback_on_failure=True                 # Rollback on any failure
)
```

### **Health Check Configuration**

```python
from aurum.canary import HealthCheck

health_check = HealthCheck(
    name="caiso_api_health",
    description="CAISO API connectivity check",
    check_type="http",
    endpoint="https://api.caiso.com/health",
    expected_status_codes=[200, 201, 202],
    timeout_seconds=10,
    retry_count=3
)
```

### **Traffic Routing Rules**

```python
from aurum.canary import RoutingRule, TrafficStrategy

routing_rule = RoutingRule(
    name="caiso-traffic-split",
    strategy=TrafficStrategy.WEIGHTED,
    baseline_endpoints=[baseline_endpoint],
    canary_endpoints=[canary_endpoint],
    canary_traffic_percent=20.0,
    conditions={
        "header:X-API-Key": "canary-users"
    }
)
```

## 📊 **Monitoring & Observability**

### **Key Metrics**

```python
# Deployment metrics
metrics = await orchestrator.get_deployment_status(deployment_name)
print(f"Success Rate: {metrics['deployment_metrics']['success_rate_percent']}%")
print(f"Error Rate: {metrics['deployment_metrics']['error_rate_percent']}%")
print(f"Avg Latency: {metrics['deployment_metrics']['avg_latency_ms']}ms")

# Traffic metrics
traffic_stats = await traffic_manager.get_traffic_stats(rule_name)
print(f"Canary Traffic: {traffic_stats['canary_percent']}%")
print(f"Baseline Traffic: {traffic_stats['baseline_percent']}%")

# Health metrics
health_report = await health_monitor.get_deployment_health(deployment_name)
print(f"Overall Health: {health_report.overall_status.value}")
print(f"Health Confidence: {health_report.confidence_score}")
```

### **Alert Configuration**

```python
async def alert_callback(alert: Dict[str, Any]):
    """Handle alerts from the canary deployment system."""
    print(f"🚨 ALERT: {alert['severity']} - {alert['message']}")

    if alert['severity'] == 'critical':
        # Trigger immediate rollback
        await orchestrator.execute_decision(deployment_name, DeploymentDecision.ROLLBACK)

# Register alert callback
await health_monitor.add_alert_callback(alert_callback)
```

## 🔧 **Integration with Existing Systems**

### **Pipeline Integration**

```python
from aurum.external.pipeline.orchestrator import PipelineOrchestrator
from aurum.canary import get_canary_orchestrator

# Integrate with existing pipeline
pipeline_orchestrator = PipelineOrchestrator(config)
canary_orchestrator = await get_canary_orchestrator()

# Deploy pipeline with canary
deployment_name = await canary_orchestrator.deploy_with_canary(
    canary_config,
    baseline_endpoints,
    canary_endpoints
)

# Monitor both pipeline and canary
pipeline_status = await pipeline_orchestrator.health_check()
canary_status = await canary_orchestrator.get_deployment_status(deployment_name)
```

### **Quota Management Integration**

```python
from aurum.external.quota_manager import get_quota_manager

# Quota-aware canary deployment
quota_manager = await get_quota_manager()

# Adjust traffic based on quota usage
quota_status = quota_manager.get_quota_status("caiso_lmp")

if quota_status['records_percent'] > 80:
    # Reduce canary traffic during high quota usage
    await traffic_manager.update_traffic_percentage(rule_name, 5.0)
```

## 🛡️ **Safety Features**

### **Automatic Rollback Triggers**
- **Performance Degradation**: High latency or error rates
- **Health Check Failures**: Component health issues
- **Quota Violations**: Excessive resource usage
- **Manual Override**: Administrative intervention

### **Gradual Rollout Controls**
- **Traffic Limits**: Maximum traffic percentages per phase
- **Time-based Constraints**: Minimum evaluation periods
- **Health Requirements**: Required healthy component checks
- **Success Thresholds**: Minimum performance requirements

### **Monitoring & Alerting**
- **Real-time Health Checks**: Continuous system monitoring
- **Performance Tracking**: Detailed metrics collection
- **Alert Correlation**: Intelligent alert aggregation
- **Historical Analysis**: Trend analysis and reporting

## 📈 **Performance Characteristics**

### **Scalability**
- **Horizontal Scaling**: Support for multiple concurrent deployments
- **Resource Efficiency**: Minimal overhead during normal operation
- **Asynchronous Processing**: Non-blocking health checks and evaluations

### **Reliability**
- **Fault Tolerance**: Graceful handling of component failures
- **Idempotent Operations**: Safe retry mechanisms
- **Data Consistency**: Atomic deployment state management

### **Observability**
- **Comprehensive Metrics**: Detailed performance and health data
- **Structured Logging**: Consistent log format and levels
- **Alert Integration**: Rich alerting with context and metadata

## 🎯 **Best Practices**

### **Deployment Strategy**
1. **Start Small**: Begin with 5% traffic to minimize risk
2. **Monitor Closely**: Use short evaluation intervals initially
3. **Set Clear Thresholds**: Define success criteria upfront
4. **Prepare Rollback Plan**: Always have a rollback strategy

### **Health Check Design**
1. **Keep Checks Simple**: Fast, reliable health indicators
2. **Use Multiple Checks**: Combine different check types
3. **Set Appropriate Timeouts**: Balance speed vs. reliability
4. **Test Check Logic**: Validate checks work as expected

### **Traffic Management**
1. **Use Consistent Routing**: Ensure predictable traffic patterns
2. **Monitor Distribution**: Verify traffic is splitting as expected
3. **Gradual Increases**: Use small traffic increments
4. **Session Awareness**: Consider session-based routing for stateful apps

## 🔍 **Troubleshooting**

### **Common Issues**

**High Error Rates**
- Check canary endpoint health
- Verify configuration differences
- Review traffic routing setup
- Analyze error patterns

**Stalled Deployments**
- Verify health check endpoints
- Check monitoring system status
- Review decision policy logic
- Examine deployment logs

**Traffic Not Routing**
- Validate routing rule configuration
- Check endpoint health status
- Verify traffic manager setup
- Test with direct requests

### **Debugging Commands**

```python
# Get detailed deployment status
status = await orchestrator.get_deployment_status(deployment_name)

# Check traffic distribution
traffic_stats = await traffic_manager.get_traffic_stats(rule_name)

# Get health report
health_report = await health_monitor.get_deployment_health(deployment_name)

# Get component metrics
metrics = await orchestrator.get_deployment_status(deployment_name)
```

## 🚀 **Quick Start**

1. **Configure Endpoints**: Define baseline and canary service endpoints
2. **Set Up Health Checks**: Configure monitoring for key components
3. **Create Deployment**: Define canary configuration and start deployment
4. **Monitor Progress**: Track deployment status and metrics
5. **Review Results**: Analyze performance and make promotion decision

The canary deployment system provides a robust, production-ready solution for safe rollouts of ISO data ingestion pipelines with comprehensive monitoring, intelligent decision-making, and automatic safety mechanisms.
