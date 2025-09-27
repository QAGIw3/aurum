"""Multi-region deployment and global scale capabilities for Phase 4."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class Region(str, Enum):
    """Supported deployment regions."""
    US_EAST_1 = "us-east-1"
    US_WEST_2 = "us-west-2"
    EU_WEST_1 = "eu-west-1"
    EU_CENTRAL_1 = "eu-central-1"
    ASIA_PACIFIC_1 = "ap-southeast-1"
    ASIA_PACIFIC_2 = "ap-northeast-1"


class ServiceHealth(str, Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class FailoverTrigger(str, Enum):
    """Failover trigger conditions."""
    MANUAL = "manual"
    HEALTH_CHECK_FAILURE = "health_check_failure"
    LATENCY_THRESHOLD = "latency_threshold"
    ERROR_RATE_THRESHOLD = "error_rate_threshold"
    RESOURCE_EXHAUSTION = "resource_exhaustion"


@dataclass
class RegionConfig:
    """Regional deployment configuration."""
    
    region: Region
    primary: bool = False
    endpoints: Dict[str, str] = field(default_factory=dict)
    database_endpoints: Dict[str, str] = field(default_factory=dict)
    cache_endpoints: Dict[str, str] = field(default_factory=dict)
    cdn_distribution: Optional[str] = None
    auto_scaling_config: Dict[str, Any] = field(default_factory=dict)
    data_residency_compliant: bool = True
    backup_region: Optional[Region] = None
    
    def __post_init__(self):
        if not self.auto_scaling_config:
            self.auto_scaling_config = {
                "min_instances": 2,
                "max_instances": 20,
                "target_cpu_utilization": 70,
                "target_memory_utilization": 80,
                "scale_up_cooldown": 300,  # 5 minutes
                "scale_down_cooldown": 600  # 10 minutes
            }


@dataclass
class HealthCheck:
    """Health check configuration and result."""
    
    endpoint: str
    region: Region
    service: str
    check_type: str = "http"  # http, tcp, custom
    interval_seconds: int = 30
    timeout_seconds: int = 10
    failure_threshold: int = 3
    success_threshold: int = 2
    last_check: Optional[datetime] = None
    status: ServiceHealth = ServiceHealth.UNKNOWN
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    response_time_ms: Optional[float] = None
    error_message: Optional[str] = None


@dataclass
class FailoverRecord:
    """Record of a failover event."""
    
    failover_id: str
    triggered_at: datetime
    trigger: FailoverTrigger
    source_region: Region
    target_region: Region
    services_affected: List[str]
    estimated_rto_minutes: int
    actual_rto_minutes: Optional[int] = None
    completed_at: Optional[datetime] = None
    rollback_at: Optional[datetime] = None
    success: bool = False
    details: Dict[str, Any] = field(default_factory=dict)


class MultiRegionManager:
    """Multi-region deployment and failover manager."""

    def __init__(self):
        self.regions: Dict[Region, RegionConfig] = {}
        self.health_checks: List[HealthCheck] = []
        self.failover_history: List[FailoverRecord] = []
        self.active_failovers: Dict[str, FailoverRecord] = {}
        self.traffic_routing: Dict[str, Dict[Region, float]] = {}  # service -> region -> weight
        self._initialize_default_regions()

    def _initialize_default_regions(self) -> None:
        """Initialize default regional configurations."""
        # Primary region (US East)
        us_east = RegionConfig(
            region=Region.US_EAST_1,
            primary=True,
            endpoints={
                "api": "https://api-us-east-1.aurum.example.com",
                "websocket": "wss://ws-us-east-1.aurum.example.com"
            },
            database_endpoints={
                "primary": "postgres-us-east-1.aurum.example.com:5432",
                "read_replica": "postgres-ro-us-east-1.aurum.example.com:5432"
            },
            cache_endpoints={
                "redis": "redis-us-east-1.aurum.example.com:6379"
            },
            cdn_distribution="E1234567890ABC",
            backup_region=Region.US_WEST_2
        )
        
        # Secondary regions
        us_west = RegionConfig(
            region=Region.US_WEST_2,
            endpoints={
                "api": "https://api-us-west-2.aurum.example.com",
                "websocket": "wss://ws-us-west-2.aurum.example.com"
            },
            database_endpoints={
                "primary": "postgres-us-west-2.aurum.example.com:5432",
                "read_replica": "postgres-ro-us-west-2.aurum.example.com:5432"
            },
            cache_endpoints={
                "redis": "redis-us-west-2.aurum.example.com:6379"
            },
            backup_region=Region.EU_WEST_1
        )
        
        eu_west = RegionConfig(
            region=Region.EU_WEST_1,
            endpoints={
                "api": "https://api-eu-west-1.aurum.example.com",
                "websocket": "wss://ws-eu-west-1.aurum.example.com"
            },
            database_endpoints={
                "primary": "postgres-eu-west-1.aurum.example.com:5432",
                "read_replica": "postgres-ro-eu-west-1.aurum.example.com:5432"
            },
            cache_endpoints={
                "redis": "redis-eu-west-1.aurum.example.com:6379"
            },
            data_residency_compliant=True  # GDPR compliant
        )
        
        self.regions[Region.US_EAST_1] = us_east
        self.regions[Region.US_WEST_2] = us_west
        self.regions[Region.EU_WEST_1] = eu_west
        
        # Initialize traffic routing (100% to primary initially)
        self.traffic_routing = {
            "api": {Region.US_EAST_1: 1.0, Region.US_WEST_2: 0.0, Region.EU_WEST_1: 0.0},
            "websocket": {Region.US_EAST_1: 1.0, Region.US_WEST_2: 0.0, Region.EU_WEST_1: 0.0}
        }

    def register_region(self, config: RegionConfig) -> None:
        """Register a new region configuration."""
        self.regions[config.region] = config
        logger.info(f"Registered region: {config.region} (primary: {config.primary})")

    def add_health_check(self, health_check: HealthCheck) -> None:
        """Add a health check for monitoring."""
        self.health_checks.append(health_check)
        logger.info(f"Added health check: {health_check.service} in {health_check.region}")

    async def perform_health_checks(self) -> Dict[Region, Dict[str, ServiceHealth]]:
        """Perform health checks across all regions."""
        results = {}
        
        for health_check in self.health_checks:
            region = health_check.region
            if region not in results:
                results[region] = {}
                
            # Simulate health check (in real implementation, make actual HTTP/TCP calls)
            try:
                # Mock health check logic
                import random
                if random.random() > 0.95:  # 5% chance of failure for testing
                    health_check.status = ServiceHealth.UNHEALTHY
                    health_check.consecutive_failures += 1
                    health_check.consecutive_successes = 0
                    health_check.error_message = "Service unavailable"
                else:
                    health_check.status = ServiceHealth.HEALTHY
                    health_check.consecutive_successes += 1
                    health_check.consecutive_failures = 0
                    health_check.response_time_ms = random.uniform(50, 200)
                    health_check.error_message = None
                
                health_check.last_check = datetime.now()
                results[region][health_check.service] = health_check.status
                
            except Exception as e:
                health_check.status = ServiceHealth.UNHEALTHY
                health_check.consecutive_failures += 1
                health_check.error_message = str(e)
                results[region][health_check.service] = ServiceHealth.UNHEALTHY
                logger.error(f"Health check failed for {health_check.service} in {region}: {e}")
        
        return results

    def should_trigger_failover(self, health_results: Dict[Region, Dict[str, ServiceHealth]]) -> Optional[FailoverTrigger]:
        """Determine if failover should be triggered based on health check results."""
        primary_region = self.get_primary_region()
        if not primary_region:
            return None
        
        primary_health = health_results.get(primary_region, {})
        
        # Check if primary region services are unhealthy
        unhealthy_services = [
            service for service, status in primary_health.items()
            if status == ServiceHealth.UNHEALTHY
        ]
        
        if len(unhealthy_services) > 0:
            # Check if failures persist (using health check consecutive failures)
            critical_failures = []
            for health_check in self.health_checks:
                if (health_check.region == primary_region and 
                    health_check.service in unhealthy_services and
                    health_check.consecutive_failures >= health_check.failure_threshold):
                    critical_failures.append(health_check.service)
            
            if critical_failures:
                logger.warning(f"Critical failures detected in primary region: {critical_failures}")
                return FailoverTrigger.HEALTH_CHECK_FAILURE
        
        return None

    def get_primary_region(self) -> Optional[Region]:
        """Get the primary region."""
        for region, config in self.regions.items():
            if config.primary:
                return region
        return None

    def get_backup_region(self, source_region: Region) -> Optional[Region]:
        """Get the backup region for a given source region."""
        config = self.regions.get(source_region)
        return config.backup_region if config else None

    async def execute_failover(self, trigger: FailoverTrigger, source_region: Region,
                             target_region: Optional[Region] = None) -> FailoverRecord:
        """Execute failover from source to target region."""
        if not target_region:
            target_region = self.get_backup_region(source_region)
            if not target_region:
                raise ValueError(f"No backup region configured for {source_region}")
        
        failover_id = f"failover_{int(datetime.now().timestamp())}"
        
        failover_record = FailoverRecord(
            failover_id=failover_id,
            triggered_at=datetime.now(),
            trigger=trigger,
            source_region=source_region,
            target_region=target_region,
            services_affected=list(self.traffic_routing.keys()),
            estimated_rto_minutes=15  # Recovery Time Objective
        )
        
        self.active_failovers[failover_id] = failover_record
        logger.info(f"Starting failover {failover_id}: {source_region} -> {target_region}")
        
        try:
            # Step 1: Update traffic routing
            await self._update_traffic_routing(source_region, target_region)
            
            # Step 2: Update DNS/load balancer configuration
            await self._update_dns_configuration(source_region, target_region)
            
            # Step 3: Ensure target region readiness
            await self._ensure_target_region_readiness(target_region)
            
            # Step 4: Update primary region designation
            self._update_primary_region(target_region)
            
            failover_record.completed_at = datetime.now()
            failover_record.success = True
            
            # Calculate actual RTO
            rto_delta = failover_record.completed_at - failover_record.triggered_at
            failover_record.actual_rto_minutes = int(rto_delta.total_seconds() / 60)
            
            logger.info(f"Failover {failover_id} completed successfully in {failover_record.actual_rto_minutes} minutes")
            
        except Exception as e:
            failover_record.success = False
            failover_record.details["error"] = str(e)
            logger.error(f"Failover {failover_id} failed: {e}")
            raise
        
        finally:
            self.failover_history.append(failover_record)
            del self.active_failovers[failover_id]
        
        return failover_record

    async def _update_traffic_routing(self, source_region: Region, target_region: Region) -> None:
        """Update traffic routing to direct traffic from source to target region."""
        for service in self.traffic_routing:
            # Gradually shift traffic to avoid sudden load
            self.traffic_routing[service][source_region] = 0.0
            self.traffic_routing[service][target_region] = 1.0
            
        logger.info(f"Updated traffic routing: {source_region} -> {target_region}")

    async def _update_dns_configuration(self, source_region: Region, target_region: Region) -> None:
        """Update DNS configuration for failover."""
        # In real implementation, this would update Route 53 or similar DNS service
        await asyncio.sleep(1)  # Simulate DNS propagation delay
        logger.info(f"Updated DNS configuration for failover: {source_region} -> {target_region}")

    async def _ensure_target_region_readiness(self, target_region: Region) -> None:
        """Ensure target region is ready to handle traffic."""
        config = self.regions.get(target_region)
        if not config:
            raise ValueError(f"Target region {target_region} not configured")
        
        # Check if auto-scaling should be triggered
        await self._trigger_auto_scaling(target_region)
        
        # Warm up caches
        await self._warm_up_caches(target_region)
        
        logger.info(f"Target region {target_region} is ready")

    async def _trigger_auto_scaling(self, region: Region) -> None:
        """Trigger auto-scaling in target region."""
        config = self.regions.get(region)
        if not config:
            return
        
        scaling_config = config.auto_scaling_config
        # In real implementation, this would trigger Kubernetes HPA or cloud auto-scaling
        logger.info(f"Triggered auto-scaling in {region}: min={scaling_config['min_instances']}, max={scaling_config['max_instances']}")

    async def _warm_up_caches(self, region: Region) -> None:
        """Warm up caches in target region."""
        # In real implementation, this would pre-populate Redis/CDN caches
        await asyncio.sleep(1)  # Simulate cache warming
        logger.info(f"Warmed up caches in {region}")

    def _update_primary_region(self, new_primary: Region) -> None:
        """Update primary region designation."""
        # Remove primary designation from all regions
        for config in self.regions.values():
            config.primary = False
        
        # Set new primary
        if new_primary in self.regions:
            self.regions[new_primary].primary = True
            logger.info(f"Updated primary region to: {new_primary}")

    def get_optimal_region_for_user(self, user_location: str, 
                                  data_residency_requirements: Optional[List[str]] = None) -> Region:
        """Get optimal region for user based on location and compliance requirements."""
        # Simple mapping based on location
        location_mapping = {
            "us": Region.US_EAST_1,
            "canada": Region.US_EAST_1,
            "europe": Region.EU_WEST_1,
            "asia": Region.ASIA_PACIFIC_1,
            "australia": Region.ASIA_PACIFIC_1
        }
        
        preferred_region = location_mapping.get(user_location.lower(), Region.US_EAST_1)
        
        # Check data residency requirements
        if data_residency_requirements:
            if "gdpr" in data_residency_requirements and preferred_region not in [Region.EU_WEST_1, Region.EU_CENTRAL_1]:
                preferred_region = Region.EU_WEST_1
        
        # Ensure region is available and healthy
        if preferred_region in self.regions:
            # Check recent health status
            recent_failures = [
                hc for hc in self.health_checks
                if hc.region == preferred_region and hc.consecutive_failures > 0
            ]
            
            if not recent_failures:
                return preferred_region
        
        # Fallback to primary region
        primary = self.get_primary_region()
        return primary if primary else Region.US_EAST_1

    def get_disaster_recovery_metrics(self) -> Dict[str, Any]:
        """Get disaster recovery metrics and status."""
        total_failovers = len(self.failover_history)
        successful_failovers = len([f for f in self.failover_history if f.success])
        
        # Calculate average RTO
        completed_failovers = [f for f in self.failover_history if f.actual_rto_minutes is not None]
        avg_rto = sum(f.actual_rto_minutes for f in completed_failovers) / len(completed_failovers) if completed_failovers else 0
        
        # Get current health status
        current_health = {}
        for region in self.regions.keys():
            region_health = [
                hc for hc in self.health_checks
                if hc.region == region
            ]
            healthy_services = len([hc for hc in region_health if hc.status == ServiceHealth.HEALTHY])
            total_services = len(region_health)
            current_health[region.value] = {
                "healthy_services": healthy_services,
                "total_services": total_services,
                "health_percentage": (healthy_services / total_services * 100) if total_services > 0 else 0
            }
        
        return {
            "total_failovers": total_failovers,
            "successful_failovers": successful_failovers,
            "failover_success_rate": (successful_failovers / total_failovers * 100) if total_failovers > 0 else 100,
            "average_rto_minutes": round(avg_rto, 2),
            "rto_target_minutes": 60,  # < 1 hour target
            "rpo_target_minutes": 15,  # < 15 minutes target
            "active_failovers": len(self.active_failovers),
            "regional_health": current_health,
            "primary_region": self.get_primary_region().value if self.get_primary_region() else None
        }


# Global multi-region manager instance
multi_region_manager = MultiRegionManager()