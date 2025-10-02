"""Plugin Marketplace API Endpoints.

This module provides API endpoints for:
- Plugin discovery and browsing
- Plugin installation and configuration
- Plugin marketplace UI data
- Plugin rating and review system
- Plugin usage analytics
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Body, Path, Depends
from pydantic import BaseModel, Field

from ...telemetry.context import get_request_id, get_tenant_id, log_structured
from ...observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..services.plugin_system_shim import (
    PluginContract,
    PluginInstance,
    PluginSecurityLevel,
    PluginStatus,
    get_plugin_system_service
)
from aurum.services.platform.plugin_system import PluginSystemService
from ..services.base_service import ServiceInterface


class PluginReview(BaseModel):
    """Plugin review and rating."""

    review_id: str
    plugin_name: str
    user_id: str
    rating: int  # 1-5 stars
    review_text: str
    created_at: datetime
    helpful_count: int = 0


class PluginUsageStats(BaseModel):
    """Plugin usage statistics."""

    plugin_name: str
    total_installs: int
    active_instances: int
    average_rating: float
    total_reviews: int
    weekly_downloads: int
    last_updated: datetime


class PluginMarketplaceListing(BaseModel):
    """Plugin marketplace listing."""

    plugin_name: str
    display_name: str
    version: str
    description: str
    author: str
    category: str
    tags: List[str]
    security_level: PluginSecurityLevel
    rating: float
    review_count: int
    install_count: int
    last_updated: datetime
    documentation_url: Optional[str] = None
    source_url: Optional[str] = None
    license: str = "MIT"
    verified: bool = False


class PluginConfigurationTemplate(BaseModel):
    """Template for plugin configuration."""

    template_name: str
    description: str
    configuration: Dict[str, Any]
    use_case: str
    difficulty_level: str = "beginner"  # "beginner", "intermediate", "advanced"


class PluginMarketplaceService(ServiceInterface):
    """Plugin marketplace service."""

    def __init__(self):
        """Initialize plugin marketplace service."""
        self.plugin_service = get_plugin_system_service()
        self.telemetry = get_telemetry_facade()

        # Marketplace data
        self._marketplace_listings: Dict[str, PluginMarketplaceListing] = {}
        self._plugin_reviews: Dict[str, List[PluginReview]] = {}
        self._usage_stats: Dict[str, PluginUsageStats] = {}
        self._config_templates: Dict[str, List[PluginConfigurationTemplate]] = {}

        # Categories and tags
        self._categories = {
            "data_ingestion": "Data collection and ingestion plugins",
            "analytics": "Data analysis and visualization plugins",
            "forecasting": "Forecasting and prediction plugins",
            "risk_management": "Risk assessment and management plugins",
            "integration": "Third-party system integration plugins",
            "utilities": "General utility and helper plugins"
        }

        # Initialize marketplace data
        self._initialize_marketplace_data()

    def _initialize_marketplace_data(self) -> None:
        """Initialize marketplace with sample data."""
        # Sample marketplace listings
        sample_listings = [
            PluginMarketplaceListing(
                plugin_name="weather_feed",
                display_name="Weather Data Integration",
                version="1.0.0",
                description="Real-time weather data integration with energy market impact analysis",
                author="Aurum Team",
                category="data_ingestion",
                tags=["weather", "energy", "forecasting", "api"],
                security_level=PluginSecurityLevel.RESTRICTED,
                rating=4.5,
                review_count=12,
                install_count=156,
                last_updated=datetime.utcnow() - timedelta(days=7),
                documentation_url="https://docs.aurum.dev/plugins/weather-feed",
                source_url="https://github.com/aurum-platform/weather-feed-plugin",
                license="MIT",
                verified=True
            ),
            PluginMarketplaceListing(
                plugin_name="carbon_tracker",
                display_name="Carbon Footprint Tracker",
                version="2.1.0",
                description="Track and analyze carbon emissions across energy portfolios",
                author="Climate Analytics Inc",
                category="analytics",
                tags=["carbon", "emissions", "sustainability", "reporting"],
                security_level=PluginSecurityLevel.RESTRICTED,
                rating=4.2,
                review_count=28,
                install_count=89,
                last_updated=datetime.utcnow() - timedelta(days=3),
                documentation_url="https://docs.climate-analytics.com/carbon-tracker",
                license="Apache 2.0",
                verified=False
            ),
            PluginMarketplaceListing(
                plugin_name="risk_calculator",
                display_name="Advanced Risk Calculator",
                version="3.0.1",
                description="Monte Carlo risk simulation with real-time market data",
                author="RiskTech Solutions",
                category="risk_management",
                tags=["risk", "monte-carlo", "simulation", "portfolio"],
                security_level=PluginSecurityLevel.TRUSTED,
                rating=4.8,
                review_count=45,
                install_count=203,
                last_updated=datetime.utcnow() - timedelta(days=1),
                documentation_url="https://docs.risktech.com/risk-calculator",
                license="Commercial",
                verified=True
            )
        ]

        for listing in sample_listings:
            self._marketplace_listings[listing.plugin_name] = listing

        # Sample usage statistics
        for listing in sample_listings:
            self._usage_stats[listing.plugin_name] = PluginUsageStats(
                plugin_name=listing.plugin_name,
                total_installs=listing.install_count,
                active_instances=listing.install_count // 3,  # Rough estimate
                average_rating=listing.rating,
                total_reviews=listing.review_count,
                weekly_downloads=listing.install_count // 10,
                last_updated=datetime.utcnow()
            )

        # Sample configuration templates
        self._config_templates["weather_feed"] = [
            PluginConfigurationTemplate(
                template_name="Basic Weather Monitoring",
                description="Simple weather data collection for energy trading",
                configuration={
                    "api_key": "your_weatherapi_key",
                    "polling_interval_minutes": 60,
                    "polling_locations": ["New York", "California"]
                },
                use_case="Monitor weather conditions affecting energy demand",
                difficulty_level="beginner"
            ),
            PluginConfigurationTemplate(
                template_name="Advanced Energy Impact Analysis",
                description="Comprehensive weather impact analysis with forecasting",
                configuration={
                    "api_key": "your_weatherapi_key",
                    "forecast_days": 14,
                    "polling_interval_minutes": 30,
                    "polling_locations": ["US", "Europe", "Asia"],
                    "cache_ttl_hours": 2,
                    "include_energy_impact": True
                },
                use_case="Detailed energy market impact assessment",
                difficulty_level="advanced"
            )
        ]

    async def get_plugin_listings(
        self,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
        security_level: Optional[PluginSecurityLevel] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[PluginMarketplaceListing]:
        """Get plugin listings with filtering.

        Args:
            category: Filter by category
            tags: Filter by tags (must have all specified tags)
            security_level: Filter by security level
            limit: Maximum number of results
            offset: Pagination offset

        Returns:
            List of plugin listings
        """
        listings = list(self._marketplace_listings.values())

        # Apply filters
        if category:
            listings = [l for l in listings if l.category == category]

        if tags:
            listings = [
                l for l in listings
                if all(tag in l.tags for tag in tags)
            ]

        if security_level:
            listings = [l for l in listings if l.security_level == security_level]

        # Sort by rating and install count
        listings.sort(
            key=lambda x: (x.rating, x.install_count),
            reverse=True
        )

        # Apply pagination
        return listings[offset:offset + limit]

    async def get_plugin_details(self, plugin_name: str) -> Dict[str, Any]:
        """Get detailed information about a plugin.

        Args:
            plugin_name: Plugin name

        Returns:
            Plugin details including reviews and usage stats
        """
        if plugin_name not in self._marketplace_listings:
            raise HTTPException(status_code=404, detail="Plugin not found")

        listing = self._marketplace_listings[plugin_name]

        # Get reviews
        reviews = self._plugin_reviews.get(plugin_name, [])

        # Get usage stats
        usage_stats = self._usage_stats.get(plugin_name)

        # Get configuration templates
        config_templates = self._config_templates.get(plugin_name, [])

        # Get plugin contract from registry
        plugin_contracts = await self.plugin_service.discover_plugins()
        plugin_contract = next(
            (c for c in plugin_contracts if c.name == plugin_name),
            None
        )

        return {
            "listing": listing.dict(),
            "reviews": [r.dict() for r in reviews],
            "usage_stats": usage_stats.dict() if usage_stats else None,
            "config_templates": [t.dict() for t in config_templates],
            "plugin_contract": plugin_contract.dict() if plugin_contract else None,
            "categories": self._categories,
            "security_levels": [level.value for level in PluginSecurityLevel]
        }

    async def search_plugins(
        self,
        query: str,
        category: Optional[str] = None,
        limit: int = 20
    ) -> List[PluginMarketplaceListing]:
        """Search plugins by name, description, or tags.

        Args:
            query: Search query
            category: Optional category filter
            limit: Maximum results

        Returns:
            Matching plugin listings
        """
        listings = list(self._marketplace_listings.values())
        query_lower = query.lower()

        # Filter by search query
        filtered_listings = []
        for listing in listings:
            if (query_lower in listing.display_name.lower() or
                query_lower in listing.description.lower() or
                any(query_lower in tag.lower() for tag in listing.tags)):
                filtered_listings.append(listing)

        # Apply category filter
        if category:
            filtered_listings = [l for l in filtered_listings if l.category == category]

        # Sort by relevance (rating * install_count)
        filtered_listings.sort(
            key=lambda x: x.rating * x.install_count,
            reverse=True
        )

        return filtered_listings[:limit]

    async def install_plugin(
        self,
        plugin_name: str,
        tenant_id: str,
        configuration: Dict[str, Any],
        config_template: Optional[str] = None
    ) -> Dict[str, Any]:
        """Install and configure a plugin for a tenant.

        Args:
            plugin_name: Plugin to install
            tenant_id: Tenant identifier
            configuration: Plugin configuration
            config_template: Optional configuration template to use

        Returns:
            Installation result
        """
        try:
            # Validate plugin exists
            if plugin_name not in self._marketplace_listings:
                raise HTTPException(status_code=404, detail="Plugin not found")

            # Apply configuration template if specified
            if config_template:
                templates = self._config_templates.get(plugin_name, [])
                template = next(
                    (t for t in templates if t.template_name == config_template),
                    None
                )
                if template:
                    configuration.update(template.configuration)

            # Install plugin using plugin service
            instance_ids = await discover_and_load_plugins(
                tenant_id,
                [plugin_name]
            )

            if not instance_ids:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to install plugin"
                )

            # Update installation statistics
            if plugin_name in self._usage_stats:
                self._usage_stats[plugin_name].total_installs += 1
                self._usage_stats[plugin_name].last_updated = datetime.utcnow()

            self.telemetry.info(
                "Plugin installed",
                plugin_name=plugin_name,
                tenant_id=tenant_id,
                instance_id=instance_ids[0]
            )

            return {
                "status": "success",
                "plugin_name": plugin_name,
                "instance_id": instance_ids[0],
                "configuration": configuration,
                "installed_at": datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.telemetry.error("Plugin installation failed", plugin_name=plugin_name, error=str(e))
            raise HTTPException(status_code=500, detail=f"Installation failed: {str(e)}")

    async def add_plugin_review(
        self,
        plugin_name: str,
        user_id: str,
        rating: int,
        review_text: str
    ) -> PluginReview:
        """Add a review for a plugin.

        Args:
            plugin_name: Plugin name
            user_id: User identifier
            rating: Rating (1-5)
            review_text: Review text

        Returns:
            Created review
        """
        if plugin_name not in self._marketplace_listings:
            raise HTTPException(status_code=404, detail="Plugin not found")

        if not 1 <= rating <= 5:
            raise HTTPException(status_code=400, detail="Rating must be 1-5")

        review = PluginReview(
            review_id=str(uuid4()),
            plugin_name=plugin_name,
            user_id=user_id,
            rating=rating,
            review_text=review_text,
            created_at=datetime.utcnow()
        )

        # Add review to list
        if plugin_name not in self._plugin_reviews:
            self._plugin_reviews[plugin_name] = []

        self._plugin_reviews[plugin_name].append(review)

        # Update average rating
        if plugin_name in self._usage_stats:
            reviews = self._plugin_reviews[plugin_name]
            avg_rating = sum(r.rating for r in reviews) / len(reviews)
            self._usage_stats[plugin_name].average_rating = avg_rating
            self._usage_stats[plugin_name].total_reviews = len(reviews)

        self.telemetry.info(
            "Plugin review added",
            plugin_name=plugin_name,
            rating=rating,
            user_id=user_id
        )

        return review

    async def get_plugin_analytics(self, plugin_name: str) -> Dict[str, Any]:
        """Get analytics data for a plugin.

        Args:
            plugin_name: Plugin name

        Returns:
            Plugin analytics data
        """
        if plugin_name not in self._marketplace_listings:
            raise HTTPException(status_code=404, detail="Plugin not found")

        usage_stats = self._usage_stats.get(plugin_name)
        reviews = self._plugin_reviews.get(plugin_name, [])

        # Calculate rating distribution
        rating_distribution = {i: 0 for i in range(1, 6)}
        for review in reviews:
            rating_distribution[review.rating] += 1

        # Get recent reviews (last 30 days)
        cutoff_date = datetime.utcnow() - timedelta(days=30)
        recent_reviews = [
            r for r in reviews
            if r.created_at > cutoff_date
        ]

        return {
            "plugin_name": plugin_name,
            "usage_stats": usage_stats.dict() if usage_stats else None,
            "total_reviews": len(reviews),
            "recent_reviews": len(recent_reviews),
            "rating_distribution": rating_distribution,
            "review_trend": self._calculate_review_trend(reviews),
            "installation_trend": self._calculate_installation_trend(plugin_name)
        }

    def _calculate_review_trend(self, reviews: List[PluginReview]) -> Dict[str, float]:
        """Calculate review trend over time."""
        if len(reviews) < 2:
            return {"trend": 0.0, "average_change": 0.0}

        # Simple trend calculation - compare recent vs older reviews
        cutoff_date = datetime.utcnow() - timedelta(days=30)
        recent_reviews = [r for r in reviews if r.created_at > cutoff_date]
        older_reviews = [r for r in reviews if r.created_at <= cutoff_date]

        recent_avg = sum(r.rating for r in recent_reviews) / len(recent_reviews) if recent_reviews else 0
        older_avg = sum(r.rating for r in older_reviews) / len(older_reviews) if older_reviews else 0

        trend = recent_avg - older_avg if older_avg > 0 else 0

        return {
            "trend": trend,
            "recent_average": recent_avg,
            "older_average": older_avg,
            "trend_direction": "up" if trend > 0 else "down" if trend < 0 else "stable"
        }

    def _calculate_installation_trend(self, plugin_name: str) -> Dict[str, Any]:
        """Calculate installation trend for a plugin."""
        # Mock trend calculation
        return {
            "weekly_growth": 5.2,  # 5.2% weekly growth
            "monthly_growth": 22.1,  # 22.1% monthly growth
            "trend_direction": "growing",
            "peak_install_period": "Q4 2024"
        }

    async def get_categories(self) -> Dict[str, str]:
        """Get available plugin categories."""
        return self._categories.copy()

    async def get_featured_plugins(self, limit: int = 6) -> List[PluginMarketplaceListing]:
        """Get featured plugins (highest rated and most installed).

        Args:
            limit: Maximum number of featured plugins

        Returns:
            List of featured plugin listings
        """
        listings = list(self._marketplace_listings.values())

        # Sort by combined score (rating * log(install_count))
        def combined_score(listing):
            import math
            return listing.rating * math.log(listing.install_count + 1)

        listings.sort(key=combined_score, reverse=True)

        return listings[:limit]

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "total_plugins": len(self._marketplace_listings),
            "total_reviews": sum(len(reviews) for reviews in self._plugin_reviews.values()),
            "categories_available": len(self._categories),
            "last_updated": datetime.utcnow()
        }


def get_plugin_marketplace_service() -> PluginMarketplaceService:
    """Get the global plugin marketplace service instance."""
    return PluginMarketplaceService()


# API Router
router = APIRouter(prefix="/v2/marketplace", tags=["Plugin Marketplace"])


@router.get("/plugins", response_model=List[PluginMarketplaceListing])
async def get_plugins(
    category: Optional[str] = Query(None, description="Filter by category"),
    tags: Optional[List[str]] = Query(None, description="Filter by tags"),
    security_level: Optional[str] = Query(None, description="Filter by security level"),
    limit: int = Query(50, description="Maximum number of results"),
    offset: int = Query(0, description="Pagination offset"),
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Get plugin listings with optional filtering."""
    return await service.get_plugin_listings(
        category=category,
        tags=tags,
        security_level=PluginSecurityLevel(security_level) if security_level else None,
        limit=limit,
        offset=offset
    )


@router.get("/plugins/search", response_model=List[PluginMarketplaceListing])
async def search_plugins(
    q: str = Query(..., description="Search query"),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(20, description="Maximum results"),
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Search plugins by name, description, or tags."""
    return await service.search_plugins(q, category, limit)


@router.get("/plugins/{plugin_name}", response_model=Dict[str, Any])
async def get_plugin_details(
    plugin_name: str = Path(..., description="Plugin name"),
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Get detailed information about a specific plugin."""
    return await service.get_plugin_details(plugin_name)


@router.post("/plugins/{plugin_name}/install")
async def install_plugin(
    plugin_name: str = Path(..., description="Plugin name"),
    tenant_id: str = Body(..., description="Tenant ID"),
    configuration: Dict[str, Any] = Body(..., description="Plugin configuration"),
    config_template: Optional[str] = Body(None, description="Configuration template name"),
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Install and configure a plugin for a tenant."""
    return await service.install_plugin(
        plugin_name, tenant_id, configuration, config_template
    )


@router.post("/plugins/{plugin_name}/reviews")
async def add_plugin_review(
    plugin_name: str = Path(..., description="Plugin name"),
    user_id: str = Body(..., description="User ID"),
    rating: int = Body(..., ge=1, le=5, description="Rating (1-5)"),
    review_text: str = Body(..., description="Review text"),
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Add a review for a plugin."""
    return await service.add_plugin_review(plugin_name, user_id, rating, review_text)


@router.get("/plugins/{plugin_name}/analytics", response_model=Dict[str, Any])
async def get_plugin_analytics(
    plugin_name: str = Path(..., description="Plugin name"),
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Get analytics data for a plugin."""
    return await service.get_plugin_analytics(plugin_name)


@router.get("/categories", response_model=Dict[str, str])
async def get_categories(
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Get available plugin categories."""
    return await service.get_categories()


@router.get("/featured", response_model=List[PluginMarketplaceListing])
async def get_featured_plugins(
    limit: int = Query(6, description="Maximum number of featured plugins"),
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Get featured plugins (highest rated and most popular)."""
    return await service.get_featured_plugins(limit)


@router.get("/health")
async def get_marketplace_health(
    service: PluginMarketplaceService = Depends(get_plugin_marketplace_service)
):
    """Get plugin marketplace health status."""
    return await service.get_service_health()
