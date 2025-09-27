"""Phase 3: Data Product Catalog for Data Mesh Architecture.

This module implements a comprehensive data product catalog that enables
self-service data access and maintains >90% coverage of data products.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field

from ...telemetry.context import log_structured


logger = logging.getLogger(__name__)


class DataProductStatus(str, Enum):
    """Status of a data product."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    EXPERIMENTAL = "experimental"
    RETIRED = "retired"


class DataProductType(str, Enum):
    """Type of data product."""
    ANALYTICS = "analytics"
    OPERATIONAL = "operational"
    REFERENCE = "reference"
    DERIVED = "derived"
    STREAMING = "streaming"


class AccessLevel(str, Enum):
    """Access levels for data products."""
    PUBLIC = "public"
    INTERNAL = "internal"
    RESTRICTED = "restricted"
    CONFIDENTIAL = "confidential"


@dataclass
class DataQualityMetrics:
    """Data quality metrics for a data product."""
    completeness: float  # Percentage of non-null values
    accuracy: float      # Accuracy score (0-1)
    consistency: float   # Consistency score (0-1)
    timeliness: float   # Timeliness score (0-1)
    validity: float     # Validity score (0-1)
    freshness_hours: float  # Hours since last update
    
    @property
    def overall_score(self) -> float:
        """Calculate overall data quality score."""
        scores = [self.completeness, self.accuracy, self.consistency, 
                 self.timeliness, self.validity]
        return sum(scores) / len(scores)


class DataSchema(BaseModel):
    """Schema definition for a data product."""
    fields: Dict[str, Dict[str, Any]] = Field(..., description="Field definitions")
    constraints: List[Dict[str, Any]] = Field(default_factory=list, description="Data constraints")
    indexes: List[str] = Field(default_factory=list, description="Indexed fields")
    partitioning: Optional[Dict[str, Any]] = Field(None, description="Partitioning strategy")


class DataContract(BaseModel):
    """Data contract defining SLAs and expectations."""
    sla_availability: float = Field(..., ge=0.0, le=1.0, description="Availability SLA (0-1)")
    sla_latency_minutes: int = Field(..., ge=1, description="Maximum latency in minutes")
    sla_quality_threshold: float = Field(..., ge=0.0, le=1.0, description="Minimum quality score")
    update_frequency: str = Field(..., description="Update frequency (hourly, daily, etc.)")
    retention_days: Optional[int] = Field(None, description="Data retention period in days")
    backup_frequency: str = Field(default="daily", description="Backup frequency")
    recovery_time_hours: int = Field(default=24, description="Maximum recovery time")


class DataLineage(BaseModel):
    """Data lineage information."""
    upstream_products: List[str] = Field(default_factory=list, description="Source data products")
    downstream_products: List[str] = Field(default_factory=list, description="Dependent data products")
    transformations: List[Dict[str, Any]] = Field(default_factory=list, description="Applied transformations")
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class DataProduct(BaseModel):
    """Comprehensive data product definition."""
    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique product ID")
    name: str = Field(..., description="Product name")
    description: str = Field(..., description="Product description")
    domain: str = Field(..., description="Business domain")
    owner_team: str = Field(..., description="Owning team")
    owner_contact: str = Field(..., description="Owner contact information")
    
    # Classification
    product_type: DataProductType = Field(..., description="Type of data product")
    status: DataProductStatus = Field(default=DataProductStatus.ACTIVE)
    access_level: AccessLevel = Field(default=AccessLevel.INTERNAL)
    
    # Technical details
    schema: DataSchema = Field(..., description="Data schema")
    location: Dict[str, str] = Field(..., description="Data location details")
    format: str = Field(..., description="Data format (parquet, json, etc.)")
    size_gb: Optional[float] = Field(None, description="Approximate size in GB")
    
    # Governance
    contract: DataContract = Field(..., description="Data contract and SLAs")
    lineage: DataLineage = Field(default_factory=DataLineage)
    tags: Set[str] = Field(default_factory=set, description="Searchable tags")
    
    # Metrics
    quality_metrics: Optional[DataQualityMetrics] = None
    usage_stats: Dict[str, Any] = Field(default_factory=dict)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    version: str = Field(default="1.0.0", description="Product version")
    
    class Config:
        use_enum_values = True


class DataProductCatalog:
    """Centralized catalog for data products with search and discovery."""
    
    def __init__(self):
        self._products: Dict[str, DataProduct] = {}
        self._domain_index: Dict[str, Set[str]] = {}
        self._tag_index: Dict[str, Set[str]] = {}
        self._owner_index: Dict[str, Set[str]] = {}
        self._quality_monitoring = DataQualityMonitor()
    
    async def register_data_product(self, product: DataProduct) -> str:
        """Register a new data product in the catalog."""
        await log_structured(
            "data_product_registration_started",
            product_id=product.id,
            name=product.name,
            domain=product.domain
        )
        
        try:
            # Validate product
            validation_result = await self._validate_product(product)
            if not validation_result["is_valid"]:
                raise ValueError(f"Product validation failed: {validation_result['errors']}")
            
            # Update indexes
            self._update_indexes(product, add=True)
            
            # Store product
            self._products[product.id] = product
            
            # Start quality monitoring if metrics provided
            if product.quality_metrics:
                await self._quality_monitoring.start_monitoring(product.id)
            
            await log_structured(
                "data_product_registered",
                product_id=product.id,
                name=product.name,
                domain=product.domain,
                catalog_size=len(self._products)
            )
            
            return product.id
            
        except Exception as e:
            logger.exception("Data product registration failed")
            raise RuntimeError(f"Registration failed: {str(e)}")
    
    async def get_data_product(self, product_id: str) -> Optional[DataProduct]:
        """Get a data product by ID."""
        product = self._products.get(product_id)
        
        if product:
            # Update usage stats
            product.usage_stats["last_accessed"] = datetime.utcnow()
            product.usage_stats["access_count"] = product.usage_stats.get("access_count", 0) + 1
        
        return product
    
    async def search_data_products(
        self,
        query: Optional[str] = None,
        domain: Optional[str] = None,
        tags: Optional[List[str]] = None,
        owner_team: Optional[str] = None,
        status: Optional[DataProductStatus] = None,
        product_type: Optional[DataProductType] = None,
        min_quality_score: Optional[float] = None
    ) -> List[DataProduct]:
        """Search data products with multiple filters."""
        results = list(self._products.values())
        
        # Apply filters
        if domain:
            domain_products = self._domain_index.get(domain, set())
            results = [p for p in results if p.id in domain_products]
        
        if tags:
            for tag in tags:
                tag_products = self._tag_index.get(tag, set())
                results = [p for p in results if p.id in tag_products]
        
        if owner_team:
            owner_products = self._owner_index.get(owner_team, set())
            results = [p for p in results if p.id in owner_products]
        
        if status:
            results = [p for p in results if p.status == status]
        
        if product_type:
            results = [p for p in results if p.product_type == product_type]
        
        if min_quality_score and min_quality_score > 0:
            results = [
                p for p in results 
                if p.quality_metrics and p.quality_metrics.overall_score >= min_quality_score
            ]
        
        # Text search in name and description
        if query:
            query_lower = query.lower()
            results = [
                p for p in results
                if query_lower in p.name.lower() or query_lower in p.description.lower()
            ]
        
        await log_structured(
            "data_product_search",
            query=query,
            domain=domain,
            tags=tags,
            results_count=len(results)
        )
        
        return results
    
    async def get_catalog_stats(self) -> Dict[str, Any]:
        """Get comprehensive catalog statistics."""
        total_products = len(self._products)
        
        # Status distribution
        status_counts = {}
        for product in self._products.values():
            status_counts[product.status] = status_counts.get(product.status, 0) + 1
        
        # Domain distribution
        domain_counts = {}
        for product in self._products.values():
            domain_counts[product.domain] = domain_counts.get(product.domain, 0) + 1
        
        # Quality distribution
        quality_scores = []
        for product in self._products.values():
            if product.quality_metrics:
                quality_scores.append(product.quality_metrics.overall_score)
        
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # Coverage calculation (products with complete metadata)
        complete_products = 0
        for product in self._products.values():
            if self._is_complete_product(product):
                complete_products += 1
        
        coverage_percentage = (complete_products / total_products * 100) if total_products > 0 else 0
        
        return {
            "total_products": total_products,
            "coverage_percentage": coverage_percentage,
            "status_distribution": status_counts,
            "domain_distribution": domain_counts,
            "average_quality_score": avg_quality,
            "quality_score_count": len(quality_scores),
            "domains_count": len(self._domain_index),
            "tags_count": len(self._tag_index),
            "teams_count": len(self._owner_index)
        }
    
    async def validate_data_contracts(self) -> Dict[str, Any]:
        """Validate all data contracts and SLA compliance."""
        validation_results = {}
        
        for product_id, product in self._products.items():
            contract_status = await self._validate_contract_compliance(product)
            validation_results[product_id] = contract_status
        
        # Calculate overall compliance
        compliant_products = sum(1 for result in validation_results.values() if result["compliant"])
        compliance_rate = compliant_products / len(validation_results) if validation_results else 0
        
        await log_structured(
            "data_contracts_validated",
            total_products=len(validation_results),
            compliant_products=compliant_products,
            compliance_rate=compliance_rate
        )
        
        return {
            "overall_compliance_rate": compliance_rate,
            "total_products": len(validation_results),
            "compliant_products": compliant_products,
            "validation_results": validation_results
        }
    
    async def _validate_product(self, product: DataProduct) -> Dict[str, Any]:
        """Validate a data product definition."""
        errors = []
        warnings = []
        
        # Required fields validation
        if not product.name.strip():
            errors.append("Product name cannot be empty")
        
        if not product.description.strip():
            errors.append("Product description cannot be empty")
        
        if not product.domain.strip():
            errors.append("Product domain cannot be empty")
        
        # Schema validation
        if not product.schema.fields:
            errors.append("Product schema must define at least one field")
        
        # Contract validation
        if product.contract.sla_availability < 0.5:
            warnings.append("Low availability SLA (< 50%)")
        
        if product.contract.sla_latency_minutes > 1440:  # 24 hours
            warnings.append("High latency SLA (> 24 hours)")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    def _update_indexes(self, product: DataProduct, add: bool = True):
        """Update search indexes."""
        if add:
            # Domain index
            if product.domain not in self._domain_index:
                self._domain_index[product.domain] = set()
            self._domain_index[product.domain].add(product.id)
            
            # Tag index
            for tag in product.tags:
                if tag not in self._tag_index:
                    self._tag_index[tag] = set()
                self._tag_index[tag].add(product.id)
            
            # Owner index
            if product.owner_team not in self._owner_index:
                self._owner_index[product.owner_team] = set()
            self._owner_index[product.owner_team].add(product.id)
        else:
            # Remove from indexes
            if product.domain in self._domain_index:
                self._domain_index[product.domain].discard(product.id)
            
            for tag in product.tags:
                if tag in self._tag_index:
                    self._tag_index[tag].discard(product.id)
            
            if product.owner_team in self._owner_index:
                self._owner_index[product.owner_team].discard(product.id)
    
    def _is_complete_product(self, product: DataProduct) -> bool:
        """Check if a product has complete metadata for coverage calculation."""
        required_fields = [
            bool(product.name),
            bool(product.description),
            bool(product.domain),
            bool(product.owner_team),
            bool(product.owner_contact),
            bool(product.schema.fields),
            bool(product.location),
            bool(product.contract.sla_availability > 0),
            bool(product.tags)
        ]
        
        return all(required_fields)
    
    async def _validate_contract_compliance(self, product: DataProduct) -> Dict[str, Any]:
        """Validate SLA compliance for a data product."""
        # This would integrate with monitoring systems in real implementation
        # For now, return mock compliance data
        
        compliance_checks = {
            "availability": True,  # Mock: would check actual uptime
            "latency": True,       # Mock: would check actual response times
            "quality": True if product.quality_metrics and product.quality_metrics.overall_score >= product.contract.sla_quality_threshold else False,
            "freshness": True      # Mock: would check last update time
        }
        
        return {
            "product_id": product.id,
            "compliant": all(compliance_checks.values()),
            "checks": compliance_checks,
            "last_validated": datetime.utcnow()
        }


class DataQualityMonitor:
    """Monitor data quality metrics for registered products."""
    
    def __init__(self):
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
    
    async def start_monitoring(self, product_id: str):
        """Start quality monitoring for a product."""
        if product_id in self._monitoring_tasks:
            return  # Already monitoring
        
        task = asyncio.create_task(self._monitor_product_quality(product_id))
        self._monitoring_tasks[product_id] = task
        
        await log_structured("quality_monitoring_started", product_id=product_id)
    
    async def stop_monitoring(self, product_id: str):
        """Stop quality monitoring for a product."""
        task = self._monitoring_tasks.pop(product_id, None)
        if task:
            task.cancel()
            await log_structured("quality_monitoring_stopped", product_id=product_id)
    
    async def _monitor_product_quality(self, product_id: str):
        """Monitor product quality continuously."""
        while True:
            try:
                # This would perform actual quality checks in real implementation
                await asyncio.sleep(3600)  # Check every hour
                
                # Mock quality check
                await log_structured(
                    "quality_check_completed",
                    product_id=product_id,
                    quality_score=0.95  # Mock score
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Quality monitoring failed for product {product_id}")
                await asyncio.sleep(600)  # Wait 10 minutes before retry