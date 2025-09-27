"""Advanced Policy Tagging and Classification Service.

This service provides:
- Advanced NLP-based policy classification and tagging
- Multi-dimensional policy impact assessment
- Regulatory taxonomy and ontology management
- Policy relationship mapping and dependency analysis
- Automated policy categorization and prioritization
- Integration with regulatory databases and standards
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4
from enum import Enum

from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from ..daos.base_dao import TrinoDAO


class PolicyCategory(str, Enum):
    """Policy categories for classification."""
    ENVIRONMENTAL = "environmental"
    ECONOMIC = "economic"
    SOCIAL = "social"
    TECHNICAL = "technical"
    OPERATIONAL = "operational"
    COMPLIANCE = "compliance"
    RISK_MANAGEMENT = "risk_management"
    MARKET_STRUCTURE = "market_structure"


class PolicySubcategory(str, Enum):
    """Policy subcategories for detailed classification."""
    # Environmental
    CARBON_PRICING = "carbon_pricing"
    RENEWABLE_ENERGY = "renewable_energy"
    EMISSIONS_TRADING = "emissions_trading"
    CLIMATE_RISK = "climate_risk"

    # Economic
    TARIFF_STRUCTURE = "tariff_structure"
    MARKET_DESIGN = "market_design"
    PRICING_MECHANISMS = "pricing_mechanisms"
    COST_RECOVERY = "cost_recovery"

    # Technical
    GRID_RELIABILITY = "grid_reliability"
    INTERCONNECTION = "interconnection"
    RESOURCE_ADEQUACY = "resource_adequacy"
    OPERATIONAL_STANDARDS = "operational_standards"

    # Compliance
    REPORTING_REQUIREMENTS = "reporting_requirements"
    AUDIT_STANDARDS = "audit_standards"
    PENALTY_FRAMEWORKS = "penalty_frameworks"
    ENFORCEMENT_MECHANISMS = "enforcement_mechanisms"


class PolicyImpactDimension(str, Enum):
    """Dimensions of policy impact."""
    MARKET_EFFICIENCY = "market_efficiency"
    PRICE_VOLATILITY = "price_volatility"
    INVESTMENT_DECISIONS = "investment_decisions"
    OPERATIONAL_RISK = "operational_risk"
    COMPLIANCE_COST = "compliance_cost"
    MARKET_LIQUIDITY = "market_liquidity"
    RESOURCE_PLANNING = "resource_planning"
    GRID_RELIABILITY = "grid_reliability"


class PolicyRelationship(str, Enum):
    """Types of policy relationships."""
    SUPERSEDES = "supersedes"
    AMENDS = "amends"
    REFERENCES = "references"
    CONFLICTS_WITH = "conflicts_with"
    COMPLEMENTS = "complements"
    DEPENDS_ON = "depends_on"


class PolicyClassification(BaseModel):
    """Advanced policy classification result."""

    artifact_id: str
    primary_category: PolicyCategory
    subcategories: List[PolicySubcategory]
    impact_dimensions: List[PolicyImpactDimension]
    affected_stakeholders: List[str]
    geographic_scope: List[str]
    temporal_scope: str  # "immediate", "short_term", "medium_term", "long_term"
    implementation_complexity: str  # "low", "medium", "high"
    enforcement_mechanism: str  # "mandatory", "voluntary", "incentive_based"
    related_policies: List[str]  # IDs of related policies
    relationships: Dict[str, PolicyRelationship]  # policy_id -> relationship_type
    confidence_score: float  # 0.0 to 1.0
    classification_method: str = "enhanced_nlp"


class PolicyTaxonomy(BaseModel):
    """Policy taxonomy definition."""

    taxonomy_id: str
    name: str
    description: str
    categories: Dict[str, Dict[str, Any]]  # category -> metadata
    relationships: Dict[str, List[str]]  # policy_id -> related_policy_ids
    version: str
    effective_date: datetime
    deprecated: bool = False


class PolicyTaggingService:
    """Advanced Policy Tagging and Classification Service."""

    def __init__(self):
        """Initialize policy tagging service."""
        self.dao = TrinoDAO()
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # Policy classification data
        self._policy_taxonomy: Optional[PolicyTaxonomy] = None
        self._classification_cache: Dict[str, PolicyClassification] = {}
        self._policy_relationships: Dict[str, Dict[str, PolicyRelationship]] = {}

        # Enhanced classification patterns
        self._category_patterns = self._initialize_category_patterns()
        self._subcategory_patterns = self._initialize_subcategory_patterns()
        self._impact_dimension_patterns = self._initialize_impact_dimension_patterns()
        self._stakeholder_patterns = self._initialize_stakeholder_patterns()

        # Machine learning components (simplified)
        self._classification_models = {}
        self._training_data = []

        # Real-time classification
        self._classification_queue: asyncio.Queue = asyncio.Queue()
        self._classification_worker: Optional[asyncio.Task] = None

        self.logger = logging.getLogger(__name__)

        # Initialize taxonomy
        self._initialize_policy_taxonomy()

    def _initialize_category_patterns(self) -> Dict[PolicyCategory, List[str]]:
        """Initialize category classification patterns."""
        return {
            PolicyCategory.ENVIRONMENTAL: [
                r"\benvironmental\b", r"\bclimate\b", r"\bemission\b", r"\bcarbon\b",
                r"\bpollution\b", r"\bgreenhouse\b", r"\bsustainability\b", r"\bconservation\b"
            ],
            PolicyCategory.ECONOMIC: [
                r"\beconomic\b", r"\bfinancial\b", r"\bcost\b", r"\bpricing\b",
                r"\bmarket\b", r"\btariff\b", r"\brate\b", r"\bfee\b"
            ],
            PolicyCategory.SOCIAL: [
                r"\bsocial\b", r"\bcommunity\b", r"\bpublic\b", r"\bwelfare\b",
                r"\bhealth\b", r"\bsafety\b", r"\bequity\b", r"\bjustice\b"
            ],
            PolicyCategory.TECHNICAL: [
                r"\btechnical\b", r"\bengineering\b", r"\bstandard\b", r"\bspecification\b",
                r"\bprotocol\b", r"\bmethodology\b", r"\bprocedure\b"
            ],
            PolicyCategory.OPERATIONAL: [
                r"\boperational\b", r"\bprocedure\b", r"\bprocess\b", r"\bworkflow\b",
                r"\bimplementation\b", r"\bexecution\b", r"\bmanagement\b"
            ],
            PolicyCategory.COMPLIANCE: [
                r"\bcompliance\b", r"\bregulation\b", r"\brule\b", r"\blaw\b",
                r"\brequirement\b", r"\bmandatory\b", r"\benforcement\b"
            ],
            PolicyCategory.RISK_MANAGEMENT: [
                r"\brisk\b", r"\bhazard\b", r"\bthreat\b", r"\bvulnerability\b",
                r"\bmitigation\b", r"\bassessment\b", r"\bmanagement\b"
            ],
            PolicyCategory.MARKET_STRUCTURE: [
                r"\bmarket\s+structure\b", r"\bcompetition\b", r"\bmonopoly\b", r"\boligopoly\b",
                r"\bmarket\s+design\b", r"\btrading\b", r"\bexchange\b"
            ]
        }

    def _initialize_subcategory_patterns(self) -> Dict[PolicySubcategory, List[str]]:
        """Initialize subcategory classification patterns."""
        return {
            PolicySubcategory.CARBON_PRICING: [
                r"\bcarbon\s+pricing\b", r"\bcarbon\s+tax\b", r"\bETS\b", r"\bemissions\s+trading\b"
            ],
            PolicySubcategory.RENEWABLE_ENERGY: [
                r"\brenewable\s+energy\b", r"\bsolar\b", r"\bwind\b", r"\bhydro\b", r"\bRPS\b"
            ],
            PolicySubcategory.TARIFF_STRUCTURE: [
                r"\btariff\b", r"\brate\s+structure\b", r"\bpricing\s+structure\b"
            ],
            PolicySubcategory.RESOURCE_ADEQUACY: [
                r"\bresource\s+adequacy\b", r"\bcapacity\b", r"\bgeneration\b", r"\bplanning\b"
            ]
        }

    def _initialize_impact_dimension_patterns(self) -> Dict[PolicyImpactDimension, List[str]]:
        """Initialize impact dimension patterns."""
        return {
            PolicyImpactDimension.MARKET_EFFICIENCY: [
                r"\bmarket\s+efficiency\b", r"\bcompetition\b", r"\btransparency\b", r"\bliquidity\b"
            ],
            PolicyImpactDimension.PRICE_VOLATILITY: [
                r"\bprice\s+volatility\b", r"\bprice\s+stability\b", r"\bprice\s+risk\b"
            ],
            PolicyImpactDimension.INVESTMENT_DECISIONS: [
                r"\binvestment\b", r"\bfinancing\b", r"\bcapital\b", r"\bfunding\b"
            ]
        }

    def _initialize_stakeholder_patterns(self) -> Dict[str, List[str]]:
        """Initialize stakeholder identification patterns."""
        return {
            "utilities": [r"\butility\b", r"\belectric\s+company\b", r"\bpower\s+company\b"],
            "generators": [r"\bgenerator\b", r"\bpower\s+plant\b", r"\bproducer\b"],
            "consumers": [r"\bconsumer\b", r"\bcustomer\b", r"\bend\s+user\b"],
            "regulators": [r"\bcommission\b", r"\bagency\b", r"\bFERC\b", r"\bCFTC\b", r"\bSEC\b"],
            "investors": [r"\binvestor\b", r"\bfinancial\b", r"\bbank\b", r"\bfund\b"],
            "environmental_groups": [r"\benvironmental\b", r"\bclimate\b", r"\bgreen\b", r"\beco\b"]
        }

    def _initialize_policy_taxonomy(self) -> None:
        """Initialize policy taxonomy."""
        try:
            # Load taxonomy from configuration or create default
            self._policy_taxonomy = PolicyTaxonomy(
                taxonomy_id="energy_policy_v1.0",
                name="Energy Policy Taxonomy",
                description="Comprehensive taxonomy for energy policy classification",
                categories={
                    "environmental": {
                        "description": "Environmental protection and sustainability policies",
                        "subcategories": ["carbon_pricing", "renewable_energy", "emissions_trading"]
                    },
                    "economic": {
                        "description": "Economic and market-related policies",
                        "subcategories": ["tariff_structure", "market_design", "pricing_mechanisms"]
                    }
                },
                relationships={},
                version="1.0",
                effective_date=datetime.utcnow()
            )

            self.logger.info("Policy taxonomy initialized", taxonomy_id=self._policy_taxonomy.taxonomy_id)

        except Exception as e:
            self.logger.error("Failed to initialize policy taxonomy", error=str(e))
            self._policy_taxonomy = None

    async def classify_policy(self, artifact_id: str, text: str) -> PolicyClassification:
        """Classify a policy document using advanced NLP techniques.

        Args:
            artifact_id: Unique identifier for the policy artifact
            text: Full text of the policy document

        Returns:
            Comprehensive policy classification
        """
        try:
            # Check cache first
            cache_key = f"policy_classification:{artifact_id}"
            cached = await self.cache_manager.get(cache_key)
            if cached:
                return PolicyClassification(**cached)

            # Perform multi-dimensional classification
            primary_category = self._classify_primary_category(text)
            subcategories = self._classify_subcategories(text)
            impact_dimensions = self._classify_impact_dimensions(text)
            affected_stakeholders = self._identify_stakeholders(text)
            geographic_scope = self._determine_geographic_scope(text)
            temporal_scope = self._determine_temporal_scope(text)
            implementation_complexity = self._assess_implementation_complexity(text)
            enforcement_mechanism = self._identify_enforcement_mechanism(text)

            # Find related policies
            related_policies, relationships = await self._find_related_policies(artifact_id, text)

            # Calculate confidence score
            confidence_score = self._calculate_classification_confidence(
                text, primary_category, subcategories, impact_dimensions
            )

            # Create classification result
            classification = PolicyClassification(
                artifact_id=artifact_id,
                primary_category=primary_category,
                subcategories=subcategories,
                impact_dimensions=impact_dimensions,
                affected_stakeholders=affected_stakeholders,
                geographic_scope=geographic_scope,
                temporal_scope=temporal_scope,
                implementation_complexity=implementation_complexity,
                enforcement_mechanism=enforcement_mechanism,
                related_policies=related_policies,
                relationships=relationships,
                confidence_score=confidence_score,
                classification_method="enhanced_nlp"
            )

            # Cache result
            await self.cache_manager.set(
                cache_key,
                classification.dict(),
                ttl_seconds=86400  # 24 hour cache
            )

            # Store in database
            await self._store_classification(classification)

            self.telemetry.info(
                "Policy classification completed",
                artifact_id=artifact_id,
                primary_category=primary_category.value,
                confidence=confidence_score
            )

            return classification

        except Exception as e:
            self.telemetry.error("Policy classification failed", artifact_id=artifact_id, error=str(e))
            raise

    def _classify_primary_category(self, text: str) -> PolicyCategory:
        """Classify the primary category of a policy."""
        text_lower = text.lower()
        category_scores = {}

        for category, patterns in self._category_patterns.items():
            score = 0
            for pattern in patterns:
                matches = len(re.findall(pattern, text_lower))
                score += matches

            # Normalize by text length
            score = score / max(len(text.split()), 1)
            category_scores[category] = score

        # Return category with highest score
        best_category = max(category_scores.items(), key=lambda x: x[1])
        return best_category[0]

    def _classify_subcategories(self, text: str) -> List[PolicySubcategory]:
        """Classify policy into subcategories."""
        text_lower = text.lower()
        subcategory_matches = []

        for subcategory, patterns in self._subcategory_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text_lower):
                    subcategory_matches.append(subcategory)
                    break  # Only add once per subcategory

        return list(set(subcategory_matches))

    def _classify_impact_dimensions(self, text: str) -> List[PolicyImpactDimension]:
        """Classify the impact dimensions of a policy."""
        text_lower = text.lower()
        impact_matches = []

        for dimension, patterns in self._impact_dimension_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text_lower):
                    impact_matches.append(dimension)
                    break

        return list(set(impact_matches))

    def _identify_stakeholders(self, text: str) -> List[str]:
        """Identify affected stakeholders in the policy."""
        text_lower = text.lower()
        identified_stakeholders = []

        for stakeholder, patterns in self._stakeholder_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text_lower):
                    identified_stakeholders.append(stakeholder)
                    break

        return list(set(identified_stakeholders))

    def _determine_geographic_scope(self, text: str) -> List[str]:
        """Determine the geographic scope of the policy."""
        text_lower = text.lower()
        scope_indicators = []

        # State-level indicators
        state_patterns = [
            r"\b(state|states)\b", r"\bcalifornia\b", r"\btexas\b", r"\bnew\s+york\b",
            r"\bflorida\b", r"\bpennsylvania\b", r"\billinois\b"
        ]

        # Regional indicators
        regional_patterns = [
            r"\bregional\b", r"\bRTO\b", r"\bISO\b", r"\bPJM\b", r"\bERCOT\b", r"\bMISO\b"
        ]

        # Federal indicators
        federal_patterns = [
            r"\bfederal\b", r"\bnational\b", r"\bFERC\b", r"\bCFTC\b", r"\bDOE\b", r"\bEPA\b"
        ]

        if any(re.search(pattern, text_lower) for pattern in state_patterns):
            scope_indicators.append("state")

        if any(re.search(pattern, text_lower) for pattern in regional_patterns):
            scope_indicators.append("regional")

        if any(re.search(pattern, text_lower) for pattern in federal_patterns):
            scope_indicators.append("federal")

        return scope_indicators if scope_indicators else ["unspecified"]

    def _determine_temporal_scope(self, text: str) -> str:
        """Determine the temporal scope of the policy."""
        text_lower = text.lower()

        # Immediate indicators
        immediate_patterns = [r"\bimmediate\b", r"\beffective\s+immediately\b", r"\bforthwith\b"]
        if any(re.search(pattern, text_lower) for pattern in immediate_patterns):
            return "immediate"

        # Short-term indicators
        short_term_patterns = [r"\bwithin\s+\d+\s+days?\b", r"\bshort\s+term\b", r"\binterim\b"]
        if any(re.search(pattern, text_lower) for pattern in short_term_patterns):
            return "short_term"

        # Long-term indicators
        long_term_patterns = [r"\blong\s+term\b", r"\bpermanent\b", r"\bongoing\b"]
        if any(re.search(pattern, text_lower) for pattern in long_term_patterns):
            return "long_term"

        return "medium_term"

    def _assess_implementation_complexity(self, text: str) -> str:
        """Assess the implementation complexity of the policy."""
        text_lower = text.lower()

        # High complexity indicators
        high_complexity_patterns = [
            r"\bcomplex\b", r"\bdetailed\b", r"\bcomprehensive\b", r"\bmulti\s+stage\b",
            r"\bphased\b", r"\bcoordination\b", r"\bintegration\b"
        ]

        # Low complexity indicators
        low_complexity_patterns = [
            r"\bsimple\b", r"\bstraightforward\b", r"\bminimal\b", r"\badministrative\b"
        ]

        high_count = sum(1 for pattern in high_complexity_patterns if re.search(pattern, text_lower))
        low_count = sum(1 for pattern in low_complexity_patterns if re.search(pattern, text_lower))

        if high_count > low_count:
            return "high"
        elif low_count > high_count:
            return "low"
        else:
            return "medium"

    def _identify_enforcement_mechanism(self, text: str) -> str:
        """Identify the enforcement mechanism of the policy."""
        text_lower = text.lower()

        # Mandatory enforcement
        mandatory_patterns = [
            r"\bmust\b", r"\bshall\b", r"\brequired\b", r"\bmandatory\b",
            r"\bviolation\b", r"\bpenalty\b", r"\bfine\b", r"\bsanction\b"
        ]

        # Incentive-based enforcement
        incentive_patterns = [
            r"\bincentive\b", r"\breward\b", r"\bbenefit\b", r"\bcredit\b",
            r"\bvoluntary\b", r"\bencouraged\b", r"\brecommended\b"
        ]

        mandatory_count = sum(1 for pattern in mandatory_patterns if re.search(pattern, text_lower))
        incentive_count = sum(1 for pattern in incentive_patterns if re.search(pattern, text_lower))

        if mandatory_count > incentive_count:
            return "mandatory"
        elif incentive_count > mandatory_count:
            return "incentive_based"
        else:
            return "voluntary"

    async def _find_related_policies(self, artifact_id: str, text: str) -> Tuple[List[str], Dict[str, PolicyRelationship]]:
        """Find related policies and their relationships."""
        related_policies = []
        relationships = {}

        # Simple relationship detection based on references
        reference_patterns = [
            (r"\b(Order|FERC)\s+(\d+)\b", "references"),  # References to specific orders
            (r"\b(Docket|Case)\s+(No\.?\s*\d+)\b", "references"),  # References to dockets
            (r"\bsupersede[s]?\b", "supersedes"),
            (r"\b(amend|modify|change)[s]?\b", "amends"),
            (r"\b(conflict|inconsistent)\b", "conflicts_with"),
            (r"\b(complement|support|enhance)[s]?\b", "complements")
        ]

        for pattern, relationship_type in reference_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                policy_id = f"policy_{match[0]}_{match[1]}" if len(match) > 1 else f"policy_{match[0]}"
                related_policies.append(policy_id)
                relationships[policy_id] = PolicyRelationship(relationship_type)

        return list(set(related_policies)), relationships

    def _calculate_classification_confidence(
        self,
        text: str,
        primary_category: PolicyCategory,
        subcategories: List[PolicySubcategory],
        impact_dimensions: List[PolicyImpactDimension]
    ) -> float:
        """Calculate confidence score for the classification."""
        base_confidence = 0.5

        # Text quality bonus
        text_length = len(text)
        if text_length > 1000:
            base_confidence += 0.2
        elif text_length > 500:
            base_confidence += 0.1

        # Classification strength bonus
        classification_strength = (
            len(subcategories) * 0.1 +
            len(impact_dimensions) * 0.05
        )
        base_confidence += min(classification_strength, 0.3)

        return min(1.0, base_confidence)

    async def _store_classification(self, classification: PolicyClassification) -> None:
        """Store policy classification in database."""
        try:
            # Store in database (mock implementation)
            # In reality, would store in Trino or similar
            pass

        except Exception as e:
            self.telemetry.error("Failed to store policy classification", error=str(e))

    async def get_policy_taxonomy(self) -> Optional[PolicyTaxonomy]:
        """Get the current policy taxonomy."""
        return self._policy_taxonomy

    async def update_policy_taxonomy(self, taxonomy: PolicyTaxonomy) -> None:
        """Update the policy taxonomy."""
        try:
            self._policy_taxonomy = taxonomy

            # Clear classification cache to force reclassification
            self._classification_cache.clear()

            self.telemetry.info(
                "Policy taxonomy updated",
                taxonomy_id=taxonomy.taxonomy_id,
                version=taxonomy.version
            )

        except Exception as e:
            self.telemetry.error("Failed to update policy taxonomy", error=str(e))
            raise

    async def get_classification_analytics(self) -> Dict[str, Any]:
        """Get analytics on policy classifications."""
        try:
            classifications = list(self._classification_cache.values())

            if not classifications:
                return {"message": "No classifications available"}

            # Category distribution
            category_counts = Counter(c.primary_category.value for c in classifications)
            subcategory_counts = Counter()
            for c in classifications:
                for subcategory in c.subcategories:
                    subcategory_counts[subcategory.value] += 1

            # Impact dimension distribution
            impact_counts = Counter()
            for c in classifications:
                for dimension in c.impact_dimensions:
                    impact_counts[dimension.value] += 1

            # Confidence statistics
            confidence_scores = [c.confidence_score for c in classifications]
            avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0

            analytics = {
                "total_classifications": len(classifications),
                "category_distribution": dict(category_counts),
                "subcategory_distribution": dict(subcategory_counts),
                "impact_dimension_distribution": dict(impact_counts),
                "average_confidence": avg_confidence,
                "high_confidence_classifications": len([c for c in classifications if c.confidence_score > 0.8]),
                "low_confidence_classifications": len([c for c in classifications if c.confidence_score < 0.5]),
                "classification_methods": list(set(c.classification_method for c in classifications))
            }

            return analytics

        except Exception as e:
            self.telemetry.error("Classification analytics failed", error=str(e))
            return {"error": str(e)}

    async def reclassify_policies(self, artifact_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Reclassify policies (optionally filtered by artifact IDs)."""
        try:
            if artifact_ids:
                # Reclassify specific policies
                classifications = []
                for artifact_id in artifact_ids:
                    # Would fetch text from database and reclassify
                    # For now, just update cache
                    if artifact_id in self._classification_cache:
                        del self._classification_cache[artifact_id]
                        classifications.append(artifact_id)
            else:
                # Reclassify all policies
                self._classification_cache.clear()
                # Would trigger reclassification of all stored policies
                classifications = list(self._classification_cache.keys())

            return {
                "reclassified_policies": len(classifications),
                "policy_ids": classifications,
                "status": "completed"
            }

        except Exception as e:
            self.telemetry.error("Policy reclassification failed", error=str(e))
            return {"error": str(e)}

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "taxonomy_loaded": self._policy_taxonomy is not None,
            "classifications_cached": len(self._classification_cache),
            "patterns_configured": len(self._category_patterns),
            "stakeholder_patterns": len(self._stakeholder_patterns),
            "last_activity": datetime.utcnow()
        }


def get_policy_tagging_service() -> PolicyTaggingService:
    """Get the global policy tagging service instance."""
    return PolicyTaggingService()


async def classify_policy_document(artifact_id: str, text: str) -> PolicyClassification:
    """Classify a policy document."""
    service = get_policy_tagging_service()
    return await service.classify_policy(artifact_id, text)


async def get_policy_taxonomy() -> Optional[PolicyTaxonomy]:
    """Get the current policy taxonomy."""
    service = get_policy_tagging_service()
    return await service.get_policy_taxonomy()
