"""Regulatory and Policy Tracker Service with RSS/API ingestion and NLP tagging.

This service provides:
- Lightweight ingestion for regulatory artifacts (RSS/API)
- NLP tagging of affected markets/instruments
- Policy metadata tracking and compliance monitoring
- Regulatory change detection and alerting
- Integration with forecasting and risk management
- Historical regulatory analysis and trend detection
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4
from enum import Enum

import feedparser
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from ..daos.base_dao import TrinoDAO


class RegulatorySource(str, Enum):
    """Regulatory data sources."""
    FERC = "ferc"
    EPA = "epa"
    DOE = "doe"
    CFTC = "cftc"
    SEC = "sec"
    STATE_REGULATORS = "state_regulators"
    EU_COMMISSION = "eu_commission"
    RSS_FEEDS = "rss_feeds"
    API_SOURCES = "api_sources"


class PolicyImpactLevel(str, Enum):
    """Impact levels for regulatory policies."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RegulatoryArtifact(BaseModel):
    """Regulatory artifact with metadata."""

    artifact_id: str
    source: RegulatorySource
    title: str
    summary: str
    full_text: Optional[str]
    publication_date: datetime
    effective_date: Optional[datetime]
    expiry_date: Optional[datetime]
    url: str
    document_type: str  # "rule", "guidance", "notice", "order", "legislation"
    status: str = "active"  # "active", "superseded", "repealed", "pending"
    affected_markets: List[str] = field(default_factory=list)
    affected_instruments: List[str] = field(default_factory=list)
    nlp_tags: Dict[str, List[str]] = field(default_factory=dict)
    impact_level: PolicyImpactLevel = PolicyImpactLevel.MEDIUM
    compliance_deadline: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)


class PolicyTagging(BaseModel):
    """NLP-based policy tagging results."""

    artifact_id: str
    market_tags: List[str]  # Markets affected (PJM, ERCOT, etc.)
    instrument_tags: List[str]  # Instruments affected (LMP, capacity, etc.)
    entity_tags: List[str]  # Entities mentioned (utilities, generators, etc.)
    topic_tags: List[str]  # Policy topics (carbon, renewable, pricing, etc.)
    sentiment_score: float  # -1.0 to 1.0 (negative to positive)
    urgency_score: float  # 0.0 to 1.0 (low to high urgency)
    compliance_impact: str  # "none", "low", "medium", "high", "critical"
    key_phrases: List[str]  # Important phrases extracted
    confidence: float  # 0.0 to 1.0


class RegulatoryAlert(BaseModel):
    """Alert for regulatory changes."""

    alert_id: str
    artifact_id: str
    alert_type: str  # "new_policy", "policy_change", "deadline_approaching", "compliance_risk"
    severity: str  # "info", "warning", "error", "critical"
    title: str
    message: str
    affected_portfolios: List[str]
    affected_assets: List[str]
    action_required: str
    deadline: Optional[datetime]
    created_at: datetime = field(default_factory=datetime.utcnow)


class RegulatoryTrackerService:
    """Regulatory and Policy Tracker Service."""

    def __init__(self):
        """Initialize regulatory tracker service."""
        self.dao = TrinoDAO()
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # Regulatory data storage
        self._artifacts: Dict[str, RegulatoryArtifact] = {}
        self._tagging_cache: Dict[str, PolicyTagging] = {}
        self._alerts: List[RegulatoryAlert] = []

        # RSS/API sources
        self._rss_sources = {
            "ferc": "https://www.ferc.gov/rss/news-releases.xml",
            "doe": "https://www.energy.gov/rss/all.xml",
            "epa": "https://www.epa.gov/newsreleases/rss.xml"
        }

        self._api_sources = {
            "cftc": "https://www.cftc.gov/api",
            "sec": "https://www.sec.gov/api"
        }

        # NLP tagging patterns
        self._market_patterns = {
            "PJM": r"\bPJM\b|\bPennsylvania-New Jersey-Maryland\b",
            "ERCOT": r"\bERCOT\b|\bElectric Reliability Council of Texas\b",
            "MISO": r"\bMISO\b|\bMidcontinent Independent System Operator\b",
            "CAISO": r"\bCAISO\b|\bCalifornia Independent System Operator\b",
            "NYISO": r"\bNYISO\b|\bNew York Independent System Operator\b",
            "ISO-NE": r"\bISO-NE\b|\bISO New England\b"
        }

        self._instrument_patterns = {
            "LMP": r"\bLMP\b|\blocational marginal pricing\b|\bwholesale electricity prices\b",
            "Capacity": r"\bcapacity market\b|\bresource adequacy\b|\bforward capacity auction\b",
            "Ancillary": r"\bancillary services\b|\bfrequency regulation\b|\bspinning reserves\b",
            "Transmission": r"\btransmission planning\b|\bcongestion management\b|\binterconnection\b"
        }

    async def ingest_rss_feeds(self) -> List[RegulatoryArtifact]:
        """Ingest regulatory data from RSS feeds."""
        new_artifacts = []

        for source_name, rss_url in self._rss_sources.items():
            try:
                # Parse RSS feed
                feed = feedparser.parse(rss_url)

                for entry in feed.entries[:10]:  # Limit to recent entries
                    # Check if already processed
                    artifact_id = f"{source_name}_{hash(entry.link) % 1000000}"

                    if artifact_id in self._artifacts:
                        continue

                    # Extract content
                    title = entry.title
                    summary = entry.summary if hasattr(entry, 'summary') else ""
                    url = entry.link
                    pub_date = datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') else datetime.utcnow()

                    # Create artifact
                    artifact = RegulatoryArtifact(
                        artifact_id=artifact_id,
                        source=RegulatorySource(source_name),
                        title=title,
                        summary=summary,
                        url=url,
                        publication_date=pub_date,
                        document_type="news_release",
                        affected_markets=[],
                        affected_instruments=[]
                    )

                    # Perform NLP tagging
                    await self._perform_nlp_tagging(artifact)

                    self._artifacts[artifact_id] = artifact
                    new_artifacts.append(artifact)

                    # Generate alerts if high impact
                    if artifact.impact_level in [PolicyImpactLevel.HIGH, PolicyImpactLevel.CRITICAL]:
                        await self._generate_regulatory_alert(artifact)

            except Exception as e:
                self.telemetry.error("RSS ingestion failed", source=source_name, error=str(e))

        self.telemetry.info("RSS ingestion completed", new_artifacts=len(new_artifacts))
        return new_artifacts

    async def ingest_api_sources(self) -> List[RegulatoryArtifact]:
        """Ingest regulatory data from API sources."""
        new_artifacts = []

        for source_name, api_base in self._api_sources.items():
            try:
                # Example API call (would be customized per source)
                if source_name == "cftc":
                    # Mock CFTC API call
                    api_data = await self._call_cftc_api()
                elif source_name == "sec":
                    # Mock SEC API call
                    api_data = await self._call_sec_api()
                else:
                    continue

                for item in api_data:
                    artifact_id = f"{source_name}_{item.get('id', hash(item.get('url', '')) % 1000000)}"

                    if artifact_id in self._artifacts:
                        continue

                    # Create artifact from API data
                    artifact = RegulatoryArtifact(
                        artifact_id=artifact_id,
                        source=RegulatorySource(source_name),
                        title=item.get("title", ""),
                        summary=item.get("summary", ""),
                        full_text=item.get("full_text"),
                        url=item.get("url", ""),
                        publication_date=datetime.fromisoformat(item.get("publication_date", datetime.utcnow().isoformat())),
                        document_type=item.get("document_type", "rule"),
                        affected_markets=[],
                        affected_instruments=[]
                    )

                    # Perform NLP tagging
                    await self._perform_nlp_tagging(artifact)

                    self._artifacts[artifact_id] = artifact
                    new_artifacts.append(artifact)

            except Exception as e:
                self.telemetry.error("API ingestion failed", source=source_name, error=str(e))

        self.telemetry.info("API ingestion completed", new_artifacts=len(new_artifacts))
        return new_artifacts

    async def _call_cftc_api(self) -> List[Dict[str, Any]]:
        """Call CFTC API for regulatory data."""
        # Mock implementation - would make actual API calls
        return [
            {
                "id": "cftc_001",
                "title": "CFTC Final Rule on Energy Market Oversight",
                "summary": "New regulations for energy market transparency and reporting requirements.",
                "publication_date": datetime.utcnow().isoformat(),
                "url": "https://www.cftc.gov/rules/final",
                "document_type": "final_rule"
            }
        ]

    async def _call_sec_api(self) -> List[Dict[str, Any]]:
        """Call SEC API for regulatory data."""
        # Mock implementation - would make actual API calls
        return [
            {
                "id": "sec_001",
                "title": "SEC Climate Risk Disclosure Rules",
                "summary": "Enhanced climate risk disclosure requirements for public companies.",
                "publication_date": datetime.utcnow().isoformat(),
                "url": "https://www.sec.gov/rules/final",
                "document_type": "final_rule"
            }
        ]

    async def _perform_nlp_tagging(self, artifact: RegulatoryArtifact) -> None:
        """Perform NLP tagging on regulatory artifact."""
        try:
            # Extract text for analysis
            text = f"{artifact.title} {artifact.summary}"
            if artifact.full_text:
                text += f" {artifact.full_text}"

            # Find market mentions
            market_tags = []
            for market, pattern in self._market_patterns.items():
                if re.search(pattern, text, re.IGNORECASE):
                    market_tags.append(market)

            # Find instrument mentions
            instrument_tags = []
            for instrument, pattern in self._instrument_patterns.items():
                if re.search(pattern, text, re.IGNORECASE):
                    instrument_tags.append(instrument)

            # Simple sentiment analysis (mock)
            sentiment_score = 0.0
            urgency_score = 0.5

            # Determine compliance impact
            impact_keywords = ["mandatory", "required", "must", "shall", "prohibited", "deadline"]
            compliance_impact = "low"
            for keyword in impact_keywords:
                if keyword.lower() in text.lower():
                    compliance_impact = "high"
                    break

            # Create tagging result
            tagging = PolicyTagging(
                artifact_id=artifact.artifact_id,
                market_tags=market_tags,
                instrument_tags=instrument_tags,
                entity_tags=[],  # Would extract entities
                topic_tags=["energy", "regulation"],  # Would extract topics
                sentiment_score=sentiment_score,
                urgency_score=urgency_score,
                compliance_impact=compliance_impact,
                key_phrases=self._extract_key_phrases(text),
                confidence=0.8
            )

            self._tagging_cache[artifact.artifact_id] = tagging

            # Update artifact with tagging results
            artifact.affected_markets = market_tags
            artifact.affected_instruments = instrument_tags
            artifact.nlp_tags = {
                "markets": market_tags,
                "instruments": instrument_tags,
                "topics": tagging.topic_tags,
                "sentiment": sentiment_score,
                "urgency": urgency_score
            }

            # Determine impact level
            if len(market_tags) > 0 and compliance_impact == "high":
                artifact.impact_level = PolicyImpactLevel.HIGH
            elif len(market_tags) > 0:
                artifact.impact_level = PolicyImpactLevel.MEDIUM
            else:
                artifact.impact_level = PolicyImpactLevel.LOW

        except Exception as e:
            self.telemetry.error("NLP tagging failed", artifact_id=artifact.artifact_id, error=str(e))

    def _extract_key_phrases(self, text: str) -> List[str]:
        """Extract key phrases from text."""
        # Simple implementation - would use actual NLP
        sentences = text.split('.')
        key_phrases = []

        for sentence in sentences:
            if len(sentence.strip()) > 50:  # Longer sentences likely contain key info
                key_phrases.append(sentence.strip()[:100] + "...")

        return key_phrases[:5]  # Return top 5 phrases

    async def _generate_regulatory_alert(self, artifact: RegulatoryArtifact) -> None:
        """Generate regulatory alert for high-impact policies."""
        alert = RegulatoryAlert(
            alert_id=str(uuid4()),
            artifact_id=artifact.artifact_id,
            alert_type="new_policy",
            severity="warning" if artifact.impact_level == PolicyImpactLevel.HIGH else "info",
            title=f"New {artifact.source.value.upper()} Policy: {artifact.title}",
            message=f"A new regulatory policy has been published that may affect {', '.join(artifact.affected_markets)} markets.",
            affected_portfolios=[],  # Would determine based on portfolio analysis
            affected_assets=artifact.affected_instruments,
            action_required="review_compliance" if artifact.impact_level == PolicyImpactLevel.HIGH else "monitor",
            deadline=artifact.compliance_deadline
        )

        self._alerts.append(alert)
        self.telemetry.info("Regulatory alert generated", alert_id=alert.alert_id)

    async def get_artifacts_by_market(self, market: str, limit: int = 50) -> List[RegulatoryArtifact]:
        """Get regulatory artifacts affecting a specific market."""
        matching_artifacts = []

        for artifact in self._artifacts.values():
            if market in artifact.affected_markets:
                matching_artifacts.append(artifact)

        # Sort by publication date (most recent first)
        matching_artifacts.sort(key=lambda x: x.publication_date, reverse=True)
        return matching_artifacts[:limit]

    async def get_artifacts_by_instrument(self, instrument: str, limit: int = 50) -> List[RegulatoryArtifact]:
        """Get regulatory artifacts affecting a specific instrument."""
        matching_artifacts = []

        for artifact in self._artifacts.values():
            if instrument in artifact.affected_instruments:
                matching_artifacts.append(artifact)

        # Sort by publication date (most recent first)
        matching_artifacts.sort(key=lambda x: x.publication_date, reverse=True)
        return matching_artifacts[:limit]

    async def get_regulatory_alerts(self, limit: int = 50) -> List[RegulatoryAlert]:
        """Get recent regulatory alerts."""
        # Sort by creation date (most recent first)
        sorted_alerts = sorted(self._alerts, key=lambda x: x.created_at, reverse=True)
        return sorted_alerts[:limit]

    async def get_impact_analysis(self, portfolio_id: str) -> Dict[str, Any]:
        """Analyze regulatory impact on a portfolio."""
        # Mock implementation
        affected_artifacts = []
        risk_score = 0.3
        compliance_deadlines = []

        for artifact in self._artifacts.values():
            if artifact.impact_level in [PolicyImpactLevel.HIGH, PolicyImpactLevel.CRITICAL]:
                affected_artifacts.append(artifact)
                if artifact.compliance_deadline:
                    compliance_deadlines.append(artifact.compliance_deadline)

        return {
            "portfolio_id": portfolio_id,
            "total_artifacts": len(affected_artifacts),
            "high_impact_artifacts": len([a for a in affected_artifacts if a.impact_level == PolicyImpactLevel.HIGH]),
            "critical_impact_artifacts": len([a for a in affected_artifacts if a.impact_level == PolicyImpactLevel.CRITICAL]),
            "risk_score": risk_score,
            "compliance_deadlines": compliance_deadlines,
            "affected_markets": list(set([market for artifact in affected_artifacts for market in artifact.affected_markets])),
            "affected_instruments": list(set([instrument for artifact in affected_artifacts for instrument in artifact.affected_instruments]))
        }

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "artifacts_tracked": len(self._artifacts),
            "alerts_generated": len(self._alerts),
            "last_ingestion": datetime.utcnow(),
            "rss_sources": len(self._rss_sources),
            "api_sources": len(self._api_sources)
        }


def get_regulatory_tracker_service() -> RegulatoryTrackerService:
    """Get the global regulatory tracker service instance."""
    return RegulatoryTrackerService()


async def ingest_regulatory_updates() -> List[RegulatoryArtifact]:
    """Ingest regulatory updates from all sources."""
    service = get_regulatory_tracker_service()

    # Ingest from RSS feeds
    rss_artifacts = await service.ingest_rss_feeds()

    # Ingest from APIs
    api_artifacts = await service.ingest_api_sources()

    return rss_artifacts + api_artifacts


async def get_regulatory_impact_for_portfolio(portfolio_id: str) -> Dict[str, Any]:
    """Get regulatory impact analysis for a specific portfolio."""
    service = get_regulatory_tracker_service()
    return await service.get_impact_analysis(portfolio_id)


async def get_market_regulatory_summary(market: str) -> Dict[str, Any]:
    """Get regulatory summary for a specific market."""
    service = get_regulatory_tracker_service()

    artifacts = await service.get_artifacts_by_market(market)

    return {
        "market": market,
        "total_artifacts": len(artifacts),
        "recent_artifacts": len([a for a in artifacts if (datetime.utcnow() - a.publication_date).days < 30]),
        "high_impact_artifacts": len([a for a in artifacts if a.impact_level == PolicyImpactLevel.HIGH]),
        "critical_impact_artifacts": len([a for a in artifacts if a.impact_level == PolicyImpactLevel.CRITICAL]),
        "affected_instruments": list(set([instrument for artifact in artifacts for instrument in artifact.affected_instruments])),
        "compliance_deadlines": [a.compliance_deadline for a in artifacts if a.compliance_deadline]
    }
