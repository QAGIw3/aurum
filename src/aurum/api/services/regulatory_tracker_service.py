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
from ..dao.experimental import TrinoDAO


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
        """Initialize enhanced regulatory tracker service."""
        self.dao = TrinoDAO()
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # Regulatory data storage
        self._artifacts: Dict[str, RegulatoryArtifact] = {}
        self._tagging_cache: Dict[str, PolicyTagging] = {}
        self._alerts: List[RegulatoryAlert] = {}

        # Enhanced RSS/API sources with more comprehensive coverage
        self._rss_sources = {
            "ferc": "https://www.ferc.gov/rss/news-releases.xml",
            "doe": "https://www.energy.gov/rss/all.xml",
            "epa": "https://www.epa.gov/newsreleases/rss.xml",
            "cftc": "https://www.cftc.gov/rss.xml",
            "sec": "https://www.sec.gov/rss/litigation-releases.xml",
            "nrel": "https://www.nrel.gov/rss.xml",
            "eia": "https://www.eia.gov/rss/updates.xml"
        }

        self._api_sources = {
            "cftc": {
                "base_url": "https://www.cftc.gov/api",
                "endpoints": ["/rules", "/enforcement", "/market-data"]
            },
            "sec": {
                "base_url": "https://www.sec.gov/api",
                "endpoints": ["/edgar", "/enforcement"]
            },
            "ferc": {
                "base_url": "https://www.ferc.gov/api",
                "endpoints": ["/orders", "/notices", "/rulemakings"]
            }
        }

        # Enhanced NLP tagging patterns with more comprehensive coverage
        self._market_patterns = {
            "PJM": r"\bPJM\b|\bPennsylvania-New Jersey-Maryland\b|\bPJM Interconnection\b",
            "ERCOT": r"\bERCOT\b|\bElectric Reliability Council of Texas\b",
            "MISO": r"\bMISO\b|\bMidcontinent Independent System Operator\b|\bMidwest ISO\b",
            "CAISO": r"\bCAISO\b|\bCalifornia Independent System Operator\b",
            "NYISO": r"\bNYISO\b|\bNew York Independent System Operator\b",
            "ISO-NE": r"\bISO-NE\b|\bISO New England\b|\bNew England ISO\b",
            "SPP": r"\bSPP\b|\bSouthwest Power Pool\b",
            "WECC": r"\bWECC\b|\bWestern Electricity Coordinating Council\b"
        }

        self._instrument_patterns = {
            "LMP": r"\bLMP\b|\blocational marginal pricing\b|\bwholesale electricity prices\b|\bday-ahead pricing\b",
            "Capacity": r"\bcapacity market\b|\bresource adequacy\b|\bforward capacity auction\b|\bcapacity auction\b",
            "Ancillary": r"\bancillary services\b|\bfrequency regulation\b|\bspinning reserves\b|\bvoltage support\b",
            "Transmission": r"\btransmission planning\b|\bcongestion management\b|\binterconnection\b|\btransmission rights\b",
            "RTO": r"\bRTO\b|\bRegional Transmission Organization\b",
            "ISO": r"\bISO\b|\bIndependent System Operator\b"
        }

        # Enhanced topic patterns for better classification
        self._topic_patterns = {
            "carbon": r"\bcarbon\b|\bCO2\b|\bgreenhouse gas\b|\bemissions\b|\bclimate\b",
            "renewable": r"\brenewable\b|\bsolar\b|\bwind\b|\bhydro\b|\bgreen energy\b",
            "pricing": r"\bpricing\b|\brates\b|\btariffs\b|\bmarket rates\b",
            "capacity": r"\bcapacity\b|\bresource adequacy\b|\bgeneration\b",
            "transmission": r"\btransmission\b|\bgrid\b|\binterconnection\b|\bcongestion\b",
            "compliance": r"\bcompliance\b|\bregulation\b|\bpenalty\b|\benforcement\b"
        }

        # Real-time monitoring configuration
        self._monitoring_enabled = True
        self._polling_interval_minutes = 15
        self._alert_thresholds = {
            "critical": 0.8,
            "high": 0.6,
            "medium": 0.4,
            "low": 0.2
        }

        # Change detection and trend analysis
        self._change_detection_window = timedelta(days=7)
        self._trend_analysis_enabled = True

        # Initialize advanced NLP processing
        self._initialize_nlp_processor()

    def _initialize_nlp_processor(self) -> None:
        """Initialize advanced NLP processing capabilities."""
        try:
            # Try to initialize spaCy or similar NLP library
            try:
                import spacy
                self._nlp_processor = spacy.load("en_core_web_sm")
                self._nlp_available = True
            except ImportError:
                self._nlp_processor = None
                self._nlp_available = False
                self.logger.warning("Advanced NLP processing not available - using regex patterns only")

            # Initialize sentiment analysis (simplified)
            self._sentiment_keywords = {
                "positive": ["benefit", "advantage", "improvement", "enhancement", "support", "approval"],
                "negative": ["concern", "issue", "problem", "risk", "penalty", "violation", "enforcement"],
                "urgent": ["immediate", "urgent", "critical", "deadline", "requirement", "mandatory"]
            }

        except Exception as e:
            self.logger.error("Failed to initialize NLP processor", error=str(e))
            self._nlp_processor = None
            self._nlp_available = False

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
        """Perform enhanced NLP tagging on regulatory artifact."""
        try:
            # Extract text for analysis
            text = f"{artifact.title} {artifact.summary}"
            if artifact.full_text:
                text += f" {artifact.full_text}"

            # Find market mentions with enhanced patterns
            market_tags = []
            for market, pattern in self._market_patterns.items():
                if re.search(pattern, text, re.IGNORECASE):
                    market_tags.append(market)

            # Find instrument mentions with enhanced patterns
            instrument_tags = []
            for instrument, pattern in self._instrument_patterns.items():
                if re.search(pattern, text, re.IGNORECASE):
                    instrument_tags.append(instrument)

            # Find topic mentions
            topic_tags = []
            for topic, pattern in self._topic_patterns.items():
                if re.search(pattern, text, re.IGNORECASE):
                    topic_tags.append(topic)

            # Enhanced sentiment analysis
            sentiment_score = self._calculate_sentiment_score(text)

            # Enhanced urgency analysis
            urgency_score = self._calculate_urgency_score(text)

            # Enhanced compliance impact analysis
            compliance_impact = self._calculate_compliance_impact(text)

            # Extract entities (simplified - would use NER in production)
            entity_tags = self._extract_entities(text)

            # Extract key phrases with improved logic
            key_phrases = self._extract_key_phrases_enhanced(text)

            # Calculate confidence based on text quality and pattern matches
            confidence = self._calculate_confidence_score(text, market_tags, instrument_tags, topic_tags)

            # Create enhanced tagging result
            tagging = PolicyTagging(
                artifact_id=artifact.artifact_id,
                market_tags=market_tags,
                instrument_tags=instrument_tags,
                entity_tags=entity_tags,
                topic_tags=topic_tags,
                sentiment_score=sentiment_score,
                urgency_score=urgency_score,
                compliance_impact=compliance_impact,
                key_phrases=key_phrases,
                confidence=confidence
            )

            self._tagging_cache[artifact.artifact_id] = tagging

            # Update artifact with enhanced tagging results
            artifact.affected_markets = market_tags
            artifact.affected_instruments = instrument_tags
            artifact.nlp_tags = {
                "markets": market_tags,
                "instruments": instrument_tags,
                "topics": topic_tags,
                "entities": entity_tags,
                "sentiment": sentiment_score,
                "urgency": urgency_score,
                "compliance_impact": compliance_impact,
                "key_phrases": key_phrases,
                "confidence": confidence
            }

            # Enhanced impact level determination
            artifact.impact_level = self._determine_enhanced_impact_level(
                market_tags, instrument_tags, topic_tags, urgency_score, compliance_impact
            )

        except Exception as e:
            self.telemetry.error("Enhanced NLP tagging failed", artifact_id=artifact.artifact_id, error=str(e))

    def _calculate_sentiment_score(self, text: str) -> float:
        """Calculate sentiment score using keyword analysis."""
        text_lower = text.lower()

        positive_count = sum(1 for keyword in self._sentiment_keywords["positive"] if keyword in text_lower)
        negative_count = sum(1 for keyword in self._sentiment_keywords["negative"] if keyword in text_lower)

        total_sentiment_words = positive_count + negative_count

        if total_sentiment_words == 0:
            return 0.0

        # Normalize to [-1, 1] range
        sentiment_score = (positive_count - negative_count) / total_sentiment_words

        return max(-1.0, min(1.0, sentiment_score))

    def _calculate_urgency_score(self, text: str) -> float:
        """Calculate urgency score based on urgent keywords and deadlines."""
        text_lower = text.lower()
        urgent_count = sum(1 for keyword in self._sentiment_keywords["urgent"] if keyword in text_lower)

        # Check for deadline mentions
        deadline_patterns = [
            r"\bdeadline\b", r"\bby\s+\d{1,2}/\d{1,2}/\d{4}\b", r"\bwithin\s+\d+\s+days?\b",
            r"\beffective\s+\d{1,2}/\d{1,2}/\d{4}\b", r"\bimmediately\b"
        ]

        deadline_mentions = sum(1 for pattern in deadline_patterns if re.search(pattern, text, re.IGNORECASE))

        # Combine urgency keywords and deadline mentions
        total_urgency_indicators = urgent_count + deadline_mentions

        # Normalize to [0, 1] range
        return min(1.0, total_urgency_indicators / 5.0)

    def _calculate_compliance_impact(self, text: str) -> str:
        """Calculate compliance impact level."""
        text_lower = text.lower()

        # High impact indicators
        high_impact_keywords = [
            "mandatory", "required", "must", "shall", "prohibited", "violation", "penalty",
            "enforcement", "compliance", "deadline", "requirement"
        ]

        # Medium impact indicators
        medium_impact_keywords = [
            "recommended", "guidance", "suggested", "encouraged", "should", "may"
        ]

        high_count = sum(1 for keyword in high_impact_keywords if keyword in text_lower)
        medium_count = sum(1 for keyword in medium_impact_keywords if keyword in text_lower)

        if high_count > 0:
            return "high"
        elif medium_count > 0:
            return "medium"
        else:
            return "low"

    def _extract_entities(self, text: str) -> List[str]:
        """Extract entities from text (simplified implementation)."""
        entities = []

        # Extract organization names (simplified patterns)
        org_patterns = [
            r"\b(FERC|CFTC|SEC|EPA|DOE|NREL|EIA)\b",
            r"\b(Commission|Agency|Department|Administration)\b",
            r"\b(Interconnection|Corporation|Company|LLC|Inc)\b"
        ]

        for pattern in org_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            entities.extend(matches)

        return list(set(entities))  # Remove duplicates

    def _extract_key_phrases_enhanced(self, text: str) -> List[str]:
        """Extract key phrases using enhanced logic."""
        # Split into sentences
        sentences = re.split(r'[.!?]+', text)

        # Score sentences based on importance indicators
        scored_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) < 20:  # Skip very short sentences
                continue

            score = 0

            # Length bonus (longer sentences likely contain more info)
            score += min(len(sentence) / 100, 1.0)

            # Keyword bonuses
            for category, keywords in self._sentiment_keywords.items():
                for keyword in keywords:
                    if keyword in sentence.lower():
                        score += 0.2

            # Technical term bonuses
            technical_terms = ["regulation", "policy", "rule", "requirement", "compliance", "market", "pricing"]
            for term in technical_terms:
                if term in sentence.lower():
                    score += 0.1

            scored_sentences.append((sentence, score))

        # Sort by score and return top phrases
        scored_sentences.sort(key=lambda x: x[1], reverse=True)
        top_sentences = [sentence for sentence, _ in scored_sentences[:5]]

        return top_sentences

    def _calculate_confidence_score(
        self,
        text: str,
        market_tags: List[str],
        instrument_tags: List[str],
        topic_tags: List[str]
    ) -> float:
        """Calculate confidence score for NLP tagging."""
        base_confidence = 0.5

        # Text length bonus
        text_length = len(text)
        if text_length > 500:
            base_confidence += 0.2
        elif text_length > 200:
            base_confidence += 0.1

        # Pattern match bonus
        match_count = len(market_tags) + len(instrument_tags) + len(topic_tags)
        if match_count > 0:
            base_confidence += min(match_count * 0.1, 0.3)

        # Source reliability (mock - would be based on source credibility)
        base_confidence += 0.1

        return min(1.0, base_confidence)

    def _determine_enhanced_impact_level(
        self,
        market_tags: List[str],
        instrument_tags: List[str],
        topic_tags: List[str],
        urgency_score: float,
        compliance_impact: str
    ) -> PolicyImpactLevel:
        """Determine enhanced impact level based on multiple factors."""
        # Base scoring
        base_score = 0.0

        # Market impact
        if len(market_tags) > 0:
            base_score += 0.3

        # Instrument impact
        if len(instrument_tags) > 0:
            base_score += 0.2

        # Topic impact
        critical_topics = ["carbon", "compliance", "pricing"]
        if any(topic in critical_topics for topic in topic_tags):
            base_score += 0.3

        # Urgency impact
        if urgency_score > 0.7:
            base_score += 0.3
        elif urgency_score > 0.4:
            base_score += 0.1

        # Compliance impact
        if compliance_impact == "high":
            base_score += 0.2
        elif compliance_impact == "medium":
            base_score += 0.1

        # Determine final impact level
        if base_score >= 0.8:
            return PolicyImpactLevel.CRITICAL
        elif base_score >= 0.6:
            return PolicyImpactLevel.HIGH
        elif base_score >= 0.3:
            return PolicyImpactLevel.MEDIUM
        else:
            return PolicyImpactLevel.LOW

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
        """Get enhanced service health status."""
        return {
            "status": "healthy",
            "artifacts_tracked": len(self._artifacts),
            "alerts_generated": len(self._alerts),
            "last_ingestion": datetime.utcnow(),
            "rss_sources": len(self._rss_sources),
            "api_sources": len(self._api_sources),
            "monitoring_enabled": self._monitoring_enabled,
            "polling_interval_minutes": self._polling_interval_minutes,
            "trend_analysis_enabled": self._trend_analysis_enabled,
            "nlp_processor_available": self._nlp_available,
            "quality_checks_configured": len(self.quality_checks) if hasattr(self, 'quality_checks') else 0
        }

    async def start_real_time_monitoring(self) -> None:
        """Start real-time monitoring of regulatory sources."""
        if not self._monitoring_enabled:
            return

        try:
            # Start background monitoring task
            asyncio.create_task(self._monitoring_loop())

            self.telemetry.info(
                "Real-time regulatory monitoring started",
                polling_interval_minutes=self._polling_interval_minutes
            )

        except Exception as e:
            self.telemetry.error("Failed to start real-time monitoring", error=str(e))

    async def stop_real_time_monitoring(self) -> None:
        """Stop real-time monitoring."""
        self._monitoring_enabled = False
        self.telemetry.info("Real-time regulatory monitoring stopped")


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
