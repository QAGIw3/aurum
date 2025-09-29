"""Natural language query processing for Aurum search platform.

Provides parsing, entity extraction, and DSL building capabilities
to transform natural language queries into structured search requests.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json

from aurum.core.settings import get_settings
from aurum.core import AurumSettings
from .synonyms_and_fuzzy import get_search_enhancement_service, enhance_search_query


logger = logging.getLogger(__name__)


@dataclass
class ParsedQuery:
    """Result of query parsing and analysis."""
    original_query: str
    normalized_query: str
    entities: Dict[str, List[str]] = field(default_factory=dict)
    filters: Dict[str, Any] = field(default_factory=dict)
    date_ranges: Dict[str, Any] = field(default_factory=dict)
    operators: List[str] = field(default_factory=list)
    intent: Optional[str] = None
    confidence: float = 0.0
    explanation: List[str] = field(default_factory=list)


@dataclass
class QueryEntity:
    """Represents an extracted entity from query text."""
    type: str  # 'location', 'asset_class', 'iso', 'date', etc.
    value: str
    confidence: float = 1.0
    position: Optional[Tuple[int, int]] = None


class EntityExtractor:
    """Extracts entities from natural language queries."""

    # Domain-specific entity patterns
    ISO_CODES = {
        'ERCOT', 'PJM', 'MISO', 'CAISO', 'SPP', 'NYISO', 'ISO-NE', 'IESO',
        'AEP', 'AESO', 'BPA', 'DEAA', 'DOPD', 'EEI', 'EPE', 'GRDA', 'GRIF',
        'GSEC', 'HECO', 'HGMA', 'HQT', 'HST', 'IESO', 'IID', 'IPCO', 'ISONE',
        'JEA', 'KCPL', 'LDWP', 'LGEE', 'MHEB', 'MISO', 'MP', 'NEVP', 'NPPD',
        'NSB', 'NWMT', 'NYIS', 'OKGE', 'OPPD', 'OTP', 'OVEC', 'PACE', 'PACW',
        'PGE', 'PJM', 'PNM', 'PSCO', 'PSEI', 'SCE', 'SDGE', 'SEC', 'SECI',
        'SEPA', 'SOCO', 'SPA', 'SPC', 'SPP', 'SPS', 'SRP', 'SWPP', 'TAL',
        'TEC', 'TEPC', 'TIDC', 'TPWR', 'TVA', 'WACM', 'WALC', 'WAUE', 'WECC',
        'WECI', 'WPKY', 'WR'
    }

    ASSET_CLASSES = {
        'power', 'electricity', 'gas', 'natural_gas', 'coal', 'oil', 'renewables',
        'solar', 'wind', 'hydro', 'nuclear', 'battery', 'storage', 'transmission',
        'distribution', 'generation', 'load', 'demand', 'supply', 'capacity'
    }

    LOCATIONS = {
        'texas', 'california', 'new_york', 'florida', 'illinois', 'pennsylvania',
        'ohio', 'georgia', 'north_carolina', 'michigan', 'new_jersey', 'virginia',
        'washington', 'arizona', 'massachusetts', 'tennessee', 'indiana', 'missouri',
        'maryland', 'wisconsin', 'colorado', 'minnesota', 'south_carolina', 'alabama',
        'louisiana', 'kentucky', 'oregon', 'oklahoma', 'connecticut', 'utah', 'iowa',
        'nevada', 'arkansas', 'mississippi', 'kansas', 'new_mexico', 'nebraska',
        'west_virginia', 'idaho', 'hawaii', 'new_hampshire', 'maine', 'rhode_island',
        'montana', 'delaware', 'south_dakota', 'north_dakota', 'alaska', 'vermont',
        'wyoming'
    }

    DATE_PATTERNS = [
        # Relative dates
        (r'last\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)', 'relative'),
        (r'past\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)', 'relative'),
        (r'previous\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)', 'relative'),
        (r'next\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)', 'relative'),

        # Absolute dates
        (r'(\d{4})-(\d{1,2})-(\d{1,2})', 'absolute'),  # YYYY-MM-DD
        (r'(\d{1,2})/(\d{1,2})/(\d{4})', 'absolute'),  # MM/DD/YYYY
        (r'(\d{1,2})-(\d{1,2})-(\d{4})', 'absolute'),  # MM-DD-YYYY

        # Named periods
        (r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})', 'named_month'),
        (r'q([1-4])\s+(\d{4})', 'quarter'),  # Q1 2024
        (r'(\d{4})\s+q([1-4])', 'quarter'),  # 2024 Q1

        # Time periods
        (r'(this|current)\s+(week|month|quarter|year)', 'current_period'),
        (r'(yesterday|today|tomorrow)', 'relative_day'),
    ]

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize entity extractor.

        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings or get_settings()

    def extract_entities(self, query: str) -> List[QueryEntity]:
        """Extract entities from query text.

        Args:
            query: Query string to analyze

        Returns:
            List of extracted entities
        """
        entities = []
        query_lower = query.lower()

        # Extract ISO codes
        for iso in self.ISO_CODES:
            if iso.lower() in query_lower:
                entities.append(QueryEntity(
                    type='iso',
                    value=iso,
                    confidence=0.9
                ))

        # Extract asset classes
        for asset in self.ASSET_CLASSES:
            if asset.replace('_', ' ') in query_lower or asset in query_lower:
                entities.append(QueryEntity(
                    type='asset_class',
                    value=asset,
                    confidence=0.8
                ))

        # Extract locations
        for location in self.LOCATIONS:
            if location.replace('_', ' ') in query_lower:
                entities.append(QueryEntity(
                    type='location',
                    value=location,
                    confidence=0.7
                ))

        # Extract dates
        date_entities = self._extract_dates(query)
        entities.extend(date_entities)

        return entities

    def _extract_dates(self, query: str) -> List[QueryEntity]:
        """Extract date entities from query."""
        entities = []

        for pattern, pattern_type in self.DATE_PATTERNS:
            matches = re.finditer(pattern, query, re.IGNORECASE)
            for match in matches:
                entity = self._parse_date_match(match, pattern_type)
                if entity:
                    entities.append(entity)

        return entities

    def _parse_date_match(self, match: re.Match, pattern_type: str) -> Optional[QueryEntity]:
        """Parse a date pattern match into entity."""
        groups = match.groups()

        try:
            if pattern_type == 'relative':
                # last 7 days, past 3 months, etc.
                number = int(groups[0])
                unit = groups[1].lower()

                if unit in ['day', 'days']:
                    start_date = datetime.now() - timedelta(days=number)
                    end_date = datetime.now()
                elif unit in ['week', 'weeks']:
                    start_date = datetime.now() - timedelta(weeks=number)
                    end_date = datetime.now()
                elif unit in ['month', 'months']:
                    start_date = datetime.now() - timedelta(days=number * 30)  # Approximation
                    end_date = datetime.now()
                elif unit in ['year', 'years']:
                    start_date = datetime.now() - timedelta(days=number * 365)  # Approximation
                    end_date = datetime.now()
                else:
                    return None

                return QueryEntity(
                    type='date_range',
                    value=f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                    confidence=0.8
                )

            elif pattern_type == 'absolute':
                # Parse absolute dates
                if len(groups) == 3:
                    year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                    date = datetime(year, month, day)
                    return QueryEntity(
                        type='date',
                        value=date.strftime('%Y-%m-%d'),
                        confidence=0.9
                    )

            elif pattern_type == 'named_month':
                month_name = groups[0].lower()
                year = int(groups[1])

                month_map = {
                    'january': 1, 'february': 2, 'march': 3, 'april': 4,
                    'may': 5, 'june': 6, 'july': 7, 'august': 8,
                    'september': 9, 'october': 10, 'november': 11, 'december': 12
                }

                if month_name in month_map:
                    month = month_map[month_name]
                    date = datetime(year, month, 1)
                    return QueryEntity(
                        type='date',
                        value=date.strftime('%Y-%m-%d'),
                        confidence=0.8
                    )

            elif pattern_type == 'quarter':
                quarter = int(groups[0])
                year = int(groups[1])

                # Calculate quarter start date
                month = (quarter - 1) * 3 + 1
                date = datetime(year, month, 1)
                return QueryEntity(
                    type='quarter',
                    value=f"Q{quarter} {year}",
                    confidence=0.8
                )

            elif pattern_type == 'current_period':
                period = groups[1].lower()
                now = datetime.now()

                if period == 'week':
                    # Start of current week (Monday)
                    start_date = now - timedelta(days=now.weekday())
                    return QueryEntity(
                        type='date_range',
                        value=f"{start_date.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}",
                        confidence=0.7
                    )
                elif period == 'month':
                    # Current month
                    start_date = now.replace(day=1)
                    return QueryEntity(
                        type='date_range',
                        value=f"{start_date.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}",
                        confidence=0.7
                    )
                elif period == 'year':
                    # Current year
                    start_date = now.replace(month=1, day=1)
                    return QueryEntity(
                        type='date_range',
                        value=f"{start_date.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}",
                        confidence=0.7
                    )

            elif pattern_type == 'relative_day':
                day = groups[0].lower()

                if day == 'yesterday':
                    date = datetime.now() - timedelta(days=1)
                elif day == 'today':
                    date = datetime.now()
                elif day == 'tomorrow':
                    date = datetime.now() + timedelta(days=1)
                else:
                    return None

                return QueryEntity(
                    type='date',
                    value=date.strftime('%Y-%m-%d'),
                    confidence=0.9
                )

        except (ValueError, IndexError) as e:
            logger.debug(f"Failed to parse date pattern: {e}")
            return None

        return None


class QueryProcessor:
    """Main query processor for natural language search."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize query processor.

        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings or get_settings()
        self.entity_extractor = EntityExtractor(settings)

        # Common query patterns and their intents
        self.intent_patterns = {
            'documentation': [
                r'(?:docs?|documentation|manual|guide|tutorial|help)',
                r'(?:how\s+to|explain|describe|what\s+is)',
            ],
            'data': [
                r'(?:data|dataset|information|records?|values?)',
                r'(?:find|search|get|retrieve|show)',
            ],
            'analysis': [
                r'(?:analysis|analytics|insights?|trends?|patterns?)',
                r'(?:analyze|examine|study|investigate)',
            ],
            'comparison': [
                r'(?:compare|comparison|vs|versus|against)',
                r'(?:difference|similar|better|worse)',
            ],
            'forecast': [
                r'(?:forecast|prediction|projection|outlook)',
                r'(?:predict|project|estimate|anticipate)',
            ],
        }

    def process_query(self, query: str, enhanced: bool = True) -> ParsedQuery:
        """Process a natural language query into structured components.

        Args:
            query: Natural language query string
            enhanced: Whether to apply query enhancement (synonyms, fuzzy matching)

        Returns:
            Parsed query with extracted entities, filters, and metadata
        """
        if not query or not query.strip():
            return ParsedQuery(
                original_query=query,
                normalized_query="",
                confidence=0.0,
                explanation=["Empty query"]
            )

        # Enhance query with synonyms and fuzzy matching if enabled
        enhanced_query = query
        enhancement_info = {}

        if enhanced:
            enhancement = enhance_search_query(query)
            enhanced_query = " ".join(enhancement['expanded_terms'])
            enhancement_info = {
                'original_terms': self._tokenize_query(query),
                'enhanced_terms': list(enhancement['expanded_terms']),
                'synonym_suggestions': enhancement['synonym_suggestions'],
                'fuzzy_suggestions': enhancement['fuzzy_suggestions'],
                'typo_corrections': enhancement['typo_corrections']
            }

        # Normalize query
        normalized_query = self._normalize_query(enhanced_query)

        # Extract entities
        entities = self.entity_extractor.extract_entities(query)

        # Determine intent
        intent, confidence = self._determine_intent(query)

        # Extract filters and operators
        filters, operators = self._extract_filters_and_operators(query, entities)

        # Extract date ranges
        date_ranges = self._extract_date_ranges(entities)

        # Build explanation
        explanation = self._build_explanation(entities, filters, date_ranges, intent)
        if enhancement_info:
            explanation.append(f"Query enhanced with {len(enhancement_info['enhanced_terms'])} terms")

        return ParsedQuery(
            original_query=query,
            normalized_query=normalized_query,
            entities={entity.type: [e.value for e in entities if e.type == entity.type] for entity in set(e.type for e in entities)},
            filters=filters,
            date_ranges=date_ranges,
            operators=operators,
            intent=intent,
            confidence=confidence,
            explanation=explanation
        )

    def _normalize_query(self, query: str) -> str:
        """Normalize query for better matching."""
        # Remove extra whitespace
        normalized = re.sub(r'\s+', ' ', query.strip())

        # Remove common stop words that don't affect meaning
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        words = normalized.split()
        filtered_words = [word for word in words if word.lower() not in stop_words]

        return ' '.join(filtered_words)

    def _determine_intent(self, query: str) -> Tuple[Optional[str], float]:
        """Determine query intent and confidence."""
        query_lower = query.lower()
        intent_scores = {}

        for intent, patterns in self.intent_patterns.items():
            score = 0.0
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    score += 0.5  # Base score for pattern match

            if score > 0:
                intent_scores[intent] = score

        if intent_scores:
            best_intent = max(intent_scores.items(), key=lambda x: x[1])
            return best_intent[0], min(best_intent[1], 1.0)

        return None, 0.0

    def _extract_filters_and_operators(self, query: str, entities: List[QueryEntity]) -> Tuple[Dict[str, Any], List[str]]:
        """Extract filters and logical operators from query."""
        filters = {}
        operators = []

        query_lower = query.lower()

        # Look for comparison operators
        if 'greater than' in query_lower or '>' in query:
            operators.append('>')
        if 'less than' in query_lower or '<' in query:
            operators.append('<')
        if 'equal to' in query_lower or '=' in query:
            operators.append('=')
        if 'not equal' in query_lower or '!=' in query:
            operators.append('!=')
        if 'between' in query_lower:
            operators.append('between')
        if 'contains' in query_lower or 'like' in query_lower:
            operators.append('contains')

        # Extract field-specific filters
        # Asset class filters
        for entity in entities:
            if entity.type == 'asset_class':
                filters['asset_class'] = entity.value

        # ISO filters
        for entity in entities:
            if entity.type == 'iso':
                filters['iso'] = entity.value

        # Location filters
        for entity in entities:
            if entity.type == 'location':
                filters['location'] = entity.value

        # Date filters
        for entity in entities:
            if entity.type == 'date':
                filters['date'] = entity.value
            elif entity.type == 'date_range':
                # Parse date range
                parts = entity.value.split(' to ')
                if len(parts) == 2:
                    filters['date_from'] = parts[0]
                    filters['date_to'] = parts[1]

        return filters, operators

    def _extract_date_ranges(self, entities: List[QueryEntity]) -> Dict[str, Any]:
        """Extract date ranges from entities."""
        date_ranges = {}

        for entity in entities:
            if entity.type == 'date_range':
                parts = entity.value.split(' to ')
                if len(parts) == 2:
                    date_ranges['gte'] = parts[0]
                    date_ranges['lte'] = parts[1]

        return date_ranges

    def _build_explanation(self, entities: List[QueryEntity], filters: Dict[str, Any],
                          date_ranges: Dict[str, Any], intent: Optional[str]) -> List[str]:
        """Build human-readable explanation of query processing."""
        explanation = []

        if entities:
            entity_types = list(set(e.type for e in entities))
            explanation.append(f"Detected entities: {', '.join(entity_types)}")

        if filters:
            filter_fields = list(filters.keys())
            explanation.append(f"Applied filters: {', '.join(filter_fields)}")

        if date_ranges:
            explanation.append("Applied date range filter")

        if intent:
            explanation.append(f"Query intent: {intent}")

        if not explanation:
            explanation.append("No structured elements detected")

        return explanation

    def build_search_dsl(self, parsed_query: ParsedQuery) -> Dict[str, Any]:
        """Build Elasticsearch DSL from parsed query.

        Args:
            parsed_query: Parsed query result

        Returns:
            Elasticsearch query DSL
        """
        dsl = {
            "query": {
                "bool": {
                    "must": [],
                    "should": [],
                    "filter": [],
                    "must_not": []
                }
            }
        }

        # Add text query
        if parsed_query.normalized_query:
            dsl["query"]["bool"]["must"].append({
                "multi_match": {
                    "query": parsed_query.normalized_query,
                    "fields": ["title^5", "name^3", "description^2", "content_text^1", "tags^2"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            })

        # Add filters
        for field, value in parsed_query.filters.items():
            if field in ['asset_class', 'iso', 'location', 'tags']:
                dsl["query"]["bool"]["filter"].append({
                    "term": {field: value}
                })
            elif field == 'date':
                dsl["query"]["bool"]["filter"].append({
                    "term": {"created_at": value}
                })
            elif field in ['date_from', 'date_to']:
                range_filter = {"range": {"created_at": {}}}
                if 'date_from' in parsed_query.filters:
                    range_filter["range"]["created_at"]["gte"] = parsed_query.filters['date_from']
                if 'date_to' in parsed_query.filters:
                    range_filter["range"]["created_at"]["lte"] = parsed_query.filters['date_to']
                if range_filter["range"]["created_at"]:
                    dsl["query"]["bool"]["filter"].append(range_filter)

        # Add date range filters
        if parsed_query.date_ranges:
            dsl["query"]["bool"]["filter"].append({
                "range": {"created_at": parsed_query.date_ranges}
            })

        return dsl

    def suggest_query_improvements(self, parsed_query: ParsedQuery) -> List[str]:
        """Suggest improvements to make query more effective.

        Args:
            parsed_query: Parsed query result

        Returns:
            List of improvement suggestions
        """
        suggestions = []

        if not parsed_query.entities:
            suggestions.append("Consider including specific terms like ISO codes (ERCOT, PJM) or asset classes (power, gas)")

        if not parsed_query.filters and len(parsed_query.normalized_query.split()) < 3:
            suggestions.append("Try adding filters like location, date range, or asset type for better results")

        if parsed_query.confidence < 0.5:
            suggestions.append("Consider using more specific terms or adding context to help understand your intent")

        return suggestions


# Global query processor instance
_query_processor: Optional[QueryProcessor] = None


def get_query_processor(settings: Optional[AurumSettings] = None) -> QueryProcessor:
    """Get or create global query processor instance.

    Args:
        settings: Application settings. If None, uses global settings.

    Returns:
        Query processor instance
    """
    global _query_processor
    if _query_processor is None:
        _query_processor = QueryProcessor(settings)
    return _query_processor


def parse_query(query: str, settings: Optional[AurumSettings] = None) -> ParsedQuery:
    """Parse a natural language query.

    Args:
        query: Query string to parse
        settings: Application settings. If None, uses global settings.

    Returns:
        Parsed query result
    """
    processor = get_query_processor(settings)
    return processor.process_query(query)


def build_search_dsl_from_query(query: str, settings: Optional[AurumSettings] = None) -> Dict[str, Any]:
    """Build Elasticsearch DSL from natural language query.

    Args:
        query: Natural language query string
        settings: Application settings. If None, uses global settings.

    Returns:
        Elasticsearch query DSL
    """
    processor = get_query_processor(settings)
    parsed = processor.process_query(query)
    return processor.build_search_dsl(parsed)
