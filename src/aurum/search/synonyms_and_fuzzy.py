"""Synonyms and fuzziness support for enhanced search.

Provides domain-specific synonym expansion, fuzzy matching,
and phonetic search capabilities to improve query recall and user experience.
"""

import logging
import re
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from fuzzywuzzy import fuzz, process
import difflib

from aurum.core.settings import get_settings
from aurum.core import AurumSettings


logger = logging.getLogger(__name__)


@dataclass
class SynonymRule:
    """Represents a synonym rule with directionality."""
    term: str
    synonyms: List[str]
    bidirectional: bool = True
    weight: float = 1.0  # Weight for synonym matches vs exact matches


@dataclass
class FuzzyMatch:
    """Result of fuzzy matching."""
    term: str
    matched_term: str
    similarity: float
    match_type: str = "fuzzy"  # fuzzy, phonetic, typo


class SynonymManager:
    """Manages domain-specific synonyms for search enhancement."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize synonym manager.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.synonym_rules = self._load_synonym_rules()

    def _load_synonym_rules(self) -> Dict[str, SynonymRule]:
        """Load domain-specific synonym rules."""
        rules = {}

        # Energy trading domain synonyms
        energy_synonyms = {
            'power': SynonymRule('power', ['electricity', 'energy', 'electric'], bidirectional=True),
            'gas': SynonymRule('gas', ['natural gas', 'ng', 'methane'], bidirectional=True),
            'oil': SynonymRule('oil', ['crude oil', 'petroleum', 'black gold'], bidirectional=True),
            'renewables': SynonymRule('renewables', ['renewable energy', 'green energy', 'clean energy', 'solar', 'wind', 'hydro'], bidirectional=True),
            'demand': SynonymRule('demand', ['load', 'consumption', 'usage'], bidirectional=True),
            'supply': SynonymRule('supply', ['generation', 'production', 'output'], bidirectional=True),
            'price': SynonymRule('price', ['cost', 'rate', 'tariff', 'pricing'], bidirectional=True),
            'forecast': SynonymRule('forecast', ['prediction', 'projection', 'outlook', 'estimate'], bidirectional=True),
            'capacity': SynonymRule('capacity', ['capability', 'size', 'volume', 'scale'], bidirectional=True),
            'grid': SynonymRule('grid', ['network', 'system', 'infrastructure'], bidirectional=True),
            'transmission': SynonymRule('transmission', ['lines', 'wires', 'cables'], bidirectional=False),
            'distribution': SynonymRule('distribution', ['delivery', 'supply', 'provision'], bidirectional=True),
            'market': SynonymRule('market', ['trading', 'exchange', 'auction'], bidirectional=True),
            'contract': SynonymRule('contract', ['agreement', 'deal', 'pact'], bidirectional=True),
            'hedge': SynonymRule('hedge', ['protection', 'insurance', 'safeguard'], bidirectional=True),
            'risk': SynonymRule('risk', ['exposure', 'vulnerability', 'threat'], bidirectional=True),
        }

        # Add energy synonyms
        rules.update(energy_synonyms)

        # Regional synonyms
        regional_synonyms = {
            'texas': SynonymRule('texas', ['tx', 'lone star state'], bidirectional=True),
            'california': SynonymRule('california', ['ca', 'golden state'], bidirectional=True),
            'ercot': SynonymRule('ercot', ['electric reliability council of texas'], bidirectional=False),
            'pjm': SynonymRule('pjm', ['pennsylvania-new jersey-maryland interconnection'], bidirectional=False),
            'miso': SynonymRule('miso', ['midcontinent independent system operator'], bidirectional=False),
            'caiso': SynonymRule('caiso', ['california independent system operator'], bidirectional=False),
        }

        rules.update(regional_synonyms)

        # Asset class synonyms
        asset_synonyms = {
            'generation': SynonymRule('generation', ['power plants', 'facilities', 'installations'], bidirectional=True),
            'load': SynonymRule('load', ['demand', 'consumption', 'usage'], bidirectional=True),
            'storage': SynonymRule('storage', ['batteries', 'energy storage', 'pumped hydro'], bidirectional=True),
            'transmission': SynonymRule('transmission', ['lines', 'grid', 'network'], bidirectional=True),
            'distribution': SynonymRule('distribution', ['utilities', 'local grids', 'retail'], bidirectional=True),
        }

        rules.update(asset_synonyms)

        return rules

    def expand_query_with_synonyms(
        self,
        query: str,
        max_synonyms_per_term: int = 3
    ) -> List[str]:
        """Expand query with synonyms.

        Args:
            query: Original query string
            max_synonyms_per_term: Maximum synonyms to add per term

        Returns:
            List of expanded query terms
        """
        expanded_terms = set()
        query_terms = self._tokenize_query(query)

        for term in query_terms:
            term_lower = term.lower()

            # Add original term
            expanded_terms.add(term)

            # Find synonym rules for this term
            if term_lower in self.synonym_rules:
                rule = self.synonym_rules[term_lower]
                synonyms_to_add = rule.synonyms[:max_synonyms_per_term]

                # Add synonyms
                expanded_terms.update(synonyms_to_add)

                # If bidirectional, also add original term as synonym for each synonym
                if rule.bidirectional:
                    for synonym in synonyms_to_add:
                        expanded_terms.add(synonym)

        return list(expanded_terms)

    def _tokenize_query(self, query: str) -> List[str]:
        """Tokenize query into individual terms."""
        # Simple tokenization - split on whitespace and punctuation
        tokens = re.findall(r'\b\w+\b', query.lower())
        return tokens

    def get_synonym_suggestions(
        self,
        term: str,
        max_suggestions: int = 5
    ) -> List[Tuple[str, float]]:
        """Get synonym suggestions for a term.

        Args:
            term: Term to find synonyms for
            max_suggestions: Maximum suggestions to return

        Returns:
            List of (synonym, confidence) tuples
        """
        term_lower = term.lower()
        suggestions = []

        if term_lower in self.synonym_rules:
            rule = self.synonym_rules[term_lower]
            for synonym in rule.synonyms:
                suggestions.append((synonym, rule.weight))

        # Also check if term appears as synonym in other rules
        for rule_term, rule in self.synonym_rules.items():
            if term_lower in rule.synonyms:
                suggestions.append((rule_term, rule.weight * 0.8))  # Slightly lower weight

        # Sort by weight and limit
        suggestions.sort(key=lambda x: x[1], reverse=True)
        return suggestions[:max_suggestions]


class FuzzyMatcher:
    """Handles fuzzy matching and typo correction."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize fuzzy matcher.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()

        # Common energy trading terms for fuzzy matching
        self.energy_terms = {
            'electricity', 'power', 'energy', 'gas', 'natural gas', 'oil', 'renewables',
            'solar', 'wind', 'hydro', 'nuclear', 'coal', 'demand', 'supply', 'price',
            'forecast', 'capacity', 'grid', 'transmission', 'distribution', 'market',
            'trading', 'contract', 'hedge', 'risk', 'ercot', 'pjm', 'miso', 'caiso',
            'texas', 'california', 'new york', 'generation', 'load', 'storage'
        }

    def find_fuzzy_matches(
        self,
        query: str,
        min_similarity: float = 70.0,
        max_suggestions: int = 5
    ) -> List[FuzzyMatch]:
        """Find fuzzy matches for query terms.

        Args:
            query: Query string to match
            min_similarity: Minimum similarity threshold (0-100)
            max_suggestions: Maximum suggestions per term

        Returns:
            List of fuzzy matches
        """
        matches = []
        query_terms = self._tokenize_query(query)

        for term in query_terms:
            if len(term) < 3:  # Skip very short terms
                continue

            # Find best matches from energy terms
            best_matches = process.extract(
                term,
                self.energy_terms,
                limit=max_suggestions * 2,  # Get extra for filtering
                scorer=fuzz.token_sort_ratio
            )

            for match_term, similarity in best_matches:
                if similarity >= min_similarity:
                    matches.append(FuzzyMatch(
                        term=term,
                        matched_term=match_term,
                        similarity=similarity / 100.0,  # Normalize to 0-1
                        match_type="fuzzy"
                    ))

        # Remove duplicates and sort by similarity
        seen = set()
        unique_matches = []

        for match in sorted(matches, key=lambda m: m.similarity, reverse=True):
            key = (match.term, match.matched_term)
            if key not in seen:
                seen.add(key)
                unique_matches.append(match)

        return unique_matches[:max_suggestions]

    def correct_typos(
        self,
        query: str,
        min_similarity: float = 80.0
    ) -> List[str]:
        """Suggest typo corrections for query.

        Args:
            query: Query string to correct
            min_similarity: Minimum similarity for suggestions

        Returns:
            List of corrected query suggestions
        """
        suggestions = []
        query_terms = self._tokenize_query(query)

        for i, term in enumerate(query_terms):
            if len(term) < 4:  # Skip short terms
                continue

            # Find similar terms
            similar_terms = difflib.get_close_matches(
                term,
                self.energy_terms,
                n=3,
                cutoff=min_similarity / 100.0
            )

            for similar_term in similar_terms:
                # Create corrected query
                corrected_terms = query_terms.copy()
                corrected_terms[i] = similar_term
                corrected_query = " ".join(corrected_terms)

                suggestions.append(corrected_query)

        return suggestions

    def _tokenize_query(self, query: str) -> List[str]:
        """Tokenize query into terms."""
        return re.findall(r'\b\w+\b', query.lower())


class PhoneticMatcher:
    """Handles phonetic matching for sound-alike terms."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize phonetic matcher.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()

        # Sound-alike mappings for energy terms
        self.phonetic_map = {
            'electricity': ['electricity', 'electricite'],
            'capacity': ['capacity', 'capacit'],
            'transmission': ['transmission', 'transmition'],
            'distribution': ['distribution', 'distribushun'],
            'generation': ['generation', 'generashun'],
            'renewables': ['renewables', 'renewabls'],
        }

    def find_phonetic_matches(
        self,
        term: str,
        min_similarity: float = 0.6
    ) -> List[FuzzyMatch]:
        """Find phonetic matches for a term.

        Args:
            term: Term to match
            min_similarity: Minimum similarity threshold

        Returns:
            List of phonetic matches
        """
        matches = []

        for canonical_term, variants in self.phonetic_map.items():
            for variant in variants:
                similarity = fuzz.ratio(term.lower(), variant.lower()) / 100.0
                if similarity >= min_similarity:
                    matches.append(FuzzyMatch(
                        term=term,
                        matched_term=canonical_term,
                        similarity=similarity,
                        match_type="phonetic"
                    ))

        return matches


class QueryExpander:
    """Expands queries using synonyms, fuzzy matching, and related terms."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize query expander.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.synonym_manager = SynonymManager(settings)
        self.fuzzy_matcher = FuzzyMatcher(settings)
        self.phonetic_matcher = PhoneticMatcher(settings)

    def expand_query(
        self,
        query: str,
        include_synonyms: bool = True,
        include_fuzzy: bool = True,
        include_phonetic: bool = True,
        min_similarity: float = 70.0
    ) -> Dict[str, Any]:
        """Expand query with synonyms and fuzzy matches.

        Args:
            query: Original query string
            include_synonyms: Whether to include synonyms
            include_fuzzy: Whether to include fuzzy matches
            include_phonetic: Whether to include phonetic matches
            min_similarity: Minimum similarity for fuzzy/phonetic matches

        Returns:
            Dictionary with expanded terms and suggestions
        """
        result = {
            'original_query': query,
            'expanded_terms': set(),
            'synonym_suggestions': [],
            'fuzzy_suggestions': [],
            'phonetic_suggestions': [],
            'typo_corrections': []
        }

        # Add original query terms
        original_terms = self.synonym_manager._tokenize_query(query)
        result['expanded_terms'].update(original_terms)

        # Add synonyms
        if include_synonyms:
            synonym_terms = self.synonym_manager.expand_query_with_synonyms(query)
            result['expanded_terms'].update(synonym_terms)

            # Get synonym suggestions for each term
            for term in original_terms:
                suggestions = self.synonym_manager.get_synonym_suggestions(term)
                result['synonym_suggestions'].extend([
                    {'term': term, 'suggestion': sugg[0], 'confidence': sugg[1]}
                    for sugg in suggestions
                ])

        # Add fuzzy matches
        if include_fuzzy:
            fuzzy_matches = self.fuzzy_matcher.find_fuzzy_matches(
                query, min_similarity, max_suggestions=10
            )
            result['fuzzy_suggestions'] = [
                {
                    'original': match.term,
                    'suggestion': match.matched_term,
                    'similarity': match.similarity,
                    'type': match.match_type
                }
                for match in fuzzy_matches
            ]
            result['expanded_terms'].update([match.matched_term for match in fuzzy_matches])

        # Add phonetic matches
        if include_phonetic:
            for term in original_terms:
                phonetic_matches = self.phonetic_matcher.find_phonetic_matches(
                    term, min_similarity
                )
                result['phonetic_suggestions'].extend([
                    {
                        'original': match.term,
                        'suggestion': match.matched_term,
                        'similarity': match.similarity,
                        'type': match.match_type
                    }
                    for match in phonetic_matches
                ])
                result['expanded_terms'].update([match.matched_term for match in phonetic_matches])

        # Add typo corrections
        typo_corrections = self.fuzzy_matcher.correct_typos(query, min_similarity)
        result['typo_corrections'] = typo_corrections

        return result

    def suggest_query_improvements(
        self,
        query: str,
        max_suggestions: int = 5
    ) -> List[str]:
        """Suggest query improvements based on expansion analysis.

        Args:
            query: Original query
            max_suggestions: Maximum suggestions to return

        Returns:
            List of improved query suggestions
        """
        expansion = self.expand_query(query)
        suggestions = []

        # Add typo corrections as top suggestions
        suggestions.extend(expansion['typo_corrections'])

        # Add fuzzy suggestions
        for fuzzy_sugg in expansion['fuzzy_suggestions'][:max_suggestions]:
            improved_query = query.replace(
                fuzzy_sugg['original'],
                fuzzy_sugg['suggestion']
            )
            if improved_query not in suggestions:
                suggestions.append(improved_query)

        # Add synonym-based suggestions
        for synonym_sugg in expansion['synonym_suggestions'][:max_suggestions]:
            if synonym_sugg['confidence'] > 0.7:
                improved_query = query.replace(
                    synonym_sugg['term'],
                    synonym_sugg['suggestion']
                )
                if improved_query not in suggestions:
                    suggestions.append(improved_query)

        return suggestions[:max_suggestions]


class SearchEnhancementService:
    """Main service for query enhancement with synonyms and fuzziness."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize search enhancement service.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.expander = QueryExpander(settings)

    def enhance_search_query(
        self,
        query: str,
        include_synonyms: bool = True,
        include_fuzzy: bool = True,
        include_phonetic: bool = True
    ) -> Dict[str, Any]:
        """Enhance search query with expansions and suggestions.

        Args:
            query: Original search query
            include_synonyms: Whether to include synonyms
            include_fuzzy: Whether to include fuzzy matches
            include_phonetic: Whether to include phonetic matches

        Returns:
            Enhanced query with expansions and suggestions
        """
        return self.expander.expand_query(
            query=query,
            include_synonyms=include_synonyms,
            include_fuzzy=include_fuzzy,
            include_phonetic=include_phonetic
        )

    def get_query_suggestions(
        self,
        query: str,
        max_suggestions: int = 5
    ) -> List[str]:
        """Get query improvement suggestions.

        Args:
            query: Original query
            max_suggestions: Maximum suggestions to return

        Returns:
            List of suggested query improvements
        """
        return self.expander.suggest_query_improvements(query, max_suggestions)


# Global service instance
_enhancement_service: Optional[SearchEnhancementService] = None


def get_search_enhancement_service(settings: Optional[AurumSettings] = None) -> SearchEnhancementService:
    """Get or create global search enhancement service.

    Args:
        settings: Application settings

    Returns:
        Search enhancement service instance
    """
    global _enhancement_service
    if _enhancement_service is None:
        _enhancement_service = SearchEnhancementService(settings)
    return _enhancement_service


def enhance_search_query(
    query: str,
    settings: Optional[AurumSettings] = None,
    **kwargs
) -> Dict[str, Any]:
    """Enhance search query with synonyms and fuzzy matching.

    Args:
        query: Original search query
        settings: Application settings
        **kwargs: Additional enhancement parameters

    Returns:
        Enhanced query with expansions and suggestions
    """
    service = get_search_enhancement_service(settings)
    return service.enhance_search_query(query, **kwargs)


def get_query_suggestions(
    query: str,
    max_suggestions: int = 5,
    settings: Optional[AurumSettings] = None
) -> List[str]:
    """Get query improvement suggestions.

    Args:
        query: Original query
        max_suggestions: Maximum suggestions to return
        settings: Application settings

    Returns:
        List of suggested query improvements
    """
    service = get_search_enhancement_service(settings)
    return service.get_query_suggestions(query, max_suggestions)
