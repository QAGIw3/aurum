"""Advanced ranking and relevance tuning for search results.

Provides sophisticated scoring mechanisms including BM25 field boosts,
business signal scoring, temporal decay, popularity scoring, and
learning-to-rank capabilities.
"""

import logging
import math
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from aurum.core.settings import get_settings
from aurum.core import AurumSettings
from .elasticsearch_engine import SearchDocument, SearchResponse, SearchResult


logger = logging.getLogger(__name__)


class ScoringComponent(Enum):
    """Different components that contribute to final relevance score."""
    BM25 = "bm25"
    QUALITY = "quality"
    POPULARITY = "popularity"
    RECENCY = "recency"
    SEMANTIC = "semantic"
    PERSONALIZATION = "personalization"


@dataclass
class ScoreComponent:
    """Individual score component with weight and explanation."""
    component: ScoringComponent
    score: float
    weight: float
    explanation: str = ""


@dataclass
class RelevanceScore:
    """Complete relevance score with breakdown."""
    total_score: float
    components: List[ScoreComponent] = field(default_factory=list)
    explanation: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RankingConfiguration:
    """Configuration for ranking and scoring parameters."""
    # BM25 field boosts
    title_boost: float = 5.0
    name_boost: float = 3.0
    description_boost: float = 2.0
    content_boost: float = 1.0
    tags_boost: float = 2.0

    # Business signal weights
    quality_weight: float = 0.3
    popularity_weight: float = 0.2
    recency_weight: float = 0.1

    # Semantic search weight
    semantic_weight: float = 0.3

    # Temporal decay parameters
    recency_decay_days: int = 30
    recency_decay_rate: float = 0.05

    # Popularity scoring parameters
    popularity_base_score: float = 0.1
    popularity_scale_factor: float = 0.1

    # Quality score normalization
    quality_score_max: float = 1.0
    quality_score_min: float = 0.0


class RelevanceScorer:
    """Computes relevance scores using multiple signals."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize relevance scorer.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.config = RankingConfiguration(
            semantic_weight=self.settings.search_semantic_weight
        )

    def score_document(
        self,
        document: SearchDocument,
        query_terms: List[str],
        semantic_score: float = 0.0,
        user_context: Optional[Dict[str, Any]] = None
    ) -> RelevanceScore:
        """Calculate comprehensive relevance score for a document.

        Args:
            document: Document to score
            query_terms: Terms from the search query
            semantic_score: Semantic similarity score (0-1)
            user_context: Optional user context for personalization

        Returns:
            Complete relevance score with breakdown
        """
        components = []

        # BM25 score (simulated based on field matches)
        bm25_score = self._calculate_bm25_score(document, query_terms)
        components.append(ScoreComponent(
            component=ScoringComponent.BM25,
            score=bm25_score,
            weight=1.0,
            explanation=f"BM25 score based on field matches"
        ))

        # Quality score
        quality_score = self._calculate_quality_score(document)
        components.append(ScoreComponent(
            component=ScoringComponent.QUALITY,
            score=quality_score,
            weight=self.config.quality_weight,
            explanation=f"Quality score: {document.quality_score or 0}"
        ))

        # Popularity score
        popularity_score = self._calculate_popularity_score(document)
        components.append(ScoreComponent(
            component=ScoringComponent.POPULARITY,
            score=popularity_score,
            weight=self.config.popularity_weight,
            explanation=f"Popularity score: {document.popularity_score or 0}"
        ))

        # Recency score
        recency_score = self._calculate_recency_score(document)
        components.append(ScoreComponent(
            component=ScoringComponent.RECENCY,
            score=recency_score,
            weight=self.config.recency_weight,
            explanation=f"Recency boost based on {document.updated_at or document.created_at}"
        ))

        # Semantic score
        components.append(ScoreComponent(
            component=ScoringComponent.SEMANTIC,
            score=semantic_score,
            weight=self.config.semantic_weight,
            explanation=f"Semantic similarity: {semantic_score}"
        ))

        # Calculate total score using weighted sum
        total_score = sum(
            component.score * component.weight
            for component in components
        )

        return RelevanceScore(
            total_score=total_score,
            components=components,
            explanation={
                "query_terms": query_terms,
                "semantic_score": semantic_score,
                "document_signals": {
                    "quality_score": document.quality_score,
                    "popularity_score": document.popularity_score,
                    "created_at": document.created_at,
                    "updated_at": document.updated_at
                }
            }
        )

    def _calculate_bm25_score(self, document: SearchDocument, query_terms: List[str]) -> float:
        """Calculate BM25-style score based on field matches."""
        score = 0.0

        # Simulate BM25 scoring based on field content
        all_text = []
        if document.title:
            all_text.append(document.title)
        if document.name:
            all_text.append(document.name)
        if document.description:
            all_text.append(document.description)
        if document.content_text:
            all_text.append(document.content_text)
        if document.tags:
            all_text.extend(document.tags)

        combined_text = " ".join(all_text).lower()

        # Count term matches
        total_matches = 0
        for term in query_terms:
            if term.lower() in combined_text:
                total_matches += 1

        # Calculate score based on matches and field boosts
        if query_terms:
            match_ratio = total_matches / len(query_terms)
            # Apply field boosts (simplified)
            field_boost = 1.0
            if document.title and any(term.lower() in document.title.lower() for term in query_terms):
                field_boost *= self.config.title_boost
            if document.tags and any(term.lower() in " ".join(document.tags).lower() for term in query_terms):
                field_boost *= self.config.tags_boost

            score = match_ratio * field_boost * 10.0  # Scale to reasonable range

        return min(score, 20.0)  # Cap at reasonable max

    def _calculate_quality_score(self, document: SearchDocument) -> float:
        """Calculate quality-based score."""
        if document.quality_score is None:
            return 0.0

        # Normalize quality score to 0-1 range
        normalized = max(0.0, min(1.0, document.quality_score))
        return normalized

    def _calculate_popularity_score(self, document: SearchDocument) -> float:
        """Calculate popularity-based score."""
        if document.popularity_score is None:
            return self.config.popularity_base_score

        # Apply scaling and ensure reasonable bounds
        scaled = self.config.popularity_base_score + (
            document.popularity_score * self.config.popularity_scale_factor
        )
        return max(0.0, min(1.0, scaled))

    def _calculate_recency_score(self, document: SearchDocument) -> float:
        """Calculate recency-based score with temporal decay."""
        reference_date = document.updated_at or document.created_at
        if not reference_date:
            return 0.0

        # Calculate days since last update
        days_old = (datetime.now() - reference_date).days

        # Apply exponential decay
        if days_old <= 0:
            return 1.0
        elif days_old > self.config.recency_decay_days:
            return 0.0
        else:
            # Exponential decay: score = e^(-rate * days)
            decay_factor = math.exp(-self.config.recency_decay_rate * days_old)
            return decay_factor


class LearningToRank:
    """Learning to rank capabilities for search results."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize LTR system.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.feature_weights = self._load_feature_weights()

    def _load_feature_weights(self) -> Dict[str, float]:
        """Load feature weights for ranking model."""
        # Default weights - in production this would be learned
        return {
            'bm25_score': 0.4,
            'quality_score': 0.25,
            'popularity_score': 0.15,
            'recency_score': 0.1,
            'semantic_score': 0.1
        }

    def rank_results(
        self,
        results: List[SearchResult],
        query_terms: List[str],
        semantic_scores: Optional[Dict[str, float]] = None
    ) -> List[SearchResult]:
        """Apply learning-to-rank to search results.

        Args:
            results: Search results to rank
            query_terms: Terms from the original query
            semantic_scores: Optional semantic scores by document ID

        Returns:
            Re-ranked results
        """
        # Calculate LTR scores for each result
        for result in results:
            ltr_score = self._calculate_ltr_score(
                result, query_terms, semantic_scores
            )
            result.score = ltr_score

        # Sort by LTR score
        results.sort(key=lambda r: r.score, reverse=True)

        # Update ranks
        for i, result in enumerate(results):
            result.rank = i + 1

        return results

    def _calculate_ltr_score(
        self,
        result: SearchResult,
        query_terms: List[str],
        semantic_scores: Optional[Dict[str, float]]
    ) -> float:
        """Calculate LTR score for a single result."""
        document = result.document
        scorer = RelevanceScorer(self.settings)

        # Get semantic score
        semantic_score = 0.0
        if semantic_scores and document.id in semantic_scores:
            semantic_score = semantic_scores[document.id]

        # Calculate individual component scores
        relevance_score = scorer.score_document(document, query_terms, semantic_score)

        # Apply LTR feature weights
        ltr_score = 0.0
        for component in relevance_score.components:
            weight = self.feature_weights.get(component.component.value, 0.0)
            ltr_score += component.score * weight

        return ltr_score

    def update_weights(self, training_data: List[Dict[str, Any]]) -> Dict[str, float]:
        """Update LTR model weights based on training data.

        Args:
            training_data: Training examples with features and labels

        Returns:
            Updated feature weights
        """
        # Simplified weight update - in production would use gradient descent
        # or other optimization algorithms

        if not training_data:
            return self.feature_weights

        # Calculate gradients (simplified)
        gradients = {feature: 0.0 for feature in self.feature_weights}

        for example in training_data:
            predicted_score = sum(
                example.get(f'{feature}_score', 0.0) * weight
                for feature, weight in self.feature_weights.items()
            )

            actual_score = example.get('relevance_label', 0.0)
            error = actual_score - predicted_score

            # Update gradients
            for feature in self.feature_weights:
                feature_score = example.get(f'{feature}_score', 0.0)
                gradients[feature] += error * feature_score

        # Update weights (simple gradient descent)
        learning_rate = 0.01
        for feature in self.feature_weights:
            self.feature_weights[feature] += learning_rate * gradients[feature]

        # Ensure weights are in reasonable range
        for feature in self.feature_weights:
            self.feature_weights[feature] = max(0.0, min(1.0, self.feature_weights[feature]))

        return self.feature_weights.copy()


class RelevanceTuner:
    """Tunes and optimizes relevance scoring parameters."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize relevance tuner.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.config = RankingConfiguration()

    def tune_parameters(
        self,
        evaluation_queries: List[Dict[str, Any]]
    ) -> RankingConfiguration:
        """Tune ranking parameters based on evaluation queries.

        Args:
            evaluation_queries: Queries with expected relevant documents

        Returns:
            Optimized ranking configuration
        """
        # Simplified parameter tuning - in production would use
        # more sophisticated optimization algorithms

        best_config = self.config
        best_score = 0.0

        # Try different parameter combinations
        for quality_weight in [0.2, 0.3, 0.4, 0.5]:
            for popularity_weight in [0.1, 0.2, 0.3]:
                for recency_weight in [0.05, 0.1, 0.15]:
                    # Ensure weights sum to reasonable value
                    total_weight = quality_weight + popularity_weight + recency_weight
                    if total_weight > 0.8:  # Leave room for BM25 and semantic
                        continue

                    # Create test config
                    test_config = RankingConfiguration(
                        quality_weight=quality_weight,
                        popularity_weight=popularity_weight,
                        recency_weight=recency_weight
                    )

                    # Evaluate config
                    score = self._evaluate_config(test_config, evaluation_queries)
                    if score > best_score:
                        best_score = score
                        best_config = test_config

        logger.info(f"Tuned parameters - Quality: {best_config.quality_weight}, "
                   f"Popularity: {best_config.popularity_weight}, "
                   f"Recency: {best_config.recency_weight}")

        return best_config

    def _evaluate_config(
        self,
        config: RankingConfiguration,
        evaluation_queries: List[Dict[str, Any]]
    ) -> float:
        """Evaluate a configuration against test queries."""
        # Simplified evaluation - in production would use NDCG, MAP, etc.
        total_score = 0.0

        for query_data in evaluation_queries:
            query = query_data['query']
            expected_docs = query_data.get('relevant_docs', [])

            # This would run actual searches and compare results
            # For now, return a mock score
            mock_score = 0.5  # Placeholder
            total_score += mock_score

        return total_score / len(evaluation_queries) if evaluation_queries else 0.0


class RankingService:
    """Main service for document ranking and relevance tuning."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize ranking service.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.scorer = RelevanceScorer(settings)
        self.ltr = LearningToRank(settings)
        self.tuner = RelevanceTuner(settings)

    def rank_search_results(
        self,
        response: SearchResponse,
        query_terms: List[str],
        semantic_scores: Optional[Dict[str, float]] = None,
        use_ltr: bool = False
    ) -> SearchResponse:
        """Rank search results using advanced scoring.

        Args:
            response: Original search response
            query_terms: Terms from the search query
            semantic_scores: Optional semantic similarity scores
            use_ltr: Whether to use learning-to-rank

        Returns:
            Re-ranked search response
        """
        if use_ltr:
            # Use learning-to-rank
            response.results = self.ltr.rank_results(
                response.results, query_terms, semantic_scores
            )
        else:
            # Use traditional relevance scoring
            for result in response.results:
                semantic_score = 0.0
                if semantic_scores and result.document.id in semantic_scores:
                    semantic_score = semantic_scores[result.document.id]

                relevance_score = self.scorer.score_document(
                    result.document, query_terms, semantic_score
                )
                result.score = relevance_score.total_score

            # Sort by relevance score
            response.results.sort(key=lambda r: r.score, reverse=True)

            # Update ranks
            for i, result in enumerate(response.results):
                result.rank = i + 1

        return response

    def calculate_relevance_breakdown(
        self,
        document: SearchDocument,
        query_terms: List[str],
        semantic_score: float = 0.0
    ) -> RelevanceScore:
        """Calculate detailed relevance score breakdown.

        Args:
            document: Document to analyze
            query_terms: Query terms for context
            semantic_score: Semantic similarity score

        Returns:
            Detailed relevance score with component breakdown
        """
        return self.scorer.score_document(document, query_terms, semantic_score)

    def tune_ranking_parameters(
        self,
        evaluation_queries: List[Dict[str, Any]]
    ) -> RankingConfiguration:
        """Tune ranking parameters using evaluation data.

        Args:
            evaluation_queries: Queries with expected relevant documents

        Returns:
            Optimized ranking configuration
        """
        return self.tuner.tune_parameters(evaluation_queries)


# Global service instance
_ranking_service: Optional[RankingService] = None


def get_ranking_service(settings: Optional[AurumSettings] = None) -> RankingService:
    """Get or create global ranking service.

    Args:
        settings: Application settings

    Returns:
        Ranking service instance
    """
    global _ranking_service
    if _ranking_service is None:
        _ranking_service = RankingService(settings)
    return _ranking_service


def rank_search_results(
    response: SearchResponse,
    query_terms: List[str],
    semantic_scores: Optional[Dict[str, float]] = None,
    use_ltr: bool = False,
    settings: Optional[AurumSettings] = None
) -> SearchResponse:
    """Rank search results using advanced scoring.

    Args:
        response: Original search response
        query_terms: Terms from the search query
        semantic_scores: Optional semantic similarity scores
        use_ltr: Whether to use learning-to-rank
        settings: Application settings

    Returns:
        Re-ranked search response
    """
    service = get_ranking_service(settings)
    return service.rank_search_results(response, query_terms, semantic_scores, use_ltr)
