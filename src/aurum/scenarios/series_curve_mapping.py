"""Learned series→curve mapping suggestions using similarity on metadata and signals.

This module provides:
- Machine learning models for learning series-to-curve mappings
- Similarity analysis based on metadata and signal patterns
- Automated suggestions for unmapped external series
- Confidence scoring and validation of mappings
- Integration with the existing series_curve_map table
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler

from ..telemetry.context import log_structured


class MappingConfig(BaseModel):
    """Configuration for series-curve mapping."""

    similarity_threshold: float = Field(default=0.7, ge=0.0, le=1.0, description="Minimum similarity score for suggestions")
    max_suggestions: int = Field(default=10, ge=1, description="Maximum number of suggestions to return")
    include_metadata_similarity: bool = Field(default=True, description="Whether to include metadata in similarity calculation")
    include_signal_similarity: bool = Field(default=True, description="Whether to include signal patterns in similarity calculation")
    cross_validation_folds: int = Field(default=5, ge=2, description="Number of CV folds for model validation")


@dataclass
class SeriesMetadata:
    """Metadata for an external series."""

    provider: str
    series_id: str
    name: str
    description: str
    units: str
    frequency: str
    geography: Optional[str] = None
    category: Optional[str] = None
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class CurveInfo:
    """Information about an internal curve."""

    curve_key: str
    name: str
    description: str
    curve_family: str
    units: str
    geography: Optional[str] = None
    category: Optional[str] = None
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class MappingSuggestion:
    """A suggested mapping between series and curve."""

    series_metadata: SeriesMetadata
    curve_info: CurveInfo
    similarity_score: float
    confidence_score: float
    reasoning: Dict[str, Any]
    suggested_by: str = "ml_model"


class SignalPattern:
    """Pattern extracted from time series signals."""

    def __init__(self, data: Union[pd.Series, np.ndarray]):
        self.data = data
        self._analyze_patterns()

    def _analyze_patterns(self):
        """Analyze statistical patterns in the signal."""
        data = self.data if isinstance(self.data, np.ndarray) else self.data.values

        # Basic statistics
        self.mean = np.mean(data)
        self.std = np.std(data)
        self.median = np.median(data)
        self.min_val = np.min(data)
        self.max_val = np.max(data)

        # Trend analysis
        if len(data) > 1:
            self.trend_slope = np.polyfit(np.arange(len(data)), data, 1)[0]
        else:
            self.trend_slope = 0.0

        # Seasonality detection (simplified)
        if len(data) > 24:  # At least one day of hourly data
            self.has_seasonality = self._detect_seasonality(data)
        else:
            self.has_seasonality = False

        # Volatility
        self.volatility = np.std(np.diff(data)) if len(data) > 1 else 0.0

        # Stationarity (simplified ADF test)
        self.is_stationary = self._check_stationarity(data)

    def _detect_seasonality(self, data: np.ndarray) -> bool:
        """Simple seasonality detection."""
        # Use autocorrelation to detect periodic patterns
        if len(data) < 48:  # Need enough data
            return False

        # Check for daily seasonality (24-hour periods)
        from scipy.stats import pearsonr
        if len(data) >= 48:
            corr, _ = pearsonr(data[:-24], data[24:])
            return abs(corr) > 0.5

        return False

    def _check_stationarity(self, data: np.ndarray) -> bool:
        """Simple stationarity check using rolling statistics."""
        if len(data) < 20:
            return True  # Assume stationary for short series

        # Compare first half vs second half statistics
        mid = len(data) // 2
        first_half_mean = np.mean(data[:mid])
        second_half_mean = np.mean(data[mid:])

        # If means are close, consider stationary
        return abs(first_half_mean - second_half_mean) < 0.1 * self.std

    def get_pattern_vector(self) -> np.ndarray:
        """Get numerical representation of the pattern."""
        return np.array([
            self.mean,
            self.std,
            self.median,
            self.min_val,
            self.max_val,
            self.trend_slope,
            1.0 if self.has_seasonality else 0.0,
            self.volatility,
            1.0 if self.is_stationary else 0.0,
        ])


class SeriesCurveMapper:
    """Machine learning model for series-to-curve mapping suggestions."""

    def __init__(self, config: MappingConfig):
        self.config = config
        self.vectorizer = TfidfVectorizer(max_features=100)
        self.scaler = StandardScaler()
        self.classifier = RandomForestClassifier(n_estimators=100, random_state=42)

        # Training data
        self._text_features_fitted = False
        self._pattern_features_fitted = False

    async def train(self, mappings: List[Tuple[SeriesMetadata, CurveInfo]]) -> None:
        """Train the mapping model on known good mappings."""

        if not mappings:
            raise ValueError("Training data is required")

        log_structured("info", "training_series_curve_mapper", num_mappings=len(mappings))

        # Extract features and labels
        X_text, X_pattern, y = self._extract_training_features(mappings)

        # Fit text vectorizer
        if X_text:
            self.vectorizer.fit(X_text)
            self._text_features_fitted = True

        # Fit pattern scaler
        if len(X_pattern) > 0:
            self.scaler.fit(X_pattern)
            self._pattern_features_fitted = True

        # Combine features
        X_combined = self._combine_features(X_text, X_pattern)

        # Train classifier
        self.classifier.fit(X_combined, y)

        # Evaluate model
        scores = cross_val_score(self.classifier, X_combined, y, cv=self.config.cross_validation_folds)
        log_structured(
            "info",
            "mapper_training_completed",
            accuracy_mean=np.mean(scores),
            accuracy_std=np.std(scores),
        )

    def _extract_training_features(self, mappings: List[Tuple[SeriesMetadata, CurveInfo]]) -> Tuple[List[str], np.ndarray, List[str]]:
        """Extract features from training mappings."""

        X_text = []
        X_pattern = []
        y = []

        for series_meta, curve_info in mappings:
            # Text features (combined metadata)
            text_features = self._create_text_features(series_meta, curve_info)
            X_text.append(text_features)

            # Pattern features (would need actual signal data in real implementation)
            # For now, use synthetic patterns based on curve characteristics
            pattern_features = self._create_pattern_features(series_meta, curve_info)
            X_pattern.append(pattern_features)

            # Label is the curve key
            y.append(curve_info.curve_key)

        return X_text, np.array(X_pattern), y

    def _create_text_features(self, series_meta: SeriesMetadata, curve_info: CurveInfo) -> str:
        """Create text features for similarity calculation."""
        text_parts = [
            series_meta.name,
            series_meta.description,
            series_meta.units,
            series_meta.category or "",
            " ".join(series_meta.tags),
            curve_info.name,
            curve_info.description,
            curve_info.curve_family,
            curve_info.category or "",
            " ".join(curve_info.tags),
        ]

        return " ".join(text_parts)

    def _create_pattern_features(self, series_meta: SeriesMetadata, curve_info: CurveInfo) -> np.ndarray:
        """Create pattern features based on series/curve characteristics."""
        # In a real implementation, this would analyze actual time series patterns
        # For now, create features based on metadata characteristics

        features = []

        # Unit compatibility
        unit_compatible = 1.0 if self._are_units_compatible(series_meta.units, curve_info.units) else 0.0
        features.append(unit_compatible)

        # Geography match
        geo_match = 1.0 if series_meta.geography == curve_info.geography else 0.0
        features.append(geo_match)

        # Category match
        cat_match = 1.0 if series_meta.category == curve_info.category else 0.0
        features.append(cat_match)

        # Frequency-based features (simplified)
        freq_score = self._calculate_frequency_score(series_meta.frequency)
        features.append(freq_score)

        # Tag overlap (Jaccard similarity)
        series_tags = set(series_meta.tags)
        curve_tags = set(curve_info.tags)
        tag_overlap = len(series_tags & curve_tags) / len(series_tags | curve_tags) if series_tags or curve_tags else 0.0
        features.append(tag_overlap)

        return np.array(features)

    def _are_units_compatible(self, series_units: str, curve_units: str) -> bool:
        """Check if units are compatible."""
        # Simple unit compatibility check
        compatible_pairs = [
            ("MWh", "MWh"), ("MW", "MW"), ("$/MWh", "$/MWh"),
            ("MW", "MWh"), ("MWh", "MW"),  # Can convert between power and energy
        ]

        return (series_units, curve_units) in compatible_pairs

    def _calculate_frequency_score(self, frequency: str) -> float:
        """Calculate frequency compatibility score."""
        # Map frequency strings to scores
        freq_scores = {
            "hourly": 1.0,
            "H": 1.0,
            "daily": 0.8,
            "D": 0.8,
            "weekly": 0.6,
            "W": 0.6,
            "monthly": 0.4,
            "M": 0.4,
        }

        return freq_scores.get(frequency.lower(), 0.5)

    def _combine_features(self, X_text: List[str], X_pattern: np.ndarray) -> np.ndarray:
        """Combine text and pattern features."""

        if not self._text_features_fitted:
            # If no text features, return pattern features only
            return self.scaler.transform(X_pattern)

        # Transform text features
        X_text_transformed = self.vectorizer.transform(X_text).toarray()

        # Transform pattern features
        X_pattern_transformed = self.scaler.transform(X_pattern)

        # Concatenate features
        return np.concatenate([X_text_transformed, X_pattern_transformed], axis=1)

    async def suggest_mappings(
        self,
        series_metadata: List[SeriesMetadata],
        available_curves: List[CurveInfo],
        existing_mappings: Optional[Dict[str, str]] = None,
    ) -> List[MappingSuggestion]:
        """Suggest mappings for unmapped series."""

        if not self.classifier.fitted_:
            raise RuntimeError("Model must be trained before making suggestions")

        if existing_mappings is None:
            existing_mappings = {}

        suggestions = []

        for series_meta in series_metadata:
            if series_meta.series_id in existing_mappings:
                continue  # Already mapped

            # Find best matching curves
            curve_suggestions = await self._find_best_curves(series_meta, available_curves)

            for curve_info, similarity_score, confidence_score, reasoning in curve_suggestions:
                suggestion = MappingSuggestion(
                    series_metadata=series_meta,
                    curve_info=curve_info,
                    similarity_score=similarity_score,
                    confidence_score=confidence_score,
                    reasoning=reasoning,
                )
                suggestions.append(suggestion)

        # Sort by confidence score and filter by threshold
        suggestions.sort(key=lambda x: x.confidence_score, reverse=True)
        suggestions = [s for s in suggestions if s.similarity_score >= self.config.similarity_threshold]

        # Limit number of suggestions
        suggestions = suggestions[:self.config.max_suggestions]

        log_structured(
            "info",
            "mapping_suggestions_generated",
            num_series=len(series_metadata),
            num_suggestions=len(suggestions),
            avg_confidence=np.mean([s.confidence_score for s in suggestions]) if suggestions else 0,
        )

        return suggestions

    async def _find_best_curves(
        self,
        series_meta: SeriesMetadata,
        available_curves: List[CurveInfo],
    ) -> List[Tuple[CurveInfo, float, float, Dict[str, Any]]]:
        """Find best matching curves for a series."""

        if not available_curves:
            return []

        # Calculate similarity scores for all curves
        similarities = []

        for curve_info in available_curves:
            similarity_score, reasoning = self._calculate_similarity(series_meta, curve_info)
            confidence_score = self._calculate_confidence(similarity_score, reasoning)

            similarities.append((curve_info, similarity_score, confidence_score, reasoning))

        # Sort by similarity score and return top matches
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:5]  # Return top 5

    def _calculate_similarity(self, series_meta: SeriesMetadata, curve_info: CurveInfo) -> Tuple[float, Dict[str, Any]]:
        """Calculate similarity between series and curve."""

        # Text similarity
        text_similarity = 0.0
        text_reasoning = {}

        if self._text_features_fitted:
            text_features = self._create_text_features(series_meta, curve_info)
            X_text = self.vectorizer.transform([text_features]).toarray()

            # Calculate cosine similarity with training examples
            # This is a simplified approach - in reality, you'd compare against known mappings
            text_similarity = 0.5  # Placeholder
            text_reasoning = {"text_similarity": "calculated"}

        # Pattern similarity
        pattern_features = self._create_pattern_features(series_meta, curve_info)
        X_pattern = self.scaler.transform([pattern_features])

        # Combine features and predict
        if self._text_features_fitted:
            X_combined = np.concatenate([X_text, X_pattern], axis=1)
        else:
            X_combined = X_pattern

        # Get prediction probability as similarity measure
        if hasattr(self.classifier, 'predict_proba'):
            proba = self.classifier.predict_proba(X_combined)[0]
            pattern_similarity = np.max(proba)
        else:
            pattern_similarity = 0.5  # Fallback

        # Combine similarities
        if self.config.include_metadata_similarity and self.config.include_signal_similarity:
            overall_similarity = 0.6 * text_similarity + 0.4 * pattern_similarity
        elif self.config.include_metadata_similarity:
            overall_similarity = text_similarity
        elif self.config.include_signal_similarity:
            overall_similarity = pattern_similarity
        else:
            overall_similarity = 0.5

        reasoning = {
            "text_similarity": text_reasoning,
            "pattern_similarity": pattern_similarity,
            "overall_method": "weighted_average",
        }

        return overall_similarity, reasoning

    def _calculate_confidence(self, similarity_score: float, reasoning: Dict[str, Any]) -> float:
        """Calculate confidence score for a mapping suggestion."""

        base_confidence = similarity_score

        # Boost confidence if multiple similarity methods agree
        if (self.config.include_metadata_similarity and
            self.config.include_signal_similarity and
            reasoning.get("text_similarity", {}).get("calculated", False)):
            base_confidence *= 1.2

        # Reduce confidence for edge cases
        if similarity_score < 0.5:
            base_confidence *= 0.8

        return min(1.0, base_confidence)

    async def evaluate_mapping(
        self,
        series_meta: SeriesMetadata,
        curve_info: CurveInfo,
        actual_data: Optional[np.ndarray] = None,
    ) -> Dict[str, Any]:
        """Evaluate the quality of a potential mapping."""

        similarity_score, reasoning = self._calculate_similarity(series_meta, curve_info)
        confidence_score = self._calculate_confidence(similarity_score, reasoning)

        evaluation = {
            "similarity_score": similarity_score,
            "confidence_score": confidence_score,
            "reasoning": reasoning,
            "recommendation": "accept" if confidence_score > 0.7 else "review" if confidence_score > 0.4 else "reject",
        }

        # Add data quality assessment if actual data is provided
        if actual_data is not None:
            data_quality = self._assess_data_quality(actual_data)
            evaluation["data_quality"] = data_quality

            # Adjust confidence based on data quality
            if data_quality["quality_score"] < 0.5:
                evaluation["confidence_score"] *= 0.8

        return evaluation

    def _assess_data_quality(self, data: np.ndarray) -> Dict[str, Any]:
        """Assess quality of time series data."""

        quality_score = 1.0

        # Check for missing values
        missing_ratio = np.isnan(data).sum() / len(data)
        if missing_ratio > 0.1:  # More than 10% missing
            quality_score *= 0.7

        # Check for outliers
        z_scores = np.abs((data - np.mean(data)) / np.std(data))
        outlier_ratio = (z_scores > 3).sum() / len(data)
        if outlier_ratio > 0.05:  # More than 5% outliers
            quality_score *= 0.9

        # Check for constant values
        if np.std(data) == 0:
            quality_score *= 0.5

        return {
            "quality_score": quality_score,
            "missing_ratio": missing_ratio,
            "outlier_ratio": outlier_ratio,
            "has_constant_values": np.std(data) == 0,
        }


class MappingEngine:
    """Main engine for series-curve mapping suggestions."""

    def __init__(self):
        self.mapper = SeriesCurveMapper(MappingConfig())
        self.trained = False

    async def initialize(self, training_mappings: Optional[List[Tuple[SeriesMetadata, CurveInfo]]] = None) -> None:
        """Initialize the mapping engine with training data."""

        if training_mappings:
            await self.mapper.train(training_mappings)
            self.trained = True
        else:
            # Load training data from existing mappings
            # This is a placeholder - in reality, this would query the database
            await self._load_existing_mappings()

    async def _load_existing_mappings(self) -> None:
        """Load existing mappings for training."""
        # Placeholder - would query series_curve_map table
        # For now, create some example training data
        training_data = [
            (
                SeriesMetadata(
                    provider="EIA",
                    series_id="ELEC.GEN.ALL-US-99.H",
                    name="Net Generation",
                    description="Net electricity generation",
                    units="MWh",
                    frequency="hourly",
                    geography="US",
                    category="generation",
                    tags=["electricity", "generation", "total"]
                ),
                CurveInfo(
                    curve_key="us_total_generation",
                    name="US Total Generation",
                    description="Total electricity generation in the US",
                    curve_family="supply",
                    units="MWh",
                    geography="US",
                    category="generation",
                    tags=["electricity", "generation", "total"]
                )
            ),
            # Add more training examples...
        ]

        await self.mapper.train(training_data)
        self.trained = True

    async def suggest_mappings_for_series(
        self,
        series_list: List[SeriesMetadata],
        available_curves: Optional[List[CurveInfo]] = None,
    ) -> List[MappingSuggestion]:
        """Suggest mappings for a list of series."""

        if not self.trained:
            await self.initialize()

        if available_curves is None:
            available_curves = await self._get_available_curves()

        return await self.mapper.suggest_mappings(series_list, available_curves)

    async def _get_available_curves(self) -> List[CurveInfo]:
        """Get list of available curves for mapping."""
        # Placeholder - would query curve catalog
        return [
            CurveInfo(
                curve_key="us_total_generation",
                name="US Total Generation",
                description="Total electricity generation in the US",
                curve_family="supply",
                units="MWh",
                geography="US",
                category="generation",
                tags=["electricity", "generation", "total"]
            ),
            # Add more curves...
        ]

    async def evaluate_potential_mapping(
        self,
        series_meta: SeriesMetadata,
        curve_key: str,
        curve_info: Optional[CurveInfo] = None,
    ) -> Dict[str, Any]:
        """Evaluate a specific series-curve mapping."""

        if curve_info is None:
            available_curves = await self._get_available_curves()
            curve_info = next((c for c in available_curves if c.curve_key == curve_key), None)

        if curve_info is None:
            raise ValueError(f"Curve {curve_key} not found")

        return await self.mapper.evaluate_mapping(series_meta, curve_info)

    def get_mapper_info(self) -> Dict[str, Any]:
        """Get information about the mapper."""
        return {
            "trained": self.trained,
            "config": self.mapper.config.dict(),
            "text_features_fitted": self.mapper._text_features_fitted,
            "pattern_features_fitted": self.mapper._pattern_features_fitted,
        }


# Global mapping engine instance
_mapping_engine: Optional[MappingEngine] = None


def get_mapping_engine() -> MappingEngine:
    """Get the global mapping engine instance."""
    global _mapping_engine
    if _mapping_engine is None:
        _mapping_engine = MappingEngine()
    return _mapping_engine
