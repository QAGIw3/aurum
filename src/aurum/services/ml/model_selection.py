"""Model Selection Service.

This service handles champion model selection algorithms, including
multi-criteria selection, promotion logic, and champion history tracking.

Extracted from the monolithic model_registry_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from uuid import uuid4

from pydantic import BaseModel, Field

from src.aurum.services.base import BaseService
from src.aurum.data.repositories.base import BaseRepository


class ChampionSelectionCriteria(BaseModel):
    """Criteria for selecting champion models."""
    
    primary_metric: str = "accuracy"
    min_performance_threshold: float = 0.0
    max_model_size_bytes: Optional[int] = None
    max_inference_time_ms: Optional[float] = None
    required_features: Optional[Set[str]] = None
    excluded_versions: Optional[Set[str]] = None
    prefer_recent: bool = True
    recent_days: int = 30
    multi_criteria_weights: Dict[str, float] = Field(default_factory=lambda: {
        "performance": 0.5,
        "efficiency": 0.2,
        "recency": 0.2,
        "stability": 0.1
    })


class ChampionChallengerSelection(BaseModel):
    """Selection pairing of champion and challenger candidates."""
    
    selection_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    champion_version_id: Optional[str] = None
    challenger_version_id: Optional[str] = None
    selection_criteria: ChampionSelectionCriteria
    selection_scores: Dict[str, float] = Field(default_factory=dict)
    selected_at: datetime = Field(default_factory=datetime.utcnow)
    selected_by: str = "system"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ChampionHistory(BaseModel):
    """Historical record of champion model changes."""
    
    history_id: str = Field(default_factory=lambda: str(uuid4()))
    model_name: str
    version_id: str
    promoted_at: datetime
    promoted_by: str
    demoted_at: Optional[datetime] = None
    reason: str
    performance_at_promotion: Dict[str, float] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SelectionRepository(BaseRepository):
    """Repository interface for model selection operations."""
    
    async def save_selection(self, selection: ChampionChallengerSelection) -> ChampionChallengerSelection:
        """Save a champion/challenger selection."""
        raise NotImplementedError
    
    async def get_selection(self, selection_id: str) -> Optional[ChampionChallengerSelection]:
        """Get a selection by ID."""
        raise NotImplementedError
    
    async def save_history(self, history: ChampionHistory) -> ChampionHistory:
        """Save champion history record."""
        raise NotImplementedError
    
    async def get_champion_history(
        self,
        model_name: str,
        limit: int = 100
    ) -> List[ChampionHistory]:
        """Get champion history for a model."""
        raise NotImplementedError
    
    async def get_current_champion(self, model_name: str) -> Optional[str]:
        """Get current champion version ID."""
        raise NotImplementedError
    
    async def set_current_champion(self, model_name: str, version_id: str) -> bool:
        """Set current champion version."""
        raise NotImplementedError


class ModelSelectionService(BaseService):
    """
    Champion model selection service.
    
    This service handles the algorithms and logic for selecting champion models,
    managing promotions, and tracking champion history.
    """
    
    def __init__(
        self,
        repository: Optional[SelectionRepository] = None,
        model_registry_client: Optional[Any] = None,  # Interface to model registry
        cache_enabled: bool = True,
        cache_ttl: int = 300
    ):
        """
        Initialize the model selection service.
        
        Args:
            repository: Repository for data persistence
            model_registry_client: Client to access model versions
            cache_enabled: Enable caching for read operations
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_default_repository()
        self.model_registry = model_registry_client  # In real impl, inject from DI
        self.logger = logging.getLogger(__name__)
        
        # In-memory state
        self._champion_cache: Dict[str, str] = {}
        self._selection_history: Dict[str, List[ChampionChallengerSelection]] = {}
    
    def _get_default_repository(self) -> SelectionRepository:
        """Get default repository from DI container."""
        # TODO: Integrate with DI container
        # For now, return a mock repository
        class MockRepository(SelectionRepository):
            def __init__(self):
                self.selections = {}
                self.history = {}
                self.champions = {}
            
            async def save_selection(self, selection: ChampionChallengerSelection) -> ChampionChallengerSelection:
                self.selections[selection.selection_id] = selection
                return selection
            
            async def get_selection(self, selection_id: str) -> Optional[ChampionChallengerSelection]:
                return self.selections.get(selection_id)
            
            async def save_history(self, history: ChampionHistory) -> ChampionHistory:
                if history.model_name not in self.history:
                    self.history[history.model_name] = []
                self.history[history.model_name].append(history)
                return history
            
            async def get_champion_history(self, model_name: str, limit: int = 100) -> List[ChampionHistory]:
                return self.history.get(model_name, [])[-limit:]
            
            async def get_current_champion(self, model_name: str) -> Optional[str]:
                return self.champions.get(model_name)
            
            async def set_current_champion(self, model_name: str, version_id: str) -> bool:
                self.champions[model_name] = version_id
                return True
        
        return MockRepository()
    
    async def select_champion_model(
        self,
        model_name: str,
        available_versions: List[Dict[str, Any]],
        selection_criteria: Optional[ChampionSelectionCriteria] = None,
        auto_promote: bool = False
    ) -> Optional[ChampionChallengerSelection]:
        """
        Select the best model version as champion based on criteria.
        
        Args:
            model_name: Name of the model
            available_versions: List of version metadata dicts
            selection_criteria: Criteria for selection
            auto_promote: Automatically promote selected champion
            
        Returns:
            ChampionChallengerSelection with selected champion/challenger
        """
        if not available_versions:
            self.logger.warning(f"No versions available for model {model_name}")
            return None
        
        criteria = selection_criteria or ChampionSelectionCriteria()
        
        try:
            # Filter versions based on criteria
            eligible_versions = await self._filter_eligible_versions(
                available_versions,
                criteria
            )
            
            if not eligible_versions:
                self.logger.warning(
                    f"No eligible versions found for model {model_name} after filtering"
                )
                return None
            
            # Score each version
            scored_versions = []
            for version in eligible_versions:
                score = await self._calculate_version_score(version, criteria)
                scored_versions.append((version, score))
            
            # Sort by score (descending)
            scored_versions.sort(key=lambda x: x[1], reverse=True)
            
            # Select champion (highest score)
            champion_version, champion_score = scored_versions[0]
            
            # Select challenger (second highest score if available)
            challenger_version = None
            challenger_score = None
            if len(scored_versions) > 1:
                challenger_version, challenger_score = scored_versions[1]
            
            # Create selection record
            selection = ChampionChallengerSelection(
                model_name=model_name,
                champion_version_id=champion_version.get("version_id"),
                challenger_version_id=challenger_version.get("version_id") if challenger_version else None,
                selection_criteria=criteria,
                selection_scores={
                    "champion_score": champion_score,
                    "challenger_score": challenger_score if challenger_score else 0
                }
            )
            
            # Save selection
            selection = await self.repository.save_selection(selection)
            
            # Update selection history
            if model_name not in self._selection_history:
                self._selection_history[model_name] = []
            self._selection_history[model_name].append(selection)
            
            self.logger.info(
                f"Selected champion for model {model_name}",
                extra={
                    "selection_id": selection.selection_id,
                    "champion_version": champion_version.get("version_number"),
                    "champion_score": champion_score
                }
            )
            
            # Auto-promote if requested
            if auto_promote and champion_version:
                await self.promote_to_champion(
                    model_name=model_name,
                    version_id=champion_version.get("version_id"),
                    reason="Auto-promoted based on selection criteria"
                )
            
            # Emit metric
            await self._emit_metric(
                "champion_selected",
                tags={
                    "model_name": model_name,
                    "auto_promoted": str(auto_promote)
                }
            )
            
            return selection
            
        except Exception as e:
            self.logger.error(
                f"Failed to select champion model: {e}",
                extra={"model_name": model_name}
            )
            raise
    
    async def _filter_eligible_versions(
        self,
        versions: List[Dict[str, Any]],
        criteria: ChampionSelectionCriteria
    ) -> List[Dict[str, Any]]:
        """Filter versions based on selection criteria."""
        eligible = []
        
        for version in versions:
            # Check performance threshold
            perf_metrics = version.get("performance_metrics", {})
            primary_metric_value = perf_metrics.get(criteria.primary_metric, 0)
            if primary_metric_value < criteria.min_performance_threshold:
                continue
            
            # Check model size constraint
            if criteria.max_model_size_bytes:
                model_size = version.get("model_size_bytes", 0)
                if model_size > criteria.max_model_size_bytes:
                    continue
            
            # Check inference time constraint
            if criteria.max_inference_time_ms:
                inference_time = perf_metrics.get("avg_inference_ms", float('inf'))
                if inference_time > criteria.max_inference_time_ms:
                    continue
            
            # Check required features
            if criteria.required_features:
                version_features = set(version.get("feature_selection", []))
                if not criteria.required_features.issubset(version_features):
                    continue
            
            # Check excluded versions
            if criteria.excluded_versions:
                if version.get("version_id") in criteria.excluded_versions:
                    continue
            
            # Check recency if preferred
            if criteria.prefer_recent:
                created_at = version.get("created_at")
                if isinstance(created_at, str):
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                if created_at:
                    age_days = (datetime.utcnow() - created_at).days
                    if age_days > criteria.recent_days:
                        continue
            
            eligible.append(version)
        
        return eligible
    
    async def _calculate_version_score(
        self,
        version: Dict[str, Any],
        criteria: ChampionSelectionCriteria
    ) -> float:
        """Calculate multi-criteria score for a model version."""
        weights = criteria.multi_criteria_weights
        scores = {}
        
        # Performance score (normalized to 0-1)
        perf_metrics = version.get("performance_metrics", {})
        primary_metric_value = perf_metrics.get(criteria.primary_metric, 0)
        # Assume metric is already in 0-1 range (like accuracy)
        # For metrics like RMSE, would need inverse normalization
        scores["performance"] = min(primary_metric_value, 1.0)
        
        # Efficiency score (based on model size and inference time)
        efficiency_components = []
        
        # Model size component (smaller is better)
        model_size = version.get("model_size_bytes", 1e9)  # Default 1GB
        size_score = 1 - min(model_size / 1e9, 1.0)  # Normalize to GB
        efficiency_components.append(size_score)
        
        # Inference time component (faster is better)
        inference_time = perf_metrics.get("avg_inference_ms", 100)
        time_score = 1 - min(inference_time / 1000, 1.0)  # Normalize to seconds
        efficiency_components.append(time_score)
        
        scores["efficiency"] = sum(efficiency_components) / len(efficiency_components)
        
        # Recency score
        created_at = version.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        if created_at:
            age_days = (datetime.utcnow() - created_at).days
            recency_score = max(0, 1 - (age_days / criteria.recent_days))
        else:
            recency_score = 0.5  # Neutral if no date
        scores["recency"] = recency_score
        
        # Stability score (based on validation results)
        validation_results = version.get("validation_results", {})
        cv_scores = validation_results.get("cross_validation_scores", [])
        if cv_scores:
            # Lower variance is better (more stable)
            import statistics
            mean_score = statistics.mean(cv_scores)
            if len(cv_scores) > 1:
                std_score = statistics.stdev(cv_scores)
                # Coefficient of variation (lower is better)
                cv = std_score / mean_score if mean_score > 0 else 1
                stability_score = 1 - min(cv, 1.0)
            else:
                stability_score = 0.5  # Neutral if only one score
        else:
            stability_score = 0.5  # Neutral if no CV scores
        scores["stability"] = stability_score
        
        # Calculate weighted total score
        total_score = 0
        for component, weight in weights.items():
            component_score = scores.get(component, 0)
            total_score += component_score * weight
        
        return total_score
    
    async def promote_to_champion(
        self,
        model_name: str,
        version_id: str,
        reason: str = "Manual promotion",
        promoted_by: str = "system"
    ) -> ChampionHistory:
        """
        Promote a model version to champion status.
        
        Args:
            model_name: Name of the model
            version_id: Version to promote
            reason: Reason for promotion
            promoted_by: User or system promoting
            
        Returns:
            ChampionHistory record
        """
        try:
            # Get current champion (if any)
            current_champion_id = await self.get_current_champion(model_name)
            
            # If there's a current champion, mark it as demoted
            if current_champion_id and current_champion_id != version_id:
                # Update previous champion history
                history_records = await self.repository.get_champion_history(
                    model_name, limit=10
                )
                for record in history_records:
                    if record.version_id == current_champion_id and not record.demoted_at:
                        record.demoted_at = datetime.utcnow()
                        await self.repository.save_history(record)
                        break
            
            # Set new champion
            await self.repository.set_current_champion(model_name, version_id)
            self._champion_cache[model_name] = version_id
            
            # Create history record
            # In real implementation, would fetch performance metrics from model registry
            history = ChampionHistory(
                model_name=model_name,
                version_id=version_id,
                promoted_at=datetime.utcnow(),
                promoted_by=promoted_by,
                reason=reason,
                performance_at_promotion={}  # Would fetch from model registry
            )
            
            history = await self.repository.save_history(history)
            
            self.logger.info(
                f"Promoted version {version_id} to champion for model {model_name}",
                extra={
                    "model_name": model_name,
                    "version_id": version_id,
                    "reason": reason,
                    "promoted_by": promoted_by
                }
            )
            
            # Emit metric
            await self._emit_metric(
                "champion_promoted",
                tags={
                    "model_name": model_name,
                    "promoted_by": promoted_by
                }
            )
            
            return history
            
        except Exception as e:
            self.logger.error(
                f"Failed to promote champion: {e}",
                extra={
                    "model_name": model_name,
                    "version_id": version_id
                }
            )
            raise
    
    async def get_current_champion(self, model_name: str) -> Optional[str]:
        """
        Get the current champion version ID for a model.
        
        Args:
            model_name: Name of the model
            
        Returns:
            Version ID of current champion, or None
        """
        # Check cache first
        if model_name in self._champion_cache:
            return self._champion_cache[model_name]
        
        # Load from repository
        champion_id = await self.repository.get_current_champion(model_name)
        if champion_id:
            self._champion_cache[model_name] = champion_id
        
        return champion_id
    
    async def get_champion_history(
        self,
        model_name: str,
        limit: int = 50
    ) -> List[ChampionHistory]:
        """
        Get champion promotion history for a model.
        
        Args:
            model_name: Name of the model
            limit: Maximum records to return
            
        Returns:
            List of ChampionHistory records
        """
        return await self.repository.get_champion_history(model_name, limit)
    
    async def select_champion_challenger(
        self,
        model_name: str,
        available_versions: List[Dict[str, Any]],
        selection_criteria: Optional[ChampionSelectionCriteria] = None
    ) -> Optional[ChampionChallengerSelection]:
        """
        Select both champion and challenger for A/B testing.
        
        This is similar to select_champion_model but explicitly returns
        both champion and challenger for scenarios where you want to
        run both in parallel.
        
        Args:
            model_name: Name of the model
            available_versions: List of version metadata
            selection_criteria: Criteria for selection
            
        Returns:
            ChampionChallengerSelection with both versions
        """
        # Use the same logic as select_champion_model
        # but ensure we have both champion and challenger
        selection = await self.select_champion_model(
            model_name=model_name,
            available_versions=available_versions,
            selection_criteria=selection_criteria,
            auto_promote=False
        )
        
        if selection and not selection.challenger_version_id:
            self.logger.warning(
                f"No challenger available for model {model_name}",
                extra={"model_name": model_name}
            )
        
        return selection
    
    async def _emit_metric(self, metric_name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Emit a metric (placeholder for actual implementation)."""
        # TODO: Integrate with telemetry service
        self.logger.debug(f"Metric: {metric_name}={value}, tags={tags}")
    
    async def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache (placeholder)."""
        # TODO: Integrate with cache service
        return None
    
    async def _set_cache(self, key: str, value: Dict[str, Any], ttl: int):
        """Set value in cache (placeholder)."""
        # TODO: Integrate with cache service
        pass
