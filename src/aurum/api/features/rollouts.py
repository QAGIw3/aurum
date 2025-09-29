"""Feature flag rollout strategies and scheduling."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum

from .models import FeatureFlagRule, UserSegment


class RolloutStrategy(Enum):
    """Rollout strategies for feature flags."""
    PERCENTAGE = "percentage"  # Percentage of users
    GRADUAL = "gradual"  # Gradual ramp-up over time
    TARGETED = "targeted"  # Specific users/segments
    SCHEDULED = "scheduled"  # Time-based rollout
    DEPENDENT = "dependent"  # Depends on other flags


@dataclass
class RolloutPlan:
    """A rollout plan for a feature flag."""
    strategy: RolloutStrategy
    # For percentage-based rollouts
    percentage: float = 100.0  # 0-100 percentage of users to rollout to

    # For gradual rollouts
    start_percentage: float = 0.0  # Starting percentage
    end_percentage: float = 100.0  # Target percentage
    step_percentage: float = 10.0  # Step size for each interval
    step_interval_hours: int = 24  # Hours between steps

    # For scheduled rollouts
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    # For targeted rollouts
    user_segments: List[UserSegment] = field(default_factory=list)
    required_flags: List[str] = field(default_factory=list)  # Flags that must be enabled
    excluded_flags: List[str] = field(default_factory=list)  # Flags that must be disabled

    # Metadata
    name: str = ""
    description: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = ""

    def get_effective_percentage(self, current_time: Optional[datetime] = None) -> float:
        """Get the effective rollout percentage based on the strategy and current time."""
        if current_time is None:
            current_time = datetime.utcnow()

        if self.strategy == RolloutStrategy.PERCENTAGE:
            return self.percentage

        elif self.strategy == RolloutStrategy.GRADUAL:
            if self.start_time is None:
                return self.start_percentage

            elapsed_hours = (current_time - self.start_time).total_seconds() / 3600

            if elapsed_hours <= 0:
                return self.start_percentage

            # Calculate which step we're on
            steps_taken = int(elapsed_hours / self.step_interval_hours)
            effective_percentage = min(
                self.start_percentage + (steps_taken * self.step_percentage),
                self.end_percentage
            )

            return effective_percentage

        elif self.strategy == RolloutStrategy.SCHEDULED:
            if self.start_time is None:
                return 0.0

            if self.end_time is not None and current_time >= self.end_time:
                return self.end_percentage

            if current_time < self.start_time:
                return 0.0

            # Linear interpolation between start and end times
            if self.end_time is not None:
                total_duration = (self.end_time - self.start_time).total_seconds()
                elapsed_duration = (current_time - self.start_time).total_seconds()

                if total_duration > 0:
                    progress = min(elapsed_duration / total_duration, 1.0)
                    return self.start_percentage + (self.end_percentage - self.start_percentage) * progress

            return self.end_percentage

        else:
            return self.end_percentage  # Default for other strategies

    def is_active_for_user(self, user_context: Dict[str, Any], current_time: Optional[datetime] = None) -> bool:
        """Check if the rollout is active for a specific user."""
        if current_time is None:
            current_time = datetime.utcnow()

        # Check time-based constraints
        if self.strategy == RolloutStrategy.SCHEDULED:
            if self.start_time is not None and current_time < self.start_time:
                return False
            if self.end_time is not None and current_time >= self.end_time:
                return False

        # Check required flags
        for flag_name in self.required_flags:
            if not user_context.get(f"flag_{flag_name}", False):
                return False

        # Check excluded flags
        for flag_name in self.excluded_flags:
            if user_context.get(f"flag_{flag_name}", False):
                return False

        # Check user segments
        user_segment = user_context.get("user_segment")
        if user_segment and self.user_segments and user_segment not in self.user_segments:
            return False

        # For percentage-based strategies, use deterministic user hashing
        if self.strategy in [RolloutStrategy.PERCENTAGE, RolloutStrategy.GRADUAL]:
            effective_percentage = self.get_effective_percentage(current_time)
            if effective_percentage < 100.0:
                user_id = user_context.get("user_id", "")
                if user_id:
                    # Deterministic rollout based on user ID
                    user_hash = hashlib.md5(user_id.encode()).hexdigest()
                    user_percentage = (int(user_hash[:8], 16) % 100)
                    return user_percentage < effective_percentage

        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convert rollout plan to dictionary for serialization."""
        return {
            "strategy": self.strategy.value,
            "percentage": self.percentage,
            "start_percentage": self.start_percentage,
            "end_percentage": self.end_percentage,
            "step_percentage": self.step_percentage,
            "step_interval_hours": self.step_interval_hours,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "user_segments": [s.value for s in self.user_segments],
            "required_flags": self.required_flags,
            "excluded_flags": self.excluded_flags,
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RolloutPlan':
        """Create rollout plan from dictionary."""
        # Handle datetime conversion
        start_time = data.get("start_time")
        if start_time:
            start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))

        end_time = data.get("end_time")
        if end_time:
            end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))

        created_at = data.get("created_at")
        if created_at:
            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))

        updated_at = data.get("updated_at")
        if updated_at:
            updated_at = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))

        # Handle user segments
        user_segments = [UserSegment(s) for s in data.get("user_segments", [])]

        return cls(
            strategy=RolloutStrategy(data.get("strategy", "percentage")),
            percentage=data.get("percentage", 100.0),
            start_percentage=data.get("start_percentage", 0.0),
            end_percentage=data.get("end_percentage", 100.0),
            step_percentage=data.get("step_percentage", 10.0),
            step_interval_hours=data.get("step_interval_hours", 24),
            start_time=start_time,
            end_time=end_time,
            user_segments=user_segments,
            required_flags=data.get("required_flags", []),
            excluded_flags=data.get("excluded_flags", []),
            name=data.get("name", ""),
            description=data.get("description", ""),
            created_at=created_at or datetime.utcnow(),
            updated_at=updated_at or datetime.utcnow(),
            created_by=data.get("created_by", ""),
        )


class RolloutEvaluator:
    """Evaluator for rollout plans and their integration with feature flags."""

    def __init__(self):
        self._plans: Dict[str, RolloutPlan] = {}  # flag_key -> plan
        self._lock = asyncio.Lock()

    async def set_rollout_plan(self, flag_key: str, plan: RolloutPlan) -> None:
        """Set a rollout plan for a feature flag."""
        async with self._lock:
            plan.updated_at = datetime.utcnow()
            self._plans[flag_key] = plan

    async def get_rollout_plan(self, flag_key: str) -> Optional[RolloutPlan]:
        """Get the rollout plan for a feature flag."""
        async with self._lock:
            return self._plans.get(flag_key)

    async def delete_rollout_plan(self, flag_key: str) -> None:
        """Delete the rollout plan for a feature flag."""
        async with self._lock:
            self._plans.pop(flag_key, None)

    async def list_rollout_plans(self) -> Dict[str, RolloutPlan]:
        """List all rollout plans."""
        async with self._lock:
            return dict(self._plans)

    def evaluate_with_rollout(
        self,
        flag_key: str,
        user_context: Dict[str, Any],
        current_time: Optional[datetime] = None
    ) -> bool:
        """Evaluate if a feature flag is enabled considering its rollout plan."""
        if current_time is None:
            current_time = datetime.utcnow()

        # Get the rollout plan for this flag
        plan = self._plans.get(flag_key)
        if not plan:
            return True  # No rollout plan means full rollout

        return plan.is_active_for_user(user_context, current_time)

    async def get_rollout_stats(self, flag_key: str) -> Dict[str, Any]:
        """Get rollout statistics for a feature flag."""
        plan = await self.get_rollout_plan(flag_key)
        if not plan:
            return {"has_rollout_plan": False}

        current_time = datetime.utcnow()
        effective_percentage = plan.get_effective_percentage(current_time)

        return {
            "has_rollout_plan": True,
            "strategy": plan.strategy.value,
            "effective_percentage": effective_percentage,
            "is_active": plan.strategy == RolloutStrategy.PERCENTAGE or
                       (plan.start_time is None or current_time >= plan.start_time),
            "next_step_time": None,  # Could calculate based on gradual rollout schedule
        }


# Global rollout evaluator instance
_rollout_evaluator: Optional[RolloutEvaluator] = None


def get_rollout_evaluator() -> RolloutEvaluator:
    """Get the global rollout evaluator."""
    global _rollout_evaluator
    if _rollout_evaluator is None:
        _rollout_evaluator = RolloutEvaluator()
    return _rollout_evaluator


async def initialize_rollout_evaluator() -> RolloutEvaluator:
    """Initialize the rollout evaluator (for testing)."""
    global _rollout_evaluator
    if _rollout_evaluator is None:
        _rollout_evaluator = RolloutEvaluator()
    return _rollout_evaluator
