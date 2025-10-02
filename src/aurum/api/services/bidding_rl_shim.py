"""Compatibility shim for bidding RL service.

Provides backward compatibility for code using the old bidding_rl_service
while redirecting to the new service architecture.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum

from aurum.services.ml.bidding_rl import BiddingRLService


class AuctionType(str, Enum):
    """Auction types."""
    DAY_AHEAD = "day_ahead"
    REAL_TIME = "real_time"
    CAPACITY = "capacity"
    ANCILLARY = "ancillary"


class PolicyStatus(str, Enum):
    """Policy training status."""
    UNTRAINED = "untrained"
    TRAINING = "training"
    TRAINED = "trained"
    OPTIMIZING = "optimizing"
    READY = "ready"


class AuctionEnvironment(BaseModel):
    """Auction environment configuration."""
    environment_id: str
    auction_type: AuctionType
    market_rules: Dict[str, Any]
    participant_count: int
    price_cap: float
    price_floor: float
    bidding_constraints: Dict[str, Any]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BiddingPolicy(BaseModel):
    """RL bidding policy."""
    policy_id: str
    policy_name: str
    algorithm: str  # "q_learning", "ppo", "dqn", etc.
    state_features: List[str]
    action_space: Dict[str, Any]
    hyperparameters: Dict[str, Any]
    training_status: PolicyStatus = PolicyStatus.UNTRAINED
    performance_metrics: Dict[str, float] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class RLTrainingSession(BaseModel):
    """RL training session details."""
    session_id: str
    policy_id: str
    environment_id: str
    training_episodes: int
    batch_size: int
    learning_rate: float
    gamma: float  # Discount factor
    epsilon: float  # Exploration rate
    training_start: datetime
    training_end: Optional[datetime] = None
    status: str  # "running", "completed", "failed"
    metrics: Dict[str, Any] = Field(default_factory=dict)


class PolicyEvaluation(BaseModel):
    """Policy evaluation results."""
    evaluation_id: str
    policy_id: str
    environment_id: str
    test_episodes: int
    win_rate: float
    average_profit: float
    total_revenue: float
    total_cost: float
    strategy_effectiveness: float
    market_impact: float
    evaluation_date: datetime = Field(default_factory=datetime.utcnow)
    detailed_metrics: Dict[str, Any] = Field(default_factory=dict)


class AuctionResult(BaseModel):
    """Individual auction result."""
    auction_id: str
    timestamp: datetime
    policy_id: str
    bid_price: float
    bid_quantity: float
    clearing_price: float
    awarded_quantity: float
    won: bool
    profit: float
    market_state: Dict[str, Any]
    action_taken: Dict[str, Any]


# Singleton instance
_service_instance = None


def get_bidding_rl_service() -> BiddingRLService:
    """Get singleton bidding RL service instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = BiddingRLService()
    return _service_instance


async def simulate_auction_scenario(
    policy_id: str,
    environment_id: str,
    market_conditions: Dict[str, Any]
) -> AuctionResult:
    """Simulate a single auction scenario."""
    service = get_bidding_rl_service()
    
    # Call the service to simulate
    result = await service.simulate_auction(
        policy_name=policy_id,
        auction_parameters={
            "environment_id": environment_id,
            "market_conditions": market_conditions
        }
    )
    
    # Convert to legacy format
    if result.success and result.data:
        data = result.data
        return AuctionResult(
            auction_id=f"auction_{datetime.now().timestamp()}",
            timestamp=datetime.now(),
            policy_id=policy_id,
            bid_price=data.get("winning_bid", 0),
            bid_quantity=100.0,  # Mock quantity
            clearing_price=data.get("clearing_price", 0),
            awarded_quantity=100.0 if data.get("auction_result") == "won" else 0,
            won=data.get("auction_result") == "won",
            profit=data.get("profit", 0),
            market_state=market_conditions,
            action_taken={"bid_price": data.get("winning_bid", 0)}
        )
    else:
        # Return a failed auction result
        return AuctionResult(
            auction_id=f"auction_{datetime.now().timestamp()}",
            timestamp=datetime.now(),
            policy_id=policy_id,
            bid_price=0,
            bid_quantity=0,
            clearing_price=0,
            awarded_quantity=0,
            won=False,
            profit=0,
            market_state=market_conditions,
            action_taken={}
        )


async def evaluate_bidding_policy(
    policy_id: str,
    environment_id: str,
    test_episodes: int = 100
) -> PolicyEvaluation:
    """Evaluate a bidding policy's performance."""
    service = get_bidding_rl_service()
    
    # Create evaluation scenarios
    scenarios = [
        {"episode": i, "market_volatility": 0.1 + (i % 10) * 0.05}
        for i in range(test_episodes)
    ]
    
    # Call the service
    result = await service.evaluate_policy_performance(
        policy_name=policy_id,
        evaluation_scenarios=scenarios
    )
    
    # Convert to legacy format
    if result.success and result.data:
        data = result.data
        return PolicyEvaluation(
            evaluation_id=f"eval_{datetime.now().timestamp()}",
            policy_id=policy_id,
            environment_id=environment_id,
            test_episodes=test_episodes,
            win_rate=data.get("win_rate", 0),
            average_profit=data.get("avg_profit", 0),
            total_revenue=data.get("total_profit", 0) * 1.2,  # Mock revenue
            total_cost=data.get("total_profit", 0) * 0.2,  # Mock cost
            strategy_effectiveness=data.get("win_rate", 0) * 0.8,
            market_impact=0.05,  # Mock impact
            detailed_metrics=data
        )
    else:
        # Return empty evaluation
        return PolicyEvaluation(
            evaluation_id=f"eval_{datetime.now().timestamp()}",
            policy_id=policy_id,
            environment_id=environment_id,
            test_episodes=test_episodes,
            win_rate=0,
            average_profit=0,
            total_revenue=0,
            total_cost=0,
            strategy_effectiveness=0,
            market_impact=0
        )
