"""v2 Bidding Toolkit API for RL-based auction/bid simulations.

This module provides REST endpoints for:
- Creating and managing auction simulation environments
- Training and evaluating RL bidding policies
- Running auction simulations with different policies
- Analyzing bidding performance and risk metrics
- Real-time bidding policy deployment
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.bidding_rl_service import (
    get_bidding_rl_service,
    AuctionEnvironment,
    BiddingPolicy,
    RLTrainingSession,
    PolicyEvaluation,
    AuctionResult
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/bidding", tags=["bidding"])


class EnvironmentCreateRequest(BaseModel):
    """Request to create an auction environment."""

    environment_id: str = Field(..., description="Environment identifier")
    auction_type: str = Field(..., description="Auction type (day_ahead, real_time, capacity, ancillary)")
    market: str = Field(..., description="Market (pjm, ercot, miso, nyiso)")
    geography: str = Field(..., description="Geographic scope")
    capacity_mw: float = Field(..., description="Capacity in MW")
    time_horizon_hours: int = Field(24, description="Time horizon for simulation")
    price_volatility: float = Field(0.3, description="Price volatility factor")
    demand_uncertainty: float = Field(0.2, description="Demand uncertainty factor")
    competition_level: str = Field("moderate", description="Competition level")
    risk_aversion: float = Field(0.5, description="Risk aversion factor")


class PolicyCreateRequest(BaseModel):
    """Request to create a bidding policy."""

    policy_id: str = Field(..., description="Policy identifier")
    policy_name: str = Field(..., description="Policy name")
    algorithm: str = Field(..., description="RL algorithm (dqn, ppo, a2c, sac, td3)")
    state_dim: int = Field(..., description="State dimension")
    action_dim: int = Field(..., description="Action dimension")
    hidden_layers: List[int] = Field([64, 32], description="Hidden layer sizes")
    learning_rate: float = Field(0.001, description="Learning rate")
    gamma: float = Field(0.99, description="Discount factor")
    epsilon: float = Field(0.1, description="Exploration rate")
    batch_size: int = Field(32, description="Batch size")
    memory_size: int = Field(10000, description="Replay buffer size")
    training_episodes: int = Field(1000, description="Training episodes")
    evaluation_episodes: int = Field(100, description="Evaluation episodes")


class EnvironmentResponse(BaseModel):
    """Response containing environment information."""

    environment_id: str
    auction_type: str
    market: str
    geography: str
    capacity_mw: float
    time_horizon_hours: int
    price_volatility: float
    demand_uncertainty: float
    competition_level: str
    risk_aversion: float
    created_at: datetime


class PolicyResponse(BaseModel):
    """Response containing policy information."""

    policy_id: str
    policy_name: str
    algorithm: str
    state_dim: int
    action_dim: int
    hidden_layers: List[int]
    learning_rate: float
    gamma: float
    epsilon: float
    training_episodes: int
    performance_metrics: Dict[str, float]
    created_at: datetime


class TrainingSessionResponse(BaseModel):
    """Response containing training session information."""

    session_id: str
    policy_id: str
    environment_id: str
    status: str
    current_episode: int
    total_episodes: int
    best_reward: float
    average_reward: float
    exploration_rate: float
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]


class EvaluationResponse(BaseModel):
    """Response containing policy evaluation information."""

    evaluation_id: str
    policy_id: str
    test_period: Dict[str, datetime]
    total_episodes: int
    average_reward: float
    win_rate: float
    profit_margin: float
    risk_adjusted_return: float
    sharpe_ratio: float
    max_drawdown: float
    benchmark_comparison: Dict[str, float]
    recommendations: List[str]


class SimulationResponse(BaseModel):
    """Response containing simulation results."""

    simulation_id: str
    results: List[Dict[str, any]]
    total_revenue: float
    total_profit: float
    average_profit_margin: float
    market_share: float
    risk_metrics: Dict[str, float]


@router.post("/environments", response_model=EnvironmentResponse, status_code=201)
async def create_auction_environment(
    request: Request,
    environment_data: EnvironmentCreateRequest
) -> EnvironmentResponse:
    """Create a new auction simulation environment."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()

        # Create environment configuration
        env_config = AuctionEnvironment(
            environment_id=environment_data.environment_id,
            auction_type=environment_data.auction_type,
            market=environment_data.market,
            geography=environment_data.geography,
            capacity_mw=environment_data.capacity_mw,
            time_horizon_hours=environment_data.time_horizon_hours,
            price_volatility=environment_data.price_volatility,
            demand_uncertainty=environment_data.demand_uncertainty,
            competition_level=environment_data.competition_level,
            risk_aversion=environment_data.risk_aversion
        )

        # Create environment
        env_id = await service.create_auction_environment(env_config)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="create_auction_environment",
            query_time_ms=query_time_ms
        )

        return EnvironmentResponse(
            environment_id=env_config.environment_id,
            auction_type=env_config.auction_type,
            market=env_config.market,
            geography=env_config.geography,
            capacity_mw=env_config.capacity_mw,
            time_horizon_hours=env_config.time_horizon_hours,
            price_volatility=env_config.price_volatility,
            demand_uncertainty=env_config.demand_uncertainty,
            competition_level=env_config.competition_level,
            risk_aversion=env_config.risk_aversion,
            created_at=datetime.utcnow()
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="create_auction_environment",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create auction environment: {str(exc)}"
        )


@router.get("/environments", response_model=Dict[str, any])
async def list_environments(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """List available auction environments."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()

        # Get service health to extract environment info
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_environments",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": {
                "environments": health.get("active_environments", 0),
                "available_environments": ["pjm_da", "ercot_rt"],  # Mock
                "environments": [
                    {
                        "environment_id": "pjm_da",
                        "auction_type": "day_ahead",
                        "market": "pjm",
                        "capacity_mw": 1000.0
                    },
                    {
                        "environment_id": "ercot_rt",
                        "auction_type": "real_time",
                        "market": "ercot",
                        "capacity_mw": 500.0
                    }
                ]
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_environments",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list environments: {str(exc)}"
        )


@router.post("/policies", response_model=PolicyResponse, status_code=201)
async def create_bidding_policy(
    request: Request,
    policy_data: PolicyCreateRequest
) -> PolicyResponse:
    """Create a new RL bidding policy."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()

        # Create policy configuration
        policy_config = BiddingPolicy(
            policy_id=policy_data.policy_id,
            policy_name=policy_data.policy_name,
            algorithm=policy_data.algorithm,
            state_dim=policy_data.state_dim,
            action_dim=policy_data.action_dim,
            hidden_layers=policy_data.hidden_layers,
            learning_rate=policy_data.learning_rate,
            gamma=policy_data.gamma,
            epsilon=policy_data.epsilon,
            batch_size=policy_data.batch_size,
            memory_size=policy_data.memory_size,
            training_episodes=policy_data.training_episodes,
            evaluation_episodes=policy_data.evaluation_episodes
        )

        # Create policy
        policy_id = await service.create_bidding_policy(policy_config)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="create_bidding_policy",
            query_time_ms=query_time_ms
        )

        return PolicyResponse(
            policy_id=policy_config.policy_id,
            policy_name=policy_config.policy_name,
            algorithm=policy_config.algorithm,
            state_dim=policy_config.state_dim,
            action_dim=policy_config.action_dim,
            hidden_layers=policy_config.hidden_layers,
            learning_rate=policy_config.learning_rate,
            gamma=policy_config.gamma,
            epsilon=policy_config.epsilon,
            training_episodes=policy_config.training_episodes,
            performance_metrics=policy_config.performance_metrics,
            created_at=datetime.utcnow()
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="create_bidding_policy",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create bidding policy: {str(exc)}"
        )


@router.post("/policies/{policy_id}/train", response_model=Dict[str, any], status_code=202)
async def start_policy_training(
    request: Request,
    policy_id: str,
    environment_id: str = Query(..., description="Environment for training")
) -> Dict[str, any]:
    """Start RL training for a bidding policy."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()

        # Start training
        session_id = await service.start_rl_training(policy_id, environment_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="start_policy_training",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="start_policy_training",
                query_time_ms=query_time_ms
            ),
            "data": {
                "session_id": session_id,
                "status": "running",
                "message": "Policy training started successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="start_policy_training",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start policy training: {str(exc)}"
        )


@router.get("/training/{session_id}", response_model=TrainingSessionResponse)
async def get_training_status(
    request: Request,
    session_id: str
) -> TrainingSessionResponse:
    """Get training session status."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()
        session = await service.get_training_status(session_id)

        if not session:
            raise HTTPException(status_code=404, detail="Training session not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_training_status",
            query_time_ms=query_time_ms
        )

        return TrainingSessionResponse(
            session_id=session.session_id,
            policy_id=session.policy_id,
            environment_id=session.environment_id,
            status=session.status,
            current_episode=session.current_episode,
            total_episodes=session.total_episodes,
            best_reward=session.best_reward,
            average_reward=session.average_reward,
            exploration_rate=session.exploration_rate,
            started_at=session.started_at,
            completed_at=session.completed_at,
            error_message=session.error_message
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_training_status",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get training status: {str(exc)}"
        )


@router.post("/simulate", response_model=SimulationResponse, status_code=202)
async def simulate_auction(
    request: Request,
    environment_id: str = Query(..., description="Environment for simulation"),
    policy_id: str = Query(..., description="Policy for simulation"),
    hours: int = Query(24, description="Simulation duration in hours")
) -> SimulationResponse:
    """Run auction simulation with specified policy."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()

        # Run simulation
        simulation_id = await service.simulate_auction(environment_id, policy_id, hours)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="simulate_auction",
            query_time_ms=query_time_ms
        )

        # Mock results
        results = []
        total_revenue = 0.0
        total_profit = 0.0

        for hour in range(hours):
            revenue = 50.0 * 100.0  # Mock revenue
            profit = revenue * 0.2  # Mock profit margin

            total_revenue += revenue
            total_profit += profit

            results.append({
                "hour": hour,
                "cleared_price": 50.0,
                "cleared_quantity": 100.0,
                "revenue": revenue,
                "profit": profit,
                "profit_margin": 0.2
            })

        return SimulationResponse(
            simulation_id=simulation_id,
            results=results,
            total_revenue=total_revenue,
            total_profit=total_profit,
            average_profit_margin=0.2,
            market_share=0.15,
            risk_metrics={"volatility": 0.3, "max_drawdown": 0.1}
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="simulate_auction",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to simulate auction: {str(exc)}"
        )


@router.post("/policies/{policy_id}/evaluate", response_model=EvaluationResponse, status_code=202)
async def evaluate_policy(
    request: Request,
    policy_id: str,
    test_period_days: int = Query(30, description="Test period in days")
) -> EvaluationResponse:
    """Evaluate bidding policy performance."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()

        # Evaluate policy
        evaluation_id = await service.evaluate_policy(policy_id, test_period_days)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="evaluate_policy",
            query_time_ms=query_time_ms
        )

        # Mock evaluation results
        return EvaluationResponse(
            evaluation_id=evaluation_id,
            policy_id=policy_id,
            test_period={
                "start": datetime.utcnow() - timedelta(days=test_period_days),
                "end": datetime.utcnow()
            },
            total_episodes=100,
            average_reward=150.0,
            win_rate=0.75,
            profit_margin=0.25,
            risk_adjusted_return=0.18,
            sharpe_ratio=1.2,
            max_drawdown=0.15,
            benchmark_comparison={
                "baseline": -0.05,
                "best_competitor": 0.08
            },
            recommendations=[
                "Policy performs well in volatile markets",
                "Consider increasing risk tolerance for higher returns"
            ]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="evaluate_policy",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to evaluate policy: {str(exc)}"
        )


@router.get("/policies/{policy_id}/performance", response_model=Dict[str, any])
async def get_policy_performance(
    request: Request,
    policy_id: str
) -> Dict[str, any]:
    """Get policy performance metrics."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()
        performance = await service.get_policy_performance(policy_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_policy_performance",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": performance
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_policy_performance",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get policy performance: {str(exc)}"
        )


@router.get("/health")
async def get_bidding_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get bidding service health status."""
    start_time = time.perf_counter()

    try:
        service = get_bidding_rl_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_bidding_health",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": health
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_bidding_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get bidding health: {str(exc)}"
        )
