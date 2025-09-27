"""Tests for the bidding RL analytical service."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock

import numpy as np
import pytest

from src.aurum.api.services.bidding_rl_service import (
    AuctionEnvironment,
    BiddingPolicy,
    BiddingRLService,
    ReplayBuffer,
)


@pytest.fixture(autouse=True)
def _stub_infrastructure(monkeypatch):
    """Stub infrastructure dependencies for the service."""

    monkeypatch.setattr(
        "src.aurum.api.services.bidding_rl_service.TrinoDAO",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "src.aurum.api.services.bidding_rl_service.get_unified_cache_manager",
        lambda: Mock(),
    )

    telemetry = Mock()
    monkeypatch.setattr(
        "src.aurum.api.services.bidding_rl_service.get_telemetry_facade",
        lambda: telemetry,
    )
    return telemetry


@pytest.fixture
def bidding_service() -> BiddingRLService:
    """Return a service instance with default synthetic environments."""

    return BiddingRLService()


@pytest.fixture
def background_tasks(monkeypatch):
    """Capture background training tasks so tests can await their completion."""

    tasks: list[asyncio.Task] = []
    create_task = asyncio.create_task

    def track_tasks(coro):
        task = create_task(coro)
        tasks.append(task)
        return task

    monkeypatch.setattr(
        "src.aurum.api.services.bidding_rl_service.asyncio.create_task",
        track_tasks,
    )
    return tasks


def test_replay_buffer_sampling_behavior():
    """Replay buffer should return expected structures when capacity is reached."""

    buffer = ReplayBuffer(capacity=3)
    rng = np.random.default_rng(42)

    first_sample = buffer.sample(batch_size=2, rng=rng)
    assert first_sample is None

    for idx in range(3):
        state = np.ones(4, dtype=np.float32) * idx
        next_state = np.ones(4, dtype=np.float32) * (idx + 0.5)
        buffer.add(state=state, action=idx, reward=float(idx), next_state=next_state, done=idx % 2 == 0)

    sample = buffer.sample(batch_size=2, rng=rng)
    assert sample is not None

    states, actions, rewards, next_states, dones = sample
    assert states.shape == (2, 4)
    assert actions.shape == (2,)
    assert rewards.dtype == np.float32
    assert next_states.shape == (2, 4)
    assert set(np.unique(dones)).issubset({0.0, 1.0})


@pytest.mark.asyncio
async def test_training_flow_and_simulation_outputs(bidding_service: BiddingRLService, background_tasks):
    """End-to-end training, simulation, and evaluation should produce metrics."""

    environment = AuctionEnvironment(
        environment_id="unit_test_env",
        auction_type="day_ahead",
        market="pjm",
        geography="pjm-test",
        capacity_mw=150.0,
        time_horizon_hours=12,
        price_volatility=0.25,
        demand_uncertainty=0.15,
        competition_level="moderate",
        risk_aversion=0.4,
    )

    await bidding_service.create_auction_environment(environment)

    policy = BiddingPolicy(
        policy_id="unit_test_policy",
        policy_name="Test Policy",
        algorithm="dqn",
        state_dim=8,
        action_dim=4,
        hidden_layers=[32, 16],
        learning_rate=0.005,
        gamma=0.95,
        epsilon=0.3,
        batch_size=8,
        memory_size=256,
        target_update_freq=20,
        training_episodes=6,
        evaluation_episodes=3,
    )

    await bidding_service.create_bidding_policy(policy)

    session_id = await bidding_service.start_rl_training(policy.policy_id, environment.environment_id)

    if background_tasks:
        await asyncio.gather(*background_tasks)

    session = await bidding_service.get_training_status(session_id)
    assert session is not None
    assert session.status == "completed"
    assert session.current_episode == session.total_episodes
    assert session.best_reward > float("-inf")

    performance = await bidding_service.get_policy_performance(policy.policy_id)
    performance_metrics = performance["performance_metrics"]
    assert performance_metrics["episodes_completed"] == policy.training_episodes
    assert performance_metrics["average_reward_training"] != 0 or performance_metrics["best_reward"] != 0
    assert performance["replay_buffer_size"] >= policy.batch_size
    assert performance["recent_losses"]

    simulation_id = await bidding_service.simulate_auction(environment.environment_id, policy.policy_id, hours=environment.time_horizon_hours)
    cached_simulation = bidding_service._simulation_cache[simulation_id]

    assert cached_simulation["environment_id"] == environment.environment_id
    assert cached_simulation["policy_id"] == policy.policy_id
    assert cached_simulation["summary"]["steps"] == len(cached_simulation["results"])
    assert cached_simulation["summary"]["win_rate"] >= 0.0

    evaluation_id = await bidding_service.evaluate_policy(policy.policy_id)
    evaluation = bidding_service._evaluations[evaluation_id]

    assert evaluation.policy_id == policy.policy_id
    assert evaluation.total_episodes == policy.evaluation_episodes
    assert evaluation.test_period["end"] >= datetime.utcnow() - timedelta(minutes=1)
    assert evaluation.recommendations
    assert "reward_uplift" in evaluation.benchmark_comparison

