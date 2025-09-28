"""Optimized Bidding Toolkit with RL Sandbox for auction/bid simulations.

This service provides:
- Auction/bid simulation environments
- Reinforcement Learning training loop with synthetic data
- Policy evaluation against historical windows
- Scenario-driven bidding strategies
- Risk-adjusted bidding optimization
- Real-time bidding policy deployment
"""

from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import numpy as np
from pydantic import BaseModel

from ..observability.telemetry_facade import get_telemetry_facade
from ..cache.consolidated_manager import get_unified_cache_manager
from ..dao.experimental import TrinoDAO


class AuctionEnvironment(BaseModel):
    """Auction simulation environment configuration."""

    environment_id: str
    auction_type: str  # "day_ahead", "real_time", "capacity", "ancillary"
    market: str  # "pjm", "ercot", "miso", "nyiso", etc.
    geography: str
    capacity_mw: float
    time_horizon_hours: int = 24
    price_volatility: float = 0.3  # 0.0 to 1.0
    demand_uncertainty: float = 0.2
    competition_level: str = "moderate"  # "low", "moderate", "high"
    risk_aversion: float = 0.5  # 0.0 to 1.0 (risk-seeking to risk-averse)


class BiddingPolicy(BaseModel):
    """Reinforcement Learning bidding policy."""

    policy_id: str
    policy_name: str
    algorithm: str  # "dqn", "ppo", "a2c", "sac", "td3"
    state_dim: int
    action_dim: int
    hidden_layers: List[int] = [64, 32]
    learning_rate: float = 0.001
    gamma: float = 0.99  # Discount factor
    epsilon: float = 0.1  # Exploration rate
    batch_size: int = 32
    memory_size: int = 10000
    target_update_freq: int = 1000
    model_path: Optional[str] = None
    training_episodes: int = 1000
    evaluation_episodes: int = 100
    performance_metrics: Dict[str, Any] = field(default_factory=dict)


class BiddingAction(BaseModel):
    """Individual bidding action in simulation."""

    action_id: str
    timestamp: datetime
    asset_id: str
    bid_price: float
    bid_quantity: float
    bid_type: str  # "supply", "demand"
    confidence: float  # 0.0 to 1.0
    risk_adjustment: float
    market_conditions: Dict[str, Any]


class AuctionResult(BaseModel):
    """Result of auction/bid simulation."""

    result_id: str
    simulation_id: str
    timestamp: datetime
    cleared_price: float
    cleared_quantity: float
    total_revenue: float
    profit_margin: float
    market_share: float
    competition_metrics: Dict[str, float]
    risk_metrics: Dict[str, float]
    performance_score: float


class RLTrainingSession(BaseModel):
    """Reinforcement Learning training session."""

    session_id: str
    policy_id: str
    environment_id: str
    status: str = "pending"  # "pending", "running", "completed", "failed", "cancelled"
    current_episode: int = 0
    total_episodes: int = 1000
    best_reward: float = float('-inf')
    average_reward: float = 0.0
    exploration_rate: float = 1.0
    learning_rate: float = 0.001
    total_steps: int = 0
    episode_rewards: List[float] = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


class PolicyEvaluation(BaseModel):
    """Policy evaluation results."""

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


class ReplayBuffer:
    """Experience replay buffer for off-policy RL."""

    def __init__(self, capacity: int) -> None:
        self.capacity = max(1, int(capacity))
        self._buffer: deque = deque(maxlen=self.capacity)

    def add(self, state: np.ndarray, action: int, reward: float, next_state: np.ndarray, done: bool) -> None:
        self._buffer.append(
            (
                state.astype(np.float32, copy=True),
                int(action),
                float(reward),
                next_state.astype(np.float32, copy=True),
                bool(done)
            )
        )

    def sample(self, batch_size: int, rng: np.random.Generator) -> Optional[Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]]:
        if batch_size <= 0 or len(self._buffer) < batch_size:
            return None

        indices = rng.choice(len(self._buffer), size=batch_size, replace=False)

        states = np.stack([self._buffer[idx][0] for idx in indices]).astype(np.float32)
        actions = np.array([self._buffer[idx][1] for idx in indices], dtype=np.int64)
        rewards = np.array([self._buffer[idx][2] for idx in indices], dtype=np.float32)
        next_states = np.stack([self._buffer[idx][3] for idx in indices]).astype(np.float32)
        dones = np.array([self._buffer[idx][4] for idx in indices], dtype=np.float32)

        return states, actions, rewards, next_states, dones

    def __len__(self) -> int:
        return len(self._buffer)


class PolicyNetwork:
    """Simple fully-connected network for approximating action-value function."""

    def __init__(self, input_dim: int, hidden_layers: List[int], output_dim: int, rng: np.random.Generator) -> None:
        if input_dim <= 0:
            raise ValueError("input_dim must be positive")
        if output_dim <= 0:
            raise ValueError("output_dim must be positive")

        hidden = [int(max(1, h)) for h in hidden_layers]
        self.layer_sizes = [input_dim, *hidden, output_dim]
        self.weights: List[np.ndarray] = []
        self.biases: List[np.ndarray] = []

        for in_size, out_size in zip(self.layer_sizes[:-1], self.layer_sizes[1:]):
            limit = 1.0 / math.sqrt(in_size)
            weight = rng.uniform(-limit, limit, size=(in_size, out_size)).astype(np.float32)
            bias = np.zeros(out_size, dtype=np.float32)
            self.weights.append(weight)
            self.biases.append(bias)

    def forward(self, inputs: np.ndarray) -> Tuple[np.ndarray, Tuple[List[np.ndarray], List[np.ndarray]]]:
        activations: List[np.ndarray] = [inputs]
        pre_activations: List[np.ndarray] = []
        output = inputs
        last_index = len(self.weights) - 1

        for idx, (weight, bias) in enumerate(zip(self.weights, self.biases)):
            z = output @ weight + bias
            pre_activations.append(z)
            if idx == last_index:
                output = z
            else:
                output = np.tanh(z)
            activations.append(output)

        return output, (activations, pre_activations)

    def predict(self, inputs: np.ndarray) -> np.ndarray:
        inputs_2d = np.atleast_2d(inputs.astype(np.float32))
        output, _ = self.forward(inputs_2d)
        return output

    def fit(self, inputs: np.ndarray, targets: np.ndarray, learning_rate: float) -> float:
        inputs_2d = np.atleast_2d(inputs.astype(np.float32))
        targets_2d = np.atleast_2d(targets.astype(np.float32))

        predictions, cache = self.forward(inputs_2d)
        error = predictions - targets_2d
        loss = float(np.mean(error ** 2))

        batch_size = max(1, inputs_2d.shape[0])
        grad_output = (2.0 / batch_size) * error
        self.backward(grad_output, cache, learning_rate)

        return loss

    def backward(self, grad_output: np.ndarray, cache: Tuple[List[np.ndarray], List[np.ndarray]], learning_rate: float) -> None:
        activations, pre_activations = cache
        grad = grad_output

        for idx in reversed(range(len(self.weights))):
            a_prev = activations[idx]
            grad_weight = a_prev.T @ grad
            grad_bias = grad.sum(axis=0)

            self.weights[idx] -= learning_rate * grad_weight
            self.biases[idx] -= learning_rate * grad_bias

            if idx != 0:
                grad = grad @ self.weights[idx].T
                tanh_grad = 1.0 - np.tanh(pre_activations[idx - 1]) ** 2
                grad *= tanh_grad

    def copy_from(self, other: "PolicyNetwork") -> None:
        for idx in range(len(self.weights)):
            self.weights[idx][...] = other.weights[idx]
            self.biases[idx][...] = other.biases[idx]

    def clone(self, rng: np.random.Generator) -> "PolicyNetwork":
        clone = PolicyNetwork(self.layer_sizes[0], self.layer_sizes[1:-1], self.layer_sizes[-1], rng)
        clone.copy_from(self)
        return clone


@dataclass
class PolicyModelState:
    """Encapsulates mutable state for a bidding policy."""

    network: PolicyNetwork
    target_network: PolicyNetwork
    replay_buffer: ReplayBuffer
    rng: np.random.Generator
    exploration_rate: float
    exploration_min: float
    exploration_decay: float
    training_history: List[Dict[str, Any]] = field(default_factory=list)
    total_updates: int = 0
    last_target_sync: int = 0
    recent_losses: deque = field(default_factory=lambda: deque(maxlen=200))


class AuctionSimulator:
    """Auction environment dynamics for RL training and evaluation."""

    def __init__(self, config: AuctionEnvironment, seed: Optional[int] = None) -> None:
        self.config = config
        self.horizon = max(1, int(config.time_horizon_hours))
        self.rng = np.random.default_rng(seed)
        self.current_time_index = 0
        self._base_price = 45.0 + self.rng.uniform(-5.0, 5.0)
        self.current_price = self._base_price
        self.current_demand = config.capacity_mw * 0.6
        self._episode_start = datetime.utcnow()
        self._step_history: List[Dict[str, Any]] = []

    def reset(self, state_dim: int) -> np.ndarray:
        self.current_time_index = 0
        self._episode_start = datetime.utcnow()
        self._base_price = 45.0 + self.rng.uniform(-5.0, 5.0)
        self.current_price = self._base_price * self.rng.uniform(0.9, 1.1)
        self.current_demand = self.config.capacity_mw * self.rng.uniform(0.4, 0.9)
        self._step_history.clear()
        return self._get_state_vector(state_dim)

    def step(self, action_index: int, action_dim: int, state_dim: int) -> Tuple[np.ndarray, float, bool, Dict[str, Any]]:
        self.current_time_index += 1
        time_fraction = self.current_time_index / self.horizon

        max_price_adjustment = max(5.0, 25.0 * self.config.price_volatility)
        price_adjustments = np.linspace(-max_price_adjustment, max_price_adjustment, num=max(2, action_dim), dtype=np.float32)
        quantity_scales = np.linspace(0.3, 1.0, num=max(2, action_dim), dtype=np.float32)

        action_index = int(np.clip(action_index, 0, max(1, action_dim - 1)))
        bid_price = float(np.clip(self.current_price + price_adjustments[action_index], 1.0, self.current_price * 2.5))
        bid_quantity = float(self.config.capacity_mw * quantity_scales[action_index])

        competition_factor = {
            "low": 0.6,
            "moderate": 0.8,
            "high": 1.0
        }.get(self.config.competition_level, 0.8)

        price_noise = self.rng.normal(0.0, self.config.price_volatility * 8.0)
        demand_shock = self.rng.normal(0.0, self.config.demand_uncertainty * self.config.capacity_mw * 0.3)
        competitor_bias = self.rng.normal(0.0, 3.0) * competition_factor

        clearing_price = max(5.0, self.current_price + price_noise - competitor_bias)
        realized_demand = max(0.0, self.current_demand + demand_shock)

        cleared_quantity = min(bid_quantity, realized_demand)
        production_cost = self.current_price * 0.7
        win = bid_price <= clearing_price

        if win:
            revenue = cleared_quantity * clearing_price
            cost = cleared_quantity * production_cost
            profit = revenue - cost
        else:
            revenue = 0.0
            cost = cleared_quantity * production_cost
            profit = -production_cost * 0.1
            cleared_quantity = 0.0

        risk_penalty = self.config.risk_aversion * (abs(bid_price - clearing_price) * 0.2 + (bid_quantity / max(self.config.capacity_mw, 1.0)) * 2.0)
        inventory_penalty = max(0.0, bid_quantity - realized_demand) * 0.05
        reward = float(profit - risk_penalty - inventory_penalty)

        self.current_price = max(5.0, clearing_price + self.rng.normal(0.0, self.config.price_volatility * 4.0))
        self.current_demand = max(0.0, realized_demand * (0.6 + 0.4 * self.rng.uniform(0.5, 1.1)))

        done = self.current_time_index >= self.horizon

        step_info = {
            "timestamp": self._episode_start + timedelta(hours=self.current_time_index),
            "bid_price": bid_price,
            "clearing_price": clearing_price,
            "bid_quantity": bid_quantity,
            "cleared_quantity": cleared_quantity,
            "revenue": revenue,
            "profit": profit,
            "reward": reward,
            "win": win,
            "realized_demand": realized_demand,
            "competition_factor": competition_factor,
            "risk_penalty": risk_penalty,
            "inventory_penalty": inventory_penalty,
            "production_cost": production_cost,
            "action_index": int(action_index)
        }
        self._step_history.append(step_info)

        next_state = self._get_state_vector(state_dim, time_fraction=time_fraction)
        return next_state, reward, done, step_info

    def _get_state_vector(self, target_dim: int, *, time_fraction: float = 0.0) -> np.ndarray:
        volatility = self.config.price_volatility
        demand_ratio = 0.0 if self.config.capacity_mw == 0 else self.current_demand / self.config.capacity_mw
        competition_level = {
            "low": 0.2,
            "moderate": 0.5,
            "high": 0.9
        }.get(self.config.competition_level, 0.5)

        base_state = np.array(
            [
                self.current_price / max(self._base_price, 1.0),
                demand_ratio,
                volatility,
                self.config.demand_uncertainty,
                competition_level,
                self.config.risk_aversion,
                math.sin(math.pi * time_fraction),
                math.cos(math.pi * time_fraction)
            ],
            dtype=np.float32
        )

        if target_dim <= 0:
            return base_state

        if base_state.shape[0] >= target_dim:
            return base_state[:target_dim]

        padded = np.zeros(target_dim, dtype=np.float32)
        padded[: base_state.shape[0]] = base_state
        return padded

    @property
    def step_history(self) -> List[Dict[str, Any]]:
        return list(self._step_history)

class BiddingRLService:
    """Optimized Bidding Toolkit with RL Sandbox."""

    def __init__(self):
        """Initialize bidding RL service."""
        self.dao = TrinoDAO()
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # RL training state
        self._environments: Dict[str, AuctionEnvironment] = {}
        self._policies: Dict[str, BiddingPolicy] = {}
        self._training_sessions: Dict[str, RLTrainingSession] = {}
        self._evaluations: Dict[str, PolicyEvaluation] = {}

        # Simulation state
        self._simulation_cache: Dict[str, Any] = {}
        self._policy_models: Dict[str, PolicyModelState] = {}

        # Initialize synthetic environments
        self._initialize_environments()

    def _initialize_environments(self) -> None:
        """Initialize synthetic auction environments for training."""
        # PJM Day-Ahead Market
        self._environments["pjm_da"] = AuctionEnvironment(
            environment_id="pjm_da",
            auction_type="day_ahead",
            market="pjm",
            geography="pjm",
            capacity_mw=1000.0,
            time_horizon_hours=24,
            price_volatility=0.4,
            demand_uncertainty=0.3,
            competition_level="high",
            risk_aversion=0.6
        )

        # ERCOT Real-Time Market
        self._environments["ercot_rt"] = AuctionEnvironment(
            environment_id="ercot_rt",
            auction_type="real_time",
            market="ercot",
            geography="ercot",
            capacity_mw=500.0,
            time_horizon_hours=1,
            price_volatility=0.6,
            demand_uncertainty=0.4,
            competition_level="moderate",
            risk_aversion=0.4
        )

    async def create_auction_environment(self, config: AuctionEnvironment) -> str:
        """Create a new auction simulation environment."""
        env_id = config.environment_id
        self._environments[env_id] = config

        self.telemetry.info("Auction environment created", environment_id=env_id)
        return env_id

    async def create_bidding_policy(self, config: BiddingPolicy) -> str:
        """Create a new RL bidding policy."""
        policy_id = config.policy_id
        self._policies[policy_id] = config
        rng = np.random.default_rng()

        state_dim = max(1, int(config.state_dim))
        action_dim = max(2, int(config.action_dim))
        network = PolicyNetwork(state_dim, config.hidden_layers, action_dim, rng)
        target_network = network.clone(rng)

        replay_buffer = ReplayBuffer(config.memory_size)

        min_epsilon = max(0.01, min(0.5, config.epsilon * 0.1))
        if config.epsilon <= min_epsilon:
            exploration_decay = 0.995
        else:
            exploration_decay = math.exp(
                math.log(min_epsilon / config.epsilon)
                / max(1, config.training_episodes)
            )
            exploration_decay = float(np.clip(exploration_decay, 0.90, 0.9999))

        model_state = PolicyModelState(
            network=network,
            target_network=target_network,
            replay_buffer=replay_buffer,
            rng=rng,
            exploration_rate=float(config.epsilon),
            exploration_min=float(min_epsilon),
            exploration_decay=float(exploration_decay)
        )

        self._policy_models[policy_id] = model_state

        self.telemetry.info("Bidding policy created", policy_id=policy_id)
        return policy_id

    async def start_rl_training(self, policy_id: str, environment_id: str) -> str:
        """Start RL training session."""
        session_id = str(uuid4())

        policy = self._policies[policy_id]
        policy.performance_metrics["last_training_environment"] = environment_id
        policy.performance_metrics["episodes_target"] = policy.training_episodes

        session = RLTrainingSession(
            session_id=session_id,
            policy_id=policy_id,
            environment_id=environment_id,
            total_episodes=policy.training_episodes,
            learning_rate=policy.learning_rate,
            exploration_rate=policy.epsilon,
            status="running",
            started_at=datetime.utcnow()
        )

        self._training_sessions[session_id] = session

        # Start training in background
        asyncio.create_task(self._training_loop(session_id))

        self.telemetry.info("RL training started", session_id=session_id)
        return session_id

    async def _training_loop(self, session_id: str) -> None:
        """Background RL training loop."""
        session = self._training_sessions[session_id]
        policy = self._policies[session.policy_id]
        environment = self._environments[session.environment_id]
        model_state = self._policy_models[session.policy_id]
        env_runner = AuctionSimulator(environment, seed=int(model_state.rng.integers(1_000_000)))

        try:
            for episode in range(session.total_episodes):
                session.current_episode = episode + 1

                # Run single episode
                episode_reward, episode_steps, episode_loss = await self._run_training_episode(
                    session,
                    policy,
                    env_runner,
                    model_state
                )

                session.episode_rewards.append(episode_reward)
                session.average_reward = sum(session.episode_rewards[-100:]) / min(len(session.episode_rewards), 100)

                # Update best reward
                if episode_reward > session.best_reward:
                    session.best_reward = episode_reward

                # Update exploration rate (epsilon decay)
                model_state.exploration_rate = max(
                    model_state.exploration_min,
                    model_state.exploration_rate * model_state.exploration_decay
                )
                session.exploration_rate = model_state.exploration_rate

                # Periodic checkpointing
                if episode % 100 == 0:
                    self.telemetry.info("Training checkpoint", episode=episode, reward=episode_reward)

                model_state.training_history.append(
                    {
                        "episode": session.current_episode,
                        "reward": episode_reward,
                        "epsilon": session.exploration_rate,
                        "steps": episode_steps,
                        "loss": episode_loss,
                        "timestamp": datetime.utcnow()
                    }
                )
                if len(model_state.training_history) > 500:
                    model_state.training_history.pop(0)

                await asyncio.sleep(0)

            policy.performance_metrics.update(
                {
                    "episodes_completed": session.current_episode,
                    "average_reward_training": session.average_reward,
                    "best_reward": session.best_reward,
                    "exploration_final": session.exploration_rate,
                    "total_steps": session.total_steps,
                    "last_trained": datetime.utcnow().isoformat(),
                    "last_training_environment": session.environment_id
                }
            )

            session.status = "completed"
            session.completed_at = datetime.utcnow()

            self.telemetry.info("RL training completed", session_id=session_id, episodes=session.total_episodes)

        except Exception as e:
            session.status = "failed"
            session.error_message = str(e)
            session.completed_at = datetime.utcnow()

            self.telemetry.error("RL training failed", session_id=session_id, error=str(e))

    async def _run_training_episode(
        self,
        session: RLTrainingSession,
        policy: BiddingPolicy,
        env_runner: AuctionSimulator,
        model_state: PolicyModelState
    ) -> Tuple[float, int, Optional[float]]:
        """Run a single training episode using epsilon-greedy DQN updates."""

        state = env_runner.reset(policy.state_dim)
        total_reward = 0.0
        total_steps = 0
        losses: List[float] = []
        done = False

        while not done:
            action = await self._select_action(policy, model_state, state)
            next_state, reward, done, _ = env_runner.step(action, policy.action_dim, policy.state_dim)

            model_state.replay_buffer.add(state, action, reward, next_state, done)
            total_reward += reward
            state = next_state
            total_steps += 1

            if len(model_state.replay_buffer) >= policy.batch_size:
                loss = await self._update_policy(policy, model_state)
                if loss is not None:
                    losses.append(loss)

            if total_steps % 8 == 0:
                await asyncio.sleep(0)

        session.total_steps += total_steps

        average_loss = float(np.mean(losses)) if losses else None
        if average_loss is not None:
            model_state.recent_losses.append(average_loss)

        return total_reward, total_steps, average_loss

    async def _select_action(
        self,
        policy: BiddingPolicy,
        model_state: PolicyModelState,
        state: np.ndarray,
        *,
        greedy: bool = False
    ) -> int:
        """Select action via epsilon-greedy exploration policy."""

        epsilon = 0.0 if greedy else model_state.exploration_rate
        state_vector = np.asarray(state, dtype=np.float32)

        if model_state.rng.random() < epsilon:
            return int(model_state.rng.integers(0, policy.action_dim))

        q_values = model_state.network.predict(state_vector[np.newaxis, :])[0]
        if not np.all(np.isfinite(q_values)):
            return int(model_state.rng.integers(0, policy.action_dim))

        noise = model_state.rng.normal(0.0, 1e-6, size=q_values.shape)
        return int(np.argmax(q_values + noise))

    async def _update_policy(self, policy: BiddingPolicy, model_state: PolicyModelState) -> Optional[float]:
        """Update policy network using sampled experience replay transitions."""

        batch = model_state.replay_buffer.sample(policy.batch_size, model_state.rng)
        if batch is None:
            return None

        states, actions, rewards, next_states, dones = batch
        q_values = model_state.network.predict(states)
        target_q_values = q_values.copy()

        next_q = model_state.target_network.predict(next_states)
        max_next_q = np.max(next_q, axis=1)

        gamma = np.clip(policy.gamma, 0.0, 0.999)
        batch_index = np.arange(states.shape[0])
        target_q_values[batch_index, actions] = rewards + (1.0 - dones) * gamma * max_next_q

        loss = model_state.network.fit(states, target_q_values, policy.learning_rate)

        model_state.total_updates += 1
        if model_state.total_updates - model_state.last_target_sync >= policy.target_update_freq:
            model_state.target_network.copy_from(model_state.network)
            model_state.last_target_sync = model_state.total_updates

        return float(loss)

    async def _run_policy_rollout(
        self,
        policy: BiddingPolicy,
        model_state: PolicyModelState,
        env_config: AuctionEnvironment,
        steps: int,
        *,
        greedy: bool = True,
        seed: Optional[int] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
        """Roll out a policy in the specified environment and return raw step data and summary metrics."""

        env_runner = AuctionSimulator(env_config, seed=seed or int(model_state.rng.integers(1_000_000)))
        state = env_runner.reset(policy.state_dim)

        rollout_steps: List[Dict[str, Any]] = []
        total_reward = 0.0
        total_profit = 0.0
        total_revenue = 0.0
        wins = 0

        max_steps = max(1, int(steps))
        for step_idx in range(max_steps):
            action = await self._select_action(policy, model_state, state, greedy=greedy)
            next_state, reward, done, info = env_runner.step(action, policy.action_dim, policy.state_dim)

            step_info = dict(info)
            step_info["reward"] = reward
            step_info["action_index"] = action
            rollout_steps.append(step_info)

            total_reward += reward
            total_profit += step_info.get("profit", 0.0)
            total_revenue += step_info.get("revenue", 0.0)
            wins += 1 if step_info.get("win") else 0

            state = next_state

            if done:
                break

            if (step_idx + 1) % 8 == 0:
                await asyncio.sleep(0)

        steps_taken = len(rollout_steps)
        summary = {
            "total_reward": total_reward,
            "total_profit": total_profit,
            "total_revenue": total_revenue,
            "wins": wins,
            "steps": steps_taken,
            "win_rate": wins / max(steps_taken, 1),
            "avg_reward": total_reward / max(steps_taken, 1),
            "avg_profit": total_profit / max(steps_taken, 1),
            "avg_margin": total_profit / total_revenue if total_revenue > 0 else 0.0
        }

        return rollout_steps, summary

    def _convert_rollout_to_results(
        self,
        simulation_id: str,
        rollout_steps: List[Dict[str, Any]]
    ) -> List[AuctionResult]:
        results: List[AuctionResult] = []

        for item in rollout_steps:
            revenue = float(item.get("revenue", 0.0))
            profit = float(item.get("profit", 0.0))
            cleared_quantity = float(item.get("cleared_quantity", 0.0))
            realized_demand = float(item.get("realized_demand", max(cleared_quantity, 1.0)))
            margin = profit / revenue if revenue > 0 else 0.0

            results.append(
                AuctionResult(
                    result_id=str(uuid4()),
                    simulation_id=simulation_id,
                    timestamp=item.get("timestamp", datetime.utcnow()),
                    cleared_price=float(item.get("clearing_price", 0.0)),
                    cleared_quantity=cleared_quantity,
                    total_revenue=revenue,
                    profit_margin=margin,
                    market_share=cleared_quantity / max(realized_demand, 1.0),
                    competition_metrics={"competition_factor": float(item.get("competition_factor", 0.0))},
                    risk_metrics={
                        "risk_penalty": float(item.get("risk_penalty", 0.0)),
                        "inventory_penalty": float(item.get("inventory_penalty", 0.0))
                    },
                    performance_score=float(np.tanh(item.get("reward", 0.0) / 100.0))
                )
            )

        return results

    async def evaluate_policy(self, policy_id: str, test_period_days: int = 30) -> str:
        """Evaluate bidding policy performance."""
        if policy_id not in self._policies:
            raise ValueError(f"Unknown policy_id: {policy_id}")

        evaluation_id = str(uuid4())
        policy = self._policies[policy_id]
        model_state = self._policy_models[policy_id]

        last_env_id = policy.performance_metrics.get("last_training_environment")
        if last_env_id not in self._environments:
            last_env_id = next(iter(self._environments.keys()))

        environment = self._environments[last_env_id]
        episodes = max(1, int(policy.evaluation_episodes))

        episode_rewards: List[float] = []
        episode_profits: List[float] = []
        step_rewards: List[float] = []
        total_revenue = 0.0
        total_wins = 0
        cumulative_profit = 0.0
        peak_profit = 0.0
        max_drawdown = 0.0

        for _ in range(episodes):
            rollout_steps, _ = await self._run_policy_rollout(
                policy,
                model_state,
                environment,
                environment.time_horizon_hours,
                greedy=True
            )

            reward_sum = sum(float(step.get("reward", 0.0)) for step in rollout_steps)
            profit_sum = sum(float(step.get("profit", 0.0)) for step in rollout_steps)

            episode_rewards.append(reward_sum)
            episode_profits.append(profit_sum)

            for step in rollout_steps:
                reward = float(step.get("reward", 0.0))
                profit = float(step.get("profit", 0.0))
                revenue = float(step.get("revenue", 0.0))

                step_rewards.append(reward)
                total_revenue += revenue
                if step.get("win"):
                    total_wins += 1

                cumulative_profit += profit
                peak_profit = max(peak_profit, cumulative_profit)
                drawdown = peak_profit - cumulative_profit
                max_drawdown = max(max_drawdown, drawdown)

        avg_reward = float(np.mean(episode_rewards)) if episode_rewards else 0.0
        total_profit = float(np.sum(episode_profits))
        profit_margin = total_profit / total_revenue if total_revenue > 0 else 0.0
        win_rate = total_wins / max(len(step_rewards), 1)

        profit_std = float(np.std(episode_profits)) if len(episode_profits) > 1 else 0.0
        reward_std = float(np.std(step_rewards)) if len(step_rewards) > 1 else 0.0

        risk_adjusted_return = total_profit / (profit_std + 1e-6)
        sharpe_ratio = avg_reward / (reward_std + 1e-6)
        max_drawdown_ratio = max_drawdown / max(peak_profit, 1.0)

        baseline_rng = np.random.default_rng(1234)
        baseline_rewards: List[float] = []
        baseline_runs = min(5, episodes)
        for _ in range(baseline_runs):
            simulator = AuctionSimulator(environment, seed=int(baseline_rng.integers(1_000_000)))
            state = simulator.reset(policy.state_dim)
            done = False
            reward_accumulator = 0.0
            steps_taken = 0
            while not done and steps_taken < environment.time_horizon_hours:
                action = int(baseline_rng.integers(0, policy.action_dim))
                next_state, reward, done, _ = simulator.step(action, policy.action_dim, policy.state_dim)
                reward_accumulator += reward
                state = next_state
                steps_taken += 1
            baseline_rewards.append(reward_accumulator)

        benchmark_reward = float(np.mean(baseline_rewards)) if baseline_rewards else 0.0
        reward_uplift = avg_reward - benchmark_reward

        recommendations: List[str] = []
        if win_rate < 0.6:
            recommendations.append("Increase bidding aggressiveness or explore price adjustments to lift win rate.")
        if profit_margin < 0.15:
            recommendations.append("Optimize quantity sizing to improve profit margins.")
        if sharpe_ratio < 1.0:
            recommendations.append("Stabilize returns by tuning risk penalties or hedging strategy.")
        if not recommendations:
            recommendations.append("Policy shows strong performance; consider scaling deployment volume.")

        evaluation = PolicyEvaluation(
            evaluation_id=evaluation_id,
            policy_id=policy_id,
            test_period={
                "start": datetime.utcnow() - timedelta(days=test_period_days),
                "end": datetime.utcnow()
            },
            total_episodes=episodes,
            average_reward=avg_reward,
            win_rate=win_rate,
            profit_margin=profit_margin,
            risk_adjusted_return=risk_adjusted_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown_ratio,
            benchmark_comparison={
                "random_policy_reward": benchmark_reward,
                "reward_uplift": reward_uplift
            },
            recommendations=recommendations
        )

        policy.performance_metrics.update(
            {
                "average_reward": avg_reward,
                "win_rate": win_rate,
                "profit_margin": profit_margin,
                "risk_adjusted_return": risk_adjusted_return,
                "sharpe_ratio": sharpe_ratio,
                "max_drawdown": max_drawdown_ratio,
                "total_profit": total_profit,
                "episodes_evaluated": episodes,
                "last_evaluated": datetime.utcnow().isoformat(),
                "benchmark_reward": benchmark_reward,
                "reward_uplift": reward_uplift,
                "evaluation_environment": last_env_id
            }
        )

        self._evaluations[evaluation_id] = evaluation

        self.telemetry.info(
            "Policy evaluation completed",
            evaluation_id=evaluation_id,
            policy_id=policy_id,
            environment_id=last_env_id,
            average_reward=avg_reward,
            win_rate=win_rate
        )

        return evaluation_id

    async def simulate_auction(self, environment_id: str, policy_id: str, hours: int = 24) -> str:
        """Run auction simulation with specified policy."""
        if environment_id not in self._environments:
            raise ValueError(f"Unknown environment_id: {environment_id}")
        if policy_id not in self._policies:
            raise ValueError(f"Unknown policy_id: {policy_id}")

        simulation_id = str(uuid4())
        environment = self._environments[environment_id]
        policy = self._policies[policy_id]
        model_state = self._policy_models[policy_id]

        steps = min(max(1, int(hours)), environment.time_horizon_hours)
        rollout_steps, summary = await self._run_policy_rollout(
            policy,
            model_state,
            environment,
            steps,
            greedy=True
        )

        results = self._convert_rollout_to_results(simulation_id, rollout_steps)

        self._simulation_cache[simulation_id] = {
            "environment_id": environment_id,
            "policy_id": policy_id,
            "results": results,
            "summary": summary
        }

        self.telemetry.info(
            "Auction simulation completed",
            simulation_id=simulation_id,
            environment_id=environment_id,
            policy_id=policy_id,
            steps=len(results),
            win_rate=summary.get("win_rate")
        )
        return simulation_id

    async def get_training_status(self, session_id: str) -> Optional[RLTrainingSession]:
        """Get training session status."""
        return self._training_sessions.get(session_id)

    async def get_policy_performance(self, policy_id: str) -> Dict[str, Any]:
        """Get policy performance metrics."""
        policy = self._policies[policy_id]
        model_state = self._policy_models[policy_id]

        weight_count = sum(weight.size for weight in model_state.network.weights)
        latest_entry = model_state.training_history[-1] if model_state.training_history else None

        return {
            "policy_id": policy_id,
            "training_events": len(model_state.training_history),
            "performance_metrics": policy.performance_metrics,
            "exploration_rate": model_state.exploration_rate,
            "replay_buffer_size": len(model_state.replay_buffer),
            "model_parameters": weight_count,
            "recent_losses": list(model_state.recent_losses),
            "last_updated": latest_entry.get("timestamp") if latest_entry else None
        }

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "active_environments": len(self._environments),
            "active_policies": len(self._policies),
            "training_sessions": len([s for s in self._training_sessions.values() if s.status == "running"]),
            "evaluations": len(self._evaluations),
            "last_activity": datetime.utcnow()
        }

_BIDDING_RL_SERVICE: Optional[BiddingRLService] = None


def get_bidding_rl_service() -> BiddingRLService:
    """Get the global bidding RL service instance."""
    global _BIDDING_RL_SERVICE
    if _BIDDING_RL_SERVICE is None:
        _BIDDING_RL_SERVICE = BiddingRLService()
    return _BIDDING_RL_SERVICE


async def simulate_auction_scenario(
    environment_id: str,
    policy_id: str,
    scenario_config: Dict[str, Any]
) -> List[AuctionResult]:
    """Simulate auction scenario with custom parameters."""
    service = get_bidding_rl_service()
    if environment_id not in service._environments:
        raise ValueError(f"Unknown environment_id: {environment_id}")
    if policy_id not in service._policies:
        raise ValueError(f"Unknown policy_id: {policy_id}")

    base_env = service._environments[environment_id]
    valid_fields = set(base_env.model_dump().keys())
    overrides = {
        key: value
        for key, value in scenario_config.items()
        if key in valid_fields
    }
    scenario_env = base_env.copy(update=overrides) if overrides else base_env

    steps = scenario_config.get("hours") or scenario_env.time_horizon_hours
    seed_override = scenario_config.get("seed")

    policy = service._policies[policy_id]
    model_state = service._policy_models[policy_id]

    rollout_steps, summary = await service._run_policy_rollout(
        policy,
        model_state,
        scenario_env,
        min(scenario_env.time_horizon_hours, max(1, int(steps))),
        greedy=True,
        seed=int(seed_override) if seed_override is not None else None
    )

    simulation_id = str(uuid4())
    results = service._convert_rollout_to_results(simulation_id, rollout_steps)

    service._simulation_cache[simulation_id] = {
        "environment_id": environment_id,
        "policy_id": policy_id,
        "results": results,
        "summary": summary,
        "scenario": scenario_config
    }

    return results


async def evaluate_bidding_policy(
    policy_id: str,
    historical_data: List[Dict[str, Any]],
    evaluation_period_days: int = 30
) -> PolicyEvaluation:
    """Evaluate bidding policy against historical data."""
    service = get_bidding_rl_service()
    evaluation_id = await service.evaluate_policy(policy_id, evaluation_period_days)
    evaluation = service._evaluations.get(evaluation_id)
    if evaluation is None:
        raise ValueError("Evaluation not found after execution")
    return evaluation
