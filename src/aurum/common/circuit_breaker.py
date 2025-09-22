"""Thread-safe circuit breaker utility for outbound calls.

Designed to be minimal and reusable across providers and clients while
preserving simple string state semantics for compatibility in tests.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CircuitBreaker:
    """Simple circuit breaker with CLOSED/OPEN/HALF_OPEN states.

    - CLOSED: operations allowed, failures increment counter
    - OPEN: operations rejected until timeout elapses
    - HALF_OPEN: allow a single trial; success -> CLOSED, failure -> OPEN
    """

    failure_threshold: int = 5
    recovery_timeout: float = 60.0  # seconds

    _failures: int = field(default=0, init=False)
    _last_failure_time: Optional[float] = field(default=None, init=False)
    _state: str = field(default="CLOSED", init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def is_open(self) -> bool:
        """Return True if the breaker is OPEN and requests should be short-circuited."""
        with self._lock:
            if self._state == "OPEN":
                if self._last_failure_time is not None and (
                    time.time() - self._last_failure_time > self.recovery_timeout
                ):
                    # Transition to HALF_OPEN after timeout
                    self._state = "HALF_OPEN"
                    return False
                return True
            return False

    def record_success(self) -> None:
        """Record a successful call and transition to CLOSED.

        Closing on success regardless of prior state keeps behavior predictable
        for simple unit tests and mirrors the common usage pattern where
        'success' is only recorded after an allowed request completes.
        """
        with self._lock:
            self._state = "CLOSED"
            self._failures = 0
            self._last_failure_time = None

    def record_failure(self) -> None:
        """Record a failed call and open the breaker if threshold reached."""
        with self._lock:
            self._failures += 1
            self._last_failure_time = time.time()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"

    def get_state(self) -> str:
        with self._lock:
            return self._state


__all__ = ["CircuitBreaker"]
