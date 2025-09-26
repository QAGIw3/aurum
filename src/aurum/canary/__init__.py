"""Test-friendly canary monitoring facade.

The original production canary implementation is quite large and pulls in a
number of optional dependencies.  For unit and integration tests we expose a
leaner surface by re-exporting the lightweight manager, runner, health checker,
and alerting utilities.  The orchestration modules remain available for
downstream usage, but the exports below prioritise the classes the test-suite
depends on.
"""

from .canary_manager import (
    CanaryConfig,
    CanaryDataset,
    CanaryManager,
    CanaryStatus,
)
from .api_health_checker import (
    APIHealthChecker,
    APIHealthCheck,
    APIHealthResult,
    APIHealthStatus,
)
from .canary_runner import (
    CanaryRunner,
    CanaryResult,
)
from .alert_manager import (
    CanaryAlertManager,
    CanaryAlertRule,
    CanaryAlertSeverity,
    CanaryAlert,
)

__all__ = [
    "CanaryConfig",
    "CanaryDataset",
    "CanaryManager",
    "CanaryStatus",
    "APIHealthChecker",
    "APIHealthCheck",
    "APIHealthResult",
    "APIHealthStatus",
    "CanaryRunner",
    "CanaryResult",
    "CanaryAlertManager",
    "CanaryAlertRule",
    "CanaryAlertSeverity",
    "CanaryAlert",
]
