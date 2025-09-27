"""Observability package for comprehensive monitoring and tracing.

This package may be imported in tooling contexts (e.g., docs build) where
optional submodules are unavailable. Import submodules defensively to avoid
hard failures during import.
"""

__all__ = []

# Primary telemetry facade (recommended)
try:
    from . import telemetry_facade  # type: ignore
    __all__.append("telemetry_facade")
except Exception:  # pragma: no cover - optional at import time
    telemetry_facade = None  # type: ignore

try:
    from . import metrics  # type: ignore
    __all__.append("metrics")
except Exception:  # pragma: no cover - optional at import time
    metrics = None  # type: ignore

try:
    from . import tracing  # type: ignore
    __all__.append("tracing")
except Exception:  # pragma: no cover
    tracing = None  # type: ignore

try:
    from . import logging  # type: ignore
    __all__.append("logging")
except Exception:  # pragma: no cover
    logging = None  # type: ignore

try:
    from . import instrumentation  # type: ignore
    __all__.append("instrumentation")
except Exception:  # pragma: no cover
    instrumentation = None  # type: ignore

try:
    from . import profiling  # type: ignore
    __all__.append("profiling")
except Exception:  # pragma: no cover
    profiling = None  # type: ignore

# API endpoints are optional; failure to import should not block package import
try:
    from . import api  # type: ignore
    __all__.append("api")
except Exception:  # pragma: no cover
    api = None  # type: ignore
