"""Legacy API compatibility shims for tests.

Provides minimal exports expected by older tests.
"""

try:
    from .models import CurvePoint, CurveDiffPoint, Meta  # type: ignore
except Exception:  # pragma: no cover - fallback for missing legacy models
    class _Placeholder:
        pass
    CurvePoint = CurveDiffPoint = Meta = _Placeholder


