"""Deprecated global state accessors.

This module previously exposed global singletons used across the API. As part
of the "Eliminate Global State" refactor, all consumers must migrate to
dependency-injected access via FastAPI dependencies.

What to use instead:
- Import `AurumSettings` via FastAPI dependency using `aurum.api.deps.get_settings`
- Access cache via FastAPI dependency or via container service providers
- Avoid module-level initialization side effects
"""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
	from aurum.core import get_settings as _core_get_settings  # unified re-export
	_LIBS_AVAILABLE = True
except Exception:  # pragma: no cover - conservative fallback
	_LIBS_AVAILABLE = False

if TYPE_CHECKING:
	from aurum.core.settings import AurumSettings as _CoreAurumSettings  # for type hints only


def get_settings():
	"""Unified settings accessor for API codepaths.

	Prefer aurum.core.get_settings to eliminate duplicate env parsing.
	Falls back to aurum.core.settings.get_settings if re-export unavailable.
	"""
	if _LIBS_AVAILABLE:
		return _core_get_settings()
	from aurum.core.settings import get_settings as _direct_core_get_settings
	return _direct_core_get_settings()
