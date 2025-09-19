from __future__ import annotations

"""Compatibility shim for the optional `requests` dependency."""

from typing import Any

try:  # pragma: no cover - deferred import to keep runtime lightweight
    import requests as _requests  # type: ignore[import]
except ImportError:  # pragma: no cover - handled by RequestsProxy
    _requests = None


class RequestsProxy:
    """Lightweight proxy exposing a subset of the requests API.

    When the real dependency is installed, attributes resolve to the genuine
    implementation. Otherwise, attribute access raises an informative
    ModuleNotFoundError unless the attribute has been monkeypatched (e.g. in
    tests).
    """

    __slots__ = {"_module", "_overrides"}

    def __init__(self) -> None:
        object.__setattr__(self, "_module", _requests)
        object.__setattr__(self, "_overrides", {})

    def __getattr__(self, name: str) -> Any:
        overrides: dict[str, Any] = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]

        module = object.__getattribute__(self, "_module")
        if module is None:
            def _missing(*_: Any, **__: Any) -> None:
                raise ModuleNotFoundError(
                    "The 'requests' package is required for this operation. Install it via 'pip install requests'."
                )

            overrides[name] = _missing
            return _missing
        return getattr(module, name)

    def __setattr__(self, name: str, value: Any) -> None:
        overrides: dict[str, Any] = object.__getattribute__(self, "_overrides")
        overrides[name] = value

    def __delattr__(self, name: str) -> None:
        overrides: dict[str, Any] = object.__getattribute__(self, "_overrides")
        overrides.pop(name, None)


requests = RequestsProxy()

__all__ = ["requests", "RequestsProxy"]
