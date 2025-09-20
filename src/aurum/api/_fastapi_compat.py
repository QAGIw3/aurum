from __future__ import annotations

"""Compatibility helpers to keep mypy happy while using FastAPI's `examples`.

At runtime we re-export FastAPI's Query and Path. For type checking, we provide
loose stubs that accept arbitrary keyword args (including `examples`).
"""
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    def Query(default: Any = None, **kwargs: Any) -> Any:  # type: ignore[override]
        ...

    def Path(default: Any = ..., **kwargs: Any) -> Any:  # type: ignore[override]
        ...
else:  # pragma: no cover - thin re-export at runtime
    from fastapi import Query, Path  # type: ignore

__all__ = ["Query", "Path"]

