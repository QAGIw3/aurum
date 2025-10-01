"""Helpers for validation scripts to keep assertions concise and consistent."""

from __future__ import annotations

from enum import Enum
from typing import Any, Callable, Iterable, Sequence, Type, TypeVar

TEnum = TypeVar("TEnum", bound=Enum)


def assert_enum_values(
    enum_cls: Type[TEnum],
    expected_values: Iterable[Any],
    *,
    label: str,
    value_extractor: Callable[[TEnum], Any] | None = None,
) -> None:
    """Assert an enum exposes exactly the provided values in any order."""

    members = list(enum_cls)
    extractor = value_extractor or (lambda member: member.value)
    actual_values = [extractor(member) for member in members]

    expected = set(expected_values)
    actual = set(actual_values)

    assert len(actual_values) == len(expected), (
        f"{label}: expected {len(expected)} items, got {len(actual_values)}"
    )
    assert actual == expected, (
        f"{label}: values mismatch, expected {expected}, got {actual}"
    )

    print(f"✅ {label}")


def print_summary(lines: Sequence[str]) -> None:
    """Render a shared summary block for validation scripts."""

    if not lines:
        return

    print("📊 Summary:")
    for line in lines:
        print(f"- {line}")
