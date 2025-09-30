from __future__ import annotations

import asyncio
from typing import Iterable

import pytest

from aurum.external_contracts.publisher import ExternalContractsPublisher, PublishResult


@pytest.mark.asyncio
async def test_publish_invokes_run_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[str, ...], bool, bool]] = []

    async def fake_run_once(providers: Iterable[str], *, catalog: bool, observations: bool) -> None:
        calls.append((tuple(providers), catalog, observations))

    monkeypatch.setattr("aurum.external_contracts.publisher.run_once", fake_run_once)

    publisher = ExternalContractsPublisher(providers=("eia",))
    results = await publisher.publish(["fred"], catalog=False, observations=True)

    assert calls == [("fred",), False, True]
    assert results == [PublishResult(provider="fred", status="success", error=None)]


def test_publish_sync_wraps_async(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    async def fake_publish(self, providers=None, catalog=True, observations=True):  # type: ignore[override]
        calls.append("called")
        return [PublishResult(provider="eia", status="success")]

    monkeypatch.setattr(ExternalContractsPublisher, "publish", fake_publish, raising=False)

    publisher = ExternalContractsPublisher()
    result = publisher.publish_sync(["eia"])

    assert calls == ["called"]
    assert result == [PublishResult(provider="eia", status="success")]
