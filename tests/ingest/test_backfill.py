from __future__ import annotations

from aurum.scripts.ingest import backfill


def test_backfill_dry_run(monkeypatch, capsys):
    calls: list = []
    monkeypatch.setattr(backfill, "update_ingest_watermark", lambda *args, **kwargs: calls.append((args, kwargs)))

    exit_code = backfill.main([
        "--source",
        "demo",
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-02",
        "--dry-run",
    ])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "would update watermark" in captured.out
    assert calls == []


def test_backfill_updates(monkeypatch):
    updates = []

    def fake_update(source, key, ts):
        updates.append((source, key, ts.isoformat()))

    monkeypatch.setattr(backfill, "update_ingest_watermark", fake_update)
    backfill.main([
        "--source",
        "demo",
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-01",
        "--watermark-time",
        "12:00:00",
    ])
    assert updates == [("demo", "default", "2024-01-01T12:00:00+00:00")]
