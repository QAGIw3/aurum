from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from aurum.iceberg import maintenance


class DummySnapshot:
    def __init__(self, snapshot_id: int, timestamp_ms: int) -> None:
        self.snapshot_id = snapshot_id
        self.timestamp_ms = timestamp_ms


class DummyExpireResult:
    def __init__(self, deleted_snapshots: list[int], deleted_data_files: list[str]) -> None:
        self.deleted_snapshot_ids = deleted_snapshots
        self.deleted_data_files = deleted_data_files


class DummyRewriteResult:
    def __init__(self, rewritten: list[str], added: list[str]) -> None:
        self.rewritten_data_files = rewritten
        self.added_data_files = added


class DummyRewriteBuilder:
    def __init__(self, result: DummyRewriteResult) -> None:
        self._size = None
        self._result = result

    def with_target_file_size_in_bytes(self, size: int) -> "DummyRewriteBuilder":
        self._size = size
        return self

    def with_target_file_size_bytes(self, size: int) -> "DummyRewriteBuilder":
        self._size = size
        return self

    def options(self, **_: int) -> "DummyRewriteBuilder":
        return self

    def execute(self) -> DummyRewriteResult:
        assert self._result is not None
        return self._result


class DummyManifestResult:
    def __init__(self, added: list[str], deleted: list[str]) -> None:
        self.added_manifests = added
        self.deleted_manifests = deleted


class DummyManifestBuilder:
    def __init__(self, result: DummyManifestResult) -> None:
        self._result = result
        self._min = None
        self._max = None

    def with_min_input_files(self, count: int) -> "DummyManifestBuilder":
        self._min = count
        return self

    def with_target_size_in_bytes(self, size: int) -> "DummyManifestBuilder":
        self._max = size
        return self

    def with_max_file_group_size_in_bytes(self, size: int) -> "DummyManifestBuilder":
        self._max = size
        return self

    def options(self, **_: int) -> "DummyManifestBuilder":
        return self

    def execute(self) -> DummyManifestResult:
        return self._result


class DummyRemoveOrphanResult:
    def __init__(self, locations: list[str]) -> None:
        self.orphan_file_locations = locations


class DummyRemoveOrphanBuilder:
    def __init__(self, result: DummyRemoveOrphanResult) -> None:
        self._result = result
        self._cutoff = None

    def older_than(self, cutoff) -> "DummyRemoveOrphanBuilder":  # type: ignore[override]
        self._cutoff = cutoff
        return self

    def older_than_ts(self, cutoff_ts: int) -> "DummyRemoveOrphanBuilder":  # type: ignore[override]
        self._cutoff = cutoff_ts
        return self

    def execute(self) -> DummyRemoveOrphanResult:
        return self._result


class DummyTable:
    def __init__(self) -> None:
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        old = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000)
        self._snapshots = [DummySnapshot(1, old), DummySnapshot(2, now)]
        self._expire_result = DummyExpireResult([1], ["file-a.parquet"])
        self._rewrite_result = DummyRewriteResult(["file-a.parquet"], ["file-b.parquet"])
        self._manifest_result = DummyManifestResult(["manifest-new.avro"], ["manifest-old.avro"])
        self._remove_orphan_result = DummyRemoveOrphanResult(["s3://bucket/data/file.parquet"])

    def snapshots(self):  # noqa: D401 - simple iterator
        return list(self._snapshots)

    def expire_snapshots(self, expire_timestamp_ms: int) -> DummyExpireResult:
        assert isinstance(expire_timestamp_ms, int)
        return self._expire_result

    def rewrite_data_files(self) -> DummyRewriteBuilder:
        return DummyRewriteBuilder(self._rewrite_result)

    def rewrite_manifests(self) -> DummyManifestBuilder:
        return DummyManifestBuilder(self._manifest_result)

    def remove_orphan_files(self) -> DummyRemoveOrphanBuilder:
        return DummyRemoveOrphanBuilder(self._remove_orphan_result)


@pytest.fixture(autouse=True)
def patch_loader(monkeypatch):
    dummy = DummyTable()
    monkeypatch.setattr(maintenance, "_load_catalog_table", lambda table_name: dummy)
    yield


def test_expire_snapshots_dry_run():
    result = maintenance.expire_snapshots("iceberg.market.curve_observation", older_than_days=7, dry_run=True)
    assert result["dry_run"] is True
    assert result["eligible_snapshots"] >= 1


def test_expire_snapshots_executes():
    result = maintenance.expire_snapshots("iceberg.market.curve_observation", older_than_days=7, dry_run=False)
    assert result["deleted_snapshots"] == 1
    assert result["deleted_data_files"] == 1


def test_rewrite_data_files_executes():
    result = maintenance.rewrite_data_files("iceberg.market.curve_observation", target_file_size_mb=64)
    assert result["rewritten_files"] == 1
    assert result["added_files"] == 1
    assert result["target_file_size_mb"] == 64


def test_rewrite_data_files_dry_run():
    result = maintenance.rewrite_data_files(
        "iceberg.market.curve_observation",
        target_file_size_mb=32,
        dry_run=True,
    )
    assert result["dry_run"] is True
    assert result["target_file_size_mb"] == 32


def test_rewrite_manifests_executes():
    result = maintenance.rewrite_manifests(
        "iceberg.market.curve_observation",
        min_count_to_merge=2,
        max_group_size_mb=256,
    )
    assert result["added_manifests"] == 1
    assert result["deleted_manifests"] == 1
    assert result["min_count_to_merge"] == 2


def test_rewrite_manifests_dry_run():
    result = maintenance.rewrite_manifests(
        "iceberg.market.curve_observation",
        dry_run=True,
        min_count_to_merge=8,
        max_group_size_mb=128,
    )
    assert result["dry_run"] is True
    assert result["min_count_to_merge"] == 8
    assert result["max_group_size_mb"] == 128


def test_purge_orphan_files_executes():
    result = maintenance.purge_orphan_files(
        "iceberg.market.curve_observation",
        older_than_hours=12,
    )
    assert result["removed_orphans"] == 1
    assert result["older_than_hours"] == 12


def test_purge_orphan_files_dry_run():
    result = maintenance.purge_orphan_files(
        "iceberg.market.curve_observation",
        older_than_hours=6,
        dry_run=True,
    )
    assert result["dry_run"] is True
    assert result["older_than_hours"] == 6
