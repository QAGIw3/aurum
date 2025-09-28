"""Unit tests for the ModelRegistryService workflows."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from aurum.api.services.model_registry_service import (
    ModelConfig,
    ModelRegistryService,
    ModelVersion,
)


def _build_config(model_type: str = "xgboost") -> ModelConfig:
    """Helper to construct a reusable ModelConfig instance."""

    return ModelConfig(
        model_type=model_type,
        hyperparameters={"n_estimators": 100, "max_depth": 6},
        feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
        target_variable="lmp_price",
        training_period_days=30,
        validation_period_days=7,
        test_period_days=7,
        cross_validation_folds=3,
        early_stopping_rounds=5,
        random_seed=13,
    )


def _build_version(
    model_name: str,
    version_number: str,
    accuracy: float,
    status: str = "active",
) -> ModelVersion:
    """Create a ModelVersion with deterministic metadata for tests."""

    config = _build_config()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=config.training_period_days)

    return ModelVersion(
        version_id=str(uuid4()),
        model_name=model_name,
        version_number=version_number,
        description=f"{model_name} {version_number}",
        config=config,
        training_start_date=start_time,
        training_end_date=end_time,
        model_path=f"/models/{model_name}/{version_number}",
        model_size_bytes=1_500_000,
        performance_metrics={
            "accuracy": accuracy,
            "rmse": max(0.01, 0.2 - accuracy / 10),
            "r2_score": accuracy,
        },
        feature_importance={
            "temperature": 0.3,
            "load_mw": 0.25,
            "humidity": 0.2,
            "wind_speed": 0.25,
        },
        validation_results={
            "cross_validation_scores": [accuracy] * 5,
            "mean_cv_score": accuracy,
        },
        status=status,
        created_by="unit-tests",
        tags={"team": "ml"},
    )


@pytest.fixture()
def model_registry_service() -> ModelRegistryService:
    """Provide a fresh ModelRegistryService for each test."""

    return ModelRegistryService()


@pytest.mark.asyncio
async def test_register_model_version_tracks_metadata(model_registry_service: ModelRegistryService) -> None:
    """Registering a version should populate model metadata and champion state."""

    version = _build_version("lmp_forecast", "v1.0", accuracy=0.89)

    registered = await model_registry_service.register_model_version(version)

    assert registered.version_id == version.version_id

    models = await model_registry_service.list_models()
    assert len(models) == 1
    metadata = models[0]
    assert metadata.name == "lmp_forecast"
    assert metadata.latest_version == "v1.0"
    assert metadata.total_versions == 1

    versions = await model_registry_service.list_model_versions("lmp_forecast")
    assert len(versions) == 1
    assert versions[0].version_number == "v1.0"

    champion = model_registry_service.get_current_champion_model("lmp_forecast")
    assert champion is not None
    assert champion.version_id == version.version_id


@pytest.mark.asyncio
async def test_promote_model_updates_champion_and_statuses(model_registry_service: ModelRegistryService) -> None:
    """Promoting a model should update champion mapping and demote others."""

    base = _build_version("price_forecast", "v1.0", accuracy=0.86)
    challenger = _build_version("price_forecast", "v2.0", accuracy=0.91)

    await model_registry_service.register_model_version(base)
    await model_registry_service.register_model_version(challenger)

    # Ensure default champion is the first registered version
    champion_before = model_registry_service.get_current_champion_model("price_forecast")
    assert champion_before and champion_before.version_id == base.version_id

    result = model_registry_service.promote_model("price_forecast", "v2.0")
    assert result is True

    new_champion = model_registry_service.get_current_champion_model("price_forecast")
    assert new_champion and new_champion.version_id == challenger.version_id
    versions = await model_registry_service.list_model_versions("price_forecast")
    status_by_version = {version.version_number: version.status for version in versions}
    assert status_by_version["v2.0"] == "champion"
    assert status_by_version["v1.0"] == "deprecated"


@pytest.mark.asyncio
async def test_compare_models_contains_model_reference(model_registry_service: ModelRegistryService) -> None:
    """Comparisons should include model name context and actionable recommendation."""

    champion = _build_version("wind_forecast", "v1.0", accuracy=0.82)
    challenger = _build_version("wind_forecast", "v2.0", accuracy=0.90)

    await model_registry_service.register_model_version(champion)
    await model_registry_service.register_model_version(challenger)

    comparison = await model_registry_service.compare_models(
        model_name="wind_forecast",
        champion_version=champion.version_id,
        challenger_version=challenger.version_id,
    )

    assert comparison.model_name == "wind_forecast"
    assert comparison.champion_version == champion.version_id
    assert comparison.challenger_version == challenger.version_id
    assert comparison.recommendation in {"promote_challenger", "needs_more_data", "keep_champion"}
    assert "accuracy_improvement" in comparison.comparison_metrics


@pytest.mark.asyncio
async def test_training_job_listing_supports_offset(model_registry_service: ModelRegistryService) -> None:
    """Training jobs should support pagination via limit and offset."""

    config = _build_config()
    for idx in range(3):
        await model_registry_service.start_training_job(f"training_model_{idx}", config=config)

    jobs = await model_registry_service.list_training_jobs(limit=2, offset=1)
    assert len(jobs) == 2
    assert jobs[0].created_at >= jobs[1].created_at


@pytest.mark.asyncio
async def test_list_retrain_schedules_filters_enabled(model_registry_service: ModelRegistryService) -> None:
    """Retrain schedules should honour enabled_only filtering."""

    model_registry_service.create_retrain_schedule("alpha_model", enabled=True)
    disabled_id = model_registry_service.create_retrain_schedule("beta_model", enabled=False)

    schedules = await model_registry_service.list_retrain_schedules(enabled_only=True)
    assert len(schedules) == 1
    assert schedules[0].model_name == "alpha_model"

    # Ensure the disabled schedule remains accessible when not filtering
    all_schedules = await model_registry_service.list_retrain_schedules()
    identifiers = {schedule.schedule_id for schedule in all_schedules}
    assert disabled_id in identifiers


@pytest.mark.asyncio
async def test_audit_metadata_recorded_for_register_compare_promote(
    model_registry_service: ModelRegistryService
) -> None:
    """Audit trail should capture register, compare, and promote operations."""

    register_audit = {"requested_by": "qa-user", "tenant_id": "tenant-a", "request_id": "req-1"}
    champion = _build_version("audit_model", "v1.0", accuracy=0.82)
    challenger = _build_version("audit_model", "v2.0", accuracy=0.91)

    registered = await model_registry_service.register_model_version(champion, audit_metadata=register_audit)
    await model_registry_service.register_model_version(
        challenger,
        audit_metadata={"requested_by": "analyst", "tenant_id": "tenant-a"},
    )

    register_event = model_registry_service.get_latest_audit_event(
        "register_model_version",
        registered.version_id,
    )
    assert register_event is not None
    assert register_event.audit.requested_by == "qa-user"
    assert register_event.reference["version_id"] == registered.version_id

    comparison = await model_registry_service.compare_models(
        model_name="audit_model",
        champion_version=registered.version_id,
        challenger_version=challenger.version_id,
        audit_metadata={"requested_by": "analyst", "notes": "side-by-side"},
    )

    compare_event = model_registry_service.get_latest_audit_event(
        "compare_models",
        comparison.comparison_id,
    )
    assert compare_event is not None
    assert compare_event.audit.requested_by == "analyst"
    assert compare_event.reference["challenger_version"] == challenger.version_id

    promoted = model_registry_service.promote_model(
        "audit_model",
        challenger.version_number,
        audit_metadata={"requested_by": "lead"},
    )
    assert promoted is True

    promote_event = model_registry_service.get_latest_audit_event(
        "promote_model",
        challenger.version_id,
    )
    assert promote_event is not None
    assert promote_event.audit.requested_by == "lead"
    assert promote_event.reference["status"] == "champion"


@pytest.mark.asyncio
async def test_register_model_version_is_immutable(model_registry_service: ModelRegistryService) -> None:
    """Registering a duplicate version number should raise a conflict."""

    base_version = _build_version("immutable_model", "v1.0", accuracy=0.81)
    await model_registry_service.register_model_version(base_version)

    duplicate = _build_version("immutable_model", "v1.0", accuracy=0.83)
    with pytest.raises(ValueError) as exc_info:
        await model_registry_service.register_model_version(duplicate)

    assert "immutable" in str(exc_info.value)


@pytest.mark.asyncio
async def test_update_model_metadata_records_changes(model_registry_service: ModelRegistryService) -> None:
    """Metadata updates should mutate fields and emit an audit event."""

    version = _build_version("metadata_model", "v1.0", accuracy=0.9)
    await model_registry_service.register_model_version(version)

    updated = model_registry_service.update_model_metadata(
        "metadata_model",
        description="Updated description",
        owners=["alice", "bob"],
        tags={"domain": "pricing"},
        metadata={"risk_level": "low"},
        audit_metadata={"requested_by": "alice"},
    )

    assert updated.description == "Updated description"
    assert updated.owners == ["alice", "bob"]
    assert updated.tags["domain"] == "pricing"
    assert updated.metadata["risk_level"] == "low"

    audit_event = model_registry_service.get_latest_audit_event("update_model_metadata")
    assert audit_event is not None
    changes = audit_event.reference.get("changes", {})
    assert changes.get("description", {}).get("current") == "Updated description"
    assert "tags_added" in changes or "tags_changed" in changes


@pytest.mark.asyncio
async def test_archive_model_sets_status_and_audit(model_registry_service: ModelRegistryService) -> None:
    """Archiving a model should update lifecycle metadata and produce audit event."""

    version = _build_version("archive_model", "v1.0", accuracy=0.88)
    await model_registry_service.register_model_version(version)

    result = model_registry_service.archive_model("archive_model", reason="deprecated")
    assert result is True

    model = model_registry_service.get_model("archive_model")
    assert model is not None
    assert model.status == "archived"
    lifecycle = model.metadata.get("lifecycle", {})
    assert lifecycle.get("reason") == "deprecated"

    audit_event = model_registry_service.get_latest_audit_event("archive_model")
    assert audit_event is not None
    assert audit_event.reference.get("reason") == "deprecated"


def test_background_job_status_exposes_counts(model_registry_service: ModelRegistryService) -> None:
    """Background job status should reflect idle state with zero counts by default."""

    status = model_registry_service.get_background_job_status()

    assert status["scheduler_state"] == "idle"
    assert status["trainer_state"] == "idle"
    assert status["pending_jobs"] == 0
    assert status["running_jobs"] == 0
    assert status["completed_jobs"] == 0


@pytest.mark.asyncio
async def test_select_champion_challenger_returns_pair(
    model_registry_service: ModelRegistryService
) -> None:
    """Champion/challenger selection should return both candidates and record audit."""

    base = _build_version("pair_model", "v1.0", accuracy=0.80)
    upgraded = _build_version("pair_model", "v2.0", accuracy=0.93)

    await model_registry_service.register_model_version(base)
    await model_registry_service.register_model_version(upgraded)

    selection = await model_registry_service.select_champion_challenger(
        "pair_model",
        selection_criteria={"min_accuracy": 0.5},
        audit_metadata={"requested_by": "ops"},
    )

    assert selection is not None
    assert selection.champion_version_id == upgraded.version_id
    assert selection.challenger_version_id == base.version_id

    selection_event = model_registry_service.get_latest_audit_event(
        "select_champion_challenger",
        selection.selection_id,
    )
    assert selection_event is not None
    assert selection_event.audit.requested_by == "ops"
