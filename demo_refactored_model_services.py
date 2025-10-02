#!/usr/bin/env python3
"""Demonstration of the refactored model services architecture."""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Also add the libs directory specifically
libs_path = Path(__file__).parent / "src" / "libs"
if libs_path.exists():
    sys.path.insert(0, str(libs_path))

# Set up environment for demo
os.environ["AURUM_ENVIRONMENT"] = "demo"
os.environ["AURUM_DEBUG"] = "true"

from aurum.api.services.model.models import ModelConfig, ModelVersion
from aurum.api.services.model.management_service import ModelManagementService
from aurum.api.services.model.training_service import ModelTrainingService
from aurum.api.services.model.comparison_service import ModelComparisonService
from aurum.api.services.model.scheduling_service import ModelSchedulingService
from aurum.api.services.model.service_factory import (
    get_model_service_factory,
    get_model_management_service,
    get_model_training_service,
    get_model_comparison_service,
    get_model_scheduling_service
)


async def demo_model_management():
    """Demonstrate model management functionality."""
    print("🏗️  MODEL MANAGEMENT SERVICE")
    print("=" * 50)

    # Create management service
    management_service = get_model_management_service()

    # Register a new model
    model = await management_service.register_model(
        model_name="price_forecasting",
        description="Machine learning model for electricity price forecasting",
        model_type="xgboost",
        created_by="ml_engineer"
    )

    print(f"✓ Registered model: {model.model_name}")
    print(f"  Description: {model.description}")
    print(f"  Type: {model.model_type}")
    print()

    # Register a model version
    config = ModelConfig(
        model_type="xgboost",
        hyperparameters={"n_estimators": 100, "max_depth": 6},
        feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
        target_variable="lmp_price"
    )

    model_version = ModelVersion(
        version_id="v1.0.0-001",
        model_name="price_forecasting",
        version_number="v1.0.0",
        description="Initial XGBoost model with basic features",
        config=config,
        training_start_date=datetime.utcnow(),
        training_end_date=datetime.utcnow(),
        model_path="models/price_forecasting/v1.0.0",
        model_size_bytes=1024 * 1024,
        performance_metrics={"accuracy": 0.94, "rmse": 0.06, "r2_score": 0.96},
        feature_importance={"temperature": 0.35, "load_mw": 0.30, "humidity": 0.20, "wind_speed": 0.15},
        created_by="ml_engineer",
        status="active"
    )

    registered_version = await management_service.register_model_version(
        model_name="price_forecasting",
        version=model_version,
        created_by="ml_engineer"
    )

    print(f"✓ Registered model version: {registered_version.version_number}")
    print(f"  Performance: {registered_version.performance_metrics}")
    print(f"  Status: {registered_version.status}")
    print()

    # List models and versions
    models = await management_service.list_models()
    print(f"✓ Total registered models: {len(models)}")

    versions = await management_service.list_model_versions("price_forecasting")
    print(f"✓ Model versions for price_forecasting: {len(versions)}")
    print()


async def demo_model_training():
    """Demonstrate model training functionality."""
    print("🏃 MODEL TRAINING SERVICE")
    print("=" * 50)

    # Create training service
    training_service = get_model_training_service()

    # Start a training job
    config = ModelConfig(
        model_type="xgboost",
        hyperparameters={"n_estimators": 200, "max_depth": 8},
        feature_selection=["temperature", "humidity", "wind_speed", "load_mw", "solar_generation"],
        target_variable="lmp_price"
    )

    job_id = await training_service.start_training_job(
        model_name="price_forecasting",
        config=config,
        created_by="ml_engineer"
    )

    print(f"✓ Started training job: {job_id}")

    # Monitor progress
    await asyncio.sleep(0.1)  # Give it time to start

    job = await training_service.get_training_job(job_id)
    if job:
        print(f"  Status: {job.status}")
        print(f"  Progress: {job.progress:.1%}")
        print(f"  Stage: {job.current_stage}")

        # Update progress manually (simulating external progress updates)
        await training_service.update_training_job_progress(
            job_id=job_id,
            progress=0.5,
            stage="feature_engineering",
            metrics={"features_selected": 150, "current_rmse": 0.08}
        )

        print("  Updated progress: 50% (feature_engineering)")

    print()

    # List all training jobs
    jobs = await training_service.list_training_jobs()
    print(f"✓ Total training jobs: {len(jobs)}")
    print()


async def demo_model_comparison():
    """Demonstrate model comparison functionality."""
    print("⚖️  MODEL COMPARISON SERVICE")
    print("=" * 50)

    # Create comparison service
    comparison_service = get_model_comparison_service()

    # Create a champion/challenger selection
    selection = await comparison_service.create_champion_challenger_selection(
        model_name="price_forecasting",
        champion_version="v1.0.0",
        challenger_versions=["v1.1.0", "v1.2.0"],
        selection_criteria={
            "accuracy": 0.4,
            "rmse": 0.3,
            "model_size": 0.1,
            "training_time": 0.1,
            "business_impact": 0.1
        },
        created_by="ml_engineer"
    )

    print(f"✓ Created champion/challenger selection: {selection.model_name}")
    print(f"  Champion: {selection.champion_version}")
    print(f"  Challengers: {selection.challenger_versions}")
    print(f"  Criteria: {selection.selection_criteria}")
    print()

    # Select champion model
    champion = await comparison_service.select_champion_model(
        model_name="price_forecasting",
        selected_by="ml_engineer"
    )

    if champion:
        print(f"✓ Selected champion: {champion.version_number}")
        print(f"  Score: {champion.champion_score}")
        print(f"  Status: {champion.status}")
    else:
        print("⚠️  No champion selected")

    print()


async def demo_model_scheduling():
    """Demonstrate model scheduling functionality."""
    print("⏰ MODEL SCHEDULING SERVICE")
    print("=" * 50)

    # Create scheduling service
    scheduling_service = get_model_scheduling_service()

    # Create a retraining schedule
    config = ModelConfig(
        model_type="xgboost",
        hyperparameters={"n_estimators": 100, "max_depth": 6},
        feature_selection=["temperature", "humidity", "wind_speed", "load_mw"],
        target_variable="lmp_price"
    )

    schedule = await scheduling_service.create_retrain_schedule(
        model_name="price_forecasting",
        cron_expression="0 2 * * 1",  # Weekly on Monday at 2 AM
        config=config,
        created_by="ml_engineer"
    )

    print(f"✓ Created retrain schedule: {schedule.schedule_id}")
    print(f"  Model: {schedule.model_name}")
    print(f"  Cron: {schedule.cron_expression}")
    print(f"  Next run: {schedule.next_run}")
    print(f"  Enabled: {schedule.enabled}")
    print()

    # List schedules
    schedules = await scheduling_service.list_retrain_schedules()
    print(f"✓ Total retrain schedules: {len(schedules)}")

    # Get scheduler status
    status = scheduling_service.get_scheduler_status()
    print(f"✓ Scheduler status: {status['scheduler_state']}")
    print(f"  Active schedules: {status['active_schedules']}")
    print()


async def demo_service_integration():
    """Demonstrate how services work together."""
    print("🔗 SERVICE INTEGRATION")
    print("=" * 50)

    # Get all services from factory
    factory = get_model_service_factory()
    services = factory.get_all_services()

    print(f"✓ Created {len(services)} services:")
    for name, service in services.items():
        print(f"  - {name}: {type(service).__name__}")

    print()

    # Demonstrate cross-service workflow
    management_service = services["management"]
    training_service = services["training"]
    comparison_service = services["comparison"]

    # Register model and version through management service
    model = await management_service.register_model(
        model_name="integration_demo",
        description="Demo model for service integration",
        model_type="xgboost",
        created_by="demo_user"
    )

    print(f"✓ Registered model via management service: {model.model_name}")

    # Start training job through training service
    config = ModelConfig(
        model_type="xgboost",
        hyperparameters={"n_estimators": 50, "max_depth": 4},
        feature_selection=["feature1", "feature2"],
        target_variable="target"
    )

    job_id = await training_service.start_training_job(
        model_name="integration_demo",
        config=config,
        created_by="demo_user"
    )

    print(f"✓ Started training job via training service: {job_id}")

    # Wait a bit for training to complete
    await asyncio.sleep(0.5)

    # Check if training completed and model was registered
    versions = await management_service.list_model_versions("integration_demo")
    print(f"✓ Model versions after training: {len(versions)}")

    if versions:
        latest_version = versions[0]
        print(f"  Latest version: {latest_version.version_number}")
        print(f"  Status: {latest_version.status}")
        print(f"  Performance: {latest_version.performance_metrics}")

    print()


async def main():
    """Run all demonstrations."""
    print("🚀 AURUM REFACTORED MODEL SERVICES DEMO")
    print("=" * 60)
    print()

    try:
        await demo_model_management()
        await demo_model_training()
        await demo_model_comparison()
        await demo_model_scheduling()
        await demo_service_integration()

        print("🎉 DEMO COMPLETED SUCCESSFULLY!")
        print()
        print("✅ Refactoring Benefits Demonstrated:")
        print("  • Single Responsibility: Each service has one clear purpose")
        print("  • Dependency Injection: Services are loosely coupled")
        print("  • Interface Segregation: Clean, focused interfaces")
        print("  • Testability: Each service can be tested independently")
        print("  • Maintainability: Smaller, focused codebases")
        print()

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # Clean up services
        factory = get_model_service_factory()
        factory.stop_all_services()

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
