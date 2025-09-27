"""Tests for auto-reforecast service."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch

from src.aurum.api.services.auto_reforecast_service import (
    AutoReforecastService,
    create_weather_trigger,
    create_price_trigger,
    ForecastTrigger,
    TriggerCondition,
    DebounceConfig,
    BackpressureConfig,
    TriggerEvent
)


class TestTriggerCreation:
    """Test trigger creation functions."""

    def test_create_weather_trigger_default_params(self):
        """Test creating weather trigger with default parameters."""
        trigger = create_weather_trigger()
        
        assert trigger.trigger_id is not None
        assert trigger.name == "Weather trigger - US"
        assert "weather changes by 5.0%" in trigger.description
        assert len(trigger.conditions) == 1
        
        condition = trigger.conditions[0]
        assert condition.data_source == "weather"
        assert condition.geography == "US"
        assert condition.threshold_type == "percentage"
        assert condition.threshold_value == 0.05  # 5% as decimal
        
        assert trigger.priority == 0.8
        assert trigger.enabled is True

    def test_create_weather_trigger_custom_params(self):
        """Test creating weather trigger with custom parameters."""
        trigger = create_weather_trigger(
            geography="CAISO",
            threshold_percentage=10.0,
            forecast_horizon_hours=48
        )
        
        assert trigger.name == "Weather trigger - CAISO"
        assert "weather changes by 10.0%" in trigger.description
        
        condition = trigger.conditions[0]
        assert condition.geography == "CAISO"
        assert condition.threshold_value == 0.1  # 10% as decimal
        
        # Check forecast config horizon
        forecast_end = trigger.forecast_config.end_date
        forecast_start = trigger.forecast_config.start_date
        duration = forecast_end - forecast_start
        assert duration == timedelta(hours=48)

    def test_create_price_trigger_default_params(self):
        """Test creating price trigger with default parameters."""
        trigger = create_price_trigger()
        
        assert trigger.trigger_id is not None  
        assert trigger.name == "Price trigger - US"
        assert "price changes by $10.0" in trigger.description
        assert len(trigger.conditions) == 1
        
        condition = trigger.conditions[0]
        assert condition.data_source == "price"
        assert condition.geography == "US"
        assert condition.threshold_type == "absolute"
        assert condition.threshold_value == 10.0
        
        assert trigger.priority == 0.9
        assert trigger.enabled is True

    def test_create_price_trigger_custom_params(self):
        """Test creating price trigger with custom parameters."""
        trigger = create_price_trigger(
            geography="MISO",
            threshold_absolute=25.0,
            forecast_horizon_hours=72
        )
        
        assert trigger.name == "Price trigger - MISO"
        assert "price changes by $25.0" in trigger.description
        
        condition = trigger.conditions[0]
        assert condition.geography == "MISO"
        assert condition.threshold_value == 25.0
        
        # Check forecast config
        assert trigger.forecast_config.forecast_type.value == "price"
        assert trigger.forecast_config.target_variable == "lmp_price"


class TestAutoReforecastService:
    """Test AutoReforecastService methods."""

    @pytest.fixture
    def service(self):
        """Create service instance for testing."""
        debounce_config = DebounceConfig(enabled=True, window_seconds=60)
        backpressure_config = BackpressureConfig(enabled=True)
        
        with patch('src.aurum.api.services.auto_reforecast_service.get_telemetry_facade') as mock_telemetry:
            mock_telemetry.return_value = Mock()
            service = AutoReforecastService(
                debounce_config=debounce_config,
                backpressure_config=backpressure_config
            )
            return service

    def test_service_initialization(self, service):
        """Test service initializes correctly."""
        assert service.debounce_config.enabled is True
        assert service.backpressure_config.enabled is True
        assert len(service.triggers) == 0
        assert len(service.active_triggers) == 0

    @pytest.mark.asyncio
    async def test_create_and_get_trigger(self, service):
        """Test creating and retrieving triggers."""
        # Create weather trigger
        weather_trigger = create_weather_trigger(geography="TEST")
        
        # Add to service
        created_trigger = await service.create_trigger(weather_trigger)
        assert created_trigger.trigger_id == weather_trigger.trigger_id
        
        # Retrieve trigger  
        retrieved_trigger = await service.get_trigger(weather_trigger.trigger_id)
        assert retrieved_trigger is not None
        assert retrieved_trigger.name == weather_trigger.name
        assert retrieved_trigger.conditions[0].geography == "TEST"

    @pytest.mark.asyncio
    async def test_list_triggers(self, service):
        """Test listing triggers with pagination."""
        # Add multiple triggers
        triggers = []
        for i in range(5):
            trigger = create_weather_trigger(geography=f"REGION_{i}")
            await service.create_trigger(trigger)
            triggers.append(trigger)
        
        # Test listing all
        all_triggers = await service.list_triggers(limit=10)
        assert len(all_triggers) == 5
        
        # Test pagination
        page1 = await service.list_triggers(limit=3, offset=0)
        assert len(page1) == 3
        
        page2 = await service.list_triggers(limit=3, offset=3)
        assert len(page2) == 2

    @pytest.mark.asyncio
    async def test_list_triggers_enabled_only(self, service):
        """Test listing only enabled triggers."""
        # Add enabled and disabled triggers
        enabled_trigger = create_weather_trigger(geography="ENABLED")
        disabled_trigger = create_weather_trigger(geography="DISABLED")
        disabled_trigger.enabled = False
        
        await service.create_trigger(enabled_trigger)
        await service.create_trigger(disabled_trigger)
        
        # List all triggers
        all_triggers = await service.list_triggers()
        assert len(all_triggers) == 2
        
        # List only enabled
        enabled_triggers = await service.list_triggers(enabled_only=True)
        assert len(enabled_triggers) == 1
        assert enabled_triggers[0].conditions[0].geography == "ENABLED"

    @pytest.mark.asyncio
    async def test_update_trigger(self, service):
        """Test updating a trigger."""
        # Create trigger
        trigger = create_weather_trigger(geography="ORIGINAL")
        await service.create_trigger(trigger)
        
        # Update trigger
        trigger.name = "Updated Name"
        trigger.enabled = False
        updated_trigger = await service.update_trigger(trigger)
        
        assert updated_trigger.name == "Updated Name"
        assert updated_trigger.enabled is False
        
        # Verify it's updated in service
        retrieved = await service.get_trigger(trigger.trigger_id)
        assert retrieved.name == "Updated Name"
        assert retrieved.enabled is False

    @pytest.mark.asyncio
    async def test_delete_trigger(self, service):
        """Test deleting a trigger."""
        # Create trigger
        trigger = create_weather_trigger()
        await service.create_trigger(trigger)
        
        # Verify it exists
        assert await service.get_trigger(trigger.trigger_id) is not None
        
        # Delete trigger
        success = await service.delete_trigger(trigger.trigger_id)
        assert success is True
        
        # Verify it's gone
        assert await service.get_trigger(trigger.trigger_id) is None
        
        # Try deleting non-existent trigger
        success = await service.delete_trigger("non-existent")
        assert success is False

    @pytest.mark.asyncio
    async def test_get_debounce_config(self, service):
        """Test getting debounce configuration."""
        config = await service.get_debounce_config()
        assert config.enabled is True
        assert config.window_seconds == 60

    @pytest.mark.asyncio
    async def test_update_debounce_config(self, service):
        """Test updating debounce configuration."""
        new_config = DebounceConfig(
            enabled=False,
            window_seconds=120,
            max_concurrent_triggers=10
        )
        
        updated_config = await service.update_debounce_config(new_config)
        assert updated_config.enabled is False
        assert updated_config.window_seconds == 120
        assert updated_config.max_concurrent_triggers == 10
        
        # Verify it's persisted
        retrieved_config = await service.get_debounce_config()
        assert retrieved_config.enabled is False
        assert retrieved_config.window_seconds == 120

    @pytest.mark.asyncio
    async def test_list_jobs_empty(self, service):
        """Test listing jobs when none exist."""
        jobs = await service.list_jobs()
        assert len(jobs) == 0

    @pytest.mark.asyncio
    async def test_list_trigger_events_empty(self, service):
        """Test listing trigger events when none exist."""
        events = await service.list_trigger_events()
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_service_health(self, service):
        """Test service health check."""
        health = await service.get_service_health()
        
        assert health["service"] == "auto_reforecast"
        assert health["status"] == "healthy"
        assert "event_queue_size" in health
        assert "pending_jobs" in health
        assert "debounce_enabled" in health
        assert "backpressure_enabled" in health


class TestDebounceLogic:
    """Test debounce and backpressure logic."""

    @pytest.fixture
    def service_with_debounce(self):
        """Create service with debounce enabled."""
        debounce_config = DebounceConfig(
            enabled=True,
            window_seconds=300,  # 5 minutes
            max_concurrent_triggers=2
        )
        
        with patch('src.aurum.api.services.auto_reforecast_service.get_telemetry_facade') as mock_telemetry:
            mock_telemetry.return_value = Mock()
            service = AutoReforecastService(debounce_config=debounce_config)
            return service

    def test_should_trigger_when_debounce_disabled(self, service_with_debounce):
        """Test triggering when debounce is disabled."""
        service_with_debounce.debounce_config.enabled = False
        
        trigger = create_weather_trigger()
        event = TriggerEvent(
            event_id="test_event",
            trigger_id=trigger.trigger_id,
            data_source="weather",
            geography="US",
            timestamp=datetime.utcnow(),
            data_changes={"temperature": 0.1}
        )
        
        should_trigger = service_with_debounce._should_trigger(trigger, event)
        assert should_trigger is True

    def test_should_trigger_with_cooldown(self, service_with_debounce):
        """Test triggering respects cooldown period."""
        trigger = create_weather_trigger()
        trigger.cooldown_minutes = 30
        trigger.last_triggered = datetime.utcnow() - timedelta(minutes=15)  # Within cooldown
        
        event = TriggerEvent(
            event_id="test_event",
            trigger_id=trigger.trigger_id,
            data_source="weather",
            geography="US", 
            timestamp=datetime.utcnow(),
            data_changes={"temperature": 0.1}
        )
        
        should_trigger = service_with_debounce._should_trigger(trigger, event)
        assert should_trigger is False
        
        # Test after cooldown expires
        trigger.last_triggered = datetime.utcnow() - timedelta(minutes=35)  # Past cooldown
        should_trigger = service_with_debounce._should_trigger(trigger, event)
        assert should_trigger is True