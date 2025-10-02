"""Unit tests for Curve aggregate."""

from datetime import datetime
from decimal import Decimal

import pytest

from aurum.domain.energy.models.curve import (
    Curve,
    CurveId,
    CurveMetadata,
    CurvePoint,
    TenorType,
    CurvePointAddedEvent,
    CurvePointUpdatedEvent,
    CurvePointRemovedEvent,
)
from aurum.domain.shared_kernel.value_objects import TenantId
from aurum.domain.shared_kernel.exceptions import ValidationError, BusinessRuleViolation


class TestCurvePoint:
    """Tests for CurvePoint value object."""
    
    def test_create_curve_point(self):
        """Test creating a valid curve point."""
        point = CurvePoint(
            tenor=Decimal('1.0'),
            value=Decimal('100.50'),
        )
        
        assert point.tenor == Decimal('1.0')
        assert point.value == Decimal('100.50')
        assert point.timestamp is None
        assert point.quality_flag is None
    
    def test_curve_point_with_metadata(self):
        """Test curve point with timestamp and quality flag."""
        now = datetime.utcnow()
        point = CurvePoint(
            tenor=Decimal('2.0'),
            value=Decimal('105.75'),
            timestamp=now,
            quality_flag="observed"
        )
        
        assert point.timestamp == now
        assert point.quality_flag == "observed"
    
    def test_curve_point_decimal_conversion(self):
        """Test automatic conversion to Decimal."""
        point = CurvePoint(tenor=1.5, value=100.5)
        
        assert isinstance(point.tenor, Decimal)
        assert isinstance(point.value, Decimal)
        assert point.tenor == Decimal('1.5')
        assert point.value == Decimal('100.5')


class TestCurveMetadata:
    """Tests for CurveMetadata value object."""
    
    def test_create_minimal_metadata(self):
        """Test creating metadata with required fields only."""
        metadata = CurveMetadata(
            curve_key="PJM_DA",
            as_of_date=datetime.utcnow(),
        )
        
        assert metadata.curve_key == "PJM_DA"
        assert metadata.currency == "USD"  # default
        assert metadata.tenor_type is None
    
    def test_create_full_metadata(self):
        """Test creating metadata with all fields."""
        metadata = CurveMetadata(
            curve_key="PJM_DA_Q1_2025",
            as_of_date=datetime(2025, 1, 1),
            currency="USD",
            tenor_type=TenorType.MONTHLY,
            price_type="forward",
            day_count="ACT/360",
            calendar="NERC",
            asset_class="power",
            source="market_data"
        )
        
        assert metadata.curve_key == "PJM_DA_Q1_2025"
        assert metadata.tenor_type == TenorType.MONTHLY
        assert metadata.price_type == "forward"
        assert metadata.source == "market_data"
    
    def test_metadata_validation_empty_key(self):
        """Test validation fails with empty curve key."""
        with pytest.raises(ValidationError, match="Curve key cannot be empty"):
            CurveMetadata(
                curve_key="",
                as_of_date=datetime.utcnow(),
            )
    
    def test_metadata_validation_invalid_currency(self):
        """Test validation fails with invalid currency code."""
        with pytest.raises(ValidationError, match="Currency must be 3-letter ISO code"):
            CurveMetadata(
                curve_key="TEST",
                as_of_date=datetime.utcnow(),
                currency="US"  # Invalid: too short
            )


class TestCurve:
    """Tests for Curve aggregate root."""
    
    @pytest.fixture
    def curve_id(self):
        """Generate a curve ID."""
        return CurveId.generate()
    
    @pytest.fixture
    def tenant_id(self):
        """Generate a tenant ID."""
        return TenantId.generate()
    
    @pytest.fixture
    def metadata(self):
        """Create curve metadata."""
        return CurveMetadata(
            curve_key="TEST_CURVE",
            as_of_date=datetime.utcnow(),
        )
    
    @pytest.fixture
    def sample_points(self):
        """Create sample curve points."""
        return [
            CurvePoint(tenor=Decimal('1'), value=Decimal('100')),
            CurvePoint(tenor=Decimal('2'), value=Decimal('105')),
            CurvePoint(tenor=Decimal('3'), value=Decimal('103')),
        ]
    
    def test_create_curve(self, curve_id, tenant_id, metadata, sample_points):
        """Test creating a valid curve."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        assert curve.id == curve_id
        assert curve.tenant_id == tenant_id
        assert curve.metadata == metadata
        assert len(curve.points) == 3
        assert curve.measure == "value"
    
    def test_curve_validation_empty_points(self, curve_id, tenant_id, metadata):
        """Test curve must have at least one point."""
        with pytest.raises(ValidationError, match="must have at least one point"):
            Curve(
                id=curve_id,
                tenant_id=tenant_id,
                metadata=metadata,
                points=[],
            )
    
    def test_curve_validation_duplicate_tenors(self, curve_id, tenant_id, metadata):
        """Test curve cannot have duplicate tenor points."""
        with pytest.raises(ValidationError, match="duplicate tenor"):
            Curve(
                id=curve_id,
                tenant_id=tenant_id,
                metadata=metadata,
                points=[
                    CurvePoint(tenor=Decimal('1'), value=Decimal('100')),
                    CurvePoint(tenor=Decimal('1'), value=Decimal('105')),
                ],
            )
    
    def test_curve_auto_sort_points(self, curve_id, tenant_id, metadata):
        """Test curve automatically sorts points by tenor."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=[
                CurvePoint(tenor=Decimal('3'), value=Decimal('103')),
                CurvePoint(tenor=Decimal('1'), value=Decimal('100')),
                CurvePoint(tenor=Decimal('2'), value=Decimal('105')),
            ],
        )
        
        assert curve.points[0].tenor == Decimal('1')
        assert curve.points[1].tenor == Decimal('2')
        assert curve.points[2].tenor == Decimal('3')
    
    def test_add_point(self, curve_id, tenant_id, metadata, sample_points):
        """Test adding a point to a curve."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        new_point = CurvePoint(tenor=Decimal('4'), value=Decimal('108'))
        curve.add_point(new_point)
        
        assert len(curve.points) == 4
        assert curve.points[-1] == new_point
        
        # Check domain event was recorded
        events = curve.domain_events
        assert len(events) == 1
        assert isinstance(events[0], CurvePointAddedEvent)
        assert events[0].tenor == Decimal('4')
        assert events[0].value == Decimal('108')
    
    def test_add_point_duplicate_tenor(self, curve_id, tenant_id, metadata, sample_points):
        """Test cannot add point with duplicate tenor."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        with pytest.raises(BusinessRuleViolation, match="already exists"):
            curve.add_point(CurvePoint(tenor=Decimal('2'), value=Decimal('110')))
    
    def test_update_point(self, curve_id, tenant_id, metadata, sample_points):
        """Test updating a curve point."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        curve.update_point(Decimal('2'), Decimal('110'))
        
        point = curve.get_value_at_tenor(Decimal('2'))
        assert point == Decimal('110')
        
        # Check domain event
        events = curve.domain_events
        assert len(events) == 1
        assert isinstance(events[0], CurvePointUpdatedEvent)
        assert events[0].tenor == Decimal('2')
        assert events[0].old_value == Decimal('105')
        assert events[0].new_value == Decimal('110')
    
    def test_update_point_not_found(self, curve_id, tenant_id, metadata, sample_points):
        """Test updating non-existent point fails."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        with pytest.raises(ValidationError, match="not found"):
            curve.update_point(Decimal('99'), Decimal('110'))
    
    def test_remove_point(self, curve_id, tenant_id, metadata, sample_points):
        """Test removing a curve point."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        curve.remove_point(Decimal('2'))
        
        assert len(curve.points) == 2
        assert curve.get_value_at_tenor(Decimal('2')) is None
        
        # Check domain event
        events = curve.domain_events
        assert len(events) == 1
        assert isinstance(events[0], CurvePointRemovedEvent)
        assert events[0].tenor == Decimal('2')
    
    def test_remove_point_last_point_fails(self, curve_id, tenant_id, metadata):
        """Test cannot remove the last point from a curve."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=[CurvePoint(tenor=Decimal('1'), value=Decimal('100'))],
        )
        
        with pytest.raises(BusinessRuleViolation, match="Cannot remove last point"):
            curve.remove_point(Decimal('1'))
    
    def test_get_value_at_tenor(self, curve_id, tenant_id, metadata, sample_points):
        """Test getting value at specific tenor."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        assert curve.get_value_at_tenor(Decimal('1')) == Decimal('100')
        assert curve.get_value_at_tenor(Decimal('2')) == Decimal('105')
        assert curve.get_value_at_tenor(Decimal('99')) is None
    
    def test_min_value(self, curve_id, tenant_id, metadata, sample_points):
        """Test getting minimum value."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        assert curve.min_value == Decimal('100')
    
    def test_max_value(self, curve_id, tenant_id, metadata, sample_points):
        """Test getting maximum value."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        assert curve.max_value == Decimal('105')
    
    def test_average_value(self, curve_id, tenant_id, metadata, sample_points):
        """Test calculating average value."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        # (100 + 105 + 103) / 3 = 102.666...
        expected = Decimal('308') / Decimal('3')
        assert curve.average_value == expected
    
    def test_domain_events_cleared(self, curve_id, tenant_id, metadata, sample_points):
        """Test clearing domain events."""
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=sample_points,
        )
        
        curve.add_point(CurvePoint(tenor=Decimal('4'), value=Decimal('108')))
        
        assert len(curve.domain_events) == 1
        
        events = curve.clear_events()
        
        assert len(events) == 1
        assert len(curve.domain_events) == 0

