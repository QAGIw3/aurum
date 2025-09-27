"""Fake CurvesDAO for testing."""

import asyncio
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock
from uuid import uuid4

from ..curves_dao import CurvesDAO, CurveFilter
from ...models import CurveResponse, CurvePoint


class FakeCurvesDAO(CurvesDAO):
    """Fake implementation of CurvesDAO for testing.

    Provides realistic async behavior with configurable delays and
    the ability to inject test data and failure scenarios.
    """

    def __init__(self, trino_config: Optional[Dict[str, Any]] = None):
        """Initialize fake Curves DAO.

        Args:
            trino_config: Configuration (ignored in fake)
        """
        super().__init__(trino_config)
        self._storage: Dict[str, CurveResponse] = {}
        self._points_storage: Dict[str, List[CurvePoint]] = {}
        self._query_delay = 0.01  # Default query delay in seconds
        self._fail_next_operations = False
        self._operation_count = 0

    async def _connect(self) -> None:
        """Fake connection - no-op."""
        await asyncio.sleep(0.001)  # Simulate connection time

    async def _disconnect(self) -> None:
        """Fake disconnection - clear storage."""
        self._storage.clear()
        self._points_storage.clear()

    async def _execute_trino_query(
        self,
        query: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Fake query execution with configurable behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated query failure")

        self._operation_count += 1

        # Simulate some realistic query results based on parameters
        if "curve_id" in parameters:
            curve_id = parameters["curve_id"]
            if curve_id in self._storage:
                curve = self._storage[curve_id]
                return [{
                    "curve_id": curve.id,
                    "curve_name": curve.name,
                    "description": curve.description,
                    "data_points": curve.data_points,
                    "created_at": curve.created_at,
                    "metadata": curve.meta
                }]

        return []

    def set_query_delay(self, delay_seconds: float) -> None:
        """Set delay for query operations.

        Args:
            delay_seconds: Delay in seconds
        """
        self._query_delay = delay_seconds

    def set_fail_next_operations(self, fail: bool = True) -> None:
        """Configure next operations to fail.

        Args:
            fail: Whether next operations should fail
        """
        self._fail_next_operations = fail

    def get_operation_count(self) -> int:
        """Get total number of operations performed.

        Returns:
            Operation count
        """
        return self._operation_count

    def inject_curve(self, curve: CurveResponse) -> None:
        """Inject a curve into storage for testing.

        Args:
            curve: Curve to inject
        """
        self._storage[curve.id] = curve

    def inject_curve_points(self, curve_id: str, points: List[CurvePoint]) -> None:
        """Inject curve points into storage for testing.

        Args:
            curve_id: Curve ID
            points: Points to inject
        """
        self._points_storage[curve_id] = points

    def clear_storage(self) -> None:
        """Clear all test data."""
        self._storage.clear()
        self._points_storage.clear()
        self._operation_count = 0

    async def create(self, curve: CurveResponse) -> CurveResponse:
        """Create curve with realistic async behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated creation failure")

        # Generate ID if not present
        if not curve.id:
            curve.id = str(uuid4())

        self._storage[curve.id] = curve
        self._operation_count += 1
        return curve

    async def get_by_id(self, curve_id: str) -> Optional[CurveResponse]:
        """Get curve by ID with realistic async behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated get failure")

        self._operation_count += 1
        return self._storage.get(curve_id)

    async def update(self, curve_id: str, curve: CurveResponse) -> Optional[CurveResponse]:
        """Update curve with realistic async behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated update failure")

        if curve_id not in self._storage:
            return None

        self._storage[curve_id] = curve
        self._operation_count += 1
        return curve

    async def delete(self, curve_id: str) -> bool:
        """Delete curve with realistic async behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated delete failure")

        if curve_id in self._storage:
            del self._storage[curve_id]
            self._points_storage.pop(curve_id, None)
            self._operation_count += 1
            return True

        return False

    async def list(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        order_desc: bool = False
    ) -> List[CurveResponse]:
        """List curves with filtering and realistic async behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated list failure")

        self._operation_count += 1

        # Apply filters
        curves = list(self._storage.values())

        if filters:
            curve_filter = CurveFilter(**filters)

            if curve_filter.curve_ids:
                curves = [c for c in curves if c.id in curve_filter.curve_ids]

            if curve_filter.curve_names:
                curves = [c for c in curves if c.name in curve_filter.curve_names]

            if curve_filter.start_date or curve_filter.end_date:
                # In real implementation, would filter by date ranges
                pass

        # Apply sorting
        if order_by:
            reverse = order_desc
            if order_by == "name":
                curves.sort(key=lambda c: c.name, reverse=reverse)
            elif order_by == "created_at":
                curves.sort(key=lambda c: c.created_at, reverse=reverse)

        # Apply pagination
        start = offset
        end = offset + limit
        return curves[start:end]

    async def get_curve_points(
        self,
        curve_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[CurvePoint]:
        """Get curve points with realistic async behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated points retrieval failure")

        self._operation_count += 1

        points = self._points_storage.get(curve_id, [])

        # Filter by date range if specified
        if start_date or end_date:
            filtered_points = []
            for point in points:
                if isinstance(point.date, str):
                    point_date = datetime.fromisoformat(point.date).date()
                else:
                    point_date = point.date

                if start_date and point_date < start_date:
                    continue
                if end_date and point_date > end_date:
                    continue
                filtered_points.append(point)

            points = filtered_points

        return points

    async def get_curve_diff(
        self,
        curve_id: str,
        from_date: date,
        to_date: date
    ) -> List[CurvePoint]:
        """Get curve differences with realistic async behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated diff calculation failure")

        self._operation_count += 1

        # Get points for both dates
        from_points = await self.get_curve_points(curve_id, from_date, from_date)
        to_points = await self.get_curve_points(curve_id, to_date, to_date)

        # Calculate differences
        diff_points = []
        for to_point in to_points:
            to_date_str = to_point.date.isoformat() if hasattr(to_point.date, 'isoformat') else str(to_point.date)

            # Find corresponding from_point
            from_point = None
            from_date_str = from_date.isoformat()
            for fp in from_points:
                fp_date_str = fp.date.isoformat() if hasattr(fp.date, 'isoformat') else str(fp.date)
                if fp_date_str == from_date_str:
                    from_point = fp
                    break

            diff_value = to_point.value
            if from_point:
                diff_value = to_point.value - from_point.value

            diff_points.append(CurvePoint(
                curve_id=curve_id,
                date=to_point.date,
                value=diff_value,
                metadata={"diff_type": "absolute", "from_date": str(from_date), "to_date": str(to_date)}
            ))

        return diff_points

    async def get_curve_strips(
        self,
        curve_id: str,
        strip_type: str = "monthly"
    ) -> List[CurvePoint]:
        """Get curve strips with realistic async behavior."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated strips calculation failure")

        self._operation_count += 1

        points = await self.get_curve_points(curve_id)

        # Group by strip type and calculate aggregates
        strips = {}
        for point in points:
            # Extract strip key based on strip_type
            if strip_type == "monthly":
                strip_key = f"{point.date.year}-{point.date.month:02d}"
            elif strip_type == "quarterly":
                quarter = (point.date.month - 1) // 3 + 1
                strip_key = f"{point.date.year}-Q{quarter}"
            else:
                strip_key = str(point.date)

            if strip_key not in strips:
                strips[strip_key] = {"total": 0.0, "count": 0, "points": []}

            strips[strip_key]["total"] += point.value
            strips[strip_key]["count"] += 1
            strips[strip_key]["points"].append(point)

        # Create strip points
        strip_points = []
        for strip_key, data in strips.items():
            avg_value = data["total"] / data["count"] if data["count"] > 0 else 0.0

            strip_points.append(CurvePoint(
                curve_id=f"{curve_id}_{strip_type}",
                date=strip_key,
                value=avg_value,
                metadata={
                    "strip_type": strip_type,
                    "point_count": data["count"],
                    "aggregation": "average"
                }
            ))

        return strip_points
