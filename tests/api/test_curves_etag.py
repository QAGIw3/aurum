"""ETag tests for v2 curves endpoints."""

from unittest.mock import AsyncMock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from aurum.api.v2.curves import router as v2_curves_router


class TestCurvesETag:
    def setup_method(self):
        app = FastAPI()
        app.include_router(v2_curves_router)
        self.client = TestClient(app)

    def test_curves_etag_generation(self):
        async def _mock_list_curves(**kwargs):
            class _StubCurve:
                def model_dump(self):
                    return {
                        "id": "curve-1",
                        "name": "Curve 1",
                        "description": None,
                        "data_points": 1,
                        "created_at": "2024-01-01T00:00:00Z",
                    }

            return ([_StubCurve()], {})

        with patch(
            "libs.services.curves_service.CurvesService.list_curves",
            new=AsyncMock(side_effect=_mock_list_curves),
        ):
            resp = self.client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert resp.status_code == 200
        assert "ETag" in resp.headers
        # ETag header includes quotes per RFC; normalize for length check
        etag = resp.headers["ETag"].strip('"')
        assert len(etag.strip('W/"')) == 64

    def test_curves_etag_304_response(self):
        async def _mock_list_curves(**kwargs):
            class _StubCurve:
                def model_dump(self):
                    return {
                        "id": "curve-1",
                        "name": "Curve 1",
                        "description": None,
                        "data_points": 1,
                        "created_at": "2024-01-01T00:00:00Z",
                    }

            return ([_StubCurve()], {})

        with patch(
            "libs.services.curves_service.CurvesService.list_curves",
            new=AsyncMock(side_effect=_mock_list_curves),
        ):
            r1 = self.client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )
            etag = r1.headers["ETag"]
            r2 = self.client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test", "If-None-Match": etag},
            )
        assert r2.status_code == 304
