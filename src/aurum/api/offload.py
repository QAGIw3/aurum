"""HTTP routes for inspecting async offloaded job status."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException

from libs.services.async_exec import fetch_job_result
from aurum.api.auth import Permission, require_permissions


offload_router = APIRouter(
    prefix="/v1/admin/offload",
    tags=["Offload"],
    dependencies=[Depends(require_permissions(Permission.ADMIN, tenant_scoped=False))],
)


@offload_router.get("/{task_id}", summary="Fetch status for an offloaded job")
async def get_offload_status(task_id: str) -> dict:
    """Return Celery task status and result for a given task id."""

    try:
        result = await asyncio.to_thread(fetch_job_result, task_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch job status: {exc}") from exc
    return result


__all__ = ["offload_router"]
