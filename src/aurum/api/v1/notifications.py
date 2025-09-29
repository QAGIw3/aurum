"""Notification API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from aurum.notifications.service import get_notification_service

router = APIRouter(prefix="/v1/notifications", tags=["notifications"])


class NotificationAckRequest(BaseModel):
    tenant_id: str = Field(..., description="Tenant acknowledging the notification")
    recipient_id: str = Field(..., description="Recipient acknowledging the notification")
    attributes: dict[str, str] = Field(default_factory=dict)


@router.post("/ack/{notification_id}", status_code=204)
async def acknowledge_notification(notification_id: str, payload: NotificationAckRequest) -> Response:
    if not notification_id:
        raise HTTPException(status_code=400, detail="notification_id required")
    service = await get_notification_service()
    await service.record_ack(
        dispatch_id=notification_id,
        tenant_id=payload.tenant_id,
        recipient_id=payload.recipient_id,
        attributes=payload.attributes,
    )
    return Response(status_code=204)


__all__ = ["router"]
