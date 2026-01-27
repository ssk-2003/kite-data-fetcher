from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Engine
from ml_engine.api import deps
from ml_engine.crud import crud_notification
from typing import List, Dict, Any
from pydantic import BaseModel


router = APIRouter()


class AlertPreferencesUpdate(BaseModel):
    email_alerts: bool
    push_alerts: bool
    score_threshold: int


@router.get("")
def get_notifications(
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine)
) -> Dict[str, Any]:
    """
    Get user notifications.
    """
    notifications = crud_notification.get_user_notifications(
        engine=engine,
        user_id=current_user["id"]
    )
    
    return {
        "notifications": notifications,
        "count": len(notifications)
    }


@router.put("/{notification_id}/read")
def mark_as_read(
    notification_id: int,
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine)
) -> Dict[str, Any]:
    """
    Mark a notification as read.
    """
    success = crud_notification.mark_notification_read(
        engine=engine,
        notification_id=notification_id,
        user_id=current_user["id"]
    )
    
    if not success:
        raise HTTPException(status_code=404, detail="Notification not found")
        
    return {"success": True}


@router.get("/preferences")
def get_preferences(
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine)
) -> Dict[str, Any]:
    """
    Get alert preferences.
    """
    prefs = crud_notification.get_alert_preferences(
        engine=engine,
        user_id=current_user["id"]
    )
    return prefs


@router.put("/preferences")
def update_preferences(
    prefs_in: AlertPreferencesUpdate,
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine)
) -> Dict[str, Any]:
    """
    Update alert preferences.
    """
    prefs = crud_notification.update_alert_preferences(
        engine=engine,
        user_id=current_user["id"],
        email_alerts=prefs_in.email_alerts,
        push_alerts=prefs_in.push_alerts,
        score_threshold=prefs_in.score_threshold
    )
    return prefs
