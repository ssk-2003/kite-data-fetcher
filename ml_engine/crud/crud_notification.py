from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import List, Dict, Any, Optional



def create_notification(
    engine: Engine, 
    user_id: int, 
    type: str, 
    title: str, 
    message: str
) -> Dict[str, Any]:
    """Create a new notification."""
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO notifications (user_id, type, title, message)
                VALUES (:user_id, :type, :title, :message)
                RETURNING id, created_at
            """),
            {
                "user_id": user_id,
                "type": type,
                "title": title,
                "message": message
            }
        ).fetchone()
        
        return {
            "id": result.id,
            "user_id": user_id,
            "type": type,
            "title": title,
            "message": message,
            "is_read": False,
            "created_at": result.created_at
        }

def get_user_notifications(engine: Engine, user_id: int) -> List[Dict[str, Any]]:
    """Get all notifications for a user."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id, type, title, message, is_read, created_at
                FROM notifications
                WHERE user_id = :user_id
                ORDER BY created_at DESC
            """),
            {"user_id": user_id}
        ).fetchall()
        
        return [dict(row._mapping) for row in result]


def mark_notification_read(engine: Engine, notification_id: int, user_id: int) -> bool:
    """Mark a notification as read."""
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE notifications
                SET is_read = TRUE
                WHERE id = :id AND user_id = :user_id
            """),
            {"id": notification_id, "user_id": user_id}
        )
        return result.rowcount > 0


def get_alert_preferences(engine: Engine, user_id: int) -> Dict[str, Any]:
    """Get user's alert preferences."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT email_alerts, push_alerts, score_threshold
                FROM user_alert_preferences
                WHERE user_id = :user_id
            """),
            {"user_id": user_id}
        ).fetchone()
        
        if result:
            return dict(result._mapping)
        
        # Return defaults
        return {"email_alerts": True, "push_alerts": True, "score_threshold": 70}


def update_alert_preferences(
    engine: Engine,
    user_id: int,
    email_alerts: bool,
    push_alerts: bool,
    score_threshold: int
) -> Dict[str, Any]:
    """Update user's alert preferences."""
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO user_alert_preferences (user_id, email_alerts, push_alerts, score_threshold)
                VALUES (:user_id, :email_alerts, :push_alerts, :score_threshold)
                ON CONFLICT (user_id) DO UPDATE SET
                    email_alerts = EXCLUDED.email_alerts,
                    push_alerts = EXCLUDED.push_alerts,
                    score_threshold = EXCLUDED.score_threshold
                RETURNING email_alerts, push_alerts, score_threshold
            """),
            {
                "user_id": user_id,
                "email_alerts": email_alerts,
                "push_alerts": push_alerts,
                "score_threshold": score_threshold
            }
        ).fetchone()
        
        return dict(result._mapping)
