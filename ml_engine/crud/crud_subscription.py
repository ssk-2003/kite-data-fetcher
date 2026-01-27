from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Optional, List, Dict, Any
import json


def get_all_plans(engine: Engine, active_only: bool = True) -> List[Dict[str, Any]]:
    """Get all subscription plans ordered by display_order."""
    with engine.connect() as conn:
        query = """
            SELECT id, name, duration_months, original_price, founding_price,
                   per_month_label, savings_percent, features, badge, badge_icon,
                   is_highlighted, display_order, is_active, created_at
            FROM subscription_plans
        """
        if active_only:
            query += " WHERE is_active = TRUE"
        query += " ORDER BY display_order ASC"
        
        result = conn.execute(text(query))
        plans = []
        for row in result.mappings():
            plan = dict(row)
            # Parse features JSON if it's a string
            if isinstance(plan.get('features'), str):
                plan['features'] = json.loads(plan['features'])
            plans.append(plan)
        return plans


def get_plan_by_id(engine: Engine, plan_id: int) -> Optional[Dict[str, Any]]:
    """Get a single plan by ID."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id, name, duration_months, original_price, founding_price,
                       per_month_label, savings_percent, features, badge, badge_icon,
                       is_highlighted, display_order, is_active, created_at
                FROM subscription_plans WHERE id = :plan_id
            """),
            {"plan_id": plan_id}
        )
        row = result.mappings().first()
        if row:
            plan = dict(row)
            if isinstance(plan.get('features'), str):
                plan['features'] = json.loads(plan['features'])
            return plan
        return None


def get_user_subscription(engine: Engine, user_id: int) -> Optional[Dict[str, Any]]:
    """Get a user's active subscription with plan details."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT us.id, us.user_id, us.plan_id, us.status, us.started_at,
                       us.expires_at, us.payment_id, us.payment_amount, us.created_at,
                       sp.name as plan_name, sp.duration_months, sp.founding_price
                FROM user_subscriptions us
                JOIN subscription_plans sp ON us.plan_id = sp.id
                WHERE us.user_id = :user_id AND us.status = 'active'
                ORDER BY us.created_at DESC
                LIMIT 1
            """),
            {"user_id": user_id}
        )
        row = result.mappings().first()
        return dict(row) if row else None


def create_user_subscription(
    engine: Engine,
    user_id: int,
    plan_id: int,
    payment_id: Optional[str] = None,
    payment_amount: Optional[int] = None
) -> Dict[str, Any]:
    """Create a new subscription for a user."""
    with engine.begin() as conn:
        # Get plan details for expiration calculation
        plan = get_plan_by_id(engine, plan_id)
        if not plan:
            raise ValueError(f"Plan {plan_id} not found")
        
        # Calculate expiration (None for lifetime)
        expires_clause = "NULL"
        if plan['duration_months'] > 0:
            expires_clause = f"NOW() + INTERVAL '{plan['duration_months']} months'"
        
        result = conn.execute(
            text(f"""
                INSERT INTO user_subscriptions 
                (user_id, plan_id, status, started_at, expires_at, payment_id, payment_amount)
                VALUES (:user_id, :plan_id, 'active', NOW(), {expires_clause}, :payment_id, :payment_amount)
                RETURNING id, user_id, plan_id, status, started_at, expires_at, payment_id, payment_amount, created_at
            """),
            {
                "user_id": user_id,
                "plan_id": plan_id,
                "payment_id": payment_id,
                "payment_amount": payment_amount
            }
        )
        row = result.mappings().first()
        return dict(row) if row else {}


def cancel_subscription(engine: Engine, user_id: int) -> bool:
    """Cancel a user's active subscription."""
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE user_subscriptions
                SET status = 'cancelled'
                WHERE user_id = :user_id AND status = 'active'
            """),
            {"user_id": user_id}
        )
        return result.rowcount > 0

def check_subscription_status(engine: Engine, user_id: int) -> Dict[str, Any]:
    """
    Check subscription status and return formatted dict for frontend.
    """
    sub = get_user_subscription(engine, user_id)
    
    if sub:
        return {
            "is_active": True,
            "plan_id": sub["plan_id"],
            "plan_name": sub["plan_name"],
            "expires_at": sub["expires_at"]
        }
    
    return {
        "is_active": False,
        "plan_id": None,
        "plan_name": None,
        "expires_at": None
    }
