from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Engine
from ml_engine.api import deps
from ml_engine.crud import crud_subscription
from typing import List, Dict, Any
from ml_engine.schemas.subscription import SubscriptionCreate

router = APIRouter()


@router.get("/plans")
def get_subscription_plans(
    engine: Engine = Depends(deps.get_db_engine)
) -> Dict[str, Any]:
    """
    Get all active subscription plans.
    Public endpoint - no authentication required.
    """
    plans = crud_subscription.get_all_plans(engine=engine, active_only=True)
    return {
        "plans": plans,
        "count": len(plans)
    }


@router.get("/me")
def get_my_subscription(
    engine: Engine = Depends(deps.get_db_engine),
    current_user: dict = Depends(deps.get_current_user)
) -> Dict[str, Any]:
    """
    Get the current user's active subscription.
    """
    subscription = crud_subscription.get_user_subscription(
        engine=engine,
        user_id=current_user["id"]
    )
    return {
        "has_subscription": subscription is not None,
        "subscription": subscription
    }


@router.post("/subscribe")
def subscribe(
    sub_in: SubscriptionCreate,
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine)
) -> Dict[str, Any]:
    """
    Create a new subscription for the user.
    """
    subscription = crud_subscription.create_user_subscription(
        engine=engine,
        user_id=current_user["id"],
        plan_id=sub_in.plan_id,
        payment_id=sub_in.payment_id,
        payment_amount=sub_in.payment_amount
    )
    
    if not subscription:
        raise HTTPException(status_code=400, detail="Failed to create subscription")
        
    return {
        "success": True,
        "message": "Subscription activated successfully",
        "subscription": subscription
    }


@router.post("/cancel")
def cancel_subscription(
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine)
) -> Dict[str, Any]:
    """
    Cancel the user's active subscription.
    """
    success = crud_subscription.cancel_subscription(
        engine=engine,
        user_id=current_user["id"]
    )
    
    if not success:
        raise HTTPException(status_code=400, detail="No active subscription found to cancel")
        
    return {
        "success": True,
        "message": "Subscription cancelled successfully"
    }
