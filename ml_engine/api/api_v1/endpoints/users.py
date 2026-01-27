from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Engine
from sqlalchemy import text
from ml_engine.api import deps
from ml_engine.crud import crud_user
from ml_engine.schemas.user import User, UserUpdate


from ml_engine.crud import crud_subscription

router = APIRouter()


@router.get("/me", response_model=User)
def read_user_me(
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get current user profile with subscription details.
    """
    # Fetch subscription status
    sub = crud_subscription.check_subscription_status(
        engine=engine, 
        user_id=current_user["id"]
    )
    
    # Attach to user dict
    current_user["subscription"] = sub
    return current_user


@router.put("/me", response_model=User)
def update_user_me(
    user_in: UserUpdate,
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Update own profile.
    """
    if user_in.full_name:
        updated_user = crud_user.update_user(
            engine=engine,
            user_id=current_user["id"],
            full_name=user_in.full_name
        )
        return updated_user
    
    return current_user
