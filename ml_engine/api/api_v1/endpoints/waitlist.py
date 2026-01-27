from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.engine import Engine
from ml_engine.api import deps
from ml_engine.crud import crud_user, crud_waitlist
from ml_engine.schemas.user import WaitlistStatus
from pydantic import BaseModel, EmailStr

router = APIRouter()


class JoinWaitlistRequest(BaseModel):
    email: EmailStr
    full_name: str | None = None


@router.post("/join")
def join_waitlist(
    request: JoinWaitlistRequest,
    engine: Engine = Depends(deps.get_db_engine)
):
    """
    Add email to waitlist. No authentication required.
    Returns queue position and status.
    """
    result = crud_waitlist.add_to_waitlist(
        engine=engine,
        email=request.email,
        full_name=request.full_name
    )
    return result


@router.get("/status", response_model=WaitlistStatus)
def get_waitlist_status(
    engine: Engine = Depends(deps.get_db_engine),
    current_user: dict = Depends(deps.get_current_user)
):
    """
    Get the current user's waitlist status, including queue position.
    """
    stats = crud_user.get_waitlist_stats(engine=engine, user_email=current_user["email"])
    
    if stats is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return stats

