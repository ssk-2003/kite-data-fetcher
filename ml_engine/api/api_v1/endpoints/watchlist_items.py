from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Engine
from ml_engine.api import deps
from ml_engine.crud import crud_watchlist_items
from ml_engine.schemas.user import User
from typing import Dict, Any, List
from pydantic import BaseModel


router = APIRouter()


class WatchlistAddRequest(BaseModel):
    instrument_token: int
    symbol: str


@router.get("")
def get_watchlist(
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
) -> Dict[str, Any]:
    """
    Get the current user's watchlist with stock details.
    """
    items = crud_watchlist_items.get_user_watchlist(
        engine=engine,
        user_id=current_user["id"]
    )
    return {
        "items": items,
        "count": len(items)
    }


@router.post("")
def add_to_watchlist(
    request: WatchlistAddRequest,
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
) -> Dict[str, Any]:
    """
    Add a stock to the user's watchlist.
    """
    # Check limit (max 50 items)
    count = crud_watchlist_items.get_watchlist_count(
        engine=engine,
        user_id=current_user["id"]
    )
    if count >= 50:
        raise HTTPException(
            status_code=400,
            detail="Watchlist limit reached (maximum 50 items)"
        )
    
    item = crud_watchlist_items.add_to_watchlist(
        engine=engine,
        user_id=current_user["id"],
        instrument_token=request.instrument_token,
        symbol=request.symbol
    )
    
    if not item:
        raise HTTPException(
            status_code=500,
            detail="Failed to add to watchlist"
        )
    
    return {
        "success": True,
        "message": f"{request.symbol} added to watchlist",
        "item": item
    }


@router.delete("/{instrument_token}")
def remove_from_watchlist(
    instrument_token: int,
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
) -> Dict[str, Any]:
    """
    Remove a stock from the user's watchlist.
    """
    removed = crud_watchlist_items.remove_from_watchlist(
        engine=engine,
        user_id=current_user["id"],
        instrument_token=instrument_token
    )
    
    if not removed:
        raise HTTPException(
            status_code=404,
            detail="Item not found in watchlist"
        )
    
    return {
        "success": True,
        "message": "Removed from watchlist"
    }


@router.get("/check/{instrument_token}")
def check_watchlist(
    instrument_token: int,
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
) -> Dict[str, Any]:
    """
    Check if a stock is in user's watchlist.
    """
    is_added = crud_watchlist_items.is_in_watchlist(
        engine=engine,
        user_id=current_user["id"],
        instrument_token=instrument_token
    )
    
    return {
        "in_watchlist": is_added
    }
