from fastapi import APIRouter, Depends, HTTPException, Body
from sqlalchemy.engine import Engine
from sqlalchemy import text
from ml_engine.api import deps
from ml_engine.schemas.user import User
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

router = APIRouter()

class PriceAlertCreate(BaseModel):
    symbol: str
    target_price: float
    condition: Optional[str] = None # 'ABOVE' or 'BELOW'. If None, auto-detected.

class PriceAlertResponse(BaseModel):
    id: int
    symbol: str
    target_price: float
    condition: str
    is_active: bool
    created_at: datetime

@router.post("/", response_model=PriceAlertResponse)
def create_alert(
    alert: PriceAlertCreate,
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Create a new price alert.
    """
    # Auto-detect condition if not provided
    condition = alert.condition
    if not condition:
        # Get current price
        from ml_engine.crud import crud_stock
        stock = crud_stock.get_stock_by_symbol(engine, alert.symbol)
        if not stock:
             raise HTTPException(status_code=404, detail="Stock not found")
        
        current_price = stock["current_price"]
        if alert.target_price > current_price:
            condition = "ABOVE"
        else:
            condition = "BELOW"
    
    if condition not in ["ABOVE", "BELOW"]:
        raise HTTPException(status_code=400, detail="Condition must be 'ABOVE' or 'BELOW'")

    with engine.begin() as conn:
        # Get instrument token
        token_res = conn.execute(
            text("SELECT instrument_token FROM stock_master WHERE tradingsymbol = :sym"), 
            {"sym": alert.symbol}
        ).fetchone()
        
        token = token_res[0] if token_res else None

        result = conn.execute(
            text("""
                INSERT INTO price_alerts (user_id, instrument_token, symbol, target_price, condition)
                VALUES (:uid, :token, :sym, :price, :cond)
                RETURNING id, created_at
            """),
            {
                "uid": current_user["id"],
                "token": token,
                "sym": alert.symbol,
                "price": alert.target_price,
                "cond": condition
            }
        ).fetchone()
        
        return {
            "id": result.id,
            "symbol": alert.symbol,
            "target_price": alert.target_price,
            "condition": condition,
            "is_active": True,
            "created_at": result.created_at
        }

@router.get("/", response_model=List[PriceAlertResponse])
def get_alerts(
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get all active alerts for the user.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id, symbol, target_price, condition, is_active, created_at
                FROM price_alerts
                WHERE user_id = :uid AND is_active = TRUE
                ORDER BY created_at DESC
            """),
            {"uid": current_user["id"]}
        ).fetchall()
        
        return [dict(row._mapping) for row in result]

@router.delete("/{alert_id}")
def delete_alert(
    alert_id: int,
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Delete (deactivate) an alert.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("UPDATE price_alerts SET is_active = FALSE WHERE id = :id AND user_id = :uid"),
            {"id": alert_id, "uid": current_user["id"]}
        )
        
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Alert not found")
            
    return {"status": "success", "message": "Alert deleted"}
