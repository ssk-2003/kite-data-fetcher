from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class WatchlistItemBase(BaseModel):
    instrument_token: int
    symbol: str


class WatchlistItemCreate(WatchlistItemBase):
    pass


class WatchlistItem(WatchlistItemBase):
    id: int
    user_id: int
    added_at: datetime
    
    # Stock details (optional, populated from stock_master)
    name: Optional[str] = None
    exchange: Optional[str] = None
    current_price: Optional[float] = None
    change_percent: Optional[float] = None
    omre_score: Optional[float] = None
    
    class Config:
        from_attributes = True
