from typing import List, Optional, Literal
from pydantic import BaseModel
from datetime import datetime

class PositionResponse(BaseModel):
    instrument_token: int
    symbol: str
    quantity: int
    avg_price: float
    current_price: float
    current_value: float
    pnl: float
    pnl_percent: float

class PortfolioResponse(BaseModel):
    id: int
    balance: float
    total_value: float
    equity: float
    day_change: float
    day_change_percent: float
    positions: List[PositionResponse]

class TradeRequest(BaseModel):
    instrument_token: int
    symbol: str
    quantity: int
    action: Literal['BUY', 'SELL']
    order_type: Literal['MARKET', 'LIMIT'] = 'MARKET'
    limit_price: Optional[float] = None

class OrderResponse(BaseModel):
    id: int
    symbol: str
    action: str
    order_type: str
    quantity: int
    limit_price: Optional[float]
    status: str
    created_at: datetime

class TransactionResponse(BaseModel):
    id: int
    instrument_token: int
    symbol: str
    type: str
    quantity: int
    price: float
    amount: float
    timestamp: datetime

class TradeResponse(BaseModel):
    status: str
    message: str
    transaction: Optional[TransactionResponse] = None
    new_balance: float
