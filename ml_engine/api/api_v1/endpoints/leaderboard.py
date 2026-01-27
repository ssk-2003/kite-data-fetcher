from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine
from sqlalchemy import text
from ml_engine.api import deps
from ml_engine.crud import crud_stock
from typing import List, Optional
from pydantic import BaseModel
import datetime

router = APIRouter()

class LeaderboardEntry(BaseModel):
    rank: int
    user_id: int
    user_name: str
    user_avatar: Optional[str]
    total_value: float
    return_percent: float
    trades_count: int

@router.get("/", response_model=List[LeaderboardEntry])
def get_leaderboard(
    limit: int = 50,
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get top performing portfolios.
    Note: Real-time calculation can be heavy. Consider caching or background job for production.
    """
    with engine.begin() as conn:
        # Fetch all portfolios with user info
        portfolios = conn.execute(
            text("""
                SELECT 
                    p.id as portfolio_id, 
                    p.balance, 
                    p.user_id,
                    u.full_name,
                    u.email
                FROM portfolios p
                JOIN users u ON p.user_id = u.id
            """)
        ).fetchall()
        
        results = []
        
        # We need to fetch positions to calculate equity
        # Bulk fetch all positions is better than N+1
        all_positions = conn.execute(
            text("SELECT portfolio_id, symbol, quantity, avg_price FROM positions WHERE quantity != 0")
        ).fetchall()
        
        # Organize positions by portfolio
        pos_map = {}
        unique_symbols = set()
        
        for pos in all_positions:
            pid = pos.portfolio_id
            if pid not in pos_map:
                pos_map[pid] = []
            pos_map[pid].append(pos)
            unique_symbols.add(pos.symbol)
            
        # Bulk fetch prices
        price_map = {}
        if unique_symbols:
            ticker_data = crud_stock.get_ticker_data(engine, list(unique_symbols))
            price_map = {item['symbol']: item['price'] for item in ticker_data}
            
        # Calculate Returns
        for p in portfolios:
            current_equity = p.balance
            start_equity = 1000000.0 # Default starting cash
            
            p_positions = pos_map.get(p.portfolio_id, [])
            trade_count = 0 # Placeholder or fetch from transactions count
            
            for pos in p_positions:
                price = price_map.get(pos.symbol, pos.avg_price)
                current_equity += (pos.quantity * price)
            
            # Simple Return: (Current - Start) / Start
            ret = ((current_equity - start_equity) / start_equity) * 100
            
            results.append({
                "user_id": p.user_id,
                "user_name": p.full_name or p.email.split('@')[0],
                "user_avatar": None,
                "total_value": current_equity,
                "return_percent": ret,
                "trades_count": len(p_positions) # Just using active positions count as proxy for activity
            })
            
        # Sort by Return Descending
        results.sort(key=lambda x: x["return_percent"], reverse=True)
        
        # Top N
        final_ranking = []
        for i, res in enumerate(results[:limit]):
            res["rank"] = i + 1
            final_ranking.append(LeaderboardEntry(**res))
            
        return final_ranking
