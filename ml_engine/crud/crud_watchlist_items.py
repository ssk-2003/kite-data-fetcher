from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import List, Dict, Any


def get_user_watchlist(*, engine: Engine, user_id: int) -> List[Dict[str, Any]]:
    """
    Get all watchlist items for a user with stock details.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT 
                    w.id,
                    w.user_id,
                    w.instrument_token,
                    w.symbol,
                    w.added_at,
                    sm.name,
                    sm.exchange,
                    sh.close as current_price,
                    sh.log_return as change_percent,
                    p.omre_score
                FROM watchlists w
                LEFT JOIN stock_master sm ON w.instrument_token = sm.instrument_token
                LEFT JOIN LATERAL (
                    SELECT close, log_return 
                    FROM stock_history 
                    WHERE instrument_token = w.instrument_token 
                    AND interval = 'day'
                    ORDER BY ts DESC 
                    LIMIT 1
                ) sh ON true
                LEFT JOIN predictions p ON w.symbol = p.symbol
                WHERE w.user_id = :user_id
                ORDER BY w.added_at DESC
            """),
            {"user_id": user_id}
        ).fetchall()
        
        return [dict(row._mapping) for row in result]


def add_to_watchlist(*, engine: Engine, user_id: int, instrument_token: int, symbol: str) -> Dict[str, Any]:
    """
    Add a stock to user's watchlist.
    Returns the created watchlist item.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO watchlists (user_id, instrument_token, symbol)
                VALUES (:user_id, :instrument_token, :symbol)
                ON CONFLICT (user_id, instrument_token) DO NOTHING
                RETURNING id, user_id, instrument_token, symbol, added_at
            """),
            {
                "user_id": user_id,
                "instrument_token": instrument_token,
                "symbol": symbol
            }
        ).fetchone()
        
        if result:
            return dict(result._mapping)
        
        # Item already exists, fetch it
        existing = conn.execute(
            text("""
                SELECT id, user_id, instrument_token, symbol, added_at
                FROM watchlists
                WHERE user_id = :user_id AND instrument_token = :instrument_token
            """),
            {"user_id": user_id, "instrument_token": instrument_token}
        ).fetchone()
        
        return dict(existing._mapping) if existing else None


def remove_from_watchlist(*, engine: Engine, user_id: int, instrument_token: int) -> bool:
    """
    Remove a stock from user's watchlist.
    Returns True if item was removed, False if not found.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                DELETE FROM watchlists
                WHERE user_id = :user_id AND instrument_token = :instrument_token
            """),
            {"user_id": user_id, "instrument_token": instrument_token}
        )
        return result.rowcount > 0


def is_in_watchlist(*, engine: Engine, user_id: int, instrument_token: int) -> bool:
    """
    Check if a stock is in user's watchlist.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT 1 FROM watchlists
                WHERE user_id = :user_id AND instrument_token = :instrument_token
            """),
            {"user_id": user_id, "instrument_token": instrument_token}
        ).fetchone()
        
        return result is not None


def get_watchlist_count(*, engine: Engine, user_id: int) -> int:
    """
    Get the count of items in user's watchlist.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM watchlists WHERE user_id = :user_id"),
            {"user_id": user_id}
        ).scalar()
        
        return result or 0
