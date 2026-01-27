from __future__ import annotations
import datetime as dt
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ml_engine.db.schema import init_db

def upsert_stock_master(engine: Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = text(
        """
        INSERT INTO stock_master (
          instrument_token, tradingsymbol, name, exchange, last_updated_at
        ) VALUES (
          :instrument_token, :tradingsymbol, :name, :exchange, NOW()
        )
        ON CONFLICT (instrument_token) DO UPDATE SET
          tradingsymbol = EXCLUDED.tradingsymbol,
          name = EXCLUDED.name,
          exchange = EXCLUDED.exchange,
          last_updated_at = NOW();
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql, rows)
    return int(getattr(result, "rowcount", 0) or 0)

def upsert_stock_history(
    *,
    engine: Engine,
    instrument_token: int,
    interval: str,
    candles: list[dict],
) -> int:
    if not candles:
        return 0


    rows: list[dict] = []
    for c in candles:
        ts = c.get("date")
        if isinstance(ts, dt.date) and not isinstance(ts, dt.datetime):
            ts = dt.datetime(ts.year, ts.month, ts.day, tzinfo=dt.timezone.utc)
        rows.append(
            {
                "instrument_token": instrument_token,
                "ts": ts,
                "interval": interval,
                "open": c.get("open"),
                "high": c.get("high"),
                "low": c.get("low"),
                "close": c.get("close"),
                "volume": c.get("volume"),
                "oi": c.get("oi"),
            }
        )

    sql = text(
        """
        INSERT INTO stock_history (
          instrument_token, ts, interval, open, high, low, close, volume, oi
        ) VALUES (
          :instrument_token, :ts, :interval, :open, :high, :low, :close, :volume, :oi
        )
        ON CONFLICT (instrument_token, ts, interval) DO NOTHING;
        """
    )

    with engine.begin() as conn:
        result = conn.execute(sql, rows)
    return int(getattr(result, "rowcount", 0) or 0)


def get_all_stocks(engine: Engine, limit: int = 100, offset: int = 0) -> list[dict]:
    """Fetch all stocks from stock_master with pagination."""
    sql = text(
        """
        SELECT instrument_token, tradingsymbol, name, exchange, last_updated_at
        FROM stock_master
        ORDER BY tradingsymbol
        LIMIT :limit OFFSET :offset;
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql, {"limit": limit, "offset": offset})
        return [dict(row._mapping) for row in result]


def get_stock_by_symbol(engine: Engine, symbol: str) -> dict | None:
    """Fetch a single stock by its trading symbol with latest price."""
    sql = text(
        """
        SELECT 
            sm.instrument_token, 
            sm.tradingsymbol, 
            sm.name, 
            sm.exchange, 
            sm.last_updated_at,
            sh.close as current_price
        FROM stock_master sm
        LEFT JOIN LATERAL (
            SELECT close
            FROM stock_history
            WHERE instrument_token = sm.instrument_token
              AND interval = 'day'
            ORDER BY ts DESC
            LIMIT 1
        ) sh ON TRUE
        WHERE UPPER(sm.tradingsymbol) = UPPER(:symbol)
        LIMIT 1;
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql, {"symbol": symbol})
        row = result.fetchone()
        return dict(row._mapping) if row else None


def get_stock_history(
    engine: Engine,
    instrument_token: int,
    interval: str = "day",
    limit: int = 100,
) -> list[dict]:
    """Fetch OHLC candle data for a stock."""
    sql = text(
        """
        SELECT ts, open, high, low, close, volume, oi
        FROM stock_history
        WHERE instrument_token = :instrument_token AND interval = :interval
        ORDER BY ts DESC
        LIMIT :limit;
        """
    )
    with engine.begin() as conn:
        result = conn.execute(
            sql,
            {"instrument_token": instrument_token, "interval": interval, "limit": limit},
        )
        return [dict(row._mapping) for row in result]


def search_stocks(engine: Engine, query: str, limit: int = 20) -> list[dict]:
    """Search stocks by symbol or name with latest price."""
    sql = text(
        """
        SELECT 
            sm.instrument_token, 
            sm.tradingsymbol, 
            sm.name, 
            sm.exchange, 
            sm.is_stable,
            sh.close as current_price
        FROM stock_master sm
        LEFT JOIN LATERAL (
            SELECT close
            FROM stock_history
            WHERE instrument_token = sm.instrument_token
              AND interval = 'day'
            ORDER BY ts DESC
            LIMIT 1
        ) sh ON TRUE
        WHERE UPPER(sm.tradingsymbol) LIKE UPPER(:query) OR UPPER(sm.name) LIKE UPPER(:query)
        ORDER BY sm.tradingsymbol
        LIMIT :limit;
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql, {"query": f"%{query}%", "limit": limit})
        return [dict(row._mapping) for row in result]


def get_ticker_data(engine: Engine, symbols: list[str]) -> list[dict]:
    """
    Fetch the latest 2 daily candles for the given symbols efficiently.
    Returns a dictionary of symbol -> {price, change, percent_change}.
    """
    if not symbols:
        return []

    sym_map = {}
    tokens = []
    
    clean_symbols = [s.upper() for s in symbols]
    
    bind_params = {f"s{i}": s for i, s in enumerate(clean_symbols)}
    bind_clause = ", ".join([f":s{i}" for i in range(len(clean_symbols))])
    
    sql_tokens = text(f"""
        SELECT instrument_token, tradingsymbol 
        FROM stock_master 
        WHERE UPPER(tradingsymbol) IN ({bind_clause})
    """)
    
    with engine.connect() as conn:
        res_tokens = conn.execute(sql_tokens, bind_params).fetchall()
        for r in res_tokens:
            sym_map[r.instrument_token] = r.tradingsymbol
            tokens.append(r.instrument_token)

    if not tokens:
        return []

    token_str = ",".join(map(str, tokens))
    
    # Optimized: Use LATERAL join pattern instead of ROW_NUMBER to avoid OOM
    # This fetches only last 2 rows per token efficiently
    sql_history = text(f"""
        SELECT t.instrument_token, h.ts, h.close
        FROM (SELECT unnest(ARRAY[{token_str}]::bigint[]) as instrument_token) t
        CROSS JOIN LATERAL (
            SELECT ts, close
            FROM stock_history
            WHERE instrument_token = t.instrument_token
              AND interval = 'day'
            ORDER BY ts DESC
            LIMIT 2
        ) h;
    """)
    
    with engine.connect() as conn:
        rows = conn.execute(sql_history).fetchall()

    grouped_data = {}
    for r in rows:
        tok = r.instrument_token
        if tok not in grouped_data:
            grouped_data[tok] = []
        grouped_data[tok].append(r)

    results = []
    for tok, candles in grouped_data.items():
        symbol = sym_map.get(tok)
        if not symbol:
            continue
            
        candles.sort(key=lambda x: x.ts, reverse=True)
        
        latest = candles[0] if candles else None
        prev = candles[1] if len(candles) > 1 else None
        
        price = latest.close if latest else 0.0
        prev_close = prev.close if prev else price # fallback if no history
        
        change = price - prev_close
        pct = (change / prev_close * 100) if prev_close else 0.0
        
        results.append({
            "symbol": symbol,
            "price": price,
            "change": round(change, 2),
            "changePercent": round(pct, 2),
            "isUp": change >= 0
        })
        
    return results


def get_stock_indicators(engine: Engine, instrument_token: int) -> dict | None:
    """Fetch the latest technical indicators for a stock from DB."""
    sql = text("""
        SELECT 
            ts,
            close,
            rsi_14,
            ema_50_div,
            ema_200_div,
            atr_14_norm,
            rvol,
            log_return,
            adx_14,
            rel_strength,
            bb_width,
            dist_52wh,
            momentum_strength,
            panic_buy_signal,
            ema_50_zscore,
            trend_regime,
            is_breakout
        FROM stock_history
        WHERE instrument_token = :instrument_token 
          AND interval = 'day'
          AND rsi_14 IS NOT NULL
        ORDER BY ts DESC
        LIMIT 1;
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"instrument_token": instrument_token})
        row = result.fetchone()
        if not row:
            return None
        return dict(row._mapping)


def get_stock_fundamentals(engine: Engine, instrument_token: int) -> dict | None:
    """Fetch fundamental metrics (D/E, ROE, P/E, etc.) for a stock from DB."""
    import math
    
    sql = text("""
        SELECT 
            instrument_token,
            tradingsymbol,
            debt_to_equity,
            promoter_holding,
            roe,
            pe_ratio,
            market_cap,
            book_value,
            dividend_yield,
            profit_margin,
            current_ratio,
            revenue_growth,
            updated_at
        FROM stock_fundamentals
        WHERE instrument_token = :instrument_token
        LIMIT 1;
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"instrument_token": instrument_token})
        row = result.fetchone()
        if not row:
            return None
        data = dict(row._mapping)
        for key, value in data.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                data[key] = None
        return data


def get_stock_prediction(engine: Engine, instrument_token: int) -> dict | None:
    """Fetch the latest prediction (OMRE Score, Twin Match, etc.) for a stock."""
    sql = text("""
        SELECT 
            symbol,
            omre_score,
            signal,
            score_ai,
            score_tech,
            score_sim,
            score_fund,
            score_news,
            sim_match_date,
            sim_return,
            created_at
        FROM predictions
        WHERE instrument_token = :instrument_token
        ORDER BY created_at DESC
        LIMIT 1;
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"instrument_token": instrument_token})
        row = result.fetchone()
        if not row:
            return None
        return dict(row._mapping)


def get_top_scorers(engine: Engine, limit: int = 10) -> list[dict]:
    """Fetch top stocks by OMRE Score with metadata from stock_master."""
    sql = text("""
        SELECT DISTINCT ON (p.symbol)
            p.symbol,
            p.omre_score,
            p.signal,
            p.score_ai,
            p.score_tech,
            p.score_sim,
            p.score_fund,
            p.score_news,
            p.sim_match_date,
            p.sim_return,
            m.name,
            m.exchange,
            m.instrument_token
        FROM predictions p
        JOIN stock_master m ON p.instrument_token = m.instrument_token
        ORDER BY p.symbol, p.created_at DESC
    """)
    
    # Wrap in subquery to order by omre_score
    full_sql = text("""
        SELECT * FROM (
            SELECT DISTINCT ON (p.symbol)
                p.symbol,
                p.omre_score,
                p.signal,
                p.score_ai,
                p.score_tech,
                p.score_sim,
                p.score_fund,
                p.score_news,
                p.sim_match_date,
                p.sim_return,
                m.name,
                m.exchange,
                m.instrument_token
            FROM predictions p
            JOIN stock_master m ON p.instrument_token = m.instrument_token
            ORDER BY p.symbol, p.created_at DESC
        ) latest
        ORDER BY omre_score DESC
        LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(full_sql, {"limit": limit})
        return [dict(row._mapping) for row in result]


def get_most_volatile(engine: Engine, limit: int = 25) -> list[dict]:
    """Fetch stocks with highest fluctuation (absolute log_return) with latest price data."""
    full_sql = text("""
        SELECT 
            m.tradingsymbol as symbol,
            m.name,
            m.exchange,
            m.instrument_token,
            h.log_return,
            h.close as price,
            h.ts
        FROM stock_master m
        JOIN LATERAL (
            SELECT log_return, close, ts
            FROM stock_history 
            WHERE instrument_token = m.instrument_token 
              AND interval = 'day' 
              AND log_return IS NOT NULL
            ORDER BY ts DESC 
            LIMIT 1
        ) h ON TRUE
        ORDER BY ABS(h.log_return) DESC
        LIMIT :limit
    """)
    
    with engine.connect() as conn:
        result = conn.execute(full_sql, {"limit": limit})
        rows = [dict(row._mapping) for row in result]
    
    # Calculate price change from log_return
    for row in rows:
        log_ret = row.get('log_return') or 0
        price = row.get('price') or 0
        # log_return is stored as percentage, convert to price change
        row['change'] = round(price * (log_ret / 100), 2)
        row['changePercent'] = round(log_ret, 2)
        row['isUp'] = log_ret >= 0
    
    return rows


def get_major_indices(engine: Engine) -> list[dict]:
    """Fetch major market indices (NIFTY 50, BANKNIFTY, SENSEX, MIDCPNIFTY) with latest prices."""
    # Major indices to fetch - stored in stock_master
    indices = [
        {"symbol": "NIFTY 50", "display_name": "NIFTY 50"},
        {"symbol": "NIFTY BANK", "display_name": "BANK NIFTY"},
        {"symbol": "SENSEX", "display_name": "SENSEX"},
        {"symbol": "NIFTY MID SELECT", "display_name": "MIDCAP NIFTY"},
        {"symbol": "NIFTY IT", "display_name": "NIFTY IT"},
        {"symbol": "NIFTY HEALTHCARE", "display_name": "NIFTY HEALTHCARE"},
        {"symbol": "NIFTY AUTO", "display_name": "NIFTY AUTO"},
        {"symbol": "NIFTY FMCG", "display_name": "NIFTY FMCG"},
        {"symbol": "NIFTY METAL", "display_name": "NIFTY METAL"},
        {"symbol": "NIFTY ENERGY", "display_name": "NIFTY ENERGY"},
        {"symbol": "NIFTY FIN SERVICE", "display_name": "NIFTY FIN SERVICE"},
    ]
    
    index_symbols = [idx["symbol"] for idx in indices]
    symbol_to_display = {idx["symbol"]: idx["display_name"] for idx in indices}
    
    # Get tokens for these indices
    bind_params = {f"s{i}": s for i, s in enumerate(index_symbols)}
    bind_clause = ", ".join([f":s{i}" for i in range(len(index_symbols))])
    
    sql_tokens = text(f"""
        SELECT instrument_token, tradingsymbol 
        FROM stock_master 
        WHERE tradingsymbol IN ({bind_clause})
    """)
    
    sym_map = {}
    tokens = []
    
    with engine.connect() as conn:
        res_tokens = conn.execute(sql_tokens, bind_params).fetchall()
        for r in res_tokens:
            sym_map[r.instrument_token] = r.tradingsymbol
            tokens.append(r.instrument_token)
    
    if not tokens:
        return []
    
    token_str = ",".join(map(str, tokens))
    
    # Fetch latest 2 candles for price change calculation
    sql_history = text(f"""
        SELECT t.instrument_token, h.ts, h.close
        FROM (SELECT unnest(ARRAY[{token_str}]::bigint[]) as instrument_token) t
        CROSS JOIN LATERAL (
            SELECT ts, close
            FROM stock_history
            WHERE instrument_token = t.instrument_token
              AND interval = 'day'
            ORDER BY ts DESC
            LIMIT 2
        ) h;
    """)
    
    with engine.connect() as conn:
        rows = conn.execute(sql_history).fetchall()
    
    grouped_data = {}
    for r in rows:
        tok = r.instrument_token
        if tok not in grouped_data:
            grouped_data[tok] = []
        grouped_data[tok].append(r)
    
    results = []
    for tok, candles in grouped_data.items():
        symbol = sym_map.get(tok)
        if not symbol:
            continue
            
        candles.sort(key=lambda x: x.ts, reverse=True)
        
        latest = candles[0] if candles else None
        prev = candles[1] if len(candles) > 1 else None
        
        price = latest.close if latest else 0.0
        prev_close = prev.close if prev else price
        
        change = price - prev_close
        pct = (change / prev_close * 100) if prev_close else 0.0
        
        results.append({
            "symbol": symbol,
            "display_name": symbol_to_display.get(symbol, symbol),
            "instrument_token": tok,
            "price": price,
            "change": round(change, 2),
            "changePercent": round(pct, 2),
            "isUp": change >= 0
        })
    
    # Sort results by the original indices order
    order_map = {s: i for i, s in enumerate(index_symbols)}
    results.sort(key=lambda x: order_map.get(x["symbol"], 99))
    
    return results


def get_stock_returns(engine: Engine, symbol: str) -> dict:
    """
    Calculate 5, 10, 15, 25 day returns for a stock.
    Returns percentage change from N days ago to today.
    """
    with engine.connect() as conn:
        token_res = conn.execute(
            text("SELECT instrument_token FROM stock_master WHERE tradingsymbol = :symbol"),
            {"symbol": symbol}
        ).fetchone()

        if not token_res:
            return {
                "return_5d": None,
                "return_10d": None,
                "return_15d": None,
                "return_25d": None
            }
            
        token = token_res[0]
        
        result = conn.execute(text("""
            WITH daily_closes AS (
                SELECT 
                    close,
                    ts::date as date,
                    ROW_NUMBER() OVER (ORDER BY ts DESC) as row_num
                FROM stock_history h
                WHERE h.instrument_token = :token 
                  AND h.interval = 'day'
                ORDER BY ts DESC
                LIMIT 30
            )
            SELECT 
                (SELECT close FROM daily_closes WHERE row_num = 1) as close_today,
                (SELECT close FROM daily_closes WHERE row_num = 6) as close_5d,
                (SELECT close FROM daily_closes WHERE row_num = 11) as close_10d,
                (SELECT close FROM daily_closes WHERE row_num = 16) as close_15d,
                (SELECT close FROM daily_closes WHERE row_num = 26) as close_25d
        """), {"token": token})
        
        row = result.fetchone()
        
        if not row or not row[0]:
            return {
                "return_5d": None,
                "return_10d": None,
                "return_15d": None,
                "return_25d": None
            }
        
        close_today = row[0]
        close_5d = row[1]
        close_10d = row[2]
        close_15d = row[3]
        close_25d = row[4]
        
        def calc_return(today, past):
            if past and past != 0:
                return round(((today - past) / past) * 100, 2)
            return None
        
        return {
            "return_5d": calc_return(close_today, close_5d),
            "return_10d": calc_return(close_today, close_10d),
            "return_15d": calc_return(close_today, close_15d),
            "return_25d": calc_return(close_today, close_25d)
        }


def filter_stocks(
    engine: Engine,
    min_score: float | None = None,
    max_score: float | None = None,
    signal: str | None = None,
    min_market_cap: float | None = None,
    max_market_cap: float | None = None,
    min_pe: float | None = None,
    max_pe: float | None = None,
    max_price: float | None = None,
    exchange: str | None = None,
    symbols: list[str] | None = None,
    limit: int = 50,
    offset: int = 0
) -> list[dict]:
    """
    Filter stocks based on multiple criteria:
    - OMRE Score range
    - Market Cap range
    - P/E Ratio range
    - Signal (BUY/SELL)
    - Exchange
    - Specific Symbols (for Watchlist)
    """
    
    # Base query joining mostly used tables
    # using DISTINCT ON (p.symbol) to get latest prediction
    
    query = """
        SELECT * FROM (
            SELECT DISTINCT ON (m.tradingsymbol)
                m.instrument_token,
                m.tradingsymbol as symbol,
                m.name,
                m.name as company_name,
                m.exchange,
                p.omre_score,
                p.signal,
                p.score_ai,
                p.score_tech,
                p.score_fund,
                p.score_news,
                f.market_cap,
                f.pe_ratio,
                f.roe,
                f.debt_to_equity,
                p.created_at,
                h.close as current_price,
                h.log_return as change_percent
            FROM stock_master m
            LEFT JOIN predictions p ON m.instrument_token = p.instrument_token
            LEFT JOIN stock_fundamentals f ON m.instrument_token = f.instrument_token
            LEFT JOIN LATERAL (
                SELECT close, log_return
                FROM stock_history
                WHERE instrument_token = m.instrument_token
                  AND interval = 'day'
                ORDER BY ts DESC
                LIMIT 1
            ) h ON TRUE
            ORDER BY m.tradingsymbol, p.created_at DESC
        ) latest
        WHERE 1=1
    """
    
    params = {"limit": limit, "offset": offset}
    
    if symbols:
        query += " AND symbol = ANY(:symbols)"
        params["symbols"] = symbols

    if min_score is not None:
        query += " AND omre_score >= :min_score"
        params["min_score"] = min_score
        
    if max_score is not None:
        query += " AND omre_score <= :max_score"
        params["max_score"] = max_score
        
    if signal:
        query += " AND UPPER(signal) = UPPER(:signal)"
        params["signal"] = signal
        
    if min_market_cap is not None:
        query += " AND market_cap >= :min_market_cap"
        params["min_market_cap"] = min_market_cap
        
    if max_market_cap is not None:
        query += " AND market_cap <= :max_market_cap"
        params["max_market_cap"] = max_market_cap

    if min_pe is not None:
        query += " AND pe_ratio >= :min_pe"
        params["min_pe"] = min_pe
        
    if max_pe is not None:
        query += " AND pe_ratio <= :max_pe"
        params["max_pe"] = max_pe
        
    if exchange:
        query += " AND UPPER(exchange) = UPPER(:exchange)"
        params["exchange"] = exchange
        
    if max_price is not None:
        query += " AND current_price <= :max_price"
        params["max_price"] = max_price
        
    # Default sort by score desc if filtering by score, otherwise symbol
    if min_score is not None or max_score is not None:
        query += " ORDER BY omre_score DESC"
    else:
        # Fixed: must use 'symbol' because outer query has renamed 'tradingsymbol' to 'symbol'
        query += " ORDER BY symbol"
        
    query += " LIMIT :limit OFFSET :offset"
    
    import math

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = [dict(row._mapping) for row in result]
        
        # Sanitize data to remove NaN/Infinity which cause JSON serialization errors
        for row in rows:
            for key, value in row.items():
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    row[key] = None
                    
        return rows


def get_ghost_data(
    engine: Engine, 
    instrument_token: int, 
    match_date: dt.date
) -> list[dict]:
    """
    Fetch historical data for the 'Ghost Chart' around a specific match date.
    Range: 180 days before match_date to 60 days after match_date.
    This provides context (past pattern) and future (prediction).
    """
    start_date = match_date - dt.timedelta(days=180)
    end_date = match_date + dt.timedelta(days=60)
    
    sql = text("""
        SELECT ts, close
        FROM stock_history
        WHERE instrument_token = :instrument_token 
          AND interval = 'day'
          AND ts >= :start_date 
          AND ts <= :end_date
        ORDER BY ts ASC;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(sql, {
            "instrument_token": instrument_token,
            "start_date": start_date,
            "end_date": end_date
        })
        return [dict(row._mapping) for row in result]

