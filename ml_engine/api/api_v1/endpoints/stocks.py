from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.engine import Engine
from sqlalchemy import text
from ml_engine.api import deps
from ml_engine.crud import crud_stock

router = APIRouter()


@router.get("/")
def list_stocks(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    List all stocks from the master list with pagination.
    """
    stocks = crud_stock.get_all_stocks(engine=engine, limit=limit, offset=offset)
    return {"stocks": stocks, "count": len(stocks)}


@router.get("/search")
def search_stocks(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(20, ge=1, le=100),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Search stocks by symbol or name.
    """
    stocks = crud_stock.search_stocks(engine=engine, query=q, limit=limit)
    return {"results": stocks}



from ml_engine.core.cache import stock_cache, ticker_cache

@router.get("/ticker")
def get_ticker_tape(
    symbols: str = Query(..., description="Comma separated list of symbols"),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get ticker data (price, change) for multiple stocks efficiently.
    Uses caching to reduce DB load.
    """
    cache_key = f"ticker:{symbols}"
    cached = ticker_cache.get(cache_key)
    if cached:
        return cached

    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    data = crud_stock.get_ticker_data(engine=engine, symbols=symbol_list)
    
    ticker_cache.set(cache_key, data)
    return data


@router.get("/top-scorers")
def get_top_scorers(
    limit: int = Query(10, ge=1, le=50),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get top stocks by OMRE Score.
    Returns stocks from predictions table ordered by omre_score DESC.
    Includes full score breakdown (AI, Tech, Sim, Fund, News).
    """
    cache_key = f"top_scorers:{limit}"
    cached = stock_cache.get(cache_key)
    if cached:
        return cached
    
    stocks = crud_stock.get_top_scorers(engine=engine, limit=limit)
    response = {"stocks": stocks, "count": len(stocks)}
    
    stock_cache.set(cache_key, response)
    return response


@router.get("/most-volatile")
def get_most_volatile(
    limit: int = Query(25, ge=1, le=50),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get stocks with highest price fluctuation (by log_return).
    Returns stocks ordered by ABS(log_return) DESC.
    Useful for ticker displays showing most active movers.
    """
    cache_key = f"most_volatile:{limit}"
    cached = ticker_cache.get(cache_key)
    if cached:
        return cached
    
    stocks = crud_stock.get_most_volatile(engine=engine, limit=limit)
    response = {"stocks": stocks, "count": len(stocks)}
    
    ticker_cache.set(cache_key, response)
    return response



@router.get("/screener")
def screener(
    min_score: float = Query(None, ge=0, le=100),
    max_score: float = Query(None, ge=0, le=100),
    signal: str = Query(None),
    min_market_cap: float = Query(None),
    max_market_cap: float = Query(None),
    min_pe: float = Query(None),
    max_pe: float = Query(None),
    max_price: float = Query(None),
    exchange: str = Query(None),
    symbols: list[str] = Query(None),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Filter stocks based on various criteria for the Screener page.
    """
    stocks = crud_stock.filter_stocks(
        engine=engine,
        min_score=min_score,
        max_score=max_score,
        signal=signal,
        min_market_cap=min_market_cap,
        max_market_cap=max_market_cap,
        min_pe=min_pe,
        max_pe=max_pe,
        max_price=max_price,
        exchange=exchange,
        symbols=symbols,
        limit=limit,
        offset=offset
    )
    return {"stocks": stocks, "count": len(stocks)}


@router.get("/indices")
def get_market_indices(
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get major market indices (NIFTY 50, BANKNIFTY, SENSEX, MIDCPNIFTY) with live prices.
    Returns dynamic data from the database instead of hardcoded values.
    """
    cache_key = "market_indices"
    cached = ticker_cache.get(cache_key)
    if cached:
        return cached
    
    indices = crud_stock.get_major_indices(engine=engine)
    response = {"indices": indices, "count": len(indices)}
    
    ticker_cache.set(cache_key, response)
    return response


@router.get("/{symbol}/returns")
def get_stock_returns(
    symbol: str,
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get 5, 10, 15, 25 day returns for a stock.
    Returns percentage change from N days ago to today.
    """
    cache_key = f"stock:returns:{symbol.upper()}"
    cached = stock_cache.get(cache_key)
    if cached:
        return cached
    
    returns = crud_stock.get_stock_returns(engine=engine, symbol=symbol)
    
    stock_cache.set(cache_key, returns)
    return returns


@router.get("/{symbol}/full")
def get_stock_full(
    symbol: str,
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    BATCH ENDPOINT: Get ALL stock data in one request.
    Combines stock info, indicators, fundamentals, and predictions.
    Reduces 4 API calls to 1, with 5-minute caching.
    """
    cache_key = f"stock:full:{symbol.upper()}"
    cached = stock_cache.get(cache_key)
    if cached:
        return cached
    
    stock = crud_stock.get_stock_by_symbol(engine=engine, symbol=symbol)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    token = stock["instrument_token"]
    
    # Fetch all data at once
    indicators_raw = crud_stock.get_stock_indicators(engine, token)
    fundamentals = crud_stock.get_stock_fundamentals(engine, token)
    prediction = crud_stock.get_stock_prediction(engine, token)
    
    # Format indicators
    indicators = None
    if indicators_raw:
        indicators = {
            "ts": indicators_raw.get("ts"),
            "price": indicators_raw.get("close"),
            "rsi_14": round(indicators_raw.get("rsi_14") or 0, 2),
            "ema_50_div": round((indicators_raw.get("ema_50_div") or 0) * 100, 2),
            "ema_200_div": round((indicators_raw.get("ema_200_div") or 0) * 100, 2),
            "atr_14_norm": round((indicators_raw.get("atr_14_norm") or 0) * 100, 2),
            "rvol": round(indicators_raw.get("rvol") or 0, 2),
            "adx_14": round(indicators_raw.get("adx_14") or 0, 2),
            "trend_regime": indicators_raw.get("trend_regime") or 0,
            "is_breakout": indicators_raw.get("is_breakout") or 0,
        }
    
    response = {
        "symbol": symbol.upper(),
        "stock": stock,
        "indicators": indicators,
        "fundamentals": fundamentals,
        "prediction": prediction,
    }
    
    stock_cache.set(cache_key, response)
    return response


@router.get("/{symbol}/indicators")
def get_stock_indicators(
    symbol: str,
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get technical indicators (RSI, EMA, ATR, etc.) for a stock.
    Returns the latest values from the database.
    """
    # Check cache first
    cache_key = f"indicators:{symbol.upper()}"
    cached = stock_cache.get(cache_key)
    if cached:
        return cached
    
    try:
        stock = crud_stock.get_stock_by_symbol(engine=engine, symbol=symbol)
        if not stock:
            raise HTTPException(status_code=404, detail="Stock not found")
        
        indicators = crud_stock.get_stock_indicators(
            engine=engine,
            instrument_token=stock["instrument_token"],
        )
        
        if not indicators:
            raise HTTPException(status_code=404, detail="No indicator data available")
        
        # Format response with meaningful names
        response = {
            "symbol": symbol,
            "ts": indicators.get("ts"),
            "price": indicators.get("close"),
            "indicators": {
                "rsi_14": round(indicators.get("rsi_14") or 0, 2),
                "ema_50_div": round((indicators.get("ema_50_div") or 0) * 100, 2),
                "ema_200_div": round((indicators.get("ema_200_div") or 0) * 100, 2),
                "atr_14_norm": round((indicators.get("atr_14_norm") or 0) * 100, 2),
                "rvol": round(indicators.get("rvol") or 0, 2),
                "log_return": round(indicators.get("log_return") or 0, 2),
                "adx_14": round(indicators.get("adx_14") or 0, 2),
                "rel_strength": round(indicators.get("rel_strength") or 0, 2),
                "bb_width": round((indicators.get("bb_width") or 0) * 100, 2),
                "trend_regime": indicators.get("trend_regime") or 0,
                "is_breakout": indicators.get("is_breakout") or 0,
                "momentum_strength": round(indicators.get("momentum_strength") or 0, 2),
                "panic_buy_signal": indicators.get("panic_buy_signal") or 0,
                "ema_50_zscore": round(indicators.get("ema_50_zscore") or 0, 2),
            }
        }
        
        stock_cache.set(cache_key, response)
        return response
    except HTTPException:
        raise
    except Exception as e:
        # Log the error for debugging
        import logging
        logging.error(f"Error fetching indicators for {symbol}: {str(e)}")
        raise HTTPException(
            status_code=503, 
            detail="Database temporarily unavailable. Please try again in a moment."
        )


@router.get("/{symbol}/fundamentals")
def get_stock_fundamentals(
    symbol: str,
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get fundamental metrics (D/E Ratio, ROE, P/E, etc.) for a stock.
    Returns the latest values from the database.
    """
    # Check cache first
    cache_key = f"fundamentals:{symbol.upper()}"
    cached = stock_cache.get(cache_key)
    if cached:
        return cached
    
    stock = crud_stock.get_stock_by_symbol(engine=engine, symbol=symbol)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    fundamentals = crud_stock.get_stock_fundamentals(
        engine=engine,
        instrument_token=stock["instrument_token"],
    )
    
    if not fundamentals:
        # Return empty data instead of 404 - some stocks may not have fundamentals
        response = {
            "symbol": symbol,
            "has_data": False,
            "fundamentals": {}
        }
        stock_cache.set(cache_key, response)
        return response
    
    # Format response
    response = {
        "symbol": symbol,
        "has_data": True,
        "updated_at": fundamentals.get("updated_at"),
        "fundamentals": {
            "debt_to_equity": fundamentals.get("debt_to_equity"),
            "roe": fundamentals.get("roe"),
            "pe_ratio": fundamentals.get("pe_ratio"),
            "promoter_holding": fundamentals.get("promoter_holding"),
            "market_cap": fundamentals.get("market_cap"),
            "book_value": fundamentals.get("book_value"),
            "dividend_yield": fundamentals.get("dividend_yield"),
            "profit_margin": fundamentals.get("profit_margin"),
            "current_ratio": fundamentals.get("current_ratio"),
            "revenue_growth": fundamentals.get("revenue_growth"),
        }
    }
    
    stock_cache.set(cache_key, response)
    return response



@router.get("/{symbol}/predictions")
def get_stock_predictions(
    symbol: str,
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get latest AI predictions (Score, Signal, Twin Match).
    """
    # Check cache first
    cache_key = f"predictions:{symbol.upper()}"
    cached = stock_cache.get(cache_key)
    if cached:
        return cached
    
    stock = crud_stock.get_stock_by_symbol(engine=engine, symbol=symbol)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    prediction = crud_stock.get_stock_prediction(
        engine=engine,
        instrument_token=stock["instrument_token"],
    )
    
    if not prediction:
        # Return empty/default if no prediction yet
        response = {
            "symbol": symbol,
            "has_prediction": False,
            "data": {}
        }
        stock_cache.set(cache_key, response)
        return response
    
    response = {
        "symbol": symbol,
        "has_prediction": True,
        "updated_at": prediction.get("created_at"),
        "data": {
            "omre_score": prediction.get("omre_score"),
            "signal": prediction.get("signal"),
            "confidence": round((prediction.get("score_ai") or 0) * 2.5, 1),  # Convert score_ai (0-40) to confidence (0-100)
            "score_ai": prediction.get("score_ai"),
            "score_tech": prediction.get("score_tech"),
            "score_sim": prediction.get("score_sim"),
            "score_fund": prediction.get("score_fund"),
            "score_news": prediction.get("score_news"),
            "sim_match_date": prediction.get("sim_match_date"),
            "sim_return": prediction.get("sim_return")
        }
    }
    
    stock_cache.set(cache_key, response)
    return response


@router.get("/{symbol}")
def get_stock(
    symbol: str,
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get a single stock by its trading symbol.
    """
    stock = crud_stock.get_stock_by_symbol(engine=engine, symbol=symbol)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    return stock


@router.get("/{symbol}/history")
def get_stock_history(
    symbol: str,
    interval: str = Query("day", description="Candle interval (day, minute, etc.)"),
    limit: int = Query(100, ge=1, le=5000),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get OHLC candle history for a stock.
    """
    cache_key = f"history:{symbol}:{interval}:{limit}"
    cached = stock_cache.get(cache_key)
    if cached:
        return cached

    stock = crud_stock.get_stock_by_symbol(engine=engine, symbol=symbol)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    candles = crud_stock.get_stock_history(
        engine=engine,
        instrument_token=stock["instrument_token"],
        interval=interval,
        limit=limit,
    )
    
    response = {
        "symbol": symbol,
        "interval": interval,
        "candles": candles,
    }
    
    stock_cache.set(cache_key, response)
    return response


@router.get("/{symbol}/news")
def get_stock_news(
    symbol: str,
    limit: int = Query(10, ge=1, le=50),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get latest news for a stock with sentiment scores.
    """
    with engine.connect() as conn:
        news = conn.execute(
            text("""
                SELECT title, link, published_at, sentiment_score
                FROM stock_news
                WHERE symbol = :symbol
                ORDER BY published_at DESC
                LIMIT :limit
            """),
            {"symbol": symbol, "limit": limit}
        ).fetchall()
        
    return {
        "symbol": symbol,
        "news": [dict(row._mapping) for row in news]
    }


@router.get("/{symbol}/ghost")
def get_ghost_chart(
    symbol: str,
    match_date: str = Query(..., description="The historical date to match (YYYY-MM-DD)"),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get historical 'Ghost' data for a stock around a specific date.
    Used for the 'Time Machine' overlay.
    """
    import datetime as dt
    
    try:
        target_date = dt.datetime.strptime(match_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    stock = crud_stock.get_stock_by_symbol(engine=engine, symbol=symbol)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    ghost_data = crud_stock.get_ghost_data(
        engine=engine,
        instrument_token=stock["instrument_token"],
        match_date=target_date
    )
    
    return {
        "symbol": symbol,
        "match_date": match_date,
        "ghost_data": ghost_data
    }

