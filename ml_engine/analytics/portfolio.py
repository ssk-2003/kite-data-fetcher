from __future__ import annotations
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy.engine import Engine
from ml_engine.crud.crud_stock import get_stock_history

def analyze_portfolio_risk(engine: Engine, holdings: list[dict]) -> dict:
    """
    Analyzes portfolio risk, correlation, and diversification.
    
    Args:
        engine: DB engine
        holdings: List of dicts with 'symbol', 'qty', 'avg_price'
    
    Returns:
        dict: Health report containing score, risks, and suggestions.
    """
    if not holdings:
        return {"error": "Empty portfolio"}

    symbols = [h['symbol'].upper() for h in holdings]
    weights = {} # value weights
    
    # 1. Fetch History Data
    # We need aligned dates for correlation
    data_frames = []
    
    for holding in holdings:
        symbol = holding['symbol'].upper()
        # Fetch last 250 days (1 year)
        # We need to get instrument_token first - implying we might need a helper or use search
        # For optimization, we'll assume we can look up token from master
        # But crud functions take token. Let's do a quick lookup query here or use existing
        pass 

    # Since we need efficient lookup, let's write a dedicated optimized query here
    # to fetch history for MULTIPLE symbols at once (Pivot logic)
    
    from sqlalchemy import text
    
    clean_symbols = [s.upper() for s in symbols]
    bind_clause = ", ".join([f":s{i}" for i in range(len(clean_symbols))])
    bind_params = {f"s{i}": s for i, s in enumerate(clean_symbols)}
    
    sql_tokens = text(f"""
        SELECT instrument_token, tradingsymbol 
        FROM stock_master 
        WHERE UPPER(tradingsymbol) IN ({bind_clause})
    """)
    
    token_map = {}
    with engine.connect() as conn:
        res = conn.execute(sql_tokens, bind_params).fetchall()
        for r in res:
            token_map[r.tradingsymbol] = r.instrument_token

    # Fetch history for found tokens
    # Using a single query to get all history for these tokens
    if not token_map:
         return {"error": "Stocks not found in database"}
         
    tokens = list(token_map.values())
    token_str = ",".join(map(str, tokens))
    
    sql_history = text(f"""
        SELECT instrument_token, ts, close
        FROM stock_history
        WHERE instrument_token = ANY(ARRAY[{token_str}]::bigint[])
          AND interval = 'day'
          AND ts >= NOW() - INTERVAL '365 days'
        ORDER BY ts ASC
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(sql_history, conn)
        
    if df.empty:
        return {"error": "Insufficient data"}
    
    # Map token back to symbol for readability
    inv_token_map = {v: k for k, v in token_map.items()}
    df['symbol'] = df['instrument_token'].map(inv_token_map)
    
    # Pivot: Index=Date, Columns=Symbol, Values=Close
    pivot_df = df.pivot_table(index='ts', columns='symbol', values='close')
    pivot_df = pivot_df.ffill().bfill() # Handle missing data
    
    # Calculate Daily Returns
    returns_df = pivot_df.pct_change().dropna()
    
    if returns_df.empty:
        return {"error": "Not enough overlapping data points"}

    # --- METRICS CALCULATIONS ---
    
    # 1. Correlation Matrix
    corr_matrix = returns_df.corr().round(2)
    
    # 2. Portfolio Volatility (simplified equal weight for risk view, or value weighted)
    # Let's calculate individual volatilities
    volatilities = returns_df.std() * np.sqrt(252) * 100 # Annualized %
    
    # 3. Diversification Score
    # avg correlation of the portfolio
    # Extract upper triangle of correlation matrix without diagonal
    upper_tri = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
    avg_corr = upper_tri.stack().mean()
    
    if np.isnan(avg_corr):
        avg_corr = 1.0 # Single stock case
        
    diversification_score = max(0, min(100, (1 - avg_corr) * 100))
    
    # 4. Generate Recommendations
    recommendations = []
    risks = []
    
    # Check for High Correlation pairs
    high_corr_pairs = []
    for col in upper_tri.columns:
        for idx in upper_tri.index:
            val = upper_tri.loc[idx, col]
            if val > 0.8:
                high_corr_pairs.append(f"{idx} & {col}")
                
    if high_corr_pairs:
        risks.append(f"High Correlation overlap detected in: {', '.join(high_corr_pairs[:3])}.")
        recommendations.append("Consider replacing one asset in highly correlated pairs with a different sector (e.g. Pharma, IT).")
        
    if avg_corr > 0.7:
        risks.append("Portfolio moves largely in unison (Low Diversification).")
        
    # Check for extreme volatility
    high_vol_stocks = volatilities[volatilities > 50].index.tolist()
    if high_vol_stocks:
        risks.append(f"High Volatility detected in: {', '.join(high_vol_stocks)}.")
        
    # Overall Health Score (Heuristic)
    # Higher Diversification + Lower Volatility (penalized above 30%) = Better
    # Keeping it simple: Diversification is key "Doctor" metric
    
    health_score = int(diversification_score)
    
    # Color coding
    status = "Healthy"
    if health_score < 40:
        status = "Critical"
    elif health_score < 70:
        status = "Fair"
        
    return {
        "health_score": health_score,
        "status": status,
        "metrics": {
            "avg_correlation": round(avg_corr, 2),
            "annualized_volatility_avg": round(volatilities.mean(), 2)
        },
        "correlation_matrix": corr_matrix.to_dict(), # For heatmap
        "volatilities": volatilities.to_dict(),
        "risks": risks,
        "recommendations": recommendations,
        "analyzed_symbols": list(pivot_df.columns)
    }
