import os
import sys
import io

# Fix Windows console encoding for emojis
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import datetime as dt
import time
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

# Path Setup for Standalone Deployment
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# On Render, .env is in the KITE_WEBSITE root (BASE_DIR)
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Add BASE_DIR to sys.path so we can import ml_engine
sys.path.append(BASE_DIR)

from ml_engine.db.database import get_engine
from ml_engine.crud.crud_stock import upsert_stock_history
from ml_engine.core.kite_client import get_kite
from ml_engine.core.rate_limit import RateLimiter
from ml_engine.core.retry import retry

def get_last_dates(conn, tokens: list[int]) -> dict[int, dt.date]:
    """Fetch MAX(ts) for each token ONE BY ONE to avoid cloud database OutOfMemory errors."""
    if not tokens:
        return {}
    
    last_dates = {}
    for token in tokens:
        try:
            query = text("SELECT MAX(ts) FROM stock_history WHERE instrument_token = :token")
            result = conn.execute(query, {"token": token}).scalar()
            if result:
                last_dates[token] = result.date()
        except Exception:
            pass # Skip if error
            
    return last_dates

def fetch_and_upsert(kite, engine, token: int, symbol: str, start_date: dt.date, end_date: dt.date, rate: RateLimiter):
    """Fetch candles for a range and upsert to DB."""
    print(f"🔄 {symbol} ({token}): Fetching {start_date} -> {end_date} ...", end=" ", flush=True)
    
    all_candles = []
    current_start = start_date
    CHUNK_DAYS = 1800
    
    while current_start <= end_date:
        current_end = min(current_start + dt.timedelta(days=CHUNK_DAYS), end_date)
        def _call():
            rate.wait()
            return kite.historical_data(instrument_token=token, from_date=current_start, to_date=current_end, interval="day")

        try:
            chunk_candles = retry(_call)
            if chunk_candles:
                all_candles.extend(chunk_candles)
        except Exception as e:
            print(f"❌ Error at {current_start}: {e}")
            break
        current_start = current_end + dt.timedelta(days=1)

    if not all_candles:
        print("⚠️ No Data.")
        return 0

    wrote = upsert_stock_history(engine=engine, instrument_token=token, interval="day", candles=all_candles)
    print(f"✅ +{wrote} Rows.")
    return wrote

def daily_update():
    print("🚀 STARTING STANDALONE DAILY UPDATE...")
    print("=" * 60)
    
    engine = get_engine()
    kite = get_kite()
    rate = RateLimiter(max_per_second=3.0) 
    
    today = dt.date.today()
    default_start = dt.date(2015, 1, 1)
    
    total_updated = 0
    total_rows = 0
    
    with engine.connect() as conn:
        print("1. Fetching Master List...")
        master_df = pd.read_sql(text("SELECT instrument_token, tradingsymbol FROM stock_master"), conn)
        all_tokens = master_df['instrument_token'].tolist()
        token_map = dict(zip(master_df['instrument_token'], master_df['tradingsymbol']))
        print(f"Found {len(all_tokens)} stocks.")
        
        print("2. Checking Last Update Dates (1-by-1)...")
        last_dates = get_last_dates(conn, all_tokens)
        
    print("3. Fetching Updates...")
    for token in all_tokens:
        symbol = token_map.get(token, "UNKNOWN")
        last_date = last_dates.get(token)
        start_date = (last_date + dt.timedelta(days=1)) if last_date else default_start
        
        if start_date > today: continue
            
        rows = fetch_and_upsert(kite, engine, token, symbol, start_date, today, rate)
        if rows > 0:
            total_updated += 1
            total_rows += rows

    print("=" * 60)
    print(f"🏆 COMPLETE: {total_updated} stocks, {total_rows} rows.")

if __name__ == "__main__":
    try:
        daily_update()
    except Exception as e:
        print(f"\n❌ CRITICAL CRASH: {e}")
