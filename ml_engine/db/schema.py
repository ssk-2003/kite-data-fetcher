from sqlalchemy import text
from sqlalchemy.engine import Engine

def init_db(engine: Engine) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
    except Exception:
        pass

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS users (
                  id SERIAL PRIMARY KEY,
                  email TEXT UNIQUE NOT NULL,
                  hashed_password TEXT NOT NULL,
                  full_name TEXT,
                  is_active BOOLEAN DEFAULT TRUE,
                  approval_status TEXT DEFAULT 'pending' CHECK (approval_status IN ('pending', 'approved', 'rejected')),
                  queue_position INTEGER,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stock_master (
                  instrument_token BIGINT PRIMARY KEY,
                  tradingsymbol TEXT,
                  name TEXT,
                  exchange TEXT,
                  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  is_stable BOOLEAN DEFAULT FALSE
                );
                """
            )
        )
        try:
            conn.execute(text("ALTER TABLE stock_master DROP COLUMN IF EXISTS segment;"))
            conn.execute(text("ALTER TABLE stock_master DROP COLUMN IF EXISTS instrument_type;"))
            conn.execute(text("ALTER TABLE stock_master ADD COLUMN IF NOT EXISTS is_stable BOOLEAN DEFAULT FALSE;"))
        except Exception:
            pass
        
        try:
            conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS approval_status TEXT DEFAULT 'pending';"))
            conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS queue_position INTEGER;"))
            conn.execute(text("""
                UPDATE users 
                SET queue_position = subq.rn 
                FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM users WHERE queue_position IS NULL) subq 
                WHERE users.id = subq.id AND users.queue_position IS NULL;
            """))
        except Exception:
            pass
        
        # Create waitlist table for email-only signups
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS waitlist (
                    id SERIAL PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    full_name TEXT,
                    source TEXT DEFAULT 'early_access_modal',
                    queue_position INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    converted_to_user BOOLEAN DEFAULT FALSE,
                    converted_at TIMESTAMPTZ
                );
                """
            )
        )
        
        # Create subscription_plans table for pricing
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS subscription_plans (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    duration_months INTEGER NOT NULL,
                    original_price INTEGER NOT NULL,
                    founding_price INTEGER NOT NULL,
                    per_month_label TEXT,
                    savings_percent INTEGER DEFAULT 0,
                    features JSONB DEFAULT '[]',
                    badge TEXT,
                    badge_icon TEXT,
                    is_highlighted BOOLEAN DEFAULT FALSE,
                    display_order INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
        
        # Create user_subscriptions table to track user plans
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_subscriptions (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    plan_id INTEGER NOT NULL REFERENCES subscription_plans(id),
                    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'expired', 'cancelled')),
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ,
                    payment_id TEXT,
                    payment_amount INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )

        # Create paper trading tables
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS portfolios (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    balance DOUBLE PRECISION DEFAULT 1000000.0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT unique_user_portfolio UNIQUE (user_id)
                );
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    id SERIAL PRIMARY KEY,
                    portfolio_id INTEGER NOT NULL REFERENCES portfolios(id),
                    instrument_token BIGINT NOT NULL REFERENCES stock_master(instrument_token),
                    symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    avg_price DOUBLE PRECISION NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT unique_portfolio_token UNIQUE (portfolio_id, instrument_token)
                );
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    portfolio_id INTEGER NOT NULL REFERENCES portfolios(id),
                    instrument_token BIGINT NOT NULL REFERENCES stock_master(instrument_token),
                    symbol TEXT NOT NULL,
                    type TEXT NOT NULL CHECK (type IN ('BUY', 'SELL')),
                    quantity INTEGER NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
        
        # Create watchlists table
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS watchlists (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    instrument_token BIGINT NOT NULL,
                    symbol TEXT NOT NULL,
                    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT unique_user_watchlist UNIQUE (user_id, instrument_token)
                );
                """
            )
        )
        
        # Create notifications table
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS notifications (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    type TEXT NOT NULL CHECK (type IN ('alert', 'system', 'score_change')),
                    title TEXT NOT NULL,
                    message TEXT,
                    is_read BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
        
        # Create user_alert_preferences table
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_alert_preferences (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) UNIQUE,
                    email_alerts BOOLEAN DEFAULT TRUE,
                    push_alerts BOOLEAN DEFAULT TRUE,
                    score_threshold INTEGER DEFAULT 70,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stock_orders (
                    id SERIAL PRIMARY KEY,
                    portfolio_id INTEGER NOT NULL REFERENCES portfolios(id),
                    instrument_token BIGINT NOT NULL,
                    symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    action TEXT NOT NULL CHECK (action IN ('BUY', 'SELL')),
                    order_type TEXT NOT NULL CHECK (order_type IN ('MARKET', 'LIMIT', 'STOP')),
                    limit_price DOUBLE PRECISION,
                    status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'EXECUTED', 'CANCELLED', 'REJECTED')),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    executed_at TIMESTAMPTZ
                );
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS price_alerts (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    instrument_token BIGINT,
                    symbol TEXT NOT NULL,
                    target_price DOUBLE PRECISION NOT NULL,
                    condition TEXT NOT NULL CHECK (condition IN ('ABOVE', 'BELOW')),
                    is_active BOOLEAN DEFAULT TRUE,
                    triggered_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
        
        # Create learning_modules table
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS learning_modules (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    category TEXT NOT NULL,
                    read_time TEXT,
                    description TEXT,
                    level TEXT CHECK (level IN ('Beginner', 'Intermediate', 'Advanced')),
                    content TEXT,
                    display_order INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
        
        # Add index for user subscriptions lookup
        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_user_id 
                ON user_subscriptions(user_id);
            """))
        except Exception:
            pass
        
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stock_history (
                  instrument_token BIGINT NOT NULL,
                  ts TIMESTAMPTZ NOT NULL,
                  interval TEXT NOT NULL,
                  open DOUBLE PRECISION,
                  high DOUBLE PRECISION,
                  low DOUBLE PRECISION,
                  close DOUBLE PRECISION,
                  volume DOUBLE PRECISION,
                  oi DOUBLE PRECISION,
                  log_return DOUBLE PRECISION,
                  rsi_14 DOUBLE PRECISION,
                  ema_50_div DOUBLE PRECISION,
                  atr_14_norm DOUBLE PRECISION,
                  rvol DOUBLE PRECISION,
                  pattern_doji SMALLINT,
                  pattern_hammer SMALLINT,
                  pattern_engulfing SMALLINT,
                  pattern_morning_star SMALLINT,
                  pattern_shooting_star SMALLINT,
                  ema_200_div DOUBLE PRECISION,
                  target_3d SMALLINT,
                  news_sentiment DOUBLE PRECISION,
                  CONSTRAINT stock_history_pk PRIMARY KEY (instrument_token, ts, interval)
                );
                """
            )
        )
        feature_cols = [
            ("log_return", "DOUBLE PRECISION"),
            ("rsi_14", "DOUBLE PRECISION"),
            ("ema_50_div", "DOUBLE PRECISION"),
            ("atr_14_norm", "DOUBLE PRECISION"),
            ("rvol", "DOUBLE PRECISION"),
            ("pattern_doji", "SMALLINT"),
            ("pattern_hammer", "SMALLINT"),
            ("pattern_engulfing", "SMALLINT"),
            ("pattern_morning_star", "SMALLINT"),
            ("pattern_shooting_star", "SMALLINT"),
            ("ema_200_div", "DOUBLE PRECISION"),
            ("target_3d", "SMALLINT"),
            ("news_sentiment", "DOUBLE PRECISION")
        ]
        for col, col_type in feature_cols:
            try:
                conn.execute(text(f"ALTER TABLE stock_history ADD COLUMN IF NOT EXISTS {col} {col_type};"))
            except Exception:
                pass

        try:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_stock_history_lookup 
                ON stock_history(instrument_token, interval, ts DESC);
            """))
        except Exception:
            pass

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    SELECT create_hypertable('stock_history', 'ts', if_not_exists => TRUE);
                    """
                )
            )
    except Exception:
        pass
