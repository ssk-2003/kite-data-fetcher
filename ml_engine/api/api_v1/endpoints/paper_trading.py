from fastapi import APIRouter, Depends, HTTPException, Body
from sqlalchemy.engine import Engine
from sqlalchemy import text
from ml_engine.api import deps
from ml_engine.crud import crud_stock
from ml_engine.schemas.portfolio import PortfolioResponse, TradeRequest, TradeResponse, TransactionResponse, PositionResponse, OrderResponse
from ml_engine.schemas.user import User
from typing import List

router = APIRouter()

@router.post("/trade", response_model=TradeResponse)
def execute_trade(
    trade: TradeRequest,
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Execute a BUY or SELL transaction.
    Supports MARKET and LIMIT orders.
    """
    if trade.quantity <= 0:
         raise HTTPException(status_code=400, detail="Quantity must be positive")

    # Handle LIMIT Orders
    if trade.order_type == 'LIMIT':
        if not trade.limit_price or trade.limit_price <= 0:
            raise HTTPException(status_code=400, detail="Limit price must be positive for LIMIT orders")
            
        with engine.begin() as conn:
            # Get Portfolio ID
            portfolio = conn.execute(
                text("SELECT id, balance FROM portfolios WHERE user_id = :uid"),
                {"uid": current_user["id"]}
            ).fetchone()
            
            if not portfolio:
                 portfolio = conn.execute(
                    text("INSERT INTO portfolios (user_id, balance) VALUES (:uid, 1000000.0) RETURNING id, balance"),
                    {"uid": current_user["id"]}
                ).fetchone()
            
            portfolio_id, balance = portfolio
            
            # Insert Pending Order
            # Note: For MVP we are not blocking funds/qty at creation, but will validte at execution.
            conn.execute(
                text("""
                    INSERT INTO stock_orders (portfolio_id, instrument_token, symbol, quantity, action, order_type, limit_price)
                    VALUES (:pid, :token, :sym, :qty, :act, :type, :price)
                """),
                {
                    "pid": portfolio_id,
                    "token": trade.instrument_token,
                    "sym": trade.symbol,
                    "qty": trade.quantity,
                    "act": trade.action,
                    "type": trade.order_type,
                    "price": trade.limit_price
                }
            )
            
            return TradeResponse(
                status="success",
                message=f"Limit {trade.action} order placed for {trade.quantity} shares at ₹{trade.limit_price}",
                transaction=None,
                new_balance=balance
            )

    ticker_data = crud_stock.get_ticker_data(engine, [trade.symbol])
    current_price = next((item["price"] for item in ticker_data if item["symbol"] == trade.symbol), None)
    
    if not current_price:
         # Try fetching stock explicitly
         stock = crud_stock.get_stock_by_symbol(engine, trade.symbol)
         if not stock or not stock.get("current_price"):
              raise HTTPException(status_code=404, detail="Stock or current price not found")
         current_price = stock["current_price"]
    
    total_cost = current_price * trade.quantity
    
    with engine.begin() as conn:
        # Get Portfolio
        portfolio = conn.execute(
            text("SELECT id, balance FROM portfolios WHERE user_id = :uid FOR UPDATE"),
            {"uid": current_user["id"]}
        ).fetchone()
        
        if not portfolio:
             portfolio = conn.execute(
                text("INSERT INTO portfolios (user_id, balance) VALUES (:uid, 1000000.0) RETURNING id, balance"),
                {"uid": current_user["id"]}
            ).fetchone()
            
        portfolio_id, balance = portfolio
        
        # Handle BUY
        if trade.action == 'BUY':
            if balance < total_cost:
                raise HTTPException(status_code=400, detail=f"Insufficient funds. cost: {total_cost}, balance: {balance}")
            
            # Update Balance
            new_balance = balance - total_cost
            conn.execute(
                text("UPDATE portfolios SET balance = :bal, updated_at = NOW() WHERE id = :pid"),
                {"bal": new_balance, "pid": portfolio_id}
            )
            
            # Upsert Position
            existing_pos = conn.execute(
                text("SELECT quantity, avg_price FROM positions WHERE portfolio_id = :pid AND instrument_token = :token"),
                {"pid": portfolio_id, "token": trade.instrument_token}
            ).fetchone()
            
            if existing_pos:
                old_qty, old_avg = existing_pos
                new_qty = old_qty + trade.quantity
                
                if new_qty == 0:
                    # Position closed completely
                    conn.execute(
                        text("DELETE FROM positions WHERE portfolio_id = :pid AND instrument_token = :token"),
                        {"pid": portfolio_id, "token": trade.instrument_token}
                    )
                else:
                    # Weighted Average Price (Only update if increasing position size, strictly speaking)
                    # Simplified: Update avg if direction matches (buying into long). If covering short, avg stays same (FIFO) or simplifies.
                    # For MVP: Always update avg on expansion, keep same on reduction?
                    # Case: Short -10 @ 100. Buy 5 @ 90. New -5. Realized profit. Avg remains 100.
                    # Case: Long 10 @ 100. Buy 5 @ 110. New 15. Avg becomes 103.33.
                    if old_qty > 0: # Adding to Long
                         new_avg = ((old_qty * old_avg) + total_cost) / new_qty
                    elif old_qty < 0: # Covering Short
                         new_avg = old_avg # Avg price of short entry doesn't change when covering
                    else: # Was 0 (unexpected here due to if/else but safe)
                         new_avg = current_price

                    conn.execute(
                        text("UPDATE positions SET quantity = :qty, avg_price = :avg, updated_at = NOW() WHERE portfolio_id = :pid AND instrument_token = :token"),
                        {"qty": new_qty, "avg": new_avg, "pid": portfolio_id, "token": trade.instrument_token}
                    )
            else:
                conn.execute(
                    text("INSERT INTO positions (portfolio_id, symbol, instrument_token, quantity, avg_price) VALUES (:pid, :sym, :token, :qty, :avg)"),
                    {"pid": portfolio_id, "sym": trade.symbol, "token": trade.instrument_token, "qty": trade.quantity, "avg": current_price}
                )

        # Handle SELL
        elif trade.action == 'SELL':
            # Note: SELL now allows Short selling (going negative)
            
            # Update Balance (Selling adds cash, even if shorting)
            new_balance = balance + total_cost
            conn.execute(
                text("UPDATE portfolios SET balance = :bal, updated_at = NOW() WHERE id = :pid"),
                {"bal": new_balance, "pid": portfolio_id}
            )

            existing_pos = conn.execute(
                text("SELECT quantity, avg_price FROM positions WHERE portfolio_id = :pid AND instrument_token = :token"),
                {"pid": portfolio_id, "token": trade.instrument_token}
            ).fetchone()
            
            old_qty = existing_pos[0] if existing_pos else 0
            old_avg = existing_pos[1] if existing_pos else 0
            
            new_qty = old_qty - trade.quantity
            
            # Solvency Check: Ensure Equity > 0 (Simplest Margin Requirement)
            # Equity = Cash + (Positions * CurrentPrice)
            # Since we just updated Balance, we calculate equity with new state.
            # We assume other positions are stable for this atomic check.
            # Ideally we check TOTAL portfolio equity, but for single-stock check:
            # Liability of this position = new_qty * current_price (negative if short)
            # If new_balance + (new_qty * current_price) < 0: Insufficient Margin.
            
            # Approximate Margin Check for Shorting:
            # You must have Cash > 50% of Short Value? or just > 0 Equity?
            # Let's enforce: account must remain solvent.
            # For this MVP: If new_qty < 0 (Short), ensure new_balance > abs(new_qty * current_price). (100% Margin)
            if new_qty < 0:
                short_value = abs(new_qty * current_price)
                if new_balance < short_value:
                     raise HTTPException(status_code=400, detail=f"Insufficient Margin for Short. Net Liquidity: {new_balance}, Required: {short_value}")

            if new_qty == 0:
                conn.execute(
                    text("DELETE FROM positions WHERE portfolio_id = :pid AND instrument_token = :token"),
                    {"pid": portfolio_id, "token": trade.instrument_token}
                )
            else:
                # Update Avg Price logic for Selling
                # If Selling Long: Avg stays same (reducing pos).
                # If Shorting (going negative): Avg becomes entry price of short.
                if old_qty <= 0: # Adding to Short (or starting Short)
                     # Weighted avg for short entry
                     # Net Short Value Old = abs(old_qty) * old_avg
                     # New Short Value = total_cost
                     # New Avg = (NetOld + New) / abs(new_qty)
                     net_val_old = abs(old_qty) * old_avg
                     new_avg = (net_val_old + total_cost) / abs(new_qty)
                else: # Reducing Long
                     new_avg = old_avg

                if existing_pos:
                    conn.execute(
                        text("UPDATE positions SET quantity = :qty, avg_price = :avg, updated_at = NOW() WHERE portfolio_id = :pid AND instrument_token = :token"),
                        {"qty": new_qty, "avg": new_avg, "pid": portfolio_id, "token": trade.instrument_token}
                    )
                else:
                    conn.execute(
                        text("INSERT INTO positions (portfolio_id, symbol, instrument_token, quantity, avg_price) VALUES (:pid, :sym, :token, :qty, :avg)"),
                        {"pid": portfolio_id, "sym": trade.symbol, "token": trade.instrument_token, "qty": new_qty, "avg": current_price}
                    )

        # Record Transaction
        tx_row = conn.execute(
            text("""
                INSERT INTO transactions (portfolio_id, symbol, instrument_token, type, quantity, price, amount) 
                VALUES (:pid, :sym, :token, :type, :qty, :price, :amt) 
                RETURNING id, timestamp
            """),
            {
                "pid": portfolio_id,
                "sym": trade.symbol,
                "token": trade.instrument_token,
                "type": trade.action,
                "qty": trade.quantity,
                "price": current_price,
                "amt": total_cost
            }
        ).fetchone()
        
        transaction = TransactionResponse(
            id=tx_row[0],
            instrument_token=trade.instrument_token,
            symbol=trade.symbol,
            type=trade.action,
            quantity=trade.quantity,
            price=current_price,
            amount=total_cost,
            timestamp=tx_row[1]
        )
        
        return TradeResponse(
            status="success",
            message=f"Successfully {trade.action}ed {trade.quantity} shares of {trade.symbol}",
            transaction=transaction,
            new_balance=new_balance
        )

@router.get("/portfolio", response_model=PortfolioResponse)
def get_portfolio(
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """Get user's portfolio, balance and positions."""
    with engine.begin() as conn:
        # Get Portfolio
        portfolio = conn.execute(
            text("SELECT id, balance FROM portfolios WHERE user_id = :uid"),
            {"uid": current_user["id"]}
        ).fetchone()
        
        if not portfolio:
            # Create if not exists
            portfolio = conn.execute(
                text("INSERT INTO portfolios (user_id, balance) VALUES (:uid, 1000000.0) RETURNING id, balance"),
                {"uid": current_user["id"]}
            ).fetchone()
            
        pid, balance = portfolio
        
        # Get Positions
        pos_rows = conn.execute(
            text("SELECT instrument_token, symbol, quantity, avg_price FROM positions WHERE portfolio_id = :pid AND quantity > 0"),
            {"pid": pid}
        ).fetchall()
        
        positions = []
        total_value = balance
        equity = 0.0
        
        # Get live prices for positions to calc PBS/Equity
        # We need a list of tokens/symbols
        if pos_rows:
            symbols = [r.symbol for r in pos_rows]
            ticker_data = crud_stock.get_ticker_data(engine, symbols)
            price_map = {item['symbol']: item['price'] for item in ticker_data}
            
            for r in pos_rows:
                current_price = price_map.get(r.symbol, r.avg_price) # Fallback to avg if live missing
                curr_val = current_price * r.quantity
                equity += curr_val
                
                # Calculate PnL
                pnl = curr_val - (r.avg_price * r.quantity)
                pnl_pct = (pnl / (r.avg_price * r.quantity)) * 100 if r.avg_price > 0 else 0
                
                positions.append(PositionResponse(
                    instrument_token=r.instrument_token,
                    symbol=r.symbol,
                    quantity=r.quantity,
                    avg_price=r.avg_price,
                    current_price=current_price,
                    current_value=curr_val,
                    pnl=pnl,
                    pnl_percent=pnl_pct
                ))
                
        total_value = balance + equity
        
        return PortfolioResponse(
            id=pid,
            balance=balance,
            total_value=total_value,
            equity=equity,
            day_change=0.0, # Placeholder for day change logic
            day_change_percent=0.0,
            positions=positions
        )

@router.get("/orders", response_model=List[OrderResponse])
def get_orders(
    status: str = 'PENDING',
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """Get orders by status (default: PENDING)."""
    with engine.begin() as conn:
        pid = conn.execute(text("SELECT id FROM portfolios WHERE user_id = :uid"), {"uid": current_user["id"]}).scalar()
        if not pid:
            return []
            
        rows = conn.execute(
            text("""
                SELECT id, symbol, action, order_type, quantity, limit_price, status, created_at 
                FROM stock_orders 
                WHERE portfolio_id = :pid AND status = :status
                ORDER BY created_at DESC
            """),
            {"pid": pid, "status": status}
        ).fetchall()
        
        return [
            OrderResponse(
                id=r.id, symbol=r.symbol, action=r.action, order_type=r.order_type,
                quantity=r.quantity, limit_price=r.limit_price, status=r.status, created_at=r.created_at
            ) for r in rows
        ]

@router.post("/orders/{order_id}/cancel")
def cancel_order(
    order_id: int,
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    """Cancel a pending order."""
    with engine.begin() as conn:
        pid = conn.execute(text("SELECT id FROM portfolios WHERE user_id = :uid"), {"uid": current_user["id"]}).scalar()
        if not pid:
            raise HTTPException(status_code=404, detail="Portfolio not found")
            
        result = conn.execute(
            text("UPDATE stock_orders SET status = 'CANCELLED' WHERE id = :oid AND portfolio_id = :pid AND status = 'PENDING'"),
            {"oid": order_id, "pid": pid}
        )
        
        if result.rowcount == 0:
             raise HTTPException(status_code=400, detail="Order not found or not pending")
             
    return {"status": "success", "message": "Order cancelled"}

@router.get("/history", response_model=List[TransactionResponse])
def get_history(
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    with engine.begin() as conn:
        # Get portfolio id
        pid = conn.execute(text("SELECT id FROM portfolios WHERE user_id = :uid"), {"uid": current_user["id"]}).scalar()
        if not pid:
            return []
            
        rows = conn.execute(
            text("SELECT id, symbol, instrument_token, type, quantity, price, amount, timestamp FROM transactions WHERE portfolio_id = :pid ORDER BY timestamp DESC"),
            {"pid": pid}
        ).fetchall()
        
        return [
            TransactionResponse(
                id=r[0], symbol=r[1], instrument_token=r[2], type=r[3], quantity=r[4], price=r[5], amount=r[6], timestamp=r[7]
            ) for r in rows
        ]

@router.post("/reset")
def reset_portfolio(
    current_user: User = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine),
):
    with engine.begin() as conn:
        pid = conn.execute(text("SELECT id FROM portfolios WHERE user_id = :uid"), {"uid": current_user["id"]}).scalar()
        if pid:
            conn.execute(text("DELETE FROM positions WHERE portfolio_id = :pid"), {"pid": pid})
            conn.execute(text("DELETE FROM transactions WHERE portfolio_id = :pid"), {"pid": pid})
            conn.execute(text("DELETE FROM stock_orders WHERE portfolio_id = :pid"), {"pid": pid})
            conn.execute(text("UPDATE portfolios SET balance = 1000000.0 WHERE id = :pid"), {"pid": pid})
            
    return {"status": "success", "message": "Portfolio reset to ₹10,00,000"}
