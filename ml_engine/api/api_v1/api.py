from fastapi import APIRouter
from ml_engine.api.api_v1.endpoints import auth, stocks, waitlist, subscriptions, paper_trading, watchlist_items, users, notifications, alerts, leaderboard, learn, portfolio

api_router = APIRouter()
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(notifications.router, prefix="/notifications", tags=["notifications"])
api_router.include_router(stocks.router, prefix="/stocks", tags=["stocks"])
api_router.include_router(waitlist.router, prefix="/waitlist", tags=["waitlist"])
api_router.include_router(subscriptions.router, prefix="/subscriptions", tags=["subscriptions"])
api_router.include_router(paper_trading.router, prefix="/paper-trading", tags=["paper-trading"])
api_router.include_router(watchlist_items.router, prefix="/watchlist", tags=["watchlist"])
api_router.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
api_router.include_router(leaderboard.router, prefix="/leaderboard", tags=["leaderboard"])
api_router.include_router(learn.router, prefix="/learn", tags=["learn"])
api_router.include_router(portfolio.router, prefix="/portfolio", tags=["portfolio"])