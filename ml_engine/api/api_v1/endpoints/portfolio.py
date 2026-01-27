from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.engine import Engine
from ml_engine.api import deps
from ml_engine.analytics.portfolio import analyze_portfolio_risk

router = APIRouter()

class PortfolioItem(BaseModel):
    symbol: str
    qty: int
    avg_price: float = 0.0

class PortfolioAnalysisRequest(BaseModel):
    holdings: List[PortfolioItem]

@router.post("/analyze", response_model=dict)
def analyze_portfolio(
    *,
    engine: Engine = Depends(deps.get_db_engine),
    request: PortfolioAnalysisRequest
) -> Any:
    """
    Analyze portfolio for risk, correlation, and diversification.
    """
    if not request.holdings:
        raise HTTPException(status_code=400, detail="Portfolio cannot be empty")

    # Convert Pydantic models to dicts for the engine
    holdings_dict = [h.dict() for h in request.holdings]
    
    try:
        # Use the raw engine for pandas SQL reading inside the analytics module
        analysis = analyze_portfolio_risk(engine, holdings_dict)
        
        if "error" in analysis:
            raise HTTPException(status_code=400, detail=analysis["error"])
            
        return analysis
        
    except Exception as e:
        print(f"Portfolio Analysis Failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
