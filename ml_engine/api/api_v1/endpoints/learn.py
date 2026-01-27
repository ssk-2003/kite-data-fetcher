from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine
from sqlalchemy import text
from ml_engine.api import deps
from typing import List, Optional
from pydantic import BaseModel

router = APIRouter()

class LearningModule(BaseModel):
    id: int
    title: str
    category: str
    read_time: Optional[str]
    description: Optional[str]
    level: Optional[str]
    display_order: int

@router.get("/", response_model=List[LearningModule])
def get_learning_modules(
    engine: Engine = Depends(deps.get_db_engine),
):
    """
    Get all active learning modules.
    """
    with engine.begin() as conn:
        modules = conn.execute(
            text("""
                SELECT id, title, category, read_time, description, level, display_order 
                FROM learning_modules 
                WHERE is_active = TRUE 
                ORDER BY display_order ASC, id ASC
            """)
        ).fetchall()
        
        return [LearningModule(
            id=m.id,
            title=m.title,
            category=m.category,
            read_time=m.read_time,
            description=m.description,
            level=m.level,
            display_order=m.display_order
        ) for m in modules]
