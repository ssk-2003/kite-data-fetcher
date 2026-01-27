from __future__ import annotations
from dataclasses import dataclass
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from ml_engine.core.config import DATABASE_URL

@dataclass(frozen=True)
class Db:
    engine: Engine

def get_engine() -> Engine:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return create_engine(DATABASE_URL, pool_pre_ping=True)
