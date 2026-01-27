from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from ml_engine.api.api_v1.api import api_router
from ml_engine.api import deps
from ml_engine.db.database import get_engine
from ml_engine.db.schema import init_db

app = FastAPI(title="OMRE API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api/v1")

@app.on_event("startup")
def on_startup():
    engine = get_engine()
    init_db(engine)

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "healthy", "msg": "OMRE Stocks API is Live 🚀"}

@app.get("/api/v1/users/me")
def read_user_me(current_user: dict = Depends(deps.get_current_user)):
    """
    Fetch the currently authenticated user's profile.
    """
    return current_user
