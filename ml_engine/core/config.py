import os

from dotenv import load_dotenv


load_dotenv()


def env(key: str, default: str | None = None) -> str | None:
    value = os.getenv(key)
    if value is None:
        return default
    return value


def require_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or value == "":
        raise RuntimeError(f"{key} is not set")
    return value


KITE_API_KEY = env("KITE_API_KEY")
KITE_API_SECRET = env("KITE_API_SECRET")
KITE_REDIRECT_URL = env("KITE_REDIRECT_URL")
KITE_ACCESS_TOKEN = env("KITE_ACCESS_TOKEN")

DATABASE_URL = env("DATABASE_URL")
JWT_SECRET = env("JWT_SECRET", "supersecretkeychangeit")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days

# Google OAuth
GOOGLE_CLIENT_ID = env("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = env("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = env("GOOGLE_REDIRECT_URI", "http://localhost:8000/api/v1/auth/google/callback")
FRONTEND_URL = env("FRONTEND_URL", "http://localhost:3000")

