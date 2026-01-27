from __future__ import annotations

from kiteconnect import KiteConnect

from ml_engine.core.config import get_access_token, KITE_API_KEY, require_env


def get_kite() -> KiteConnect:
    if not KITE_API_KEY:
        require_env("KITE_API_KEY")
    kite = KiteConnect(api_key=KITE_API_KEY)
    token = get_access_token()
    if not token:
        require_env("KITE_ACCESS_TOKEN")
    kite.set_access_token(token)
    return kite
