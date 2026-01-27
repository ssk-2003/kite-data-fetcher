from __future__ import annotations

from kiteconnect import KiteConnect

from ml_engine.core.config import KITE_ACCESS_TOKEN, KITE_API_KEY, require_env


def get_kite() -> KiteConnect:
    if not KITE_API_KEY:
        require_env("KITE_API_KEY")
    kite = KiteConnect(api_key=KITE_API_KEY)
    if not KITE_ACCESS_TOKEN:
        require_env("KITE_ACCESS_TOKEN")
    kite.set_access_token(KITE_ACCESS_TOKEN)
    return kite
