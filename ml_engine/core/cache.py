import time
import threading
from collections import OrderedDict
from typing import Any, Optional, Tuple

class TTLCache:
    def __init__(self, maxsize: int = 128, ttl: int = 60):
        """
        :param maxsize: Maximum number of items in the cache.
        :param ttl: Time to live in seconds.
        """
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            
            value, expiry = self._cache[key]
            if time.time() > expiry:
                del self._cache[key]
                return None
            
            self._cache.move_to_end(key)
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._cleanup()
            
            if key in self._cache:
                self._cache.move_to_end(key)
            
            self._cache[key] = (value, time.time() + self.ttl)
            
            if len(self._cache) > self.maxsize:
                self._cache.popitem(last=False)

    def _cleanup(self) -> None:
        """Removes expired items."""
        pass
        pass

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

stock_cache = TTLCache(maxsize=1000, ttl=300)
ticker_cache = TTLCache(maxsize=10, ttl=60)
