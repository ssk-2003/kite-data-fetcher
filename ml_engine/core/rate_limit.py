import threading
import time


class RateLimiter:
    def __init__(self, max_per_second: float):
        if max_per_second <= 0:
            raise ValueError("max_per_second must be > 0")
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_for = self._min_interval - (now - self._last)
            if wait_for > 0:
                time.sleep(wait_for)
            self._last = time.monotonic()
