import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry(
    fn: Callable[[], T],
    *,
    retries: int = 5,
    base_delay_s: float = 0.5,
    max_delay_s: float = 10.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    attempt = 0
    while True:
        try:
            return fn()
        except retry_on:
            attempt += 1
            if attempt > retries:
                raise
            delay = min(max_delay_s, base_delay_s * (2 ** (attempt - 1)))
            time.sleep(delay)
