# src/test_helpers.py
import threading
import time


def square(x): return x * x
def maybe_fail(x):
    if x == "boom":
        raise RuntimeError("fail")
    return x

def _sleep_then_echo(x, delay: float = 0.05):
    time.sleep(delay)
    return x


class _Counter:
    def __init__(self):
        self.n = 0
        self._lock = threading.Lock()

    def inc_and_sleep(self, x, delay: float = 0.05):
        with self._lock:
            self.n += 1
        time.sleep(delay)
        return x

    def value(self):
        with self._lock:
            return self.n
