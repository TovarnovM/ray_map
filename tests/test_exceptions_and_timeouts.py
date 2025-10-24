import time
import pytest
from ray_map import RayMap
from test_helpers import square, maybe_fail


def slow(x):
    time.sleep(0.2)
    return x

def test_safe_exceptions_return_objects():
    rmap = RayMap(maybe_fail, batch_size=2, max_pending=2, checkpoint_path=None)
    items = ["ok", "boom", "ok2"]
    res = list(rmap.imap(items, safe_exceptions=True))
    assert res[0] == "ok"
    assert isinstance(res[1], Exception)
    assert res[2] == "ok2"

def test_timeout_yields_exception_object():
    rmap = RayMap(slow, batch_size=2, max_pending=1, checkpoint_path=None)
    res = list(rmap.imap([1, 2], timeout=0.05, safe_exceptions=True))
    assert all(isinstance(x, Exception) for x in res)
