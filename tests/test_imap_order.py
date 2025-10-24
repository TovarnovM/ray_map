import os
from ray_map import RayMap
from test_helpers import square, maybe_fail

def test_imap_preserves_order():
    rmap = RayMap(square, batch_size=4, max_pending=2, checkpoint_path=None)
    data = list(range(50))
    out = list(rmap.imap(data, keep_order=True))
    assert out == [x*x for x in data]

def test_imap_as_ready_and_ret_args():
    rmap = RayMap(square, batch_size=4, max_pending=2, checkpoint_path=None)
    data = list(range(20))
    out = list(rmap.imap(data, keep_order=False, ret_args=True))
    # same multiset of results; order may differ
    assert sorted([r for _, r in out]) == [x*x for x in data]
