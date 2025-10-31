# tests/test_imap_stream.py
import asyncio
import itertools
import time
import threading
import pytest

from ray_map import RayMap
from test_helpers import square, maybe_fail  # square(x)=x*x; maybe_fail("boom")->RuntimeError
# см. src/test_helpers.py. :contentReference[oaicite:3]{index=3}

def test_imap_stream_as_ready_basic():
    """as-ready поток без сохранения порядка должен выдать те же результаты (мультимножество)."""
    rmap = RayMap(square, batch_size=4, max_pending=2, checkpoint_path=None)
    data = list(range(40))
    out = list(rmap.imap_stream(data, keep_order=False))
    assert sorted(out) == [x * x for x in data]  # порядок не гарантирован
    # метод определён в ray_map.py. :contentReference[oaicite:4]{index=4}

def test_imap_stream_keep_order_with_window():
    """При keep_order=True сохраняется порядок; ограничиваем окно, чтобы не росла память."""
    rmap = RayMap(square, batch_size=4, max_pending=2, checkpoint_path=None)
    data = list(range(30))
    out = list(rmap.imap_stream(data, keep_order=True, ordered_window=32))
    assert out == [x * x for x in data]  # порядок гарантирован
    # см. параметры keep_order / ordered_window в ray_map.py. :contentReference[oaicite:5]{index=5}

def test_imap_stream_limit_with_infinite_iter():
    """Бесконечный итератор обрезается по limit."""
    rmap = RayMap(square, batch_size=3, max_pending=2, checkpoint_path=None)
    gen = itertools.count(0)  # бесконечный поток
    out = list(rmap.imap_stream(gen, keep_order=False, limit=25))
    assert len(out) == 25
    assert sorted(out) == [x * x for x in range(25)]
    # см. параметр limit в имлементации. :contentReference[oaicite:6]{index=6}

def test_imap_stream_ret_args_and_safe_exceptions():
    """Безопасные исключения + возврат (arg, res_or_exc)"""
    rmap = RayMap(maybe_fail, batch_size=2, max_pending=2, checkpoint_path=None)
    items = ["ok", "boom", "ok2"]
    out = list(rmap.imap_stream(items, keep_order=False, ret_args=True, safe_exceptions=True))
    # найдём по аргументам
    d = {a: r for a, r in out}
    assert d["ok"] == "ok"
    assert isinstance(d["boom"], Exception)
    assert d["ok2"] == "ok2"
    # параметры ret_args / safe_exceptions описаны в API. :contentReference[oaicite:7]{index=7}

@pytest.mark.asyncio
async def test_imap_stream_async_basic():
    """Базовая проверка async-варианта: собираем все результаты."""
    rmap = RayMap(square, batch_size=4, max_pending=2, checkpoint_path=None)
    data = list(range(35))
    got = []
    async for item in rmap.imap_stream_async(data, keep_order=False):
        got.append(item)
    assert sorted(got) == [x * x for x in data]
    # см. imap_stream_async: async-обёртка через очередь. :contentReference[oaicite:8]{index=8}

@pytest.mark.asyncio
async def test_imap_stream_async_with_limit_infinite():
    """Async + бесконечный итератор с limit."""
    rmap = RayMap(square, batch_size=3, max_pending=2, checkpoint_path=None)
    gen = itertools.count()
    got = []
    async for item in rmap.imap_stream_async(gen, keep_order=False, limit=17):
        got.append(item)
    assert len(got) == 17
    assert sorted(got) == [x * x for x in range(17)]
    # параметр limit также поддерживается в async-обёртке. :contentReference[oaicite:9]{index=9}

def test_imap_stream_stop_callable():
    """Внешняя 'кнопка стоп' через callable."""
    rmap = RayMap(square, batch_size=3, max_pending=2, checkpoint_path=None)
    gen = itertools.count()
    counter = {"n": 0}
    def should_stop():
        return counter["n"] >= 21  # остановимся после 21 выдачи
    out = []
    for y in rmap.imap_stream(gen, keep_order=False, stop=should_stop):
        out.append(y)
        counter["n"] += 1
    assert len(out) == 21
    assert sorted(out) == [x * x for x in range(21)]
    # см. параметр stop (callable|Event) в сигнатуре. :contentReference[oaicite:10]{index=10}
