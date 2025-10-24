# ray_map.py
"""
RayMap — простой и эффективный класс для параллельных map/imap на Ray.

Ключевые возможности
- Ленивая инициализация Ray (подключение/старт — при первом расчёте).
- Back-pressure по числу одновременно запущенных батчей (max_pending).
- Батчирование входных аргументов (batch_size).
- Чекпоинтинг: при перезапуске сначала «реплей» готовых результатов из файла, затем досчёт.
- Таймаут на каждую точку (per-item), реализован потоками внутри worker’а.
- Безопасная обработка исключений: можно вернуть Exception как результат, не роняя весь расчёт.
- Единая core-логика (_core_iter), а API — тонкие обёртки:
    * imap(..., timeout, safe_exceptions, keep_order, ret_args)
    * map(...) = list(imap(...))
    * imap_async(...), map_async(...)
- Совместимость: submit()/asubmit() и collect()/acollect() оставлены.

Примеры
-------
from ray_map import RayMap

def foo(x):
    if x == 3:
        raise ValueError("oops")
    return x * x

rmap = RayMap(foo, batch_size=8, max_pending=-1, checkpoint_path="res.pkl", checkpoint_every=50)

# 1) Стримом по порядку, как multiprocessing.imap
for res in rmap.imap(range(1000)):
    ...

# 2) Без падений, по готовности, вернуть (arg, res|exc)
for arg, res in rmap.imap(range(1000), safe_exceptions=True, keep_order=False, ret_args=True, timeout=2.0):
    ...

# 3) Списком
lst = rmap.map(range(1000), timeout=2.0, safe_exceptions=True)

# 4) Async-стрим
import asyncio
async def main():
    async for res in rmap.imap_async(range(1000), keep_order=True):
        ...
asyncio.run(main())
"""

from __future__ import annotations

import os
import pickle
import hashlib
import cloudpickle
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, AsyncIterator, Callable, Sequence, List, Dict, Tuple, Optional

import ray


# ───────────────────────── helpers ─────────────────────────

def _arg_key(arg: Any) -> str:
    """Стабильный ключ по аргументу (для чекпоинта/дедупа)."""
    try:
        buf = cloudpickle.dumps(arg)
    except Exception:
        buf = repr(arg).encode("utf-8")
    return hashlib.sha1(buf).hexdigest()


def _call_user_fn(fn: Callable, x: Any) -> Any:
    """Вызвать fn на элементе x: tuple → *args, dict → **kwargs, иначе → x."""
    if isinstance(x, tuple):
        return fn(*x)
    if isinstance(x, dict):
        return fn(**x)
    return fn(x)


def _chunks(it: Iterable[Any], size: int) -> Iterator[List[Any]]:
    buf: List[Any] = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


# ───────────────────────── Ray remotes ─────────────────────────

@ray.remote
def _exec_batch_safe(fn_bytes: bytes, args_list: Sequence[Any], timeout_s: Optional[float]) -> List[Any]:
    """
    Безопасный батч: каждый элемент считается в отдельном потоке с per-item timeout.
    Возвращает список значений ИЛИ объектов Exception в исходном порядке.
    """
    fn = cloudpickle.loads(fn_bytes)
    out: List[Any] = []
    max_workers = min(32, max(1, len(args_list)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_call_user_fn, fn, x) for x in args_list]
        for fut in futs:
            try:
                out.append(fut.result(timeout=None if timeout_s is None else timeout_s))
            except Exception as e:
                out.append(e)  # Возвращаем исключение как результат
    return out


@dataclass
class _Pending:
    ref: ray.ObjectRef
    idxs: List[int]
    args: List[Any]


# ───────────────────────── Iterators for backwards-compat submit ─────────────────────────

class RayMapIterator:
    """Итератор с поддержкой len() (для tqdm), отдаёт (arg, res)."""

    def __init__(self, gen: Iterator[Tuple[Any, Any]], total: Optional[int]):
        self._gen = gen
        self._total = total

    def __iter__(self) -> Iterator[Tuple[Any, Any]]:
        return self._gen

    def __len__(self) -> int:
        if self._total is None:
            raise TypeError("length unknown")
        return self._total


# ───────────────────────── Core class ─────────────────────────

class RayMap:
    def __init__(
        self,
        fn: Callable,
        *,
        batch_size: int = 1,
        max_pending: int = -1,            # -1 → авто-настройка: CPU + 16
        checkpoint_path: Optional[str] = None,
        checkpoint_every: int = 100,
        address: Optional[str] = None,    # "ray://host:port" или None → локальный
        password: Optional[str] = None,
        runtime_env: Optional[dict] = None,
        remote_options: Optional[dict] = None,
        on_error: Optional[Callable[[Any, str], None]] = None,
        init_if_needed: bool = False,     # оставлено для совместимости; по умолчанию лениво
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be >= 1")
        self.fn = fn
        self.batch_size = int(batch_size)
        self.max_pending = int(max_pending)
        self.checkpoint_path = checkpoint_path
        self.checkpoint_every = max(1, int(checkpoint_every))
        self._on_error = on_error

        # Ray init — лениво
        self._ray_initted = False
        self._ray_address = address
        self._ray_password = password
        self._ray_runtime_env = runtime_env or {}
        self._remote_options = remote_options or {}
        self._fn_ref: Optional[ray.ObjectRef] = None
        self._remote_safe: Optional[Callable] = None

        # checkpoint state
        self._completed_keys: set[str] = set()
        self._results_for_ckpt: List[Tuple[str, Any, Any]] = []  # (key, arg, res_or_exc)
        if self.checkpoint_path and os.path.exists(self.checkpoint_path):
            try:
                with open(self.checkpoint_path, "rb") as f:
                    saved: List[Tuple[str, Any, Any]] = pickle.load(f)
                self._results_for_ckpt = saved
                self._completed_keys = {k for (k, _a, _r) in saved}
            except Exception:
                self._results_for_ckpt = []
                self._completed_keys = set()
        # быстрый доступ: key -> (arg, res_or_exc)
        self._ckpt_by_key: Dict[str, Tuple[Any, Any]] = {k: (a, r) for (k, a, r) in self._results_for_ckpt}

        if init_if_needed and (address or not ray.is_initialized()):
            self._ensure_ray()

    # ───────────── internals ─────────────

    def _ensure_ray(self) -> None:
        if ray.is_initialized():
            # не вызываем ray.init повторно; только готовим ссылки
            if not self._ray_initted:
                self._ray_initted = True
                if self._fn_ref is None:
                    self._fn_ref = ray.put(cloudpickle.dumps(self.fn))
                if self._remote_safe is None:
                    self._remote_safe = _exec_batch_safe.options(**self._remote_options).remote
                if self.max_pending == -1:
                    cpu = int(ray.available_resources().get("CPU", 1) or 1)
                    self.max_pending = max(1, cpu + 16)
            return
        if self._ray_initted and ray.is_initialized():
            if self.max_pending == -1:
                try:
                    cpu = int(ray.available_resources().get("CPU", 1))
                except Exception:
                    cpu = 1
                self.max_pending = max(1, cpu + 16)
            return

        init_kw: Dict[str, Any] = {}
        if self._ray_runtime_env:
            init_kw["runtime_env"] = self._ray_runtime_env
        try:
            if self._ray_address:
                init_kw["address"] = self._ray_address
                if self._ray_password:
                    init_kw["_redis_password"] = self._ray_password
                ray.init(**init_kw)
            else:
                # локальный старт
                ray.init(**init_kw)
        except Exception:
            # фоллбек с адреса на локальный
            if "address" in init_kw:
                init_kw.pop("address", None)
                init_kw.pop("_redis_password", None)
                ray.init(**init_kw)
            else:
                raise
        self._ray_initted = True
        self._fn_ref = ray.put(cloudpickle.dumps(self.fn))
        self._remote_safe = _exec_batch_safe.options(**self._remote_options).remote

        if self.max_pending == -1:
            try:
                cpu = int(ray.available_resources().get("CPU", 1))
            except Exception:
                cpu = 1
            self.max_pending = max(1, cpu + 16)

    def _save_ckpt_maybe(self) -> None:
        if not self.checkpoint_path:
            return
        if len(self._results_for_ckpt) % self.checkpoint_every != 0:
            return
        tmp = self.checkpoint_path + ".tmp"
        with open(tmp, "wb") as f:
            pickle.dump(self._results_for_ckpt, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, self.checkpoint_path)

    def _flush_ckpt(self) -> None:
        if self.checkpoint_path and self._results_for_ckpt:
            tmp = self.checkpoint_path + ".tmp"
            with open(tmp, "wb") as f:
                pickle.dump(self._results_for_ckpt, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, self.checkpoint_path)

    # ───────────── unified core generator ─────────────

    def _core_iter(self, iterable: Iterable[Any], *, timeout: Optional[float]) -> Iterator[Tuple[int, Any, Any]]:
        """
        Единая «core»-логика:
        1) Реплей из чекпоинта: для каждого arg сначала проверяем чекпоинт и yield (idx, arg, res/Exception).
        2) Для оставшихся элементов — запуск батчей в Ray через _remote_safe (per-item timeout, возврат Exception).
        3) По мере готовности валидируем, обновляем чекпоинт и yield (idx, arg, res_or_exc).
        """
        # Сначала один проход: отдаём всё, что уже посчитано; остальные собираем в todo
        todo: List[Tuple[int, Any]] = []
        for idx, arg in enumerate(iterable):
            k = _arg_key(arg)
            cached = self._ckpt_by_key.get(k)
            if cached is not None:
                # уже посчитано — выдаём сразу
                yield (idx, arg, cached[1])
            else:
                todo.append((idx, arg))

        if not todo:
            return

        # Досчёт оставшегося
        self._ensure_ray()
        assert self._remote_safe is not None and self._fn_ref is not None

        pos = 0
        pending: List[_Pending] = []

        def submit_more() -> None:
            nonlocal pos
            while len(pending) < self.max_pending and pos < len(todo):
                end = min(pos + self.batch_size, len(todo))
                batch = todo[pos:end]
                pos = end
                idxs = [i for (i, _a) in batch]
                args = [a for (_i, a) in batch]
                ref = self._remote_safe(self._fn_ref, args, timeout)
                pending.append(_Pending(ref=ref, idxs=idxs, args=args))

        submit_more()

        while pending:
            ready_refs, _ = ray.wait([p.ref for p in pending], num_returns=1)
            ready_ref = ready_refs[0]
            j = next(i for i, p in enumerate(pending) if p.ref == ready_ref)
            pend = pending.pop(j)
            try:
                results: List[Any] = ray.get(ready_ref)  # список значений или Exception
            except Exception as e:
                results = [e] * len(pend.args)

            for idx, arg, res in zip(pend.idxs, pend.args, results):
                # чекпоинт
                k = _arg_key(arg)
                self._completed_keys.add(k)
                self._results_for_ckpt.append((k, arg, res))
                self._ckpt_by_key[k] = (arg, res)
                self._save_ckpt_maybe()

                if isinstance(res, Exception) and self._on_error:
                    try:
                        self._on_error(arg, repr(res))
                    except Exception:
                        pass

                yield (idx, arg, res)

            submit_more()

        self._flush_ckpt()

    # ───────────── public API: imap/map и async-варианты ─────────────

    def imap(
        self,
        iterable: Iterable[Any],
        *,
        timeout: Optional[float] = None,
        safe_exceptions: bool = False,
        keep_order: bool = True,
        ret_args: bool = False,
    ) -> Iterator[Any]:
        """
        Стриминговый imap.
        - safe_exceptions: True → исключения возвращаются как значения; False → поднимаем при чтении такого элемента
        - keep_order: True → выдаём в порядке входа; False → в порядке готовности
        - ret_args: True → yield (arg, res_or_exc), иначе только res_or_exc
        - timeout: per-item таймаут на воркере
        """
        if keep_order:
            # Буфер по индексу
            next_idx = 0
            buf: Dict[int, Tuple[Any, Any]] = {}
            for idx, arg, res in self._core_iter(iterable, timeout=timeout):
                buf[idx] = (arg, res)
                while next_idx in buf:
                    a, r = buf.pop(next_idx)
                    if not safe_exceptions and isinstance(r, Exception):
                        raise r
                    yield (a, r) if ret_args else r
                    next_idx += 1
        else:
            # В порядке готовности
            for _idx, arg, res in self._core_iter(iterable, timeout=timeout):
                if not safe_exceptions and isinstance(res, Exception):
                    raise res
                yield (arg, res) if ret_args else res

    def map(
        self,
        iterable: Iterable[Any],
        *,
        timeout: Optional[float] = None,
        safe_exceptions: bool = False,
        keep_order: bool = True,
        ret_args: bool = False,
    ) -> List[Any]:
        """Обычный map: просто list(imap(...))."""
        return list(
            self.imap(
                iterable,
                timeout=timeout,
                safe_exceptions=safe_exceptions,
                keep_order=keep_order,
                ret_args=ret_args,
            )
        )

    async def imap_async(
        self,
        iterable: Iterable[Any],
        *,
        timeout: Optional[float] = None,
        safe_exceptions: bool = False,
        keep_order: bool = True,
        ret_args: bool = False,
        queue_maxsize: int = 1000,
    ) -> AsyncIterator[Any]:
        """
        Async-версия imap: запускает синхронный imap в отдельном потоке
        и стримит результаты через asyncio.Queue (event loop не блокируется).
        """
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue(queue_maxsize)
        sentinel = object()

        def _producer() -> None:
            try:
                for item in self.imap(
                    iterable,
                    timeout=timeout,
                    safe_exceptions=safe_exceptions,
                    keep_order=keep_order,
                    ret_args=ret_args,
                ):
                    fut = asyncio.run_coroutine_threadsafe(q.put(item), loop)
                    try:
                        fut.result()
                    except Exception:
                        break
            finally:
                asyncio.run_coroutine_threadsafe(q.put(sentinel), loop).result()

        threading.Thread(target=_producer, daemon=True).start()

        while True:
            item = await q.get()
            if item is sentinel:
                break
            yield item

    async def map_async(
        self,
        iterable: Iterable[Any],
        *,
        timeout: Optional[float] = None,
        safe_exceptions: bool = False,
        keep_order: bool = True,
        ret_args: bool = False,
    ) -> List[Any]:
        """Async-версия map: собирает list из imap_async."""
        out: List[Any] = []
        async for item in self.imap_async(
            iterable,
            timeout=timeout,
            safe_exceptions=safe_exceptions,
            keep_order=keep_order,
            ret_args=ret_args,
        ):
            out.append(item)
        return out

    # ───────────── Back-compat: submit / asubmit / collect / acollect ─────────────

    def submit(self, iterable: Iterable[Any]) -> RayMapIterator:
        """
        Совместимость со старым API: отдаёт (arg, res_or_exc) по мере готовности.
        tqdm сможет показать total, если iterable имеет __len__.
        """
        try:
            total = len(iterable)  # type: ignore[arg-type]
        except Exception:
            total = None

        def gen() -> Iterator[Tuple[Any, Any]]:
            for item in self.imap(iterable, safe_exceptions=True, keep_order=False, ret_args=True):
                yield item  # (arg, res_or_exc)

        return RayMapIterator(gen(), total)

    def submit_safe(self, iterable: Iterable[Any], *, timeout: Optional[float] = None) -> RayMapIterator:
        """Совместимость: как submit, но с per-item timeout."""
        try:
            total = len(iterable)  # type: ignore[arg-type]
        except Exception:
            total = None

        def gen() -> Iterator[Tuple[Any, Any]]:
            for item in self.imap(
                iterable, timeout=timeout, safe_exceptions=True, keep_order=False, ret_args=True
            ):
                yield item

        return RayMapIterator(gen(), total)

    @classmethod
    def collect(
        cls,
        fn: Callable,
        iterable: Iterable[Any],
        *,
        batch_size: int = 1,
        **kw,
    ) -> List[Tuple[Any, Any]]:
        """Совместимость: собирать (arg, res_or_exc) списком."""
        r = cls(fn, batch_size=batch_size, **kw)
        return list(r.imap(iterable, safe_exceptions=True, keep_order=True, ret_args=True))

    @classmethod
    async def acollect(
        cls,
        fn: Callable,
        iterable: Iterable[Any],
        *,
        batch_size: int = 1,
        **kw,
    ) -> List[Tuple[Any, Any]]:
        """Совместимость: async-сбор (arg, res_or_exc) списком."""
        r = cls(fn, batch_size=batch_size, **kw)
        out: List[Tuple[Any, Any]] = []
        async for item in r.imap_async(iterable, safe_exceptions=True, keep_order=True, ret_args=True):
            out.append(item)
        return out
