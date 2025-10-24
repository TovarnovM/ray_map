# ray-map

Efficient Ray-powered `imap`/`map` with backpressure, checkpointing, per-item timeouts, safe exceptions, and async variants.

---

## ✨ Features

* ⚡ Parallel map/imap on **Ray** with **back-pressure** and **batching**
* 💾 **Checkpointing** + replay of already computed points
* ⏱️ **Per-item timeouts** (worker-side, via threads)
* 🧯 **Safe exceptions**: return `Exception` objects instead of failing the whole run
* 🔁 **Ordered** or **as-ready** modes, `(arg, res)` or plain `res`
* 🧰 **Async API**: `imap_async`, `map_async`
* 🧩 Single-file module `ray_map.py`, `src`-layout friendly

---

## 📦 Install

```bash
pip install ray-map
```

> Requires Python 3.9+ and Ray 2.6+.

---

## 🚀 Quickstart

```python
from ray_map import RayMap

def foo(x):
    if x == 3:
        raise ValueError("oops")
    return x * x

rmap = RayMap(foo, batch_size=8, max_pending=-1, checkpoint_path="res.pkl")

# 1) Stream (ordered). Exceptions raise by default (safe_exceptions=False)
for y in rmap.imap(range(10)):
    print(y)

# 2) Stream (as-ready), safe exceptions, return (arg, res_or_exc)
for arg, res in rmap.imap(
    range(10), keep_order=False, safe_exceptions=True, ret_args=True, timeout=2.0
):
    print(arg, "->", res)

# 3) Collect to list
lst = rmap.map(range(1000), timeout=2.0, safe_exceptions=True)
```

---

## ⚙️ ray.init / ray.shutdown — how to run Ray

`RayMap` supports **lazy initialization** of Ray: if Ray is **not** initialized when you start computing, `RayMap` will call `ray.init()` for you (local, no dashboard) with the runtime env you pass to `RayMap`.

You can also **manage Ray yourself**. Typical patterns:

### A) Manage Ray yourself (recommended in apps/tests/CI)

```python
import os, ray

# Make your codebase visible to workers. Usually the repository root.
ray.init(runtime_env={"working_dir": os.getcwd()}, include_dashboard=False)

# ... use RayMap normally

ray.shutdown()  # ← you are responsible for shutting down Ray explicitly
```

**Notes**

* If Ray is already initialized, `RayMap` will **not** call `ray.init()` again — it just prepares remote functions.
* Prefer to set `runtime_env` explicitly (see **working_dir** below).
* Call `ray.shutdown()` yourself when the program finishes or tests are done.

### B) Let RayMap do it lazily

If you don’t call `ray.init()` yourself, the first call to `imap`/`map` will:

* start a local Ray instance with `runtime_env` taken from `RayMap(..., runtime_env=...)` (if provided), otherwise a minimal default;
* auto-tune `max_pending` if set to `-1` (`CPU + 16` batches);
* no dashboard; logs suppressed by default.

You still need to shut Ray down yourself if you want a clean exit:

```python
import ray
ray.shutdown()
```

### What arguments can be passed to `ray.init()` via RayMap

`RayMap` forwards a subset of options to Ray:

* `address`: connect to a cluster, e.g. `"ray://host:port"` (requires `ray[default]`/`ray[client]`). If omitted, a local instance is started.
* `password`: `_redis_password` for legacy clusters using Redis authentication.
* `runtime_env`: the **runtime environment** for your job, see below.
* `remote_options`: forwarded to `.options(...)` of the remote function (e.g., `{"num_cpus": 0.5, "resources": {...}}`).

### 📂 About `working_dir` (runtime_env)

`working_dir` defines what source files are shipped to workers. That’s critical when your function `fn` is defined in your project code (or even inside `tests/`).

* **When you manage Ray yourself**: set it explicitly

  ```python
  ray.init(runtime_env={"working_dir": "."})  # or os.getcwd()
  ```
* **When RayMap initializes lazily**: it uses the `runtime_env` you passed to `RayMap`. If you didn’t pass any, use `RayMap(fn, runtime_env={"working_dir": "."})` to avoid import errors on workers.

> If your test functions live in `tests/`, either:
>
> * include `tests/` in the runtime env (e.g. via `py_modules: ["tests"]`), **or**
> * move helpers to `src/test_helpers.py` and import from there in tests.

### `ray.shutdown()`

`ray.shutdown()` **is not** called by `RayMap`. You should call it yourself when you want to cleanly stop Ray (end of script, end of test session, etc.). In pytest, use a `session`-scoped fixture that starts Ray once and calls `ray.shutdown()` in teardown.

---

## 🧭 API Reference

```python
RayMap.imap(iterable, *, timeout=None, safe_exceptions=False, keep_order=True, ret_args=False) -> iterator
RayMap.map(iterable,  *, timeout=None, safe_exceptions=False, keep_order=True, ret_args=False) -> list

# Async variants
RayMap.imap_async(...): async iterator
RayMap.map_async(...): list
```

* `timeout`: per-item timeout (seconds) on worker via `ThreadPoolExecutor`
* `safe_exceptions=True`: return exception objects (no crash)
* `keep_order=True`: preserve input order (1:1); `False` → yield as-ready
* `ret_args=True`: yield `(arg, res_or_exc)` instead of just `res_or_exc`

---

## 💾 Checkpointing

* Stores `(key, arg, result_or_exc)` to `checkpoint_path`.
* On restart, previously computed results are yielded first; then Ray resumes the rest.
* (Optional, future) flag to skip storing exceptions.

---

## 🧪 Examples

See `examples/` directory for quickstarts and CI-ready snippets (including pytest fixtures for `ray.init`/`ray.shutdown`).

---

## 🛠️ Ray configuration & environments (details)

* Local vs remote cluster; choosing `batch_size` / `max_pending`.
* Shipping code to workers via `runtime_env` (`working_dir`, `py_modules`).
* Error callback and tuple/dict arg calling conventions.

---

## 📐 Performance tips

* Keep `batch_size` modest (8–64). Too small → overhead; too big → latency/memory.
* Start with `max_pending=-1` and reduce if memory use spikes.
* For lower latency, consider `keep_order=False`.

---

## 🧪 Testing

Provide a pytest `conftest.py` that starts Ray once per session with proper `runtime_env` and shuts it down in teardown. See README “ray.init / ray.shutdown”.

---

## 📄 License

MIT — see `LICENSE`.
