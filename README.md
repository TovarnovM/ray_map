# ray-map

Efficient Ray-powered `imap`/`map` with backpressure, checkpointing, per-item timeouts, safe exceptions, and async variants.

## Features

- ⚡ Parallel map/imap on [Ray] with **back-pressure** and **batching**
- 💾 **Checkpointing** + replay of already computed points
- ⏱️ **Per-item timeouts** (worker-side, via threads)  
- 🧯 **Safe exceptions**: return `Exception` objects instead of failing the whole run
- 🔁 **Ordered** or **as-ready** modes, `(arg, res)` or plain `res`
- 🧰 **Async** API: `imap_async`, `map_async`
- 🧩 Works with single file module `ray_map.py`, src-layout, no extra boilerplate

## Install

```bash
pip install ray-map
```

# Quickstart

```python

from ray_map import RayMap

def foo(x):
    if x == 3:
        raise ValueError("oops")
    return x * x

rmap = RayMap(foo, batch_size=8, max_pending=-1, checkpoint_path="res.pkl")

# stream (ordered), exceptions raise by default
for y in rmap.imap(range(10)):
    print(y)

# stream (as-ready), safe exceptions, return (arg, res_or_exc)
for arg, res in rmap.imap(range(10), keep_order=False, safe_exceptions=True, ret_args=True, timeout=2.0):
    print(arg, "->", res)

# list
lst = rmap.map(range(1000), timeout=2.0, safe_exceptions=True)

```


# API essentials

```python
RayMap.imap(iterable, *, timeout=None, safe_exceptions=False, keep_order=True, ret_args=False) -> iterator
RayMap.map(iterable,  *, timeout=None, safe_exceptions=False, keep_order=True, ret_args=False) -> list

# Async variants
RayMap.imap_async(...): async iterator
RayMap.map_async(...): list
```
- `timeout`: per-item timeout (seconds) on worker via `ThreadPoolExecutor`
- `safe_exceptions=True`: return exception objects (no crash)
- `keep_order=True`: preserve input order (1:1); `False` → yield as-ready
- `ret_args=True`: yield `(arg, res_or_exc)` instead of just `res_or_exc`

# Checkpointing
- Stores `(key, arg, result_or_exc)` to `checkpoint_path`.
- On restart, previously computed results are yielded first; then Ray resumes the rest.
- (Optional) You can add a flag to skip storing exceptions in a future version