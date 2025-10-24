# src/test_helpers.py
def square(x): return x * x
def maybe_fail(x):
    if x == "boom":
        raise RuntimeError("fail")
    return x
