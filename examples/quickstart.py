from ray_map import RayMap

def foo(x):
    if x == 3:
        raise ValueError("oops")
    return x * x

rmap = RayMap(foo, batch_size=8, max_pending=-1, checkpoint_path="res.pkl")



# stream (as-ready), safe exceptions, return (arg, res_or_exc)
for arg, res in rmap.imap(range(10), keep_order=False, safe_exceptions=True, ret_args=True, timeout=2.0):
    print(arg, "->", res)

# list
lst = rmap.map(range(13), timeout=2.0, safe_exceptions=True)
print(lst)

# stream (ordered), exceptions raise by default
for y in rmap.imap(range(10)):
    if isinstance(y, Exception):
        y = str(y)
    print(y)