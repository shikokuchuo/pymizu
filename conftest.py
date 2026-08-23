# Present so pytest prepends the repo root to sys.path: the task callables
# import as `tests.helpers`, and the spawned worker processes (whose
# sys.path[0] is the repo root) resolve the same reference.
