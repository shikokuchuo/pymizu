"""Import-time behavior, checked in fresh interpreters."""

import os
import pathlib
import subprocess
import sys
import textwrap

ROOT = pathlib.Path(__file__).parent.parent


def _run(script):
    env = os.environ.copy()
    env.pop("COVERAGE_PROCESS_START", None)
    env.pop("COVERAGE_RUN", None)
    res = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        env=env,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert res.returncode == 0, res.stderr


def test_package_and_process_entries_defer_heavy_imports():
    _run(
        """
        import sys
        import pymizu
        import pymizu.child
        import pymizu.worker

        heavy = {
            "cloudpickle", "subprocess", "pymizu._r", "pymizu._executor",
            "concurrent.futures", "pymizu._map",
        }
        assert heavy.isdisjoint(sys.modules), heavy & sys.modules.keys()
        """
    )


def test_lazy_public_exports():
    _run(
        """
        import sys
        import pymizu

        assert "pymizu._executor" not in sys.modules
        LazyExecutor = pymizu.PoolExecutor
        from pymizu._executor import PoolExecutor as DirectExecutor

        assert LazyExecutor is DirectExecutor
        assert "pymizu._r" not in sys.modules

        from pymizu import PoolExecutor, r_launcher
        from pymizu._r import r_launcher as direct_r_launcher

        assert PoolExecutor is DirectExecutor
        assert r_launcher is direct_r_launcher
        """
    )
    _run(
        """
        ns = {}
        exec("from pymizu import *", ns)
        from pymizu._executor import PoolExecutor
        from pymizu._r import r_launcher

        assert ns["PoolExecutor"] is PoolExecutor
        assert ns["r_launcher"] is r_launcher
        """
    )


def test_dump_resolution_is_shared_and_loads_stay_stock():
    _run(
        """
        import pickle
        import sys
        import pymizu

        assert "cloudpickle" not in sys.modules
        stream = pickle.dumps([[1, 2], [3, 4]], 4)
        assert pymizu._pymizu._read_stream(stream) == [[1, 2], [3, 4]]
        assert "cloudpickle" not in sys.modules

        fn = pickle.loads(pymizu._pymizu._pickle_dumps(lambda x: x + 1))
        assert fn(2) == 3
        assert "cloudpickle" in sys.modules
        """
    )
    _run(
        """
        import pickle
        import sys
        import pymizu
        from pymizu import _map

        assert "cloudpickle" not in sys.modules
        fn = lambda x: x + 1
        stream = _map._write_desc(False, fn, (), {}, None, None, 1)
        loaded_fn, args, kwargs, x = pickle.loads(stream)
        assert loaded_fn(2) == 3
        assert args == () and kwargs == {} and x is None
        assert "cloudpickle" in sys.modules
        """
    )


def test_default_pool_and_channel_startup():
    _run(
        """
        import sys
        import pymizu

        def identity(x):
            return x

        assert "subprocess" not in sys.modules
        pool = pymizu.Pool.create(1)
        try:
            value = pool.submit(identity, [[1], [2]]).collect(timeout=10)
            assert value == [[1], [2]]
        finally:
            pool.stop()
        assert "subprocess" in sys.modules

        ch = pymizu.Channel.create("for m in ch: ch.send(m)")
        try:
            assert ch._proc is not None
            ch.send([[3], [4]])
            assert ch.recv(timeout=10) == [[3], [4]]
        finally:
            ch.close(timeout=10)
        """
    )


def test_idle_worker_defers_map_module():
    # the worker's idle sweep must not import pymizu._map (numpy with it):
    # the import would land on the first park and stall the first task
    _run(
        """
        import time
        import pymizu
        from tests.helpers import modules_loaded, square

        with pymizu.Pool.create(1) as pool:
            names = ["pymizu._map"]
            first = pool.submit(modules_loaded, names).collect(timeout=10)
            time.sleep(0.2)  # the worker parks, and its sweep hook fires
            idle = pool.submit(modules_loaded, names).collect(timeout=10)
            assert first == [] and idle == [], (first, idle)
            # a map loads the module, and the sweep then finds it
            assert pool.map(square, [1, 2]).collect(timeout=10) == [1, 4]
            time.sleep(0.2)
            after = pool.submit(modules_loaded, names).collect(timeout=10)
            assert after == names
        """
    )


def test_concurrent_first_dumps_on_independent_handles():
    _run(
        """
        import sys
        import threading
        import pymizu
        from tests.helpers import chan_pair

        pairs = [chan_pair() for _ in range(4)]
        barrier = threading.Barrier(len(pairs))
        errors = []

        def exchange(pair, value):
            host, peer = pair
            try:
                barrier.wait()
                host.send(value)
                assert peer.recv(timeout=10) == value
            except BaseException as exc:
                errors.append(exc)

        try:
            threads = [
                threading.Thread(
                    target=exchange, args=(pair, [[i], [i + 1], {i}])
                )
                for i, pair in enumerate(pairs)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)
            assert all(not thread.is_alive() for thread in threads)
            assert not errors
            assert "cloudpickle" in sys.modules
        finally:
            for host, peer in pairs:
                host.destroy()
                peer.destroy()
        """
    )
