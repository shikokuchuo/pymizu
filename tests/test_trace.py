"""The pool trace hook (mizu_pool_set_trace): submit-side events on the
calling thread, removal with None, and unraisable hook errors."""

import pytest

import pymizu


@pytest.fixture
def pool():
    p = pymizu.Pool.create(2)
    yield p
    p.stop()


def test_trace_submit_events(pool):
    events = []
    pool.trace(lambda ev, tid: events.append((ev, tid)))
    t1 = pool.submit(pow, 2, 10)
    t2 = pool.submit(pow, 3, 4)
    assert t1.collect(timeout=5) == 1024
    assert t2.collect(timeout=5) == 81
    # the controller handle sees "submit" only: worker-side events fire
    # on worker handles, in the worker processes
    assert len(events) == 2
    assert all(ev == "submit" for ev, _ in events)
    assert len({tid for _, tid in events}) == 2

    pool.trace(None)
    t3 = pool.submit(pow, 1, 1)
    assert t3.collect(timeout=5) == 1
    assert len(events) == 2


def test_trace_rejects_non_callable(pool):
    with pytest.raises(TypeError):
        pool.trace(42)


# the hook error is written as unraisable by design; keep pytest's
# unraisable-exception plugin from reporting it as a warning
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_trace_hook_error_is_unraisable(pool, capsys):
    def boom(ev, tid):
        raise RuntimeError("trace boom")

    pool.trace(boom)
    t = pool.submit(pow, 2, 2)
    assert t.collect(timeout=5) == 4   # the hook error fails nothing
    pool.trace(None)
