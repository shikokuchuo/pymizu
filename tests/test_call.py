import pickle

import pytest

import pymizu
from pymizu import _pymizu


def test_call_builds_name_and_source_specs():
    spec = pymizu.call("numpy.mean", [1, 2], axis=0)
    assert spec.kind == 0
    assert spec.code == "numpy.mean"
    assert spec.args == ([1, 2],)
    assert spec.kwargs == {"axis": 0}
    src = pymizu.call(source="x + 1", x=41)
    assert src.kind == 1
    assert src.kwargs == {"x": 41}
    assert src.args == ()
    with pytest.raises(TypeError, match="exactly one"):
        pymizu.call()
    with pytest.raises(TypeError, match="exactly one"):
        pymizu.call("f", source="x")
    with pytest.raises(TypeError, match="must be str"):
        pymizu.call(1)
    # an ordinary object elsewhere: it pickles
    assert pickle.loads(pickle.dumps(spec)).code == "numpy.mean"


def test_task_hooks_roundtrip():
    b = _pymizu._write_task("math.sqrt", 0, (16,), {"y": 2.5}, 2, 30064771074)
    got = _pymizu._read_task(b)
    assert got == (2, 0, 30064771074, "math.sqrt", [16], {"y": 2.5})


def test_spec_tasks_run_same_language():
    with pymizu.Pool.create(1) as p:
        t = p.submit(pymizu.call("math.sqrt", 16))
        assert t.collect() == 4.0
        t2 = p.submit(pymizu.call(source="y = x * 2\ny + 1", x=20))
        assert t2.collect() == 41
        # statements only -> None; positional args bind as _1, _2
        assert p.submit(pymizu.call(source="z = 1")).collect() is None
        t4 = p.submit(pymizu.call(None, 19, 23, source="_1 + _2"))
        assert t4.collect() == 42
        # a spec held in a variable submits the same way
        spec = pymizu.call("builtins.len", [1, 2, 3])
        assert p.submit(spec).collect() == 3
        # errors keep the rich private format same-language
        with pytest.raises(pymizu.TaskError) as ei:
            p.submit(pymizu.call("builtins.len", 1.5)).collect()
        assert ei.value.remote_type == "TypeError"
        assert ei.value.remote_traceback != ""
        # extra args with a spec are rejected
        with pytest.raises(TypeError, match="carries its own arguments"):
            p.submit(pymizu.call("math.sqrt", 16), 1)


def test_pool_word_zero_and_attach_race():
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        pool = pymizu.Pool._wrap(h)
        assert pool._h._worker_ident() is None
        with pytest.raises(pymizu.MizuError, match="no worker has joined"):
            pool.submit(pymizu.call("math.sqrt", 16))
        # a plain callable queues unchanged at word 0
        t = pool.submit(abs, -1)
        # an attached submitter reads the same word (none yet)
        att = pymizu.Pool._wrap(_pymizu._pool_attach(h.token))
        assert att._h._worker_ident() is None
        pymizu._default_worker_launcher()(h.token, 0)
        # the queued private frame runs once a worker exists, and both
        # submitters read the word without re-attaching
        assert t.collect(timeout=10) == 1
        assert pool._h._worker_ident() == (3, 7)
        assert att._h._worker_ident() == (3, 7)
    finally:
        h.destroy()


def test_private_verbs_fail_fast_on_a_foreign_pool():
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        _pymizu._pool_worker_join(h.token, 0, _ident=(2, 7))
        pool = pymizu.Pool._wrap(h)
        assert pool._h._worker_ident() == (2, 7)
        with pytest.raises(pymizu.MizuError, match="pymizu.call"):
            pool.submit(abs, -1)
        with pytest.raises(pymizu.MizuError, match="pymizu.call"):
            pool.submit_batch([abs])
        with pytest.raises(TypeError, match="pymizu.call"):
            pool.map(abs, [-1, -2])
        with pytest.raises(TypeError, match="pymizu.call"):
            pool.map_prepare(abs, [-1, -2])
        # a spec fn passes the map guard on a foreign pool (Phase 5)
        from pymizu import _map

        assert _map._map_check_native(pool, pymizu.call("math.sqrt")) == (
            True,
            2,
        )
        with pytest.raises(TypeError, match="qualified name"):
            pool.submit(pymizu.call("sqrt", 4))
    finally:
        h.destroy()


def test_qualifier_check_per_language():
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        _pymizu._pool_worker_join(h.token, 0)  # Python word
        pool = pymizu.Pool._wrap(h)
        with pytest.raises(TypeError, match="qualified name"):
            pool.submit(pymizu.call("stats::quantile", [1]))
    finally:
        h.destroy()


def test_foreign_submitter_results_errors_gate():
    with pymizu.Pool.create(1) as p:

        def submit(spec, ident=(2, 7)):
            return p._h.submit(_pymizu._call_frame(spec, ident), None)

        assert submit(pymizu.call("math.sqrt", 25)).collect() == 5.0
        # an error crosses as the neutral err stream
        with pytest.raises(pymizu.TaskError) as ei:
            submit(pymizu.call(source="raise ValueError('boom')")).collect()
        assert ei.value.remote_type == "ValueError"
        assert "boom" in str(ei.value)
        # a non-portable result fails the task with an error stream
        with pytest.raises(pymizu.TaskError) as ei2:
            submit(pymizu.call(source="{1, 2, 3}")).collect()
        assert ei2.value.remote_type == "DeclinedError"
        assert "set" in str(ei2.value)
        # unknown capability bits in the submitter identity are ignored
        assert submit(pymizu.call("math.sqrt", 36), (2, 15)).collect() == 6.0
        # nested submit inside a foreign-run task
        src = (
            "import pymizu\n"
            "pool = pymizu.current_pool()\n"
            "t = pool.submit(abs, -42)\n"
            "t.collect()"
        )
        assert submit(pymizu.call(source=src)).collect() == 42


def test_collect_any_all_across_languages():
    with pymizu.Pool.create(1) as p:

        def submit(spec):
            return p._h.submit(_pymizu._call_frame(spec, (2, 7)), None)

        t1 = submit(pymizu.call(source="1 + 1"))
        t2 = submit(pymizu.call(source="raise ValueError('boom')"))
        idx, value = p.collect_any([t1, t2])
        assert (idx, value) == (0, 2)
        t3 = submit(pymizu.call(source="1 + 1"))
        t4 = submit(pymizu.call(source="raise ValueError('boom')"))
        with pytest.raises(pymizu.TaskError) as ei:
            p.collect_all([t3, t4])
        assert ei.value.index == 1
        assert ei.value.remote_type == "ValueError"
        assert t3.collect() == 2


def test_a_task_targeting_another_language_mismatches():
    with pymizu.Pool.create(1) as p:
        # a hand-built stream whose target byte is R: the Python worker
        # fails it with the neutral err stream, never executes
        raw = bytes.fromhex(
            "4901" "12" "02" "00" "0000" "0200000007000000"
            "04" "01000000" "66"
            "0c" "0000000000000000" "0d" "0000000000000000"
        )
        t = p._h.submit(raw, None)
        with pytest.raises(pymizu.TaskError) as ei:
            t.collect()
        assert "task language mismatch" in str(ei.value)


def test_a_wrong_shape_task_fails_naming_the_shape():
    with pymizu.Pool.create(1) as p:
        # code as an int where kind 0 wants a string (a crafted stream)
        bad = _pymizu._write_task("f", 0, (), {}, 3, 30064771075)
        assert bad[15] == 0x04 and bad[20] == ord("f")
        bad = bad[:15] + bytes([0x02]) + (1).to_bytes(8, "little") + bad[21:]
        t = p._h.submit(bad, None)
        with pytest.raises(pymizu.TaskError) as ei:
            t.collect()
        assert "the code field is not a string" in str(ei.value)


def test_a_foreign_private_frame_gets_the_neutral_err_stream():
    with pymizu.Pool.create(1) as p:
        # 'R' (mizu codec magic) and R_Serialize 'B' first bytes
        t1 = p._h.submit(b"Rtask", None)
        t2 = p._h.submit(b"Binary", None)
        for t in (t1, t2):
            with pytest.raises(pymizu.TaskError) as ei:
                t.collect()
            assert "foreign private codec" in str(ei.value)


def test_non_portable_args_decline_at_submit():
    with pymizu.Pool.create(1) as p:
        with pytest.raises(pymizu.DeclinedError):
            p.submit(pymizu.call("builtins.repr", {1, 2, 3}))
