import os
import pickle
import threading
import warnings

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


def test_call_repr():
    assert repr(pymizu.call("math.sqrt", 16)) == "pymizu.call(math.sqrt, ...)"
    assert (
        repr(pymizu.call(source="x + 1")) == "pymizu.call(source='x + 1', ...)"
    )


def test_task_hooks_roundtrip():
    b = _pymizu._write_task("math.sqrt", 0, (16,), {"y": 2.5}, 2, 30064771074)
    got = _pymizu._read_task(b)
    assert got == (2, 0, 30064771074, "math.sqrt", [16], {"y": 2.5})


def test_spec_bytes_arg_keeps_type_at_size():
    # exact bytes stay the bytes form past the zero-copy gate — never a
    # uint8 array (P-11)
    with pymizu.Pool.create(1) as p:
        for n in (10, 100000):
            t = p.submit(pymizu.call("builtins.type", b"x" * n))
            assert t.collect(timeout=15) is bytes


def test_spec_task_error_escapes_surrogates():
    # a lone surrogate in the task's error text crosses escaped, never
    # blanked (P-12)
    spec = pymizu.call(source="raise ValueError('bad path: /tmp/caf\\udce9')")
    with pymizu.Pool.create(1) as p:
        t = p._h.submit(_pymizu._call_frame(spec, (2, 7)), None)
        with pytest.raises(pymizu.TaskError) as ei:
            t.collect(timeout=15)
    assert "bad path: /tmp/caf" in str(ei.value)
    assert ei.value.remote_traceback


def test_spec_task_base_exception_takes_the_worker_down():
    # a BaseException out of an 'I' task escapes the exec hook: the
    # worker goes down, the collector meets WorkerDiedError
    with pymizu.Pool.create(1) as p:
        with pytest.raises(pymizu.WorkerDiedError):
            p.submit(pymizu.call("tests.helpers.raise_system_exit")).collect(
                timeout=15
            )


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


def test_spec_task_conversion_warning_survives_the_rewrite():
    # a spec task past the inline budget rewrites its stream: the
    # conversion warning record rides the rewrite walk, so a large uint64
    # argument warns exactly once at submit (P-06)
    np = pytest.importorskip("numpy")
    with pymizu.Pool.create(1) as p:
        arr = np.full(200_000, 2**60, dtype=np.uint64)
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            assert (
                p.submit(pymizu.call("numpy.size", arr)).collect() == 200_000
            )
        conv = [w for w in rec if "uint64 value(s)" in str(w.message)]
        assert len(conv) == 1
        assert "200000 uint64 value(s)" in str(conv[0].message)


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
        assert pool._h._worker_ident() == (3, 255)  # this build's caps word
        assert att._h._worker_ident() == (3, 255)
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


def test_qualifier_defers_on_unknown_language():
    # an unknown worker language skips the local qualifier check: the
    # unqualified name crosses and the worker's own resolution verdict
    # comes back as the task's error stream
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        wk = _pymizu._pool_worker_join(h.token, 0, _ident=(7, 0))
        loop = threading.Thread(
            target=lambda: (wk.run(), wk.leave()), daemon=True
        )
        loop.start()
        pool = pymizu.Pool._wrap(h)
        assert pool._h._worker_ident() == (7, 0)
        with pytest.raises(pymizu.TaskError):
            pool.submit(pymizu.call("sqrt", 4.0)).collect(timeout=10)
        pool.stop()
        loop.join(timeout=10)
        wk.destroy()
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
            "4901"
            "12"
            "02"
            "00"
            "0000"
            "0200000007000000"
            "04"
            "01000000"
            "66"
            "0c"
            "0000000000000000"
            "0d"
            "0000000000000000"
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


def test_spec_on_a_channel_fails():
    # the spec verb is pool-only: a call frame staged on a channel errors
    h = _pymizu._channel_new(64, 1024, 1 << 16, False, b"")
    try:
        with pytest.raises(pymizu.MizuError, match="through Pool.submit"):
            h.send(_pymizu._call_frame(pymizu.call("math.sqrt", 16)))
    finally:
        h.destroy()


def test_spec_ident_override_needs_a_pair():
    # the test-only identity override is validated at stage time, not by
    # the _call_frame factory
    with pymizu.Pool.create(1) as pool:
        with pytest.raises(TypeError, match="identity pair"):
            pool._h.submit(
                _pymizu._call_frame(pymizu.call("math.sqrt", 16), [2, 7])
            )


def test_spec_malformed_gates():
    # the spec components are duck-read at stage: missing attributes
    # propagate, wrong shapes are the local malformed error
    with pymizu.Pool.create(1) as pool:
        with pytest.raises(AttributeError):
            pool._h.submit(_pymizu._call_frame(object()))

    class FakeSpec:
        code = "math.sqrt"
        kind = 0
        args = ()
        kwargs = {}

    for field, value in [
        ("code", 42),
        ("code", "\ud800"),
        ("kind", -1),
        ("kind", 300),
        ("kind", "0"),
        ("args", []),
        ("kwargs", []),
    ]:
        bad = FakeSpec()
        setattr(bad, field, value)
        with pymizu.Pool.create(1) as pool:
            with pytest.raises(pymizu.MizuError, match="malformed call spec"):
                pool._h.submit(_pymizu._call_frame(bad))


def test_spec_submit_at_word_zero():
    # the C-side race guard (the facade's own gate is ahead of it)
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        with pytest.raises(pymizu.MizuError, match="no worker has joined"):
            h.submit(_pymizu._call_frame(pymizu.call("math.sqrt", 16)))
    finally:
        h.destroy()


def test_spec_unqualified_name_fails_on_the_worker():
    # the facade validates its own specs; a duck-typed spec slips through
    # to the worker's resolver
    class FakeSpec:
        code = "nodots"
        kind = 0
        args = ()
        kwargs = {}

    with pymizu.Pool.create(1) as pool:
        with pytest.raises(pymizu.TaskError, match="not qualified"):
            pool._h.submit(_pymizu._call_frame(FakeSpec())).collect(timeout=10)


def test_spec_resolution_failures():
    with pymizu.Pool.create(1) as pool:
        with pytest.raises(pymizu.TaskError) as exc_info:
            pool.submit(pymizu.call("no_such_module_xyz.f", 1)).collect(
                timeout=10
            )
        assert exc_info.value.remote_type == "ModuleNotFoundError"
        with pytest.raises(pymizu.TaskError) as exc_info:
            pool.submit(pymizu.call("os.no_such_attr_xyz", 1)).collect(
                timeout=10
            )
        assert exc_info.value.remote_type == "AttributeError"


def test_spec_kwargs_vectorcall():
    with pymizu.Pool.create(1) as pool:
        t = pool.submit(pymizu.call("numpy.mean", [1, 2, 3], axis=0))
        assert t.collect(timeout=10) == 2.0


def test_spec_ref_args_need_taskref():
    # a foreign pool short of the bit fails the by-reference submit
    # locally rather than losing the work to a remote error
    import numpy as np

    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    w = _pymizu._pool_worker_join(h.token, 0, _ident=(2, 7))
    try:
        a = np.arange(200_000, dtype=np.float64)
        with pytest.raises(pymizu.DeclinedError, match="by-reference"):
            h.submit(_pymizu._call_frame(pymizu.call("numpy.size", a)))
    finally:
        w.leave()
        h.destroy()


def test_spec_big_arg_crosses_by_reference():
    # the zc candidate: the task stream carries the ref, the worker reads
    # off the shared pages
    import numpy as np

    with pymizu.Pool.create(1) as pool:
        a = np.arange(200_000, dtype=np.float64)
        t = pool.submit(pymizu.call("numpy.size", a))
        assert t.collect(timeout=30) == 200_000


def test_spec_rewrite_when_no_candidate_fits():
    # two candidates whose refs leave the stream over budget: the by-value
    # rewrite rides the reservation
    import numpy as np

    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    pymizu._default_worker_launcher()(h.token, 0)
    try:
        pool = pymizu.Pool._wrap(h)
        assert pool.submit(abs, -1).collect(timeout=10) == 1  # the join
        a = np.arange(70_000, dtype=np.float64)
        b = np.arange(70_000, dtype=np.float64) * 2
        t = pool.submit(pymizu.call("numpy.add", a, b))
        out = t.collect(timeout=30)
        assert out.shape == (70_000,)
        assert out[1] == 3.0
    finally:
        h.destroy()


def test_task_run_malformed_streams():
    # the worker's task decode refusals, each crossing as the task's error
    base = _pymizu._write_task("math.sqrt", 0, (), {"a": 1, "b": 2}, 3, 7)
    cases = []
    b = bytearray(base)
    b[29] = 0x0D  # the positional field as a dict
    cases.append((bytes(b), "positional field is not a list"))
    b = bytearray(base)
    b[38] = 0x0C  # the named field as a list
    cases.append((bytes(b), "named field is not a dict"))
    cases.append((bytes(base[:52]), None))  # ends after the first key
    cases.append((base + b"\x00", None))  # trailing garbage
    with pymizu.Pool.create(1) as p:
        for raw, match in cases:
            t = p._h.submit(raw, None)
            with pytest.raises(pymizu.TaskError, match=match):
                t.collect(timeout=15)


def test_read_task_malformed_named():
    # the same arms on the _read_task hook, plus the duplicate-key check
    base = _pymizu._write_task("math.sqrt", 0, (), {"a": 1, "b": 2}, 3, 7)
    with pytest.raises(pymizu.MizuError):
        _pymizu._read_task(bytes(base[:52]))
    with pytest.raises(pymizu.MizuError):
        _pymizu._read_task(base + b"\x00")
    b = bytearray(base)
    b[65] = ord("a")  # two "a" keys
    with pytest.raises(pymizu.MizuError, match="a duplicate dict key"):
        _pymizu._read_task(bytes(b))


def test_runner_run_malformed_streams():
    # the kind-2 decode refusals: name, generation, seed, trailing bytes
    base = _pymizu._write_runner("/mizu_xyz", 5, None, 3, 7)
    cases = []
    b = bytearray(base)
    b[15] = 0x02  # the name as an int
    cases.append((bytes(b), "region name is not a string"))
    b = bytearray(base)
    b[29] = 0x00  # the generation as nil
    cases.append((bytes(b), "generation is not an integer"))
    # the seed as an int
    seed_int = base[:-1] + b"\x02" + (1).to_bytes(8, "little")
    cases.append((seed_int, "seed is not nil or an i64 pair"))
    cases.append((base + b"\x00", None))  # trailing garbage
    with pymizu.Pool.create(1) as p:
        for raw, match in cases:
            t = p._h.submit(raw, None)
            with pytest.raises(pymizu.TaskError, match=match):
                t.collect(timeout=15)


def test_runner_frame_stage_gates():
    # the runner submit's local gates, ahead of any worker
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    try:
        with pytest.raises(UnicodeEncodeError):
            h.submit(_pymizu._runner_frame("\ud800", 0, None), None)
        with pytest.raises(OverflowError, match="int64 range"):
            h.submit(_pymizu._runner_frame("n", 2**63, None), None)
        with pytest.raises(pymizu.MizuError, match="malformed map runner"):
            h.submit(_pymizu._runner_frame("n", 0, ("a", "b")), None)
        with pytest.raises(pymizu.MizuError, match="no worker has joined"):
            h.submit(_pymizu._runner_frame("n", 0, None), None)
    finally:
        h.destroy()


def test_spec_strided_array_arg_declines():
    # a non-contiguous argument: the plan's candidate probe clears the
    # buffer refusal, and the value walk's nested-strided decline stands
    import numpy as np

    with pymizu.Pool.create(1) as pool:
        with pytest.raises(pymizu.DeclinedError, match="strided"):
            pool.submit(pymizu.call("numpy.sum", np.arange(64.0)[::2]))


def test_spec_view_arg_pinned_on_the_zc_path():
    # a re-sendable view beside the zc candidate: the ref leaf's emission
    # pins the spec until the claim-side release (D4)
    import numpy as np
    from tests.helpers import foreign_pair

    h1, p1 = foreign_pair()
    p1.send(np.arange(200_000, dtype=np.float64))
    v = h1.recv(10)
    try:
        with pymizu.Pool.create(1) as pool:
            buf = np.arange(200_000, dtype=np.float64)
            out = pool.submit(pymizu.call("numpy.add", v, buf)).collect(
                timeout=30
            )
            assert out[1] == 2.0
    finally:
        h1.destroy()
        p1.destroy()


def test_spec_view_arg_pinned_on_the_rewrite():
    # the same pin when no candidate fits and the by-value rewrite rides
    # the reservation
    import numpy as np
    from tests.helpers import foreign_pair

    h1, p1 = foreign_pair()
    p1.send(np.arange(70_000, dtype=np.float64) * 3)
    v = h1.recv(10)
    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    pymizu._default_worker_launcher()(h.token, 0)
    try:
        pool = pymizu.Pool._wrap(h)
        assert pool.submit(abs, -1).collect(timeout=10) == 1  # the join
        a = np.arange(70_000, dtype=np.float64)
        b = np.arange(70_000, dtype=np.float64) * 2
        out = pool.submit(
            pymizu.call(source="x + y + z", x=v, y=a, z=b)
        ).collect(timeout=30)
        assert out[1] == 3.0 + 1.0 + 2.0
    finally:
        h.destroy()
        h1.destroy()
        p1.destroy()


def test_spec_zc_checkout_failure_abandons_to_value():
    # a failed spill checkout abandons the by-reference write: the task
    # falls back to by-value, never a partial stream — the failure here
    # (fd exhaustion) also fails the reservation, surfacing the ShmError
    fcntl = pytest.importorskip("fcntl", reason="POSIX only")
    resource = pytest.importorskip("resource", reason="POSIX only")

    import numpy as np

    def open_fds():
        n = 0
        for fd in range(256):
            try:
                fcntl.fcntl(fd, fcntl.F_GETFD)
                n += 1
            except OSError:
                pass
        return n

    h = _pymizu._pool_new(1, 8, 64, 64, 64, 512)
    pymizu._default_worker_launcher()(h.token, 0)
    try:
        pool = pymizu.Pool._wrap(h)
        assert pool.submit(abs, -1).collect(timeout=10) == 1  # the join
        small = np.arange(12_000, dtype=np.float64)
        assert (
            pool.submit(pymizu.call("numpy.size", small)).collect(timeout=30)
            == 12_000
        )  # the free list holds only the small region
        big = np.arange(200_000, dtype=np.float64)
        # arm the starvation: the fd count can drift, so probe each try
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        armed = False
        for _ in range(8):
            resource.setrlimit(resource.RLIMIT_NOFILE, (open_fds(), hard))
            try:
                fd = os.open("/dev/null", os.O_RDONLY)
            except OSError:
                armed = True
                break
            os.close(fd)
        try:
            if not armed:
                pytest.skip("fd starvation not effective on this platform")
            with pytest.raises(pymizu.ShmError, match="payload region"):
                pool.submit(pymizu.call("numpy.size", big))
        finally:
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
        assert (
            pool.submit(pymizu.call("numpy.size", big)).collect(timeout=30)
            == 200_000
        )
    finally:
        h.destroy()
