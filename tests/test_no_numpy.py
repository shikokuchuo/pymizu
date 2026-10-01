"""The numpy-less paths: the ``_TAG_MV`` rows serve the memoryview casts
where ``_TAG_NP`` would serve numpy. The suite's environment has numpy, so
these run in a subprocess whose PYTHONPATH shim makes ``import numpy``
raise ImportError — pymizu's optional-numpy detection then falls back in
both the driver and its spawned workers."""

import os
import pathlib
import subprocess
import sys
import textwrap


def _run_without_numpy(tmp_path, script):
    (tmp_path / "numpy.py").write_text(
        'raise ImportError("numpy blocked by test")\n'
    )
    env = {**os.environ, "PYTHONPATH": str(tmp_path)}
    res = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        env=env,
        cwd=pathlib.Path(__file__).parent.parent,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr


def test_map_int64_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import array

        import pymizu
        from pymizu import _map
        from tests.helpers import square

        assert _map._np is None  # the shim took; else this covers nothing

        x = array.array("q", range(50))
        p = pymizu.Pool.create(2)
        try:
            # generic collect: workers index the raw x section through
            # _TAG_MV (memoryview.cast("q"))
            assert p.map(square, x) == [i * i for i in range(50)]
            # template collect: _wrap_out's memoryview path for tag 32
            out = p.map(square, x, template=array.array("q", [0]))
            assert type(out) is memoryview and out.format == "q"
            assert list(out) == [i * i for i in range(50)]
        finally:
            p.stop()
        """,
    )


def test_interop_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import pathlib

        from pymizu import _pymizu

        corpus = {}
        text = pathlib.Path("tests/interop-corpus/corpus.txt").read_text()
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            k, v = line.split("|")
            corpus[k.strip()] = v.strip()

        def read(cid):
            return _pymizu._read_stream(bytes.fromhex(corpus[cid]))

        # vector reads fall back to a memoryview copy, sentinels in place
        v = read("realv")
        assert isinstance(v, memoryview)
        assert list(v.cast("d")) == [1.5, -2.5, 0.0]
        v = read("py-intv-na")
        assert list(v.cast("i")) == [1, -2147483648, -3]

        # the homes that need numpy decline informatively
        for cid in ["mat-2x3", "date", "posixct-utc"]:
            try:
                read(cid)
                raise SystemExit(f"no decline for {cid}")
            except Exception as e:
                assert "numpy" in str(e), (cid, e)

        # a frame builds fine without numpy; to_dict() is Arrow-free:
        # numeric columns memoryviews, strings and factors lists, a
        # temporal column an informative error
        f = _pymizu._read_stream(
            _pymizu._read_stream(bytes.fromhex(corpus["frame-3col"])) and
            bytes.fromhex(corpus["frame-3col"]))
        d = f.to_dict()
        assert list(d["n"].cast("d")) == [1.5, 2.5]
        assert list(d["i"].cast("q")) == [5, -9223372036854775808]
        assert d["f"] == ["f", None]
        assert f.names == ("n", "f", "i") and f.row_names is None
        """,
    )


def test_interop_temporal_frame_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import pymizu
        from pymizu import _pymizu

        # a POSIXct column frame from an Arrow stream (the front-end)
        h = _pymizu._channel_new(64, 1024, 65536, False, b"")
        p, _ = _pymizu._channel_attach(h.token, _ident=(2, 0))
        p.ready_set()
        assert h.ready_wait(10)
        # a frame with a POSIXct column: to_dict() on the temporal
        # column needs numpy and says so
        stream = bytes.fromhex(
            "49010f0c01000000000000000f08020000000000000000000040fc54d941"
            "a20700000000f07f0d020000000000000005000000636c6173730b0200"
            "00000000000007000000504f534958637406000000504f534958740500"
            "0000747a6f6e6504030000005554430d0300000000000000050000006e"
            "616d657304010000007405000000636c617373040a000000646174612e"
            "6672616d6509000000726f772e6e616d65730702000000000000000000"
            "0080feffffff"
        )
        try:
            f = _pymizu._read_stream(stream)
        except Exception:
            raise SystemExit("frame build must not need numpy")
        try:
            f.to_dict()
            raise SystemExit("no decline")
        except Exception as e:
            assert "numpy" in str(e)
        p.destroy()
        h.destroy()
        """,
    )


def test_tree_wrap_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import pymizu

        try:
            launcher = pymizu.r_launcher()
        except pymizu.MizuError:
            print("no Rscript/mizu — skipping")
            raise SystemExit(0)

        src = '''
        x <- list(a = runif(200000), b = 1:200000 * 2L)
        mizu::mizu_send(ch, x)
        df <- data.frame(x = 1:300000, s = rep(c("a", "b", NA), 100000),
                         stringsAsFactors = FALSE)
        mizu::mizu_send(ch, df)
        m <- matrix(runif(200000), nrow = 400)
        mizu::mizu_send(ch, m)
        mizu::mizu_recv(ch, timeout = 60)
        '''
        ch = pymizu.Channel.create(src, launcher=launcher)
        try:
            # a named list: a dict of bare _ShmView exporters
            d = ch.recv(60)
            assert type(d) is dict and sorted(d) == ["a", "b"]
            assert memoryview(d["a"]).format == "d"
            assert memoryview(d["b"]).format == "i"
            assert memoryview(d["b"])[:3].tolist() == [2, 4, 6]
            # a frame: numeric columns as views, the string block a list
            f = ch.recv(60)
            cols = f.to_dict()
            assert memoryview(cols["x"])[:3].tolist() == [1, 2, 3]
            assert memoryview(cols["x"]).format == "i"
            assert cols["s"][:3] == ["a", "b", None]
            # a matrix needs numpy for the F-order reshape: the recv
            # declines informatively (consumed, the channel unharmed)
            try:
                ch.recv(60)
                raise AssertionError("expected the matrix to decline")
            except pymizu.MizuError as e:
                assert "needs numpy" in str(e)
        finally:
            ch.close()
        """,
    )


def test_mizl_frame_write_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import pyarrow as pa

        from tests.helpers import foreign_pair

        # the MIZL writer is Arrow-fed throughout: a frame past the floor
        # stages and reads back with no numpy in the process
        h, p = foreign_pair(caps=7)
        tbl = pa.table({
            "i": pa.array([k if k % 10 else None for k in range(20000)],
                          type=pa.int32()),
            "s": pa.array([f"s{k % 100}" for k in range(20000)]),
        })
        h.send(tbl)
        f = p.recv(10)
        cols = f.to_dict()
        assert memoryview(cols["i"])[:3].tolist() == [-2147483648, 1, 2]
        assert cols["s"][:2] == ["s0", "s1"]
        p.destroy()
        h.destroy()
        """,
    )


def test_task_arg_memoryview_shm_vec_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import array

        import pymizu
        from pymizu import _map

        assert _map._np is None  # the shim took; else this covers nothing

        # a memoryview arg past the zero-copy floor stages SHM_VEC: the R
        # worker receives a view (the 'I' ref leaf), not a copy
        try:
            launcher = pymizu.r_pool_launcher(
                stdout=None, stderr=None
            )
        except pymizu.MizuError:
            print("no Rscript/mizu — skipping")
            raise SystemExit(0)
        p = pymizu.Pool.create(1, launcher=launcher)
        try:
            big = memoryview(array.array("d", [1.5] * 200_000))
            res = p.submit(pymizu.call("base::sum", big)).collect()
            assert res.cast("d")[0] == 300000.0  # a raw memoryview sans numpy
            src = 'if (.Call(mizu:::mizu_zc_view_check, x)) "view" else "copy"'
            assert p.submit(pymizu.call(source=src, x=big)).collect() == "view"
        finally:
            p.stop()
        """,
    )
