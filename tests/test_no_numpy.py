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


def _run_with_numpy_stub(tmp_path, stub, script):
    # a partial numpy: the shim is a stub module carrying only selected
    # attributes, so pymizu's lazy lookups take their missing-attr arms
    (tmp_path / "numpy.py").write_text(textwrap.dedent(stub))
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
            assert p.map(square, x).collect() == [i * i for i in range(50)]
            # template collect: _wrap_out's memoryview path for tag 32
            out = p.map(square, x, template=array.array("q", [0])).collect()
            assert type(out) is memoryview and out.format == "q"
            assert list(out) == [i * i for i in range(50)]
        finally:
            p.stop()
        """,
    )


def test_map_view_x_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import pymizu
        from pymizu import _map

        assert _map._np is None  # the shim took; else this covers nothing

        # a received view as map x on a spec map crosses as one ref leaf;
        # the workers resolve it and index the memoryview (no numpy)
        x = bytes(range(256)) * 1200
        ch = pymizu.Channel.create(
            "ch.send(bytes(range(256)) * 1200); ch.recv(timeout=30)"
        )
        try:
            v = ch.recv(timeout=15)
            assert type(v).__name__ == "_ShmView"
            p = pymizu.Pool.create(2)
            try:
                out = p.map(pymizu.call(source="x + 1"), v).collect(
                    timeout=30
                )
            finally:
                p.stop()
            assert out == [b + 1 for b in x]
        finally:
            ch.close()
        """,
    )


def test_view_to_numpy_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import pymizu

        ch = pymizu.Channel.create(
            "ch.send(bytes(range(256)) * 1200); ch.recv(timeout=30)"
        )
        try:
            v = ch.recv(timeout=15)
            assert type(v).__name__ == "_ShmView"
            try:
                v.to_numpy()
                raise SystemExit("expected ImportError")
            except ImportError as e:
                assert "requires numpy" in str(e)
        finally:
            ch.close()
        """,
    )


def test_map_complex_declines_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import os
        import subprocess
        import sys

        import pymizu
        from pymizu import _map
        from tests.helpers import square

        assert _map._np is None  # the shim took; else this covers nothing


        def numpy_child_launcher(token):
            # the child keeps numpy: only this process is shimmed
            env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
            return subprocess.Popen(
                [sys.executable, "-m", "pymizu.child", token],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


        # a complex128 view (buffer protocol carries the 'Zd' format
        # without numpy): every map tier declines informatively
        src = (
            "import numpy as np; "
            "ch.send(np.arange(40000, dtype=np.complex128)); "
            "ch.recv(timeout=30)"
        )
        ch = pymizu.Channel.create(src, launcher=numpy_child_launcher)
        try:
            v = ch.recv(timeout=30)
            assert type(v).__name__ == "_ShmView"
            p = pymizu.Pool.create(2)
            try:
                # a complex buffer x declines the raw section (the list
                # fallback then reports the view is not iterable)
                try:
                    p.map(square, v, timeout=30)
                except TypeError as e:
                    assert "not iterable" in str(e)
                else:
                    raise AssertionError("no TypeError")
                # a complex template needs numpy's frombuffer
                try:
                    p.map(square, [1], template=v)
                except TypeError as e:
                    assert "complex128" in str(e)
                else:
                    raise AssertionError("no TypeError")
                # a complex replacement x declines likewise at the swap
                pm = p.map_prepare(square, [1, 2])
                try:
                    try:
                        pm.run(v, timeout=30)
                    except TypeError as e:
                        assert "not iterable" in str(e)
                    else:
                        raise AssertionError("no TypeError")
                finally:
                    pm.close()
            finally:
                p.stop()
        finally:
            ch.close()
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


def test_difftime_read_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import struct

        from pymizu import _pymizu

        def sval(v):
            b = v.encode()
            return bytes([0x04]) + len(b).to_bytes(4, "little") + b

        # a difftime realv: the temporal homes need numpy
        s = bytearray([0x49, 0x01, 0x0F])
        s += bytes([0x08]) + (1).to_bytes(8, "little") + struct.pack("<d", 1.5)
        s += bytes([0x0D]) + (2).to_bytes(8, "little")
        s += (5).to_bytes(4, "little") + b"class" + sval("difftime")
        s += (5).to_bytes(4, "little") + b"units" + sval("secs")
        try:
            _pymizu._read_stream(bytes(s))
            raise SystemExit("no decline")
        except Exception as e:
            assert "numpy" in str(e)
        """,
    )


def test_frame_date_difftime_to_dict_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        from pymizu import _pymizu

        # the temporal frame columns' to_dict() homes need numpy and say so
        f = _pymizu._frame_rebuild(["d"], [("D", [1, None])], None)
        try:
            f.to_dict()
            raise SystemExit("no decline")
        except Exception as e:
            assert "a Date column needs numpy" in str(e), e
        f = _pymizu._frame_rebuild(["u"], [("T", [1000, None])], None)
        try:
            f.to_dict()
            raise SystemExit("no decline")
        except Exception as e:
            assert "a difftime column needs numpy" in str(e), e
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


def test_current_rng_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import pymizu
        from pymizu import _map
        from tests.helpers import np_rand_elt, rand_elt, square

        assert _map._np is None  # the shim took; else this covers nothing
        # outside a seeded element the accessor needs no numpy
        assert pymizu.current_rng() is None

        p = pymizu.Pool.create(1)
        try:
            # seeded maps that never call it are untouched
            assert p.map(square, [1, 2, 3], seed=1).collect() == [1, 4, 9]
            got = p.map(rand_elt, list(range(10)), seed=1).collect()
            assert len(got) == 10
            # inside a seeded element it raises the lazy-numpy error
            try:
                p.map(np_rand_elt, [1, 2], seed=1).collect()
                raise SystemExit("no decline")
            except pymizu.TaskError as e:
                assert "numpy" in str(e)
        finally:
            p.stop()
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


def test_from_arrow_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        from tests.helpers import RawArrowStream

        import pymizu

        # a single-batch producer: every column adopts, nulls bitmap-only
        prod = RawArrowStream(
            [
                ("i", "i", [([10, 20, 30], [1, 0, 1])]),
                ("l", "l", [([10, 20, 30], [1, 0, 1])]),
                ("L", "u", [([10, 20, 30], [1, 0, 1])]),
                ("g", "x", [([1.5, 2.5, 3.5], [0, 1, 1])]),
                ("u", "s", [(([0, 1, 2, 3], b"abc"), [1, 1, 0])]),
            ]
        )
        f = pymizu.Frame.from_arrow(prod)
        d = f.to_dict()
        # the masked memoryview arms: the sentinel at null slots, never
        # garbage from the producer's buffers
        assert list(d["i"].cast("i")) == [10, -2147483648, 30]
        assert list(d["l"].cast("q")) == [10, -9223372036854775808, 30]
        assert list(d["u"].cast("q")) == [10, -9223372036854775808, 30]
        xv = d["x"].cast("d")
        assert xv[0] != xv[0] and xv[1] == 2.5  # the NA_real_ payload
        assert d["s"] == ["a", "b", None]
        """,
    )


def test_numpy_row_reads_as_python_scalar_without_numpy(tmp_path):
    _run_without_numpy(
        tmp_path,
        """
        import struct

        from pymizu import _pymizu

        def row(type_byte, form, payload):
            # the codec magic 'P', the numpy tag, wire type, form, bytes
            return bytes([0x50, 0x61, type_byte, form]) + payload

        # the codec's numpy row degrades to the Python scalar (P-18)
        rd = _pymizu._read_stream
        assert rd(row(14, 0, struct.pack("<d", 3.5))) == 3.5
        assert rd(row(13, 0, struct.pack("<i", -7))) == -7
        assert rd(row(32, 0, struct.pack("<q", 1 << 40))) == 1 << 40
        assert rd(row(24, 0, bytes([9]))) == 9
        assert rd(row(15, 0, struct.pack("<dd", 1.0, 2.0))) == 1 + 2j
        # form 1 (a 0-d array) degrades likewise
        assert rd(row(14, 1, struct.pack("<d", 2.5))) == 2.5
        """,
    )


def test_buffer_gate_partial_numpy(tmp_path):
    # a numpy stub without ndarray: the buffer gates' cached type probe
    # and the 0-d form probe clear the missing-attribute error and stage
    _run_with_numpy_stub(
        tmp_path,
        "",
        """
        import array

        import numpy  # the stub
        import pymizu

        assert numpy.__file__.endswith("numpy.py")  # the shim took

        peer = '''
        for _ in range(2):
            try:
                ch.recv(timeout=30)
            except Exception:
                pass
        '''
        ch = pymizu.Channel.create(peer)
        try:
            # the type probe's missing-ndarray arm: an ordinary buffer
            assert ch.send(array.array("d", [1.0, 2.0])) is None
            # the 0-d form probe's missing-ndarray arm
            z = memoryview(b"12345678").cast("d", [])
            assert ch.send(z) is None
        finally:
            ch.close()
        """,
    )


def test_numpy_row_read_partial_numpy(tmp_path):
    # the codec's numpy row against a partial numpy: a missing scalar
    # class and a missing array constructor fail the read informatively
    _run_with_numpy_stub(
        tmp_path,
        "float64 = float\n",
        """
        import struct

        import numpy  # the stub: float64 only
        from pymizu import _pymizu

        assert numpy.__file__.endswith("numpy.py")  # the shim took

        # a form-0 row whose scalar class the stub lacks (int64)
        row = bytes([0x50, 0x61, 32, 0]) + struct.pack("<q", 7)
        try:
            _pymizu._read_stream(row)
            raise SystemExit("expected AttributeError")
        except AttributeError:
            pass
        # a form-1 row: the scalar builds, the array constructor is
        # missing
        row = bytes([0x50, 0x61, 14, 1]) + struct.pack("<d", 1.5)
        try:
            _pymizu._read_stream(row)
            raise SystemExit("expected AttributeError")
        except AttributeError:
            pass
        """,
    )


def test_view_read_partial_numpy(tmp_path):
    # a numpy stub whose frombuffer raises, then none at all: the SHM_VEC
    # read falls back to the bare view
    _run_with_numpy_stub(
        tmp_path,
        """
        def frombuffer(*a, **k):
            raise RuntimeError("boom")
        """,
        """
        import numpy  # the stub
        import pymizu

        assert numpy.__file__.endswith("numpy.py")  # the shim took

        src = (
            "ch.send(bytes(range(256)) * 1200); "
            "ch.send(bytes(range(256)) * 1200); "
            "ch.recv(timeout=30)"
        )
        ch = pymizu.Channel.create(src)
        try:
            v1 = ch.recv(timeout=30)
            assert type(v1).__name__ == "_ShmView"  # frombuffer raised
            del numpy.frombuffer
            v2 = ch.recv(timeout=30)
            assert type(v2).__name__ == "_ShmView"  # frombuffer missing
            ch.send(b"")
        finally:
            ch.close()
        """,
    )


def test_raw_read_sink_partial_numpy(tmp_path):
    # a numpy stub whose empty returns a non-buffer: the copied-read
    # sinks' buffer request fails the read informatively, never a crash
    _run_with_numpy_stub(
        tmp_path,
        """
        def empty(n, dtype):
            return object()
        """,
        """
        import pymizu

        try:
            launcher = pymizu.r_launcher(stdout=None, stderr=None)
        except pymizu.MizuError:
            print("no Rscript/mizu — skipping")
            raise SystemExit(0)
        src = r'''
        mizu::mizu_send(ch, c(1L, NA, 3L))
        x <- rep(1L, 200000)
        x[10000] <- NA
        mizu::mizu_send(ch, x)
        mizu::mizu_recv(ch, timeout = 60)
        '''
        ch = pymizu.Channel.create(src, launcher=launcher)
        try:
            try:
                ch.recv(timeout=30)  # the int32 copy's first sink
                raise SystemExit("expected the raw-read TypeError")
            except TypeError:
                pass
            v = ch.recv(timeout=30)
            try:
                v.to_numpy()  # the float64 view sink
                raise SystemExit("expected the to_numpy TypeError")
            except TypeError:
                pass
            ch.send(b"")
        finally:
            ch.close()
        """,
    )


def test_raw_read_f64_sink_partial_numpy(tmp_path):
    # the int32-with-NAs read converts its copy to float64: the second
    # sink's alloc failure and buffer failure both surface as the read's
    # error
    _run_with_numpy_stub(
        tmp_path,
        """
        def empty(n, dtype):
            if dtype == "int32":
                return bytearray(n * 4)
            raise RuntimeError("boom")
        """,
        """
        import numpy  # the stub
        import pymizu

        try:
            launcher = pymizu.r_launcher(stdout=None, stderr=None)
        except pymizu.MizuError:
            print("no Rscript/mizu — skipping")
            raise SystemExit(0)
        src = r'''
        mizu::mizu_send(ch, c(1L, NA, 3L))
        mizu::mizu_send(ch, c(4L, NA, 6L))
        mizu::mizu_recv(ch, timeout = 60)
        '''
        ch = pymizu.Channel.create(src, launcher=launcher)
        try:
            try:
                ch.recv(timeout=30)  # the float64 sink's alloc raises
                raise SystemExit("expected RuntimeError")
            except RuntimeError:
                pass
            numpy.empty = lambda n, dtype: (
                bytearray(n * 4) if dtype == "int32" else object()
            )
            try:
                ch.recv(timeout=30)  # the float64 sink's buffer request
                raise SystemExit("expected TypeError")
            except TypeError:
                pass
            ch.send(b"")
        finally:
            ch.close()
        """,
    )


def test_dimarray_lazy_export_without_numpy(tmp_path):
    # the lazy re-export keeps numpy out of pymizu's own import path:
    # import pymizu stays numpy-free, and touching pymizu.DimArray raises
    # the import error
    _run_without_numpy(
        tmp_path,
        """
        import pymizu

        assert "numpy" not in __import__("sys").modules
        try:
            pymizu.DimArray
            raise SystemExit("expected ImportError")
        except ImportError:
            pass
        """,
    )


def test_labeled_read_without_numpy_declines(tmp_path):
    # a labeled array arriving without numpy declines the way a plain
    # dim array does today (the no-numpy read path's own decline)
    _run_without_numpy(
        tmp_path,
        """
        import pymizu
        from pymizu import _pymizu

        # attr(intv[1,2], names=strv["a","b"]) — the {names} blob's read
        data = bytes.fromhex(
            "49010f07020000000000000001000000020000000d0100000000000000"
            "050000006e616d65730b020000000000000001000000610100000062"
        )
        try:
            _pymizu._read_stream(data)
            raise SystemExit("expected MizuError")
        except pymizu.MizuError as e:
            assert "numpy" in str(e)
        """,
    )
