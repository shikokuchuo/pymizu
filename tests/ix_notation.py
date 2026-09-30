"""The golden corpus's notation, parsed to the Python homes the spec's
tag table pins — the mirror of mizu's helper-interop.R ix_parse, so the
corpus test compares against spec-authored fixtures, never against the
writer's own output. Mirrors libmizu tools/interop_corpus.py's grammar.

Requires numpy (the vector homes are arrays).
"""

import struct
from dataclasses import dataclass, field

import numpy as np

NA_REAL = 0x7FF00000000007A2  # R's verbatim NA_real_ bits


def _na_real():
    return struct.unpack("<d", struct.pack("<Q", NA_REAL))[0]


@dataclass
class ExpectedFrame:
    """A data.frame home: the to_dict() columns, names, and row_names."""
    columns: dict = field(default_factory=dict)
    names: list = field(default_factory=list)
    row_names: object = None  # None | list[str] | int32 array


def load_cases(path):
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split("|")]
        out.append({
            "id": parts[0],
            "kind": parts[1],
            "langs": parts[2],
            "value": parts[3],
            "note": parts[4] if len(parts) > 4 else "",
        })
    return out


def load_corpus(path):
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        k, v = line.split("|")
        out[k.strip()] = v.strip()
    return out


class _Parser:
    def __init__(self, text):
        self.s = text
        self.i = 0

    def ws(self):
        while self.i < len(self.s) and self.s[self.i] in " \t":
            self.i += 1

    def peek(self):
        self.ws()
        return self.s[self.i] if self.i < len(self.s) else ""

    def expect(self, ch):
        if self.peek() != ch:
            raise ValueError(f"expected {ch!r} at {self.i} in {self.s!r}")
        self.i += 1

    def token(self, stop):
        self.ws()
        j = self.i
        while self.i < len(self.s) and self.s[self.i] not in stop:
            self.i += 1
        return self.s[j:self.i].strip()

    def string(self):
        self.expect('"')
        out = []
        while True:
            if self.i >= len(self.s):
                raise ValueError("unterminated string")
            c = self.s[self.i]
            self.i += 1
            if c == '"':
                return "".join(out)
            if c == "\\":
                e = self.s[self.i]
                self.i += 1
                if e == "n":
                    out.append("\n")
                elif e == "t":
                    out.append("\t")
                elif e == "r":
                    out.append("\r")
                elif e in ('"', "\\"):
                    out.append(e)
                elif e == "u":
                    out.append(chr(int(self.s[self.i:self.i + 4], 16)))
                    self.i += 4
                else:
                    raise ValueError(f"bad escape \\{e}")
            else:
                out.append(c)

    def real_atom(self, stop):
        tok = self.token(stop)
        if tok == "na":
            return _na_real()
        if tok == "nan":
            return float("nan")
        if tok == "inf":
            return float("inf")
        if tok == "-inf":
            return float("-inf")
        if tok.startswith("bits:"):
            return struct.unpack("<d", struct.pack("<Q", int(tok[5:], 16)))[0]
        return float(tok)

    def value(self):
        self.ws()
        for name in ("lglv", "intv", "realv", "cplxv", "rawv", "strv",
                     "i64v", "list", "tuple", "dict", "attr", "lgl", "int",
                     "real", "cplx", "str", "bytes", "err", "task", "nil"):
            if self.s.startswith(name, self.i):
                self.i += len(name)
                return getattr(self, "v_" + name)()
        raise ValueError(f"bad value at {self.i} of {self.s!r}")

    def v_nil(self):
        return None

    def v_lgl(self):
        self.expect("(")
        tok = self.token(")")
        self.expect(")")
        return {"0": False, "1": True, "na": None}[tok]

    def v_int(self):
        self.expect("(")
        tok = self.token(")")
        self.expect(")")
        return None if tok == "na" else int(tok)

    def v_real(self):
        self.expect("(")
        v = self.real_atom(")")
        self.expect(")")
        return v

    def v_cplx(self):
        self.expect("(")
        re = self.real_atom(",")
        self.expect(",")
        im = self.real_atom(")")
        self.expect(")")
        return complex(re, im)

    def v_str(self):
        self.expect("(")
        v = self.string() if self.peek() == '"' else None
        if self.peek() != '"':
            self.token(")")
        self.expect(")")
        return v

    def v_bytes(self):
        self.expect("(")
        v = bytes.fromhex(self.token(")"))
        self.expect(")")
        return v

    def seq(self):
        items = []
        while self.peek() != ")":
            items.append(self.value())
            if self.peek() == ",":
                self.i += 1
        self.expect(")")
        return items

    def v_list(self):
        self.expect("(")
        return self.seq()

    def v_tuple(self):
        self.expect("(")
        return tuple(self.seq())

    def pairs(self):
        keys, values = [], []
        while self.peek() != ")":
            if self.peek() == '"':
                keys.append(self.string())
            else:
                tok = self.token("=")
                try:
                    keys.append(int(tok))   # a non-str key (wd-nonstr-key)
                except ValueError:
                    keys.append(tok)
            self.expect("=")
            values.append(self.value())
            if self.peek() == ",":
                self.i += 1
        self.expect(")")
        return keys, values

    def v_dict(self):
        self.expect("(")
        keys, values = self.pairs()
        return dict(zip(keys, values, strict=True))

    def v_attr(self):
        self.expect("(")
        x = self.value()
        self.expect(",")
        keys, values = self.pairs()
        return attr_home(x, dict(zip(keys, values, strict=True)))

    def v_strv(self):
        self.expect("[")
        elts = []
        while self.peek() != "]":
            if self.peek() == '"':
                elts.append(self.string())
            else:
                if self.token(",]") != "na":
                    raise ValueError("bad strv element")
                elts.append(None)
            if self.peek() == ",":
                self.i += 1
        self.expect("]")
        return elts

    def v_lglv(self):
        self.expect("[")
        toks = self.elt_tokens()
        if "na" in toks:
            return np.array(
                [0 if t == "na" else int(t) for t in
                 [t if t != "na" else "-2147483648" for t in toks]],
                dtype=np.int32)
        return np.array([int(t) for t in toks], dtype=np.bool_)

    def v_intv(self):
        self.expect("[")
        toks = self.elt_tokens()
        if "na" in toks:
            return np.array(
                [_na_real() if t == "na" else float(t) for t in toks],
                dtype=np.float64)
        return np.array([int(t) for t in toks], dtype=np.int32)

    def v_realv(self):
        self.expect("[")
        elts = []
        while self.peek() != "]":
            elts.append(self.real_atom(",]"))
            if self.peek() == ",":
                self.i += 1
        self.expect("]")
        return np.array(elts, dtype=np.float64)

    def v_cplxv(self):
        self.expect("[")
        elts = []
        while self.peek() != "]":
            if not self.s.startswith("cplx(", self.i):
                raise ValueError("bad cplxv element")
            self.i += 5
            re = self.real_atom(",")
            self.expect(",")
            im = self.real_atom(")")
            self.expect(")")
            elts.append(complex(re, im))
            if self.peek() == ",":
                self.i += 1
        self.expect("]")
        return np.array(elts, dtype=np.complex128)

    def v_rawv(self):
        self.expect("[")
        toks = self.elt_tokens()
        return np.array([int(t, 16) for t in toks], dtype=np.uint8)

    def v_i64v(self):
        self.expect("[")
        toks = self.elt_tokens()
        return np.array(
            [-9223372036854775808 if t == "na" else int(t) for t in toks],
            dtype=np.int64)

    def elt_tokens(self):
        toks = []
        while self.peek() != "]":
            toks.append(self.token(",]"))
            if self.peek() == ",":
                self.i += 1
        self.expect("]")
        return toks


def _str_list(v):
    """A str scalar or strv, as a list."""
    if isinstance(v, str) or v is None:
        return [v]
    return list(v)


def _factor_home(codes, levels):
    out = []
    na_bits = struct.pack("<Q", NA_REAL)
    for c in np.atleast_1d(codes):
        if isinstance(c, float) and struct.pack("<d", c) == na_bits:
            out.append(None)
        else:
            v = int(c)
            out.append(None if v == -2147483648 else levels[v - 1])
    return out


def _frame_home(cols, attrs):
    names = _str_list(attrs["names"])
    row_names = attrs["row.names"]
    if (isinstance(row_names, np.ndarray)
            and row_names.dtype == np.float64
            and np.isnan(row_names[0])):
        row_names = None  # the compact automatic form c(NA, +-n)
    elif isinstance(row_names, int):
        row_names = np.array([row_names], dtype=np.int32)
    return ExpectedFrame(columns=dict(zip(names, cols, strict=True)),
                         names=names, row_names=row_names)


def attr_home(x, attrs):
    klass = attrs.get("class")
    klass = _str_list(klass) if klass is not None else None
    if klass == ["factor"]:
        levels = _str_list(attrs["levels"])
        codes = np.atleast_1d(x)
        return _factor_home(codes, levels)
    if klass == ["data.frame"]:
        cols = x if isinstance(x, list) else [x]
        return _frame_home(cols, attrs)
    if set(attrs) == {"dim"}:
        dims = attrs["dim"]
        if isinstance(dims, int):
            dims = [dims]
        else:
            dims = dims.view(np.int32).tolist()
        if len(dims) == 1:
            return x  # the length-1 shift: a plain vector
        return np.asarray(x).reshape(dims, order="F")
    if klass == ["Date"]:
        return np.asarray(x).astype("datetime64[D]")
    if klass == ["POSIXct", "POSIXt"]:
        us = np.round(np.asarray(x, dtype=np.float64) * 1e6).astype(np.int64)
        isnat = np.array(
            [struct.pack("<d", v) == struct.pack("<Q", NA_REAL) for v in x])
        out = us.astype("datetime64[us]")
        out[isnat] = np.datetime64("NaT")
        return out
    raise ValueError(f"no Python home for attr class {klass!r}")


def parse(text):
    p = _Parser(text)
    v = p.value()
    p.ws()
    if p.i < len(p.s):
        raise ValueError(f"trailing notation at {p.i} of {text!r}")
    return v


def _float_bits(x):
    return struct.pack("<d", x)


def _cplx_bits(z):
    return struct.pack("<dd", z.real, z.imag)


def _array_same(a, b):
    if not isinstance(b, np.ndarray):
        return False
    a = np.asarray(a)
    if a.shape != b.shape:
        return False
    if a.dtype.kind in "fc" or b.dtype.kind in "fc":
        # bitwise, NaN payloads included
        av = a.view(np.uint64) if a.dtype.kind in "fc" else a
        bv = b.view(np.uint64) if b.dtype.kind in "fc" else b
        return np.array_equal(av, bv)
    if a.dtype.kind == "M" or b.dtype.kind == "M":
        return np.array_equal(a.view(np.int64), b.view(np.int64))
    return np.array_equal(a, b)


def ix_same(a, b):
    """The corpus comparison: exact, NaN-payload-bitwise."""
    if isinstance(b, ExpectedFrame):
        return frame_same(a, b)
    if isinstance(b, np.ndarray):
        return _array_same(a, b)
    if isinstance(b, float):
        return isinstance(a, float) and _float_bits(a) == _float_bits(b)
    if isinstance(b, complex):
        return isinstance(a, complex) and _cplx_bits(a) == _cplx_bits(b)
    if isinstance(b, (list, tuple)):
        if not isinstance(a, (list, tuple)) or len(a) != len(b):
            return False
        return all(ix_same(x, y) for x, y in zip(a, b, strict=True))
    if isinstance(b, dict):
        if not isinstance(a, dict) or set(a) != set(b):
            return False
        return all(ix_same(a[k], b[k]) for k in b)
    return type(a) is type(b) and a == b


def _col_same(a, b):
    if isinstance(b, np.ndarray):
        return _array_same(a, b)
    return ix_same(a, b)


def frame_same(f, exp):
    if not isinstance(f, _pymizu_frame()):
        return False
    if list(f.names) != exp.names:
        return False
    d = f.to_dict()
    if set(d) != set(exp.columns):
        return False
    if not all(_col_same(d[k], exp.columns[k]) for k in exp.columns):
        return False
    rn = f.row_names
    en = exp.row_names
    if en is None:
        return rn is None
    if isinstance(en, np.ndarray):
        return isinstance(rn, np.ndarray) and _array_same(rn, en)
    return list(rn) == list(en) if isinstance(rn, list) else False


def _pymizu_frame():
    import pymizu
    return pymizu.Frame
