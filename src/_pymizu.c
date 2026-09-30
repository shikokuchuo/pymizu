/* pymizu — Python binding for libmizu (raw CPython C API).
 *
 * The extension compiles the vendored core sources directly, so the module
 * is self-contained: no system libmizu is required or consulted.
 *
 * GIL policy, per handle kind. Submitter handles (channels, pool
 * controllers/submitters — exec is NULL) release the GIL around every verb;
 * the stage/read/check callbacks reacquire it with PyGILState_Ensure, and
 * the park hook stays NULL. Worker (exec-capable) handles hold the GIL
 * through run and collect — nested-collect help reenters exec_fn from
 * inside the core's collect path — and register the around-park hook, which
 * drops the GIL for each bounded sleep so the worker process's other
 * threads run. Staging pins nothing (pickle streams and bare vector bytes
 * are self-contained; out-of-line regions ride the core's retain table), so
 * the drop hook is NULL too.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <methodobject.h>   /* PyCFunction_CheckExact */

/* The exact function-type check, without funcobject.h: its location moves
   across versions (top-level on 3.10, cpython/ on 3.13+), and the symbol
   is all we need — a public export, redeclared as the header would. */
PyAPI_DATA(PyTypeObject) PyFunction_Type;
#define PyFunction_CheckExact(op) Py_IS_TYPE((op), &PyFunction_Type)

#include <math.h>
#include <stdio.h>
#include <string.h>

#include "mizu.h"
#include "mizu_ext.h"
#include "pymap.h"
#include "pyinterop.h"

#define MIZU_STR_(x) #x
#define MIZU_STR(x) MIZU_STR_(x)
#define MIZU_VERSION_STRING                                                \
  MIZU_STR(MIZU_VERSION_MAJOR) "." MIZU_STR(MIZU_VERSION_MINOR) "."           \
  MIZU_STR(MIZU_VERSION_PATCH)

static PyTypeObject MizuChannelType;
static PyTypeObject MizuPoolType;
static PyTypeObject MizuTaskType;
static PyTypeObject MizuCaughtType;
static PyTypeObject MizuShmViewType;
static PyTypeObject MizuShmStrViewType;
static PyTypeObject MizuShmOwnerType;
static PyTypeObject MizuTaskFrameType;

/* The REF stage for a received view re-sent whole (defined with the view
   types below; stage_impl runs first in the file). */
static int stage_ref(PyObject *obj, const Py_buffer *v, mizu_slot_hdr *hdr,
                     uint8_t *payload, uint32_t inline_max);
static int stage_ref_str(PyObject *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                         uint32_t inline_max);
static PyObject *strview_to_list(PyObject *obj, PyObject *dummy);

static PyObject *numpy_module(void);

PyObject *mizu_py_numpy_module(void) {
  return numpy_module();
}

// Module state -------------------------------------------------------------------

static PyObject *MizuError;           /* base */
static PyObject *MizuStartupError;    /* child failed to attach in time */
static PyObject *MizuShmError;        /* region create/open failure */
static PyObject *MizuSubmitTimeoutError;   /* ring full past the deadline */
static PyObject *MizuSlotsExhaustedError;
static PyObject *MizuStoppedError;
static PyObject *MizuCancelledError;
static PyObject *MizuWorkerDiedError;
static PyObject *MizuTaskError;       /* a task's own error, re-raised */
static PyObject *MizuDeclinedError;   /* a foreign-handle send-time decline */

/* traceback.format_exception, resolved lazily on the first task error (the
   error path is cold; importing it at module init is not). */
static PyObject *mizu_traceback_fmt;

/* pickle, or cloudpickle when installed (used transparently — its output is
   a standard protocol stream). Resolved at module init; protocol pinned to 4. */
static PyObject *mizu_dumps, *mizu_loads;

/* numpy, probed lazily on the first raw-vector read: the module, or Py_None
   when absent. Never a build-time dependency. */
static PyObject *mizu_numpy;
static int mizu_numpy_probed;

// Sentinels ----------------------------------------------------------------------

/* The four terminal-state singletons. Identity is the contract
   (`x is pymizu.TIMEOUT`); they are falsy so a bare `if ch.recv():` loop
   exits on any terminal state. */
typedef struct {
  PyObject_HEAD
  const char *name;
} MizuSentinel;

static PyObject *SentFull, *SentTimeout, *SentClosed, *SentGone;

static PyObject *sent_repr(PyObject *self) {
  return PyUnicode_FromFormat("pymizu.%s", ((MizuSentinel *) self)->name);
}

static int sent_bool(PyObject *Py_UNUSED(self)) {
  return 0;
}

static PyNumberMethods sent_as_number = {
  .nb_bool = sent_bool,
};

static PyTypeObject MizuSentinelType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "pymizu.Sentinel",
  .tp_basicsize = sizeof(MizuSentinel),
  .tp_repr = sent_repr,
  .tp_as_number = &sent_as_number,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "Terminal-state sentinel returned by pymizu verbs. "
            "Identity-tested against pymizu.FULL / TIMEOUT / CLOSED / PEER_GONE.",
};

static PyObject *sentinel_new(const char *name) {
  MizuSentinel *s = (MizuSentinel *) MizuSentinelType.tp_alloc(&MizuSentinelType, 0);
  if (s == NULL) return NULL;
  s->name = name;
  return (PyObject *) s;
}

// Outcome boxes (the mizu_caught mirror) --------------------------------------

/* A collect's non-OK outcome comes back boxed: the exception instance rides
   inside, so a task value that is itself an exception object stays bare.
   The collect veneer unwraps the box and raises. */
typedef struct {
  PyObject_HEAD
  PyObject *exc;
} MizuCaught;

static void Caught_dealloc(MizuCaught *self) {
  Py_DECREF(self->exc);
  MizuCaughtType.tp_free((PyObject *) self);
}

static PyTypeObject MizuCaughtType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._Caught",
  .tp_basicsize = sizeof(MizuCaught),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A collected task outcome, boxed for the veneer to raise.",
  .tp_dealloc = (destructor) Caught_dealloc,
};

static PyObject *caught_new(PyObject *exc) {   /* steals exc */
  if (exc == NULL) return NULL;
  MizuCaught *c = (MizuCaught *) MizuCaughtType.tp_alloc(&MizuCaughtType, 0);
  if (c == NULL) {
    Py_DECREF(exc);
    return NULL;
  }
  c->exc = exc;
  return (PyObject *) c;
}

// Task frames (the Pool.submit payload marker) --------------------------------

/* A plain tuple subclass, items 0/1/2 = fn/args/kwargs, no extra fields.
   The subclass marks pool task payloads for the structured frame codec;
   a user sending a plain (fn, args, kwargs) tuple over a channel keeps
   pickle semantics. Not exported as a type — the facade builds frames
   through the _task_frame factory, so users cannot build frames that
   bypass pickle semantics on channels. GC/traverse/free inherit from
   tuple. */
/* tp_base is set in PyInit: &PyTuple_Type is not a compile-time constant
   on Windows (dllimport). */
static PyTypeObject MizuTaskFrameType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._TaskFrame",
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A pool task payload marked for the structured frame codec.",
};

static PyObject *task_frame_new(PyObject *fn, PyObject *args,
                                PyObject *kwargs) {
  PyObject *f = MizuTaskFrameType.tp_alloc(&MizuTaskFrameType, 3);
  if (f == NULL) return NULL;
  Py_INCREF(fn);
  Py_INCREF(args);
  Py_INCREF(kwargs);
  PyTuple_SET_ITEM(f, 0, fn);
  PyTuple_SET_ITEM(f, 1, args);
  PyTuple_SET_ITEM(f, 2, kwargs);
  return f;
}

// Errors -------------------------------------------------------------------------

/* create/attach failure: the core composed the message (size + hint) in the
   thread-local slot. Space/existence failures carry the shm class. */
static int raise_tls(void) {
  mizu_errcat cat = mizu_last_error_category();
  const char *msg = mizu_last_error_message();
  PyObject *exc = (cat == MIZU_ERRCAT_NOSPACE || cat == MIZU_ERRCAT_NOMEMORY ||
                   cat == MIZU_ERRCAT_EXISTS) ? MizuShmError : MizuError;
  PyErr_Format(exc, "pymizu: %s", msg);
  return -1;
}

// Arrow C Data Interface ---------------------------------------------------------

/* The stable C ABI structs live in pyinterop.h (shared with interop.c,
   which adds the ArrowArrayStream half). */

// Staging ------------------------------------------------------------------------

/* Frame n bytes over the INLINE / ARENA / SHM_RAW tiers — the reference
   stager's (bytes.c) discipline: one arena chunk past the inline budget,
   else a reap and a spill region retained SPILL (surrendered to the free
   list at consumer-done). The INLINE frame stamps the keeperless claim:
   the streams this frames (pickle, the spilled codec and task frames)
   commit no retain-table entry of their own. */
static int stage_bytes(const uint8_t *src, size_t n, mizu_slot_hdr *hdr,
                       uint8_t *payload, uint32_t inline_max, mizu_handle *h) {
  if (n == 0) {
    hdr->kind = MIZU_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return 0;
  }
  if (n <= (size_t) inline_max) {
    memcpy(payload, src, n);
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = MIZU_AUX_F_KEEPERLESS;
    return 0;
  }
  uint64_t off;
  uint8_t *chunk = mizu_stage_arena_alloc(h, MIZU_ALIGN64(n), &off);
  if (chunk != NULL) {
    memcpy(chunk, src, n);
    hdr->kind = MIZU_KIND_ARENA;
    hdr->len = 0;
    hdr->aux = off;
    uint64_t n64 = (uint64_t) n;
    memcpy(payload, &n64, sizeof(n64));
    return 0;
  }
  mizu_stage_reap(h);
  mizu_shm *shm;
  if (mizu_stage_spill_get(h, n, &shm) != MIZU_OK) {
    PyErr_Format(MizuShmError, "pymizu: cannot create payload region "
                 "(%zu bytes): %s", n, mizu_last_error_message());
    return 1;
  }
  memcpy(shm->addr, src, n);
  hdr->kind = MIZU_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;   /* exact length: a recycled region carries slack */
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain(h, shm);
  return 0;
}

int mizu_py_stage_bytes(const uint8_t *src, size_t n, mizu_slot_hdr *hdr,
                        uint8_t *payload, uint32_t inline_max,
                        mizu_handle *h) {
  return stage_bytes(src, n, hdr, payload, inline_max, h);
}

/* The O(1) raw-tier gate (the mirror of R's attribute-free/non-ALTREP gate):
   C-contiguous, native byte order, at most 1-D, and a dtype that maps
   width-exactly onto a wire type. No bool: numpy bool is 1 byte/elt where
   R logical is 4 — the mapping would misread at 4x stride. Everything else
   falls to pickle. */
static int wire_type_of(const Py_buffer *v) {
  if (v->ndim > 1) return 0;
  const char *f = v->format;
  if (f == NULL) return v->itemsize == 1 ? MIZU_TYPE_RAW : 0;
  if (f[1] == '\0') {
    switch (f[0]) {
    case 'B': return v->itemsize == 1 ? MIZU_TYPE_RAW : 0;
    case 'd': return v->itemsize == 8 ? MIZU_TYPE_REAL : 0;
    case 'i': return v->itemsize == 4 ? MIZU_TYPE_INT : 0;
    /* numpy exports int64 as 8-byte 'l' (C long) on LP64, 'q' on
       Windows; int32 as 4-byte 'l' on Windows */
    case 'l': return v->itemsize == 4 ? MIZU_TYPE_INT :
      v->itemsize == 8 ? MIZU_TYPE_INT64 : 0;
    case 'q': return v->itemsize == 8 ? MIZU_TYPE_INT64 : 0;
    }
    return 0;
  }
  if (f[0] == 'Z' && f[1] == 'd' && f[2] == '\0')
    return v->itemsize == 16 ? MIZU_TYPE_CPLX : 0;
  return 0;
}

/* map.c's raw-x gate rides the same dtype map. */
int mizu_py_wire_type_of(const Py_buffer *v) {
  return wire_type_of(v);
}



/* The buffer gates' subclass rejection: an object whose type is a strict
   subclass of a known base type keeps its pickle semantics — the raw tier
   carries the base buffer only, so a MaskedArray would lose its mask, a
   units-carrying subclass its units, a bytes subclass its type. The exact
   base types (bytes, bytearray, memoryview, array.array, numpy.ndarray,
   numpy's own scalar types), and any exporter subclassing none of them (a
   pyarrow.Buffer), stay raw. numpy and array are looked up in sys.modules
   through the module dict: neither can have produced an instance unless
   already imported, and staging must not import numpy for non-numpy
   users. The type objects are cached on first sight; the hot path pays
   one pointer compare. */
static PyTypeObject *mizu_array_type, *mizu_np_ndarray, *mizu_np_generic;
static PyObject *mizu_np_scalars;   /* frozenset of numpy's own scalar types */
static int mizu_buf_types_probed;

/* One attribute fetch for the probe: NULL leaves the gate without the
   type, never with a pending exception. */
static PyTypeObject *buffer_type_attr(PyObject *mod, const char *name) {
  PyObject *t = PyObject_GetAttrString(mod, name);
  if (t != NULL && PyType_Check(t)) return (PyTypeObject *) t;
  Py_XDECREF(t);
  PyErr_Clear();
  return NULL;
}

static void buffer_types_probe(void) {
  if (mizu_buf_types_probed) return;
  mizu_buf_types_probed = 1;
  PyObject *mods = PyImport_GetModuleDict();
  PyObject *ar = PyMapping_GetItemString(mods, "array");
  if (ar == NULL) {
    PyErr_Clear();
  } else {
    if (ar != Py_None)   /* sys.modules["array"] = None blocks imports */
      mizu_array_type = buffer_type_attr(ar, "array");
    Py_DECREF(ar);
  }
  PyObject *np = PyMapping_GetItemString(mods, "numpy");
  if (np == NULL) {
    PyErr_Clear();
    return;
  }
  if (np != Py_None) {   /* sys.modules["numpy"] = None blocks imports */
    mizu_np_ndarray = buffer_type_attr(np, "ndarray");
    mizu_np_generic = buffer_type_attr(np, "generic");
    if (mizu_np_generic != NULL) {
      /* numpy's own scalar types: the np.generic subtypes in its
         namespace (the abstract intermediates are harmless members —
         never an object's type) */
      PyObject *set = PyFrozenSet_New(NULL);
      if (set != NULL) {
        PyObject *d = PyModule_GetDict(np);   /* borrowed */
        PyObject *k, *v;
        Py_ssize_t pos = 0;
        int rc = 0;
        while (rc == 0 && PyDict_Next(d, &pos, &k, &v))
          if (PyType_Check(v) &&
              PyType_IsSubtype((PyTypeObject *) v, mizu_np_generic))
            rc = PySet_Add(set, v);
        if (rc != 0) Py_CLEAR(set);
      }
      if (set == NULL) PyErr_Clear();
      mizu_np_scalars = set;
    }
  }
  Py_DECREF(np);
}

static int buffer_subclass_reject(PyObject *obj) {
  PyTypeObject *tp = Py_TYPE(obj);
  buffer_types_probe();
  if (tp == &PyBytes_Type || tp == &PyByteArray_Type ||
      tp == &PyMemoryView_Type || tp == mizu_array_type ||
      tp == mizu_np_ndarray)
    return 0;
  if (PyType_IsSubtype(tp, &PyBytes_Type) ||
      PyType_IsSubtype(tp, &PyByteArray_Type) ||
      PyType_IsSubtype(tp, &PyMemoryView_Type))
    return 1;
  if (mizu_array_type != NULL && PyType_IsSubtype(tp, mizu_array_type))
    return 1;
  if (mizu_np_ndarray != NULL && PyType_IsSubtype(tp, mizu_np_ndarray))
    return 1;
  if (mizu_np_generic != NULL && mizu_np_scalars != NULL &&
      PyType_IsSubtype(tp, mizu_np_generic)) {
    int known = PySet_Contains(mizu_np_scalars, (PyObject *) tp);
    if (known < 0) {
      PyErr_Clear();          /* unreachable: type objects hash */
      return 0;
    }
    return !known;
  }
  return 0;
}

/* interop.c's buffer-leaf gates ride the same probes. */
int mizu_py_buffer_subclass_reject(PyObject *obj) {
  return buffer_subclass_reject(obj);
}

/* 1 exact ndarray, 2 a numpy scalar, 0 neither (unknown types: 0). */
int mizu_py_np_kind(PyObject *obj) {
  buffer_types_probe();
  PyTypeObject *tp = Py_TYPE(obj);
  if (tp == mizu_np_ndarray) return 1;
  if (mizu_np_scalars != NULL &&
      PySet_Contains(mizu_np_scalars, (PyObject *) tp) == 1)
    return 2;
  return 0;
}

/* The raw-tier reserve is the core's (mizu_stage_raw): RAWVEC inline within
   the budget; past it the zero-copy SHM_VEC tier at max(inline budget,
   MIZU_ZC_FLOOR), with the copy tiers as the cheaper small end and the churn
   fallback (the channel arena copy has no region machinery to amortize up
   to MIZU_ZC_FLOOR_RAW and is churn-immune at any size; a pool's raw spill
   is itself a region, so the view's no-copy receive wins from the floor).
   The reservation fills the slot/MIZH headers and returns the destination
   pointer; the write half (a memcpy or a conversion) cannot fail. NULL on
   reservation failure: the caller falls back to a copy tier, then pickle. */

/* Identity staging: reserve, then one memcpy. Returns 0 staged, -1
   pickle. */
static int stage_raw(const Py_buffer *v, int type, mizu_slot_hdr *hdr,
                     uint8_t *payload, uint32_t inline_max, mizu_handle *h) {
  size_t n = (size_t) v->len;
  uint8_t *dst = mizu_stage_raw(h, (uint64_t) n, type, hdr, payload,
                               inline_max);
  if (dst == NULL) return -1;
  memcpy(dst, v->buf, n);
  return 0;
}

// Conversion staging -------------------------------------------------------------

/* Every fixed-width numeric dtype crosses to R, converted once at stage
   time, fused into the copy that staging always is. One converter table,
   two front-ends: PEP 3118 buffer format chars (numpy) and Arrow C Data
   Interface format strings (pyarrow, polars). Identity rows are a plain
   memcpy; the rest convert straight into the reservation — no temp
   buffer (a temp pays a second full memcpy plus a large malloc precisely
   on the spill tiers, where arrays are largest). Channel handles only —
   the gate is at the call sites: pools are Python-both-ends by
   construction and keep the lossless pickle path for non-identity
   dtypes. */

/* R's missing-value sentinels are the core's MIZU_NA_* wire constants
   (the R ABI fixes the bit patterns; pymizu cannot include R headers).
   Little-endian throughout (all supported platforms are; the codec
   relies on it). */

/* The CVT_* enum, cvt_row and cvt_warn are pyinterop.h's (shared with
   interop.c). */

static const cvt_row CVT_ROW_U8 = { MIZU_TYPE_RAW, CVT_COPY, 1, 1 };
static const cvt_row CVT_ROW_I8 = { MIZU_TYPE_INT, CVT_I8_INT, 1, 4 };
static const cvt_row CVT_ROW_I16 = { MIZU_TYPE_INT, CVT_I16_INT, 2, 4 };
static const cvt_row CVT_ROW_U16 = { MIZU_TYPE_INT, CVT_U16_INT, 2, 4 };
static const cvt_row CVT_ROW_I32 = { MIZU_TYPE_INT, CVT_COPY, 4, 4 };
static const cvt_row CVT_ROW_U32 = { MIZU_TYPE_REAL, CVT_U32_REAL, 4, 8 };
static const cvt_row CVT_ROW_I64 = { MIZU_TYPE_INT64, CVT_COPY, 8, 8 };
static const cvt_row CVT_ROW_U64 = { MIZU_TYPE_REAL, CVT_U64_REAL, 8, 8 };
static const cvt_row CVT_ROW_F32 = { MIZU_TYPE_REAL, CVT_F32_REAL, 4, 8 };
static const cvt_row CVT_ROW_F64 = { MIZU_TYPE_REAL, CVT_COPY, 8, 8 };
static const cvt_row CVT_ROW_BOOL8 = { MIZU_TYPE_LGL, CVT_BOOL8_LGL, 1, 4 };
static const cvt_row CVT_ROW_BOOLBIT = { MIZU_TYPE_LGL, CVT_BIT_LGL, 0, 4 };
static const cvt_row CVT_ROW_C64 = { MIZU_TYPE_CPLX, CVT_C64_CPLX, 8, 16 };
static const cvt_row CVT_ROW_C128 = { MIZU_TYPE_CPLX, CVT_COPY, 16, 16 };

/* Key the table by a PEP 3118 (char, itemsize) pair — as wire_type_of
   does (numpy exports int64 as 8-byte 'l' on LP64, 'q' on Windows;
   int32 as 4-byte 'l' on Windows). Byte-order prefixes: '=' and '<' are
   accepted (all supported platforms are little-endian, and ctypes
   buffers export '<i'); '>' and '!' reject — a big-endian array must
   never be misread as native. */
static const cvt_row *cvt_for_buffer(const char *f, Py_ssize_t itemsize) {
  if (f == NULL) return itemsize == 1 ? &CVT_ROW_U8 : NULL;
  if (f[0] == '=' || f[0] == '<') f++;
  if (f[1] == '\0') {
    switch (f[0]) {
    case 'B': return itemsize == 1 ? &CVT_ROW_U8 : NULL;
    case 'b': return itemsize == 1 ? &CVT_ROW_I8 : NULL;
    case 'h': return itemsize == 2 ? &CVT_ROW_I16 : NULL;
    case 'H': return itemsize == 2 ? &CVT_ROW_U16 : NULL;
    case 'i': return itemsize == 4 ? &CVT_ROW_I32 : NULL;
    case 'l': return itemsize == 4 ? &CVT_ROW_I32 :
      itemsize == 8 ? &CVT_ROW_I64 : NULL;
    case 'q': return itemsize == 8 ? &CVT_ROW_I64 : NULL;
    case 'I': return itemsize == 4 ? &CVT_ROW_U32 : NULL;
    case 'L': return itemsize == 4 ? &CVT_ROW_U32 :
      itemsize == 8 ? &CVT_ROW_U64 : NULL;
    case 'Q': return itemsize == 8 ? &CVT_ROW_U64 : NULL;
    case 'f': return itemsize == 4 ? &CVT_ROW_F32 : NULL;
    case 'd': return itemsize == 8 ? &CVT_ROW_F64 : NULL;
    case '?': return itemsize == 1 ? &CVT_ROW_BOOL8 : NULL;
    }
    return NULL;
  }
  if (f[0] == 'Z' && f[2] == '\0') {
    if (f[1] == 'f') return itemsize == 8 ? &CVT_ROW_C64 : NULL;
    if (f[1] == 'd') return itemsize == 16 ? &CVT_ROW_C128 : NULL;
  }
  return NULL;
}

/* Key the table by an Arrow C Data Interface format string (every
   supported row is a single character). */
static const cvt_row *cvt_for_arrow(const char *f) {
  if (f[1] != '\0') return NULL;
  switch (f[0]) {
  case 'C': return &CVT_ROW_U8;
  case 'c': return &CVT_ROW_I8;
  case 's': return &CVT_ROW_I16;
  case 'S': return &CVT_ROW_U16;
  case 'i': return &CVT_ROW_I32;
  case 'I': return &CVT_ROW_U32;
  case 'l': return &CVT_ROW_I64;
  case 'L': return &CVT_ROW_U64;
  case 'f': return &CVT_ROW_F32;
  case 'g': return &CVT_ROW_F64;
  case 'b': return &CVT_ROW_BOOLBIT;
  }
  return NULL;
}


/* Convert a run of n valid elements. Per-element memcpy keeps every
   access alignment-safe (a contiguous buffer can still be
   element-misaligned — np.frombuffer with a byte offset); fixed-size
   memcpys compile to single load/store pairs. */
static void cvt_run(uint8_t *dst, const uint8_t *src, size_t n,
                    const cvt_row *row, cvt_warn *warn) {
  switch (row->cvt) {
  case CVT_COPY:
    memcpy(dst, src, n * row->w_out);
    break;
  case CVT_I8_INT:
    for (size_t i = 0; i < n; i++) {
      int8_t v;
      memcpy(&v, src + i, 1);
      int32_t o = v;
      memcpy(dst + 4 * i, &o, 4);
    }
    break;
  case CVT_I16_INT:
    for (size_t i = 0; i < n; i++) {
      int16_t v;
      memcpy(&v, src + 2 * i, 2);
      int32_t o = v;
      memcpy(dst + 4 * i, &o, 4);
    }
    break;
  case CVT_U16_INT:
    for (size_t i = 0; i < n; i++) {
      uint16_t v;
      memcpy(&v, src + 2 * i, 2);
      int32_t o = v;
      memcpy(dst + 4 * i, &o, 4);
    }
    break;
  case CVT_U32_REAL:
    for (size_t i = 0; i < n; i++) {
      uint32_t v;
      memcpy(&v, src + 4 * i, 4);
      double o = (double) v;
      memcpy(dst + 8 * i, &o, 8);
    }
    break;
  case CVT_U64_REAL:
    for (size_t i = 0; i < n; i++) {
      uint64_t v;
      memcpy(&v, src + 8 * i, 8);
      if (v > 9007199254740992ULL) {
        mizu_store_na_real(dst + 8 * i);
        warn->n_range++;
      } else {
        double o = (double) v;
        memcpy(dst + 8 * i, &o, 8);
      }
    }
    break;
  case CVT_F32_REAL:
    for (size_t i = 0; i < n; i++) {
      float v;
      memcpy(&v, src + 4 * i, 4);
      double o = v;
      memcpy(dst + 8 * i, &o, 8);
    }
    break;
  case CVT_BOOL8_LGL:
    for (size_t i = 0; i < n; i++) {
      int32_t o = src[i] != 0;
      memcpy(dst + 4 * i, &o, 4);
    }
    break;
  case CVT_C64_CPLX:
    for (size_t i = 0; i < n; i++) {
      float re, im;
      memcpy(&re, src + 8 * i, 4);
      memcpy(&im, src + 8 * i + 4, 4);
      double o[2] = { re, im };
      memcpy(dst + 16 * i, o, 16);
    }
    break;
  }
}

/* Fill n lanes with the wire type's missing sentinel. */
static void cvt_fill_na(uint8_t *dst, size_t n, int wire) {
  if (wire == MIZU_TYPE_REAL) {
    for (size_t i = 0; i < n; i++) mizu_store_na_real(dst + 8 * i);
  } else if (wire == MIZU_TYPE_INT64) {
    const int64_t na = MIZU_NA_INT64;   /* reads as NA_integer64_ in R */
    for (size_t i = 0; i < n; i++) memcpy(dst + 8 * i, &na, 8);
  } else {
    const int32_t na = MIZU_NA_INT32;   /* INT and LGL share the sentinel */
    for (size_t i = 0; i < n; i++) memcpy(dst + 4 * i, &na, 4);
  }
}

/* Load lanes (<= 64) validity bits starting at bit position pos (Arrow
   LSB-first bit order), reading only the bytes that cover them. */
static uint64_t bitmap_word(const uint8_t *bm, uint64_t pos, size_t lanes) {
  const uint8_t *p = bm + (pos >> 3);
  unsigned sh = (unsigned) (pos & 7);
  if (sh == 0 && lanes == 64) {
    uint64_t w;
    memcpy(&w, p, 8);
    return w;
  }
  size_t nbytes = (sh + lanes + 7) >> 3;
  uint64_t lo = 0, hi = 0;
  for (size_t j = 0; j < nbytes; j++) {
    if (j < 8) lo |= (uint64_t) p[j] << (8 * j);
    else hi = p[j];   /* j == 8: only when sh > 0 and lanes == 64 */
  }
  uint64_t w = sh != 0 ? (lo >> sh) | (hi << (64 - sh)) : lo;
  if (lanes < 64) w &= ((uint64_t) 1 << lanes) - 1;
  return w;
}

/* The masked variant (Arrow nulls): word-wise over the validity bitmap —
   an all-ones word a 64-lane convert, a zero word a 64-sentinel fill,
   mixed the per-lane select. off is the element offset: a bit offset
   into the bitmap (the word reads absorb it). Genuine INT_MIN values are
   counted on the int32 identity row (they read as NA_integer_ in R); the
   other rows cannot produce one. */
static void convert_masked(uint8_t *dst, const uint8_t *src,
                           const uint8_t *valid, uint64_t off, size_t n,
                           const cvt_row *row, cvt_warn *warn) {
  const size_t wo = row->w_out, wi = row->w_in;
  const int count_min = row->wire == MIZU_TYPE_INT && row->cvt == CVT_COPY;
  size_t i = 0;
  while (i < n) {
    size_t lanes = n - i < 64 ? n - i : 64;
    uint64_t w = bitmap_word(valid, off + i, lanes);
    uint64_t full = lanes == 64 ? UINT64_MAX : ((uint64_t) 1 << lanes) - 1;
    if (w == full) {
      if (count_min) {
        /* copy, then scan the destination (dst is int32-aligned on every
           tier: the inline payload is 16-byte aligned, arena chunks and
           region data areas 64). The count rides a local — a warn->
           store per lane would alias-block vectorization */
        cvt_run(dst + i * wo, src + i * wi, lanes, row, warn);
        const int32_t *d = (const int32_t *) (dst + i * wo);
        uint64_t cnt = 0;
        for (size_t j = 0; j < lanes; j++) cnt += d[j] == INT32_MIN;
        warn->n_intmin += cnt;
      } else {
        cvt_run(dst + i * wo, src + i * wi, lanes, row, warn);
      }
    } else if (w == 0) {
      cvt_fill_na(dst + i * wo, lanes, row->wire);
    } else {
      for (size_t j = 0; j < lanes; j++) {
        if ((w >> j) & 1) {
          cvt_run(dst + (i + j) * wo, src + (i + j) * wi, 1, row, warn);
          if (count_min) {
            int32_t v;
            memcpy(&v, dst + (i + j) * wo, 4);
            warn->n_intmin += v == INT32_MIN;
          }
        } else {
          cvt_fill_na(dst + (i + j) * wo, 1, row->wire);
        }
      }
    }
    i += lanes;
  }
}

/* Arrow bool: the data buffer is bit-packed too, so off is a bit offset
   into both bitmaps; a lane's value is its data bit ANDed with validity. */
static void convert_bit_lgl(uint8_t *dst, const uint8_t *data,
                            const uint8_t *valid, uint64_t off, size_t n) {
  size_t i = 0;
  while (i < n) {
    size_t lanes = n - i < 64 ? n - i : 64;
    uint64_t dbits = bitmap_word(data, off + i, lanes);
    uint64_t vbits = valid != NULL ? bitmap_word(valid, off + i, lanes) :
      (lanes == 64 ? UINT64_MAX : ((uint64_t) 1 << lanes) - 1);
    uint64_t vals = dbits & vbits;
    for (size_t j = 0; j < lanes; j++) {
      int32_t o = (vbits >> j) & 1 ? (int32_t) ((vals >> j) & 1) : INT32_MIN;
      memcpy(dst + 4 * (i + j), &o, 4);
    }
    i += lanes;
  }
}

const cvt_row *mizu_py_cvt_for_buffer(const char *f, Py_ssize_t itemsize) {
  return cvt_for_buffer(f, itemsize);
}

const cvt_row *mizu_py_cvt_for_arrow(const char *f) {
  return cvt_for_arrow(f);
}

/* One batch's convert into a reserved destination (interop.c's batch
   loops ride this too): the BIT_LGL / masked / plain dispatch. The
   warning counts ride warn; mizu_py_cvt_warn raises them after the
   write half completes (never mid-write). */
void mizu_py_cvt_convert(uint8_t *dst, const uint8_t *src,
                         const uint8_t *valid, uint64_t off, size_t n,
                         const cvt_row *row, cvt_warn *warn) {
  if (row->cvt == CVT_BIT_LGL) {
    convert_bit_lgl(dst, src, valid, off, n);
  } else if (valid != NULL) {
    convert_masked(dst, src, valid, off, n, row, warn);
  } else {
    cvt_run(dst, src, n, row, warn);
  }
}

/* The conversion warnings, raised after the write half completes (never
   mid-write: under warnings-as-errors a raise must not leave a claimed
   reservation half-written — the write finishes, the warning raises, and
   the core rolls the unpublished reservation back). */
int mizu_py_cvt_warn(const cvt_warn *warn) {
  if (warn->n_range != 0 &&
      PyErr_WarnFormat(PyExc_RuntimeWarning, 1,
                       "pymizu: %llu integer value(s) beyond +/-2^53 convert "
                       "to NA on the R side",
                       (unsigned long long) warn->n_range) < 0)
    return 1;
  if (warn->n_intmin != 0 &&
      PyErr_WarnFormat(PyExc_RuntimeWarning, 1,
                       "pymizu: %llu int32 value(s) of -2147483648 read as "
                       "NA_integer_ in R",
                       (unsigned long long) warn->n_intmin) < 0)
    return 1;
  return 0;
}

/* The conversion stage: reserve n_out bytes on the raw tiers, then one
   fused convert straight into the destination. valid != NULL runs the
   masked variant (Arrow nulls); off is the element offset (the bit
   offset into the validity bitmap and, for CVT_BIT_LGL, the data).
   Returns 0 staged, 1 error (a warning raised as an error), -1
   reservation failure (fall to pickle). */
static int stage_convert(const uint8_t *src, size_t nelts,
                         const cvt_row *row, const uint8_t *valid,
                         uint64_t off, mizu_slot_hdr *hdr, uint8_t *payload,
                         uint32_t inline_max, mizu_handle *h) {
  uint8_t *dst = mizu_stage_raw(h, (uint64_t) (nelts * row->w_out),
                               row->wire, hdr, payload, inline_max);
  if (dst == NULL) return -1;
  cvt_warn warn = { 0, 0 };
  mizu_py_cvt_convert(dst, src, valid, off, nelts, row, &warn);
  return mizu_py_cvt_warn(&warn);
}

/* The buffer-protocol front-end: key the table by the (char, itemsize)
   pair. 0 staged, 1 error, -1 not convertible (fall to pickle). The
   channel-handle gate is the caller's. */
static int stage_convert_buffer(const Py_buffer *v, mizu_slot_hdr *hdr,
                                uint8_t *payload, uint32_t inline_max,
                                mizu_handle *h) {
  if (v->ndim > 1) return -1;
  const cvt_row *row = cvt_for_buffer(v->format, v->itemsize);
  if (row == NULL) return -1;
  return stage_convert((const uint8_t *) v->buf,
                       (size_t) v->len / row->w_in, row, NULL, 0,
                       hdr, payload, inline_max, h);
}

/* The Arrow import front-end. Validate before touching a buffer (all
   O(1), once per stage), then feed the data buffer to the conversion
   pass. Returns 0 staged, 1 error, -1 not an Arrow producer. */
static int stage_arrow_capsules(const ArrowSchema *schema, ArrowArray *array,
                                mizu_slot_hdr *hdr, uint8_t *payload,
                                uint32_t inline_max, mizu_handle *h) {
  /* a consumed or moved struct has release == NULL (the spec's move
     semantics); a NULL bitmap is conformant only with no nulls */
  if (schema->release == NULL || array->release == NULL ||
      schema->format == NULL || array->length < 0 || array->offset < 0 ||
      array->null_count < -1 || array->null_count > array->length ||
      array->n_buffers < 2 || array->buffers == NULL ||
      (array->buffers[1] == NULL && array->length != 0) ||
      (array->buffers[0] == NULL && array->null_count > 0)) {
    PyErr_SetString(PyExc_TypeError,
                    "pymizu: invalid Arrow C Data Interface export");
    return 1;
  }
  if (schema->n_children != 0 || array->n_children != 0) {
    PyErr_SetString(PyExc_TypeError,
                    "pymizu: nested Arrow types cannot cross");
    return 1;
  }
  if (schema->dictionary != NULL) return -1;   /* the stream front-end
                                                  writes the factor shape */
  const cvt_row *row = cvt_for_arrow(schema->format);
  if (row == NULL) return -1;   /* strings, temporals: the front-end's */
  /* a zero-length array short-circuits without touching the buffers:
     they may all be NULL, and memcpy(dst, NULL, 0) is UB */
  if (array->length == 0)
    return stage_convert(NULL, 0, row, NULL, 0, hdr, payload, inline_max, h);
  /* the fast path iff there is no bitmap to read: the spec allows
     null_count == -1 ("not yet computed") with a non-NULL bitmap, and a
     NULL bitmap only when the count is 0, so key on the pointer */
  const uint8_t *valid = (const uint8_t *) array->buffers[0];
  if (valid != NULL && array->null_count == 0) valid = NULL;
  if (valid != NULL && row->wire == MIZU_TYPE_RAW) {
    PyErr_SetString(PyExc_TypeError,
                    "pymizu: Arrow nulls cannot cross in a uint8 array "
                    "(R raw vectors have no NA)");
    return 1;
  }
  const uint8_t *data = (const uint8_t *) array->buffers[1];
  uint64_t off = (uint64_t) array->offset;
  size_t nelts = (size_t) array->length;
  if (row->cvt == CVT_BIT_LGL)
    return stage_convert(data, nelts, row, valid, off, hdr, payload,
                         inline_max, h);
  return stage_convert(data + off * row->w_in, nelts, row, valid, off,
                       hdr, payload, inline_max, h);
}

static int stage_arrow(PyObject *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                       uint32_t inline_max, mizu_handle *h) {
  if (!PyObject_HasAttrString(obj, "__arrow_c_array__"))
    return -1;   /* a stream-only producer: the front-end's case */
  PyObject *fn = PyObject_GetAttrString(obj, "__arrow_c_array__");
  if (fn == NULL) return 1;
  PyObject *pair = PyObject_CallNoArgs(fn);
  Py_DECREF(fn);
  if (pair == NULL) return 1;   /* the object claimed the interface */
  if (!PyTuple_Check(pair) || PyTuple_GET_SIZE(pair) != 2) {
    Py_DECREF(pair);
    PyErr_SetString(PyExc_TypeError, "pymizu: __arrow_c_array__ must return "
                    "a (schema, array) capsule pair");
    return 1;
  }
  /* PyCapsule_GetPointer rejects misnamed capsules by the API */
  ArrowSchema *schema = (ArrowSchema *) PyCapsule_GetPointer(
    PyTuple_GET_ITEM(pair, 0), "arrow_schema");
  ArrowArray *array = (ArrowArray *) PyCapsule_GetPointer(
    PyTuple_GET_ITEM(pair, 1), "arrow_array");
  if (schema == NULL || array == NULL) {
    Py_DECREF(pair);
    return 1;
  }
  int rc = stage_arrow_capsules(schema, array, hdr, payload, inline_max, h);
  /* staging is synchronous — nothing aliases the producer's buffers:
     release the imported structs immediately (the capsules own the
     struct memory; release frees the producer's private resources) */
  if (schema->release != NULL) schema->release(schema);
  if (array->release != NULL) array->release(array);
  Py_DECREF(pair);
  return rc;
}

// Fast-path codec --------------------------------------------------------------

/* The compact codec: the hot subset (bool, int, float, str, bytes, and one
   flat container level of those, capped) framed as a tiny binary stream —
   the analogue of R's compact codec. Anything outside the subset falls
   back to pickle, the same discipline as R's codec rejecting ALTREP.
   Streams carry a magic first byte so readers dispatch on it; the
   first-byte namespace is shared (pickle protocol 4 is 0x80, R's codec is
   'R', R native streams 'B'/'X'/'A') and owned by the vendored mizu_ext.h
   (MIZU_PYMIZU_CODEC_MAGIC is this codec's 'P'). Integers are int64,
   little-endian; all supported platforms are little-endian. */
#define PYMIZU_CODEC_CAP 64       /* container element cap: staging stays bounded */

enum {
  PYMIZU_TAG_BOOL = 'b', PYMIZU_TAG_INT = 'i', PYMIZU_TAG_FLOAT = 'f',
  PYMIZU_TAG_STR = 's', PYMIZU_TAG_BYTES = 'y',
  PYMIZU_TAG_LIST = 'l', PYMIZU_TAG_TUPLE = 't', PYMIZU_TAG_DICT = 'd',
  PYMIZU_TAG_NONE = 'n',      /* None inside frames/containers */
  PYMIZU_TAG_TASK = 'k',      /* the (fn, args, kwargs) task frame */
  PYMIZU_TAG_BUFFER = 'Y',    /* a buffer-protocol leaf, inline bytes */
  PYMIZU_TAG_BUFREF = 'r'     /* a buffer leaf by reference (SHM_VEC name) */
};

/* Exact-type checks throughout: a subclass (IntEnum, a str subclass) keeps
   its semantics on the pickle path. */
static int codec_tag_of(PyObject *o) {
  if (PyBool_Check(o)) return PYMIZU_TAG_BOOL;
  if (PyLong_CheckExact(o)) return PYMIZU_TAG_INT;
  if (PyFloat_CheckExact(o)) return PYMIZU_TAG_FLOAT;
  if (PyUnicode_CheckExact(o)) return PYMIZU_TAG_STR;
  if (PyBytes_CheckExact(o)) return PYMIZU_TAG_BYTES;
  return 0;
}

/* Size pass, 0 ok / -1 reject (not in the subset, an int past int64, a
   lone-surrogate str, over the cap). Never sets an error. */
static int codec_scalar_size(PyObject *o, uint64_t *sz) {
  if (o == Py_None) {
    *sz += 1;
    return 0;
  }
  switch (codec_tag_of(o)) {
  case PYMIZU_TAG_BOOL: *sz += 2; return 0;
  case PYMIZU_TAG_INT: {
    long long v = PyLong_AsLongLong(o);
    if (v == -1 && PyErr_Occurred()) {
      PyErr_Clear();   /* OverflowError: an exotic int rides pickle */
      return -1;
    }
    *sz += 9;
    return 0;
  }
  case PYMIZU_TAG_FLOAT: *sz += 9; return 0;
  case PYMIZU_TAG_STR: {
    Py_ssize_t n;
    if (PyUnicode_AsUTF8AndSize(o, &n) == NULL) {
      PyErr_Clear();   /* lone surrogates ride pickle */
      return -1;
    }
    *sz += 5 + (uint64_t) n;
    return 0;
  }
  case PYMIZU_TAG_BYTES:
    *sz += 5 + (uint64_t) PyBytes_GET_SIZE(o);
    return 0;
  }
  return -1;
}

static int codec_size(PyObject *o, uint64_t *sz) {
  if (PyList_CheckExact(o) || PyTuple_CheckExact(o)) {
    Py_ssize_t n = PyList_CheckExact(o) ? PyList_GET_SIZE(o) :
      PyTuple_GET_SIZE(o);
    if (n > PYMIZU_CODEC_CAP) return -1;
    *sz += 5;
    for (Py_ssize_t i = 0; i < n; i++) {
      PyObject *it = PyList_CheckExact(o) ? PyList_GET_ITEM(o, i) :
        PyTuple_GET_ITEM(o, i);
      if (codec_scalar_size(it, sz) < 0) return -1;
    }
    return 0;
  }
  if (PyDict_CheckExact(o)) {
    if (PyDict_Size(o) > PYMIZU_CODEC_CAP) return -1;
    *sz += 5;
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (PyDict_Next(o, &pos, &k, &v)) {
      if (codec_scalar_size(k, sz) < 0) return -1;
      if (codec_scalar_size(v, sz) < 0) return -1;
    }
    return 0;
  }
  return codec_scalar_size(o, sz);
}

static void codec_put32(uint8_t **p, uint32_t v) {
  (*p)[0] = (uint8_t) v;
  (*p)[1] = (uint8_t) (v >> 8);
  (*p)[2] = (uint8_t) (v >> 16);
  (*p)[3] = (uint8_t) (v >> 24);
  *p += 4;
}

static void codec_put64(uint8_t **p, uint64_t v) {
  for (int i = 0; i < 8; i++) (*p)[i] = (uint8_t) (v >> (8 * i));
  *p += 8;
}

/* The size pass already validated everything, so the write pass cannot
   fail. */
static void codec_put_scalar(uint8_t **p, PyObject *o) {
  if (o == Py_None) {
    *(*p)++ = PYMIZU_TAG_NONE;
    return;
  }
  switch (codec_tag_of(o)) {
  case PYMIZU_TAG_BOOL:
    *(*p)++ = PYMIZU_TAG_BOOL;
    *(*p)++ = (uint8_t) (o == Py_True);
    return;
  case PYMIZU_TAG_INT: {
    *(*p)++ = PYMIZU_TAG_INT;
    long long v = PyLong_AsLongLong(o);
    codec_put64(p, (uint64_t) v);
    return;
  }
  case PYMIZU_TAG_FLOAT: {
    *(*p)++ = PYMIZU_TAG_FLOAT;
    double d = PyFloat_AS_DOUBLE(o);
    uint64_t u;
    memcpy(&u, &d, 8);
    codec_put64(p, u);
    return;
  }
  case PYMIZU_TAG_STR: {
    *(*p)++ = PYMIZU_TAG_STR;
    Py_ssize_t n;
    const char *s = PyUnicode_AsUTF8AndSize(o, &n);
    codec_put32(p, (uint32_t) n);
    memcpy(*p, s, (size_t) n);
    *p += n;
    return;
  }
  default: {   /* PYMIZU_TAG_BYTES */
    *(*p)++ = PYMIZU_TAG_BYTES;
    Py_ssize_t n = PyBytes_GET_SIZE(o);
    codec_put32(p, (uint32_t) n);
    memcpy(*p, PyBytes_AS_STRING(o), (size_t) n);
    *p += n;
    return;
  }
  }
}

static void codec_put(uint8_t **p, PyObject *o) {
  if (PyList_CheckExact(o) || PyTuple_CheckExact(o)) {
    int is_list = PyList_CheckExact(o) != 0;
    Py_ssize_t n = is_list ? PyList_GET_SIZE(o) : PyTuple_GET_SIZE(o);
    *(*p)++ = (uint8_t) (is_list ? PYMIZU_TAG_LIST : PYMIZU_TAG_TUPLE);
    codec_put32(p, (uint32_t) n);
    for (Py_ssize_t i = 0; i < n; i++)
      codec_put_scalar(p, is_list ? PyList_GET_ITEM(o, i) :
                       PyTuple_GET_ITEM(o, i));
    return;
  }
  if (PyDict_CheckExact(o)) {
    *(*p)++ = PYMIZU_TAG_DICT;
    codec_put32(p, (uint32_t) PyDict_Size(o));
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (PyDict_Next(o, &pos, &k, &v)) {
      codec_put_scalar(p, k);
      codec_put_scalar(p, v);
    }
    return;
  }
  codec_put_scalar(p, o);
}

/* Try the codec; returns 0 staged, 1 error, -1 fall back to pickle. The
   inline path encodes straight into the slot payload — no allocation. */
static int stage_codec(PyObject *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                       uint32_t inline_max, mizu_handle *h) {
  uint64_t sz = 1;   /* the magic byte */
  if (codec_size(obj, &sz) < 0) return -1;
  if (sz <= (uint64_t) inline_max) {
    payload[0] = MIZU_PYMIZU_CODEC_MAGIC;
    uint8_t *p = payload + 1;
    codec_put(&p, obj);
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) sz;
    hdr->aux = MIZU_AUX_F_KEEPERLESS;   /* a flat codec stream references nothing */
    return 0;
  }
  uint8_t *buf = (uint8_t *) malloc((size_t) sz);
  if (buf == NULL) return -1;   /* pickle's own allocation failure reports */
  buf[0] = MIZU_PYMIZU_CODEC_MAGIC;
  uint8_t *p = buf + 1;
  codec_put(&p, obj);
  int rc = stage_bytes(buf, (size_t) sz, hdr, payload, inline_max, h);
  free(buf);
  return rc;
}

// Task frames ------------------------------------------------------------------

/* The facade marks pool task payloads as _TaskFrame (a tuple subclass:
   items 0/1/2 are fn/args/kwargs), and they stage as a PYMIZU_TAG_TASK
   stream — the structured frame codec that keeps a task off the
   whole-tuple pickle path. fn crosses by reference (module + qualname,
   the mirror of pickle's importable-reference semantics, which pymizu
   already requires of task callables) or as its own protocol-4 pickle;
   args/kwargs elements are codec scalars, None, one flat container level
   of those, or buffer leaves — inline bytes, or past the zero-copy floor
   a SHM_VEC region referenced by name (BUFREF, the leaf-level analogue of
   R's REF-inside-a-payload). The core's staging seam holds exactly one
   uncommitted spill checkout (fl->staging), so a frame carries at most
   one BUFREF leaf, and a frame carrying one must otherwise fit the inline
   budget (the stream cannot also spill); a second large buffer or an
   oversized rest falls the whole frame back to pickle, today's behavior.
   A BUFREF leaf's fn argument arrives as a read-only view — the channel
   view tier's crossing contract. */

/* The by-reference gate: an exact function or builtin whose qualname is a
   plain attribute path (no '<locals>', no '<lambda>') in a module that is
   not '__main__' — the worker's __main__ is pymizu.worker, not the
   submitter's script, so a same-named attribute there would resolve
   successfully but wrongly. Returns 0 with owned references, -1 not
   referenceable (no error set). */
static int fn_ref_parts(PyObject *fn, PyObject **mod_out, PyObject **qual_out) {
  if (!PyFunction_CheckExact(fn) && !PyCFunction_CheckExact(fn)) return -1;
  PyObject *mod = PyObject_GetAttrString(fn, "__module__");
  if (mod == NULL) {
    PyErr_Clear();
    return -1;
  }
  PyObject *qual = PyObject_GetAttrString(fn, "__qualname__");
  if (qual == NULL) {
    PyErr_Clear();
    Py_DECREF(mod);
    return -1;
  }
  int ok = PyUnicode_CheckExact(mod) && PyUnicode_CheckExact(qual);
  if (ok) {
    const char *m = PyUnicode_AsUTF8(mod);
    const char *q = PyUnicode_AsUTF8(qual);
    ok = m != NULL && q != NULL && strcmp(m, "__main__") != 0 &&
         strchr(q, '<') == NULL;
    if (m == NULL || q == NULL) PyErr_Clear();
  }
  if (!ok) {
    Py_DECREF(mod);
    Py_DECREF(qual);
    return -1;
  }
  *mod_out = mod;
  *qual_out = qual;
  return 0;
}

typedef struct {
  uint64_t sz;
  uint32_t inline_max;
  int churn;              /* snapshot at stage start: the passes agree */
  int bufref;             /* a BUFREF leaf is planned (one per frame) */
  int fn_kind;            /* 0 by reference, 1 pickled */
  PyObject *fn_mod;       /* kind 0 (owned) */
  PyObject *fn_qual;      /* kind 0 (owned) */
  PyObject *fn_pickled;   /* kind 1 (owned) */
} frame_plan;

static void frame_plan_clear(frame_plan *fp) {
  Py_XDECREF(fp->fn_mod);
  Py_XDECREF(fp->fn_qual);
  Py_XDECREF(fp->fn_pickled);
}

/* Buffer leaf, size pass: the same gate as stage_raw (wire_type_of, the
   zc floor, the churn snapshot), and the same subclass rejection — a
   MaskedArray argument keeps its mask on the plain-tuple pickle fallback.
   0 ok, -1 reject. */
static int frame_buf_size(PyObject *o, frame_plan *fp) {
  if (buffer_subclass_reject(o)) return -1;
  Py_buffer v;
  if (PyObject_GetBuffer(o, &v, PyBUF_C_CONTIGUOUS | PyBUF_FORMAT) < 0) {
    PyErr_Clear();
    return -1;
  }
  int type = wire_type_of(&v);
  size_t n = (size_t) v.len;
  PyBuffer_Release(&v);
  if (type == 0 || n > (SIZE_MAX >> 9)) return -1;
  size_t zc_gate = (size_t) fp->inline_max > MIZU_ZC_FLOOR ?
    (size_t) fp->inline_max : MIZU_ZC_FLOOR;
  if (!fp->churn && n >= zc_gate) {
    if (fp->bufref) return -1;   /* one staging checkout per frame */
    fp->bufref = 1;
    fp->sz += 11 + MIZU_NAME_MAX;   /* tag, type, count, name_len, name */
  } else {
    fp->sz += 10 + (uint64_t) n;
  }
  return 0;
}

/* A leaf that is not a container: a codec scalar (None included) or a
   buffer. */
static int frame_flat_size(PyObject *o, frame_plan *fp) {
  if (codec_scalar_size(o, &fp->sz) == 0) return 0;
  return frame_buf_size(o, fp);
}

static int frame_leaf_size(PyObject *o, frame_plan *fp) {
  if (PyList_CheckExact(o) || PyTuple_CheckExact(o)) {
    Py_ssize_t n = PyList_CheckExact(o) ? PyList_GET_SIZE(o) :
      PyTuple_GET_SIZE(o);
    if (n > PYMIZU_CODEC_CAP) return -1;
    fp->sz += 5;
    for (Py_ssize_t i = 0; i < n; i++)
      if (frame_flat_size(PyList_CheckExact(o) ? PyList_GET_ITEM(o, i) :
                          PyTuple_GET_ITEM(o, i), fp) < 0)
        return -1;
    return 0;
  }
  if (PyDict_CheckExact(o)) {
    if (PyDict_Size(o) > PYMIZU_CODEC_CAP) return -1;
    fp->sz += 5;
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (PyDict_Next(o, &pos, &k, &v)) {
      if (!PyUnicode_CheckExact(k) || codec_scalar_size(k, &fp->sz) < 0)
        return -1;
      if (frame_flat_size(v, fp) < 0) return -1;
    }
    return 0;
  }
  return frame_flat_size(o, fp);
}

/* The size pass: validate everything, decide fn's encoding, count the
   bytes. 0 ok, -1 reject (fall back to pickle, never an error), 1 error
   (an unpicklable fn — today's whole-tuple pickle failure surfaced at
   submit). */
static int frame_size(PyObject *frame, frame_plan *fp, uint32_t inline_max,
                      mizu_handle *h) {
  memset(fp, 0, sizeof(*fp));
  fp->sz = 2;   /* magic + task tag */
  fp->inline_max = inline_max;
  fp->churn = mizu_handle_churn(h);
  PyObject *fn = PyTuple_GET_ITEM(frame, 0);
  PyObject *args = PyTuple_GET_ITEM(frame, 1);
  PyObject *kwargs = PyTuple_GET_ITEM(frame, 2);
  if (!PyTuple_CheckExact(args) || !PyDict_CheckExact(kwargs)) return -1;
  if (fn_ref_parts(fn, &fp->fn_mod, &fp->fn_qual) == 0) {
    fp->fn_kind = 0;
    fp->sz += 1;
    codec_scalar_size(fp->fn_mod, &fp->sz);    /* validated UTF-8 already */
    codec_scalar_size(fp->fn_qual, &fp->sz);
  } else {
    fp->fn_kind = 1;
    fp->fn_pickled = PyObject_CallFunction(mizu_dumps, "Oi", fn, 4);
    if (fp->fn_pickled == NULL) return 1;
    fp->sz += 6 + (uint64_t) PyBytes_GET_SIZE(fp->fn_pickled);
  }
  Py_ssize_t na = PyTuple_GET_SIZE(args);
  if (na > PYMIZU_CODEC_CAP) goto reject;
  fp->sz += 5;
  for (Py_ssize_t i = 0; i < na; i++)
    if (frame_leaf_size(PyTuple_GET_ITEM(args, i), fp) < 0) goto reject;
  if (PyDict_Size(kwargs) > PYMIZU_CODEC_CAP) goto reject;
  fp->sz += 5;
  {
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (PyDict_Next(kwargs, &pos, &k, &v)) {
      if (!PyUnicode_CheckExact(k) || codec_scalar_size(k, &fp->sz) < 0)
        goto reject;
      if (frame_leaf_size(v, fp) < 0) goto reject;
    }
  }
  /* a BUFREF leaf holds the stage's single spill checkout, so the stream
     itself must stay inline (it cannot also spill) */
  if (fp->bufref && fp->sz > (uint64_t) inline_max) goto reject;
  return 0;
reject:
  frame_plan_clear(fp);
  return -1;
}

/* Buffer leaf, write pass: the size pass validated and decided, so only
   the BUFREF spill get can fail (a churn race) — return -1 to abandon the
   stage. Nothing is retained yet at that point (the failed get leaves no
   checkout, and a frame carries at most one BUFREF leaf), so the pickle
   fallback starts clean. */
static int frame_buf_write(uint8_t **p, PyObject *o, const frame_plan *fp,
                           mizu_handle *h) {
  Py_buffer v;
  if (PyObject_GetBuffer(o, &v, PyBUF_C_CONTIGUOUS | PyBUF_FORMAT) < 0) {
    PyErr_Clear();
    return -1;
  }
  int type = wire_type_of(&v);
  size_t n = (size_t) v.len;
  size_t zc_gate = (size_t) fp->inline_max > MIZU_ZC_FLOOR ?
    (size_t) fp->inline_max : MIZU_ZC_FLOOR;
  int rc = 0;
  if (!fp->churn && n >= zc_gate) {
    /* the stage_shm_vec body minus the slot-header writes: one MIZH
       layout write into a spill region, retained ZC (the producer loan
       releases at the frame's collect; the region recycles when the
       cross-process refcount hits zero) */
    mizu_shm *shm;
    if (mizu_stage_spill_get(h, MIZU_HEADER_SIZE + n, &shm) != MIZU_OK) {
      rc = -1;
      goto out;
    }
    uint8_t *base = (uint8_t *) shm->addr;
    mizu_mizh_write(base, type, (int64_t) (n / mizu_type_elt_size(type)));
    memcpy(base + MIZU_HEADER_SIZE, v.buf, n);
    mizu_stage_retain_zc(h, shm);
    *(*p)++ = PYMIZU_TAG_BUFREF;
    *(*p)++ = (uint8_t) type;
    codec_put64(p, (uint64_t) n);
    *(*p)++ = (uint8_t) shm->name_len;
    memcpy(*p, shm->name, shm->name_len);
    *p += shm->name_len;
  } else {
    *(*p)++ = PYMIZU_TAG_BUFFER;
    *(*p)++ = (uint8_t) type;
    codec_put64(p, (uint64_t) n);
    memcpy(*p, v.buf, n);
    *p += n;
  }
out:
  PyBuffer_Release(&v);
  return rc;
}

static int frame_flat_write(uint8_t **p, PyObject *o, const frame_plan *fp,
                            mizu_handle *h) {
  if (o == Py_None || codec_tag_of(o) != 0) {
    codec_put_scalar(p, o);
    return 0;
  }
  return frame_buf_write(p, o, fp, h);
}

static int frame_leaf_write(uint8_t **p, PyObject *o, const frame_plan *fp,
                            mizu_handle *h) {
  if (PyList_CheckExact(o) || PyTuple_CheckExact(o)) {
    int is_list = PyList_CheckExact(o) != 0;
    Py_ssize_t n = is_list ? PyList_GET_SIZE(o) : PyTuple_GET_SIZE(o);
    *(*p)++ = (uint8_t) (is_list ? PYMIZU_TAG_LIST : PYMIZU_TAG_TUPLE);
    codec_put32(p, (uint32_t) n);
    for (Py_ssize_t i = 0; i < n; i++)
      if (frame_flat_write(p, is_list ? PyList_GET_ITEM(o, i) :
                           PyTuple_GET_ITEM(o, i), fp, h) < 0)
        return -1;
    return 0;
  }
  if (PyDict_CheckExact(o)) {
    *(*p)++ = PYMIZU_TAG_DICT;
    codec_put32(p, (uint32_t) PyDict_Size(o));
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (PyDict_Next(o, &pos, &k, &v)) {
      codec_put_scalar(p, k);
      if (frame_flat_write(p, v, fp, h) < 0) return -1;
    }
    return 0;
  }
  return frame_flat_write(p, o, fp, h);
}

/* Try the frame codec; returns 0 staged, 1 error, -1 fall back to
   pickle. */
static int stage_task_frame(PyObject *frame, mizu_slot_hdr *hdr,
                            uint8_t *payload, uint32_t inline_max,
                            mizu_handle *h) {
  frame_plan fp;
  int rc = frame_size(frame, &fp, inline_max, h);
  if (rc != 0) return rc;
  uint8_t *buf = payload;
  if (fp.sz > (uint64_t) inline_max) {
    buf = (uint8_t *) malloc((size_t) fp.sz);
    if (buf == NULL) {
      frame_plan_clear(&fp);
      return -1;   /* pickle's own allocation failure reports */
    }
  }
  uint8_t *p = buf;
  *p++ = MIZU_PYMIZU_CODEC_MAGIC;
  *p++ = PYMIZU_TAG_TASK;
  *p++ = (uint8_t) fp.fn_kind;
  if (fp.fn_kind == 0) {
    codec_put_scalar(&p, fp.fn_mod);
    codec_put_scalar(&p, fp.fn_qual);
  } else {
    codec_put_scalar(&p, fp.fn_pickled);
  }
  PyObject *args = PyTuple_GET_ITEM(frame, 1);
  PyObject *kwargs = PyTuple_GET_ITEM(frame, 2);
  Py_ssize_t na = PyTuple_GET_SIZE(args);
  *p++ = PYMIZU_TAG_TUPLE;
  codec_put32(&p, (uint32_t) na);
  rc = 0;
  for (Py_ssize_t i = 0; rc == 0 && i < na; i++)
    rc = frame_leaf_write(&p, PyTuple_GET_ITEM(args, i), &fp, h);
  if (rc == 0) {
    *p++ = PYMIZU_TAG_DICT;
    codec_put32(&p, (uint32_t) PyDict_Size(kwargs));
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (rc == 0 && PyDict_Next(kwargs, &pos, &k, &v)) {
      codec_put_scalar(&p, k);
      rc = frame_leaf_write(&p, v, &fp, h);
    }
  }
  if (rc == 0) {
    size_t n = (size_t) (p - buf);
    if (buf == payload) {
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      /* keeperless even with a BUFREF leaf: its zc loan rides the
         claim-side release machinery, not the keeper-drop reap */
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
    } else {
      rc = stage_bytes(buf, n, hdr, payload, inline_max, h);
    }
  }
  if (buf != payload) free(buf);
  frame_plan_clear(&fp);
  return rc;
}

static int stage_impl(PyObject *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                      uint32_t inline_max, mizu_handle *h,
                      uint32_t peer_lang) {
  /* the reader-language policy (DESIGN.md's): same-language handles keep
     the private path ('P' codec, pickle, identity raw tiers); a foreign
     peer gets the interchange order — the conversion pass and the Arrow
     front-ends live on the foreign path only, and a decline there
     raises (DeclinedError) instead of falling to pickle */
  const int foreign = peer_lang != 0 && peer_lang != MIZU_LANG_PYTHON;
  if (obj == Py_None) {
    hdr->kind = MIZU_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return 0;
  }
  if (PyUnicode_CheckExact(obj)) {
    /* STR1, the mirror of R's length-1 string tier: UTF-8 bytes in the
       payload, aux the cetype mark (Python str has no encoding of its own;
       UTF-8 is the canonical crossing). Inline-budget gate, matching mizu's
       cap. Exact-type check: a str subclass keeps its pickle semantics. */
    Py_ssize_t n;
    const char *s = PyUnicode_AsUTF8AndSize(obj, &n);
    if (s == NULL)
      PyErr_Clear();   /* lone surrogates fall through to codec/pickle */
    else if (n <= (Py_ssize_t) inline_max) {
      memcpy(payload, s, (size_t) n);
      hdr->kind = MIZU_KIND_STR1;
      hdr->len = (uint32_t) n;
      hdr->aux = MIZU_CE_UTF8;
      return 0;
    }
  }
  if (Py_TYPE(obj) == &MizuShmStrViewType) {
    /* a region-backed string view re-sent whole: its region name (REF,
       zero payload bytes — the string view admits no buffer, so whole is
       the only case); the name-fits gate's fallback is the by-value list */
    if (stage_ref_str(obj, hdr, payload, inline_max) == 0) return 0;
    PyObject *list = strview_to_list(obj, NULL);
    if (list == NULL) return 1;
    int rc = stage_impl(list, hdr, payload, inline_max, h, peer_lang);
    Py_DECREF(list);
    return rc;
  }
  if (PyObject_CheckBuffer(obj) && !buffer_subclass_reject(obj)) {
    Py_buffer v;
    /* not PyBUF_C_CONTIGUOUS (it implies WRITABLE): a read-only buffer —
       a zero-copy view echoing back — stages fine; contiguity is verified
       as strides == NULL instead */
    if (PyObject_GetBuffer(obj, &v, PyBUF_ND | PyBUF_FORMAT) == 0) {
      int rc = -1;
      if (v.strides == NULL) {
        /* a read-only buffer may be a received view echoing back: whole,
           it crosses as its region name (REF, zero payload bytes);
           writable buffers — every ordinary send — skip the .base walk */
        if (v.readonly)
          rc = stage_ref(obj, &v, hdr, payload, inline_max);
        if (rc < 0) {
          int type = wire_type_of(&v);
          if (type != 0)
            rc = stage_raw(&v, type, hdr, payload, inline_max, h);
          else if (foreign && mizu_handle_kind(h) != MIZU_HTYPE_POOL)
            /* the conversion pass is foreign-only: same-language
               channels keep identity dtypes and pickle the rest */
            rc = stage_convert_buffer(&v, hdr, payload, inline_max, h);
        }
      }
      PyBuffer_Release(&v);
      if (rc >= 0) return rc;
    } else {
      PyErr_Clear();
    }
  }
  if (foreign) {
    /* the pinned foreign order: stage_arrow (__arrow_c_array__), the
       stream front-end (__arrow_c_stream__), then the 'I' writer —
       whose decline is the send-time DeclinedError */
    if (mizu_handle_kind(h) != MIZU_HTYPE_POOL) {
      int arc = stage_arrow(obj, hdr, payload, inline_max, h);
      if (arc >= 0) return arc;
      int src = pymizu_ix_stage_arrow_stream(obj, hdr, payload,
                                             inline_max, h);
      if (src >= 0) return src;
    }
    return pymizu_ix_stage(obj, hdr, payload, inline_max, h);
  }
  if (Py_TYPE(obj) == &MizuTaskFrameType) {
    /* before the codec: the codec's exact-type tuple check would reject
       the subclass anyway, and the direct order skips a wasted pass */
    int frc = stage_task_frame(obj, hdr, payload, inline_max, h);
    if (frc >= 0) return frc;
    /* the fallback pickles a plain tuple: a pickled _TaskFrame would not
       reconstruct on the worker (the type is not importable there) */
    PyObject *plain = PyTuple_Pack(3, PyTuple_GET_ITEM(obj, 0),
                                   PyTuple_GET_ITEM(obj, 1),
                                   PyTuple_GET_ITEM(obj, 2));
    if (plain == NULL) return 1;
    PyObject *stream =
      PyObject_CallFunction(mizu_dumps, "Oi", plain, 4);
    Py_DECREF(plain);
    if (stream == NULL) return 1;
    int rc = stage_bytes((const uint8_t *) PyBytes_AS_STRING(stream),
                         (size_t) PyBytes_GET_SIZE(stream),
                         hdr, payload, inline_max, h);
    Py_DECREF(stream);
    return rc;
  }
  int crc = stage_codec(obj, hdr, payload, inline_max, h);
  if (crc >= 0) return crc;
  PyObject *stream =
    PyObject_CallFunction(mizu_dumps, "Oi", obj, 4);   /* protocol pinned */
  if (stream == NULL) return 1;
  int rc = stage_bytes((const uint8_t *) PyBytes_AS_STRING(stream),
                       (size_t) PyBytes_GET_SIZE(stream),
                       hdr, payload, inline_max, h);
  Py_DECREF(stream);
  return rc;
}

/* The per-handle name -> owner cache's home (its functions live with the
   view types below): defined ahead of the stage hook, which reads the
   peer word through MizuHandleCtx. */
typedef struct MizuShmOwner MizuShmOwner;

typedef struct {
  MizuShmOwner *owners[MIZU_OPEN_CACHE_MAX];
  char names[MIZU_OPEN_CACHE_MAX][MIZU_NAME_MAX];
  uint64_t stamp[MIZU_OPEN_CACHE_MAX];
  uint64_t tick;
} MizuViewCache;

/* The binding.ctx container: the view cache first (every existing
   MizuViewCache * use keeps working — same address), then the peer's
   identity word (the language registry byte and the capability mask,
   Phase 0's identity exchange). peer_lang 0 is "not yet read" and
   behaves as same-language; the host reads it when ready_wait returns,
   the peer at attach. Pools are homogeneous: their word stays 0.
   err_exc is the peer shim's err-send exception (_send_error); the stage
   hook pointer-matches it and frames the err stream. NULL when idle; set
   and cleared within the _send_error veneer, whose argument roots it. */
typedef struct {
  MizuViewCache vc;
  uint32_t peer_lang;
  uint32_t peer_caps;
  PyObject *err_exc;
} MizuHandleCtx;

/* Frame an exception as an 'I' err stream INLINE (defined with the other
   task-error helpers below, ahead of publish_exc). */
static int frame_err_exc(PyObject *exc, uint8_t *payload,
                         uint32_t inline_max, mizu_slot_hdr *hdr);

/* The binding's stage_fn, registered on every channel handle. The veneer
   released the GIL around the verb; reacquire. ctx (the view cache) is
   read-side only — staging pins nothing. */
static int py_stage(void *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                    uint32_t inline_max, mizu_handle *h, void *ctx) {
  PyGILState_STATE gil = PyGILState_Ensure();
  MizuHandleCtx *hctx = (MizuHandleCtx *) ctx;
  if (hctx != NULL && obj == hctx->err_exc) {
    /* the peer shim's err send (_send_error): frame the exception as an
       'I' err stream INLINE, whatever the peer's language — the pointer
       match (one compare, cleared as it matches) bypasses the value
       codec choice below. §4.1's spec submit reuses this pattern. */
    hctx->err_exc = NULL;
    int rc = frame_err_exc(obj, payload, inline_max, hdr);
    PyGILState_Release(gil);
    return rc;
  }
  uint32_t peer_lang = hctx != NULL ? hctx->peer_lang : 0;
  int rc = stage_impl((PyObject *) obj, hdr, payload, inline_max, h,
                      peer_lang);
  PyGILState_Release(gil);
  return rc;
}

// Reading ------------------------------------------------------------------------

static PyObject *numpy_empty(void) {
  PyObject *np = numpy_module();
  if (np == NULL) return NULL;
  return PyObject_GetAttrString(np, "empty");   /* new ref */
}

/* RAWVEC/RAWSPILL materialize into a fresh numpy array (or a memoryview
   copy) before consumer-done — ring slots are reused and arena chunks
   FIFO-reclaim, so a view over them dangles. LGL and INT take the
   copied-read rule: no INT_MIN present reads numpy bool_ / int32, else
   int32 (LGL — the sentinel documented, no width room for an out-of-band
   NA numpy respects) / float64 with R's NA_real_ payload (every int32
   exact). The INT/INT64 scans are gated on a foreign writer (foreign at
   the call site): Python stages identity int32/int64 on the same tiers,
   so a same-language read stays an unscanned identity and a genuine
   -2^31/-2^63 survives Python<->Python. LGL needs no gate — R is the only
   NA-writing stager of it a Python reader meets. int64 keeps its dtype
   and warns on a detected INT64_MIN (warn-only, .to_arrow() the NA-honest
   accessor). Each scan fuses into the element copy the read already makes
   — the one deliberate exception to the raw tiers' no-per-element-scan
   rule. */
static PyObject *read_raw(const uint8_t *src, uint32_t len, int type,
                          int foreign) {
  size_t elt = mizu_type_elt_size(type);
  if (elt == 0 || len % elt != 0) {
    PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
    return NULL;
  }
  size_t nelts = len / elt;
  PyObject *empty = numpy_empty();
  if (empty != NULL) {
    /* the conversion verdict: 0 identity, 1 LGL->bool (an INT_MIN reverts
       to int32), 2 INT->float64 (the scan's verdict, foreign-gated) */
    int conv = 0;
    if (type == MIZU_TYPE_LGL) conv = 1;
    else if (type == MIZU_TYPE_INT && foreign) conv = 2;
    const char *dt;
    switch (type) {
    case MIZU_TYPE_REAL: dt = "float64"; break;
    case MIZU_TYPE_INT:
    case MIZU_TYPE_LGL: dt = conv == 1 ? "bool" : "int32"; break;
    case MIZU_TYPE_INT64: dt = "int64"; break;
    case MIZU_TYPE_CPLX: dt = "complex128"; break;
    case MIZU_TYPE_RAW: dt = "uint8"; break;
    default:
      PyErr_Format(MizuError, "pymizu: unknown wire type %d", type);
      Py_DECREF(empty);
      return NULL;
    }
    PyObject *args = PyTuple_Pack(1, PyLong_FromSize_t(nelts));
    PyObject *kw = Py_BuildValue("{s:s}", "dtype", dt);
    PyObject *arr = (args != NULL && kw != NULL) ?
      PyObject_Call(empty, args, kw) : NULL;
    Py_XDECREF(args);
    Py_XDECREF(kw);
    Py_DECREF(empty);
    if (arr == NULL) return NULL;
    Py_buffer v;
    if (PyObject_GetBuffer(arr, &v, PyBUF_ND | PyBUF_WRITABLE) < 0) {
      Py_DECREF(arr);
      return NULL;
    }
    if (conv == 1) {
      /* bool_ copy with the INT_MIN scan fused: a detected NA discards
         the bool array and re-reads as int32 (the sentinel documented) */
      const int32_t *s32 = (const int32_t *) src;
      uint8_t *d = (uint8_t *) v.buf;
      int na = 0;
      for (size_t i = 0; i < nelts; i++) {
        na |= s32[i] == MIZU_NA_INT32;
        d[i] = s32[i] != 0;
      }
      PyBuffer_Release(&v);
      if (!na) return arr;
      Py_DECREF(arr);
      return read_raw(src, len, MIZU_TYPE_INT, 0);
    }
    if (conv == 2) {
      /* int32 copy with the scan fused; a detected NA converts the copy
         in place to float64 (NA_real_ payloads; every int32 exact) */
      const int32_t *s32 = (const int32_t *) src;
      int32_t *d32 = (int32_t *) v.buf;
      int na = 0;
      for (size_t i = 0; i < nelts; i++) {
        na |= s32[i] == MIZU_NA_INT32;
        d32[i] = s32[i];
      }
      PyBuffer_Release(&v);
      if (!na) return arr;
      PyObject *f64 = NULL;
      PyObject *empty2 = numpy_empty();
      if (empty2 != NULL) {
        PyObject *a2 = PyTuple_Pack(1, PyLong_FromSize_t(nelts));
        PyObject *k2 = Py_BuildValue("{s:s}", "dtype", "float64");
        f64 = (a2 != NULL && k2 != NULL) ?
          PyObject_Call(empty2, a2, k2) : NULL;
        Py_XDECREF(a2);
        Py_XDECREF(k2);
        Py_DECREF(empty2);
      }
      if (f64 == NULL) {
        Py_DECREF(arr);
        return NULL;
      }
      Py_buffer fv;
      if (PyObject_GetBuffer(f64, &fv, PyBUF_ND | PyBUF_WRITABLE) < 0) {
        Py_DECREF(f64);
        Py_DECREF(arr);
        return NULL;
      }
      double *fd = (double *) fv.buf;
      const uint64_t na_bits = PYMIZU_NA_REAL_BITS;
      for (size_t i = 0; i < nelts; i++) {
        if (d32[i] == MIZU_NA_INT32)
          memcpy(fd + i, &na_bits, 8);
        else
          fd[i] = (double) d32[i];
      }
      PyBuffer_Release(&fv);
      Py_DECREF(arr);
      return f64;
    }
    if (type == MIZU_TYPE_INT64 && foreign) {
      /* warn-only: the dtype never changes, so the scan joins the copy */
      const int64_t *s64 = (const int64_t *) src;
      int64_t *d64 = (int64_t *) v.buf;
      int na = 0;
      for (size_t i = 0; i < nelts; i++) {
        na |= s64[i] == MIZU_NA_INT64;
        d64[i] = s64[i];
      }
      PyBuffer_Release(&v);
      if (na &&
          PyErr_WarnEx(PyExc_UserWarning,
                       "pymizu: an int64 vector from R carries NA values, "
                       "which numpy int64 cannot represent: they read as "
                       "-2^63 (use .to_arrow() for the NA-honest form)",
                       1) < 0) {
        /* warnings-as-errors: the ordinary consumed content failure
           (INTEROP.md §6) — the slot is consumed, never wedged */
        Py_DECREF(arr);
        return NULL;
      }
      return arr;
    }
    memcpy(v.buf, src, len);
    PyBuffer_Release(&v);
    return arr;
  }
  PyObject *ba = PyByteArray_FromStringAndSize((const char *) src,
                                               (Py_ssize_t) len);
  if (ba == NULL) return NULL;
  PyObject *mv = PyMemoryView_FromObject(ba);
  Py_DECREF(ba);
  return mv;
}

// Zero-copy views (SHM_VEC / REF) -------------------------------------------

/* The shared mapping owner behind the consumer-side view cache: one per
   mapped region (page 0 RW for the refcount word, the tail read-only —
   mizu_shm_open_view's split), referenced by the cache and by every live
   view onto it, so an evicted entry's mapping closes only when its last
   view is gone (Python refcounting gives the ordering, as with buffer
   exports). pid is the fork guard: a child-side dealloc must not close a
   mapping whose loans it never added. */
typedef struct MizuShmOwner {
  PyObject_HEAD
  mizu_shm *shm;
  long pid;
} MizuShmOwner;

static void owner_dealloc(MizuShmOwner *self) {
  if (self->pid == mizu_self_pid())
    mizu_shm_close(self->shm, 0);
  MizuShmOwnerType.tp_free((PyObject *) self);
}

static PyTypeObject MizuShmOwnerType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._ShmOwner",
  .tp_basicsize = sizeof(MizuShmOwner),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "The shared owner of a view-tier region mapping.",
  .tp_dealloc = (destructor) owner_dealloc,
};

static MizuShmOwner *owner_new(mizu_shm *shm) {
  MizuShmOwner *o = (MizuShmOwner *) MizuShmOwnerType.tp_alloc(&MizuShmOwnerType, 0);
  if (o == NULL) return NULL;
  o->shm = shm;
  o->pid = mizu_self_pid();
  return o;
}

/* Per-handle name -> owner cache (LRU, MIZU_OPEN_CACHE_MAX entries), the
   analogue of mizu's zc open cache: repeated receives of the same region
   pay one open/mmap instead of one per view. Only the mapping is cached —
   names never alias and a region's size is fixed for its lifetime, so the
   mapping cannot go stale; the MIZH header validation in view_wrap_region
   still runs per read. Hung off binding.ctx (copied per handle at
   create/attach) and mirrored on the Python handle object for teardown.
   No locking: verbs are single-threaded per handle role and the GIL
   serializes the read callbacks. */


static MizuShmOwner *view_cache_lookup(MizuViewCache *vc, const char *name) {
  for (int i = 0; i < MIZU_OPEN_CACHE_MAX; i++)
    if (vc->owners[i] != NULL && strcmp(vc->names[i], name) == 0) {
      vc->stamp[i] = ++vc->tick;
      return vc->owners[i];
    }
  return NULL;
}

/* The cache takes its own reference; an evicted owner's mapping closes
   when its last view is gone. */
static void view_cache_insert(MizuViewCache *vc, const char *name,
                              MizuShmOwner *owner) {
  int slot = -1;
  for (int i = 0; i < MIZU_OPEN_CACHE_MAX; i++)
    if (vc->owners[i] == NULL) {
      slot = i;
      break;
    }
  if (slot < 0) {
    slot = 0;
    for (int i = 1; i < MIZU_OPEN_CACHE_MAX; i++)
      if (vc->stamp[i] < vc->stamp[slot]) slot = i;
    Py_DECREF(vc->owners[slot]);
  }
  Py_INCREF(owner);
  vc->owners[slot] = owner;
  snprintf(vc->names[slot], MIZU_NAME_MAX, "%s", name);
  vc->stamp[slot] = ++vc->tick;
}

static void view_cache_free(MizuViewCache *vc) {
  if (vc == NULL) return;
  for (int i = 0; i < MIZU_OPEN_CACHE_MAX; i++)
    Py_XDECREF(vc->owners[i]);
  PyMem_Free(vc);
}

/* One exporter per wrapped view: holds the region's mapping owner and
   releases the consumer's refcount loan at tp_dealloc. The view crosses
   as a buffer: a memoryview — or a numpy array from np.frombuffer — holds
   a reference that keeps the mapping alive (buffer exports pin via
   view->obj, so dealloc ordering is the buffer protocol's, not GC
   timing's). pid is the fork guard: a child-side dealloc must not sub a
   count it never added. */
typedef struct {
  PyObject_HEAD
  MizuShmOwner *owner;
  uint8_t *data;          /* the region base + MIZU_HEADER_SIZE */
  Py_ssize_t len;         /* data bytes */
  int type;               /* the wire type tag */
  long pid;
  Py_ssize_t shape[1];
  Py_ssize_t strides[1];
} MizuShmView;

/* A region-backed string view (MIZS): the same owner and loan machinery as
   the MIZH view, no buffer export — to_list() is the one string copy, an
   explicit one, and __arrow_c_array__ hands Arrow the string block's
   large_utf8 buffers in place. The wrap checks the two end offsets in
   O(1); every span is bounds-checked as read (the R reader's
   discipline). */
typedef struct {
  PyObject_HEAD
  MizuShmOwner *owner;
  const uint8_t *validity;   /* ceil(n / 8) bytes, LSB-first, 1 = present */
  const int64_t *offsets;    /* (n + 1) i64 offsets */
  const uint8_t *encoding;   /* n encoding bytes (MIZU_CE_*) */
  const uint8_t *data;       /* the packed string bytes */
  int64_t n;
  int64_t str_bytes;         /* the packed area's size; bounds each span */
  long pid;
} MizuShmStrView;

static const char *view_format(int type) {
  switch (type) {
  case MIZU_TYPE_REAL: return "d";
  case MIZU_TYPE_INT:
  case MIZU_TYPE_LGL: return "i";
  case MIZU_TYPE_INT64: return "q";
  case MIZU_TYPE_CPLX: return "Zd";
  default: return "B";
  }
}

static int view_getbuffer(PyObject *obj, Py_buffer *view, int flags) {
  MizuShmView *v = (MizuShmView *) obj;
  if (flags & PyBUF_WRITABLE) {
    PyErr_SetString(PyExc_BufferError,
                    "pymizu: shared-memory views are read-only");
    return -1;
  }
  Py_ssize_t elt = (Py_ssize_t) mizu_type_elt_size(v->type);
  v->shape[0] = v->len / elt;
  v->strides[0] = elt;
  view->buf = v->data;
  view->obj = obj;
  Py_INCREF(obj);
  view->len = v->len;
  view->readonly = 1;
  view->itemsize = elt;
  view->format = (flags & PyBUF_FORMAT) ? (char *) view_format(v->type) : NULL;
  view->ndim = 1;
  view->shape = (flags & PyBUF_ND) ? v->shape : NULL;
  /* PyBUF_STRIDES contains the PyBUF_ND bit: an ND-only request (the
     stage gate's) must get NULL strides, the protocol's C-contiguous
     form, or the view fails the gate's strides == NULL contiguity check */
  view->strides = ((flags & PyBUF_STRIDES) == PyBUF_STRIDES) ?
    v->strides : NULL;
  view->suboffsets = NULL;
  view->internal = NULL;
  return 0;
}

static void view_dealloc(MizuShmView *self) {
  if (self->owner != NULL) {
    if (self->pid == mizu_self_pid())
      mizu_zc_unref(self->owner->shm);   /* the consumer's loan, once-only */
    Py_DECREF(self->owner);
  }
  MizuShmViewType.tp_free((PyObject *) self);
}

static PyObject *view_refcount(PyObject *obj, void *Py_UNUSED(closure)) {
  MizuShmView *v = (MizuShmView *) obj;
  if (v->owner == NULL) Py_RETURN_NONE;
  return PyLong_FromUnsignedLong(mizu_zc_refcount(v->owner->shm));
}

static PyObject *view_flags(PyObject *obj, void *Py_UNUSED(closure)) {
  MizuShmView *v = (MizuShmView *) obj;
  if (v->owner == NULL) Py_RETURN_NONE;
  return PyLong_FromUnsignedLong(mizu_zc_flags(v->owner->shm));
}

static PyGetSetDef view_getset[] = {
  {"refcount", view_refcount, NULL,
   "The region's cross-process view refcount (introspection).", NULL},
  {"flags", view_flags, NULL,
   "The region's zero-copy flags word (bit 0: REFHELD; introspection).",
   NULL},
  {NULL}
};

// REF stage (a received view re-sent whole) ---------------------------------------

/* The view behind a read-only buffer: the object itself when it is a
   _ShmView (the numpy-less home), else the end of its .base chain —
   np.frombuffer(view).base is the view, and a slice's base is its parent
   array. Bounded hops; NULL with no exception set when the chain ends
   anywhere else. Returns a new reference. */
static MizuShmView *view_behind(PyObject *obj) {
  PyObject *cur = Py_NewRef(obj);
  for (int hop = 0; hop < 8; hop++) {
    if (Py_TYPE(cur) == &MizuShmViewType) return (MizuShmView *) cur;
    PyObject *base = PyObject_GetAttrString(cur, "base");
    Py_DECREF(cur);
    if (base == NULL) {
      PyErr_Clear();
      return NULL;
    }
    if (base == Py_None) {
      Py_DECREF(base);
      return NULL;
    }
    cur = base;
  }
  Py_DECREF(cur);
  return NULL;
}

/* The REF emit shared by every whole-view re-send: the region name and
   zero payload bytes — the mirror of mizu's mizu_zc_ref_stage. Before the
   name goes out the region is marked REFHELD: its holder set widens beyond
   the direct peer, so the producer's death verdict must leak + unlink
   rather than force-reclaim (mizu.h). Every consumer mapping has page 0
   read-write for the counted add, so the flag store goes through the
   owner's mapping. 0 staged, -1 the name cannot ride the payload (the
   caller falls through to a by-value stage). */
static int ref_emit(mizu_shm *shm, mizu_slot_hdr *hdr, uint8_t *payload,
                    uint32_t inline_max) {
  if (shm == NULL || shm->name_len <= 0 ||
      (size_t) shm->name_len > (size_t) inline_max)
    return -1;
  atomic_fetch_or_explicit(mizu_zc_flags_(mizu_shm_addr(shm)),
                           MIZU_ZC_FLAG_REFHELD, memory_order_acq_rel);
  hdr->kind = MIZU_KIND_REF;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = 0;
  memcpy(payload, shm->name, shm->name_len);
  return 0;
}

/* REF: a received view re-sent whole crosses as its region's name and zero
   payload bytes. The buffer must be the view's exact bytes (a slice, a
   reshape or a dtype reinterpretation goes by value) at the view's wire
   type — an LGL view read as int32 included, so a logical relayed whole
   returns to R as a logical. There is no COW-materialized case to exclude
   (R's data2 rule): a view refuses writable buffers, so its bytes are
   never private. 0 staged, -1 not a whole view (the caller falls
   through). */
static int stage_ref(PyObject *obj, const Py_buffer *v, mizu_slot_hdr *hdr,
                     uint8_t *payload, uint32_t inline_max) {
  MizuShmView *view = view_behind(obj);
  if (view == NULL) return -1;
  int rc = -1;
  mizu_shm *shm = view->owner != NULL ? view->owner->shm : NULL;
  int type = wire_type_of(v);
  if (shm != NULL && v->buf == (void *) view->data && v->len == view->len &&
      (type == view->type ||
       (type == MIZU_TYPE_INT && view->type == MIZU_TYPE_LGL)))
    rc = ref_emit(shm, hdr, payload, inline_max);
  Py_DECREF(view);
  return rc;
}

/* The string view's REF half: an exact _ShmStrView re-sent whole. The view
   admits no buffer, so whole is the only case — the fallback (the name
   does not fit) is the caller's by-value list. 0 staged, -1 not
   stageable. */
static int stage_ref_str(PyObject *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                         uint32_t inline_max) {
  MizuShmStrView *sv = (MizuShmStrView *) obj;
  return ref_emit(sv->owner != NULL ? sv->owner->shm : NULL, hdr, payload,
                  inline_max);
}

// Arrow export (the view's __arrow_c_array__) ------------------------------------

/* The export holds its own mapping and its own zc loan: mizu_shm_open_view
   at export (the counted add rides the open, exactly like a view-cache
   hit), mizu_zc_unref + mizu_shm_close at release. private_data carries no
   Python reference, so the release callback is pure C — callable from any
   thread at any time (a foreign consumer may release from a non-Python
   thread or after interpreter shutdown), with no GIL and no
   finalization edge. */
typedef struct {
  mizu_shm *shm;
  long pid;             /* the fork guard, mirroring view_dealloc */
  uint8_t *bits;        /* the LGL bit-pack, built at export (owned) */
  uint8_t *valid;       /* a lazily built validity bitmap (owned) */
} arrow_loan;

static void arrow_schema_release(ArrowSchema *s) {
  s->release = NULL;   /* the format is a string literal; nothing to free */
}

static void arrow_array_release(ArrowArray *a) {
  arrow_loan *loan = (arrow_loan *) a->private_data;
  if (loan != NULL) {
    if (loan->pid == mizu_self_pid()) mizu_zc_unref(loan->shm);
    mizu_shm_close(loan->shm, 0);
    free(loan->bits);
    free(loan->valid);
    free(loan);
  }
  free((void *) a->buffers);
  a->buffers = NULL;
  a->private_data = NULL;
  a->release = NULL;
}

/* The capsules own the struct memory; release (the consumer's call, or
   the destructor's for an unconsumed export) owns the buffers array and
   the loan. */
static void arrow_schema_cap_free(PyObject *cap) {
  ArrowSchema *s = (ArrowSchema *) PyCapsule_GetPointer(cap, "arrow_schema");
  if (s == NULL) {
    PyErr_Clear();
    return;
  }
  if (s->release != NULL) s->release(s);
  free(s);
}

static void arrow_array_cap_free(PyObject *cap) {
  ArrowArray *a = (ArrowArray *) PyCapsule_GetPointer(cap, "arrow_array");
  if (a == NULL) {
    PyErr_Clear();
    return;
  }
  if (a->release != NULL) a->release(a);
  free(a);
}

PyDoc_STRVAR(arrow_c_array_doc,
"__arrow_c_array__(requested_schema=None) -> (schema capsule, array capsule)\n\n\
Export the view through the Arrow C Data Interface: any Arrow consumer\n\
(pyarrow, polars, duckdb) wraps the shared pages zero-copy. The export\n\
holds its own mapping and refcount loan, released by the consumer's\n\
release callback. A logical exports as Arrow bool with a validity bitmap\n\
(the bit-packed values built at export); an integer or int64 exports with\n\
a validity bitmap when NAs are present — R's sentinels become Arrow\n\
nulls. NA_real_ reads as a NaN payload.");

static PyObject *view_arrow_c_array(PyObject *obj, PyObject *args,
                                    PyObject *kw) {
  static char *kwlist[] = {"requested_schema", NULL};
  PyObject *requested = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:__arrow_c_array__",
                                   kwlist, &requested))
    return NULL;
  /* requested_schema is ignored: the view has exactly one Arrow
     representation per wire type, so the spec's sanctioned fallback for
     an unsupported-but-compatible request — the default export — is the
     only answer. The capsule is borrowed; never released here. */
  MizuShmView *v = (MizuShmView *) obj;
  const char *fmt;
  int pack_lgl = 0, want_valid = 0;
  switch (v->type) {
  case MIZU_TYPE_LGL:
    fmt = "b";   /* every Arrow export of LGL is Arrow bool (3.2) */
    pack_lgl = 1;
    want_valid = 1;
    break;
  case MIZU_TYPE_INT: fmt = "i"; want_valid = 1; break;
  case MIZU_TYPE_REAL: fmt = "g"; break;
  case MIZU_TYPE_INT64: fmt = "l"; want_valid = 1; break;
  case MIZU_TYPE_RAW: fmt = "C"; break;
  default:
    PyErr_SetString(PyExc_TypeError,
                    "pymizu: complex vectors have no standard Arrow type");
    return NULL;
  }
  if (v->owner == NULL) {
    PyErr_SetString(MizuError, "pymizu: the view has no region");
    return NULL;
  }
  /* a fresh mapping of the same region (pages shared); the counted add
     rides the open. Cannot fail while the view lives — its own loan
     keeps the region lent and its name resolvable — unless a dead
     producer's force-reclaim unlinked the name: then a clean MizuError
     (the view itself keeps working; its mapping survives unlink). */
  mizu_shm *shm;
  if (mizu_shm_open_view(&shm, mizu_shm_name(v->owner->shm)) != MIZU_OK) {
    raise_tls();
    return NULL;
  }
  /* capsule-wrap each struct immediately (release = NULL until fully
     initialized): on any error the destructors clean up — structs and
     the buffers array are never freed by hand once wrapped */
  ArrowSchema *schema = (ArrowSchema *) calloc(1, sizeof(ArrowSchema));
  ArrowArray *array = (ArrowArray *) calloc(1, sizeof(ArrowArray));
  const void **buffers = (const void **) calloc(2, sizeof(void *));
  arrow_loan *loan = (arrow_loan *) malloc(sizeof(arrow_loan));
  PyObject *scap = schema != NULL ?
    PyCapsule_New(schema, "arrow_schema", arrow_schema_cap_free) : NULL;
  PyObject *acap = array != NULL ?
    PyCapsule_New(array, "arrow_array", arrow_array_cap_free) : NULL;
  if (scap == NULL || acap == NULL || buffers == NULL || loan == NULL) {
    if (scap == NULL) free(schema);
    if (acap == NULL) free(array);
    Py_XDECREF(scap);
    Py_XDECREF(acap);
    free(buffers);
    free(loan);
    mizu_zc_unref(shm);
    mizu_shm_close(shm, 0);
    if (!PyErr_Occurred()) PyErr_NoMemory();
    return NULL;
  }
  loan->shm = shm;
  loan->pid = mizu_self_pid();
  loan->bits = NULL;
  loan->valid = NULL;
  const uint8_t *base = (const uint8_t *) mizu_shm_addr(shm);
  const int64_t n =
    (int64_t) (v->len / (Py_ssize_t) mizu_type_elt_size(v->type));
  const size_t nb = ((size_t) n + 7) / 8;
  /* the validity source: the region's section when present, the lazy
     sentinel build when absent ({0, 0}), none when known-NA-free
     ({0, -1}) */
  const uint8_t *validity = NULL;
  int64_t null_count = 0;
  int build_valid = 0;
  if (want_valid) {
    int64_t voff, vcount;
    memcpy(&voff, base + MIZU_HDR_VALID_OFF, 8);
    memcpy(&vcount, base + MIZU_HDR_VALID_COUNT, 8);
    if (voff > 0) {
      validity = base + voff;
      null_count = vcount;
    } else if (vcount == 0) {
      build_valid = 1;
    }
  }
  if (pack_lgl) {
    /* the bit-packed values build at export, nulls or not; the lazy
       validity build fuses into the same pass */
    loan->bits = (uint8_t *) calloc(nb != 0 ? nb : 1, 1);
    if (loan->bits == NULL) goto nomem;
    const int32_t *d32 = (const int32_t *) (base + MIZU_HEADER_SIZE);
    if (build_valid) {
      loan->valid = (uint8_t *) malloc(nb != 0 ? nb : 1);
      if (loan->valid == NULL) goto nomem;
      memset(loan->valid, 0xFF, nb);
      if (n % 8 != 0)
        loan->valid[nb - 1] &= (uint8_t) ((1u << (n % 8)) - 1);
      for (int64_t i = 0; i < n; i++) {
        if (d32[i] == MIZU_NA_INT32) {
          null_count++;
          loan->valid[i / 8] &= (uint8_t) ~(1u << (i % 8));
        } else if (d32[i] != 0) {
          loan->bits[i / 8] |= (uint8_t) (1u << (i % 8));
        }
      }
      if (null_count == 0) {
        free(loan->valid);   /* clean: no bitmap */
        loan->valid = NULL;
      } else {
        validity = loan->valid;
      }
    } else {
      /* the section (or known-NA-free) defines the nulls; the pack is
         values only (a null lane's bit is don't-care, Arrow masks it) */
      for (int64_t i = 0; i < n; i++)
        if (d32[i] != 0) loan->bits[i / 8] |= (uint8_t) (1u << (i % 8));
    }
    buffers[1] = loan->bits;
  } else {
    buffers[1] = base + MIZU_HEADER_SIZE;
    if (build_valid) {
      /* scan-only for INT/INT64: the values stay the region's pages. One
         fused pass counts and clears into an optimistically allocated
         bitmap, freed when clean. Typed sentinel constants per branch —
         an int64 sentinel variable against int32 loads miscompiles here
         (clang 17/21 -O2 widens the loads). */
      loan->valid = (uint8_t *) malloc(nb != 0 ? nb : 1);
      if (loan->valid == NULL) goto nomem;
      memset(loan->valid, 0xFF, nb);
      if (n % 8 != 0)
        loan->valid[nb - 1] &= (uint8_t) ((1u << (n % 8)) - 1);
      if (v->type == MIZU_TYPE_INT) {
        const int32_t *d32 = (const int32_t *) (base + MIZU_HEADER_SIZE);
        for (int64_t i = 0; i < n; i++)
          if (d32[i] == MIZU_NA_INT32) {
            null_count++;
            loan->valid[i / 8] &= (uint8_t) ~(1u << (i % 8));
          }
      } else {
        const int64_t *d64 = (const int64_t *) (base + MIZU_HEADER_SIZE);
        for (int64_t i = 0; i < n; i++)
          if (d64[i] == MIZU_NA_INT64) {
            null_count++;
            loan->valid[i / 8] &= (uint8_t) ~(1u << (i % 8));
          }
      }
      if (null_count == 0) {
        free(loan->valid);
        loan->valid = NULL;
      } else {
        validity = loan->valid;
      }
    }
  }
  schema->format = fmt;
  schema->release = arrow_schema_release;
  array->length = n;
  array->null_count = null_count;
  array->n_buffers = 2;
  array->buffers = buffers;
  buffers[0] = validity;
  array->private_data = loan;
  array->release = arrow_array_release;
  PyObject *out = PyTuple_New(2);
  if (out == NULL) {
    Py_DECREF(scap);
    Py_DECREF(acap);
    return NULL;
  }
  PyTuple_SET_ITEM(out, 0, scap);
  PyTuple_SET_ITEM(out, 1, acap);
  return out;
nomem:
  Py_DECREF(scap);
  Py_DECREF(acap);
  PyErr_NoMemory();
  return NULL;
}

static PyMethodDef view_methods[] = {
  {"__arrow_c_array__", (PyCFunction)(void (*)(void)) view_arrow_c_array,
   METH_VARARGS | METH_KEYWORDS, arrow_c_array_doc},
  {NULL}
};

static PyBufferProcs view_as_buffer = {
  .bf_getbuffer = view_getbuffer,
  .bf_releasebuffer = NULL,
};

static PyTypeObject MizuShmViewType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._ShmView",
  .tp_basicsize = sizeof(MizuShmView),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A zero-copy view over a shared-memory region (buffer exporter).",
  .tp_dealloc = (destructor) view_dealloc,
  .tp_as_buffer = &view_as_buffer,
  .tp_getset = view_getset,
  .tp_methods = view_methods,
};

// Region-backed string views (MIZS) ----------------------------------------------

static void strview_dealloc(MizuShmStrView *self) {
  if (self->owner != NULL) {
    if (self->pid == mizu_self_pid())
      mizu_zc_unref(self->owner->shm);   /* the consumer's loan, once-only */
    Py_DECREF(self->owner);
  }
  MizuShmStrViewType.tp_free((PyObject *) self);
}

/* The per-element span check, fused into every read loop. */
static int strview_span(const MizuShmStrView *sv, int64_t i, int64_t *lo,
                        int64_t *hi) {
  *lo = sv->offsets[i];
  *hi = sv->offsets[i + 1];
  return *lo >= 0 && *hi >= *lo && *hi <= sv->str_bytes;
}

PyDoc_STRVAR(strview_to_list_doc,
"to_list() -> list[str | None]\n\n\
Materialize the strings: the one copy the view makes, an explicit one.\n\
NA elements read as None. A latin1/bytes-marked element declines (the\n\
sender's MIZS filter keeps one off this path — a defense), as does an\n\
element that is not valid UTF-8.");

static PyObject *strview_to_list(PyObject *obj, PyObject *Py_UNUSED(dummy)) {
  MizuShmStrView *sv = (MizuShmStrView *) obj;
  PyObject *out = PyList_New((Py_ssize_t) sv->n);
  if (out == NULL) return NULL;
  for (int64_t i = 0; i < sv->n; i++) {
    PyObject *s = NULL;
    if (sv->validity[i / 8] & (1u << (i % 8))) {
      int64_t lo, hi;
      unsigned enc = sv->encoding[i];
      if (!strview_span(sv, i, &lo, &hi)) {
        PyErr_SetString(MizuError,
                        "pymizu: invalid string data in shared region");
      } else if (enc == MIZU_CE_LATIN1 || enc == MIZU_CE_BYTES) {
        PyErr_Format(MizuError,
                     "pymizu: string %lld has an encoding that does not "
                     "cross (latin1/bytes)", (long long) i);
      } else {
        /* CE_UTF8 and CE_NATIVE both decode as UTF-8 (a native-marked
           element the sender admitted is UTF-8-validated there; the strict
           decode below is the defense) */
        s = PyUnicode_FromStringAndSize((const char *) sv->data + lo,
                                        (Py_ssize_t) (hi - lo));
      }
      if (s == NULL) {
        Py_DECREF(out);
        return NULL;
      }
    } else {
      s = Py_None;
      Py_INCREF(s);
    }
    PyList_SET_ITEM(out, (Py_ssize_t) i, s);
  }
  return out;
}

/* Strict UTF-8 validation of a CE_NATIVE span at Arrow export (the
   sender's MIZS filter admits a native-marked element only when it
   validates as UTF-8; this is the reader-side defense). */
static int utf8_valid(const uint8_t *s, int64_t n) {
  int64_t i = 0;
  while (i < n) {
    uint8_t c = s[i];
    if (c < 0x80) {
      i++;
      continue;
    }
    int64_t need;
    uint32_t cp;
    if ((c & 0xE0) == 0xC0) {
      need = 1;
      cp = c & 0x1F;
      if (cp == 0) return 0;                 /* overlong */
    } else if ((c & 0xF0) == 0xE0) {
      need = 2;
      cp = c & 0x0F;
    } else if ((c & 0xF8) == 0xF0) {
      need = 3;
      cp = c & 0x07;
    } else {
      return 0;
    }
    if (i + need >= n) return 0;
    for (int64_t k = 1; k <= need; k++) {
      if ((s[i + k] & 0xC0) != 0x80) return 0;
      cp = (cp << 6) | (s[i + k] & 0x3F);
    }
    if ((need == 1 && cp < 0x80) || (need == 2 && cp < 0x800) ||
        (need == 3 && cp < 0x10000) || cp > 0x10FFFF ||
        (cp >= 0xD800 && cp <= 0xDFFF))
      return 0;                              /* overlong / out of range */
    i += need + 1;
  }
  return 1;
}

PyDoc_STRVAR(strview_arrow_c_array_doc,
"__arrow_c_array__(requested_schema=None) -> (schema capsule, array capsule)\n\n\
Export the strings through the Arrow C Data Interface as large_utf8:\n\
the validity bitmap, the i64 offsets and the packed bytes are Arrow's\n\
three buffers, handed over in place (zero-copy) on the export's own\n\
mapping and refcount loan. Every element must be UTF-8-marked; a\n\
latin1/bytes element declines the export, naming its index.");

static PyObject *strview_arrow_c_array(PyObject *obj, PyObject *args,
                                       PyObject *kw) {
  static char *kwlist[] = {"requested_schema", NULL};
  PyObject *requested = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:__arrow_c_array__",
                                   kwlist, &requested))
    return NULL;
  /* requested_schema ignored — the one Arrow representation, as for the
     MIZH view's export */
  MizuShmStrView *sv = (MizuShmStrView *) obj;
  if (sv->owner == NULL) {
    PyErr_SetString(MizuError, "pymizu: the view has no region");
    return NULL;
  }
  /* one fused pass ahead of the export: span bounds and encoding bytes
     (the consumer reads the buffers unguarded), UTF-8-validating
     CE_NATIVE spans, the null count */
  int64_t null_count = 0;
  for (int64_t i = 0; i < sv->n; i++) {
    if (!(sv->validity[i / 8] & (1u << (i % 8)))) {
      null_count++;
      continue;
    }
    int64_t lo, hi;
    unsigned enc = sv->encoding[i];
    if (!strview_span(sv, i, &lo, &hi)) {
      PyErr_SetString(MizuError,
                      "pymizu: invalid string data in shared region");
      return NULL;
    }
    if (enc == MIZU_CE_LATIN1 || enc == MIZU_CE_BYTES) {
      PyErr_Format(MizuError,
                   "pymizu: string %lld has an encoding that does not cross "
                   "to Arrow (latin1/bytes)", (long long) i);
      return NULL;
    }
    if (enc == MIZU_CE_NATIVE && !utf8_valid(sv->data + lo, hi - lo)) {
      PyErr_Format(MizuError,
                   "pymizu: string %lld is not valid UTF-8", (long long) i);
      return NULL;
    }
  }
  /* a fresh mapping of the same region (pages shared); the counted add
     rides the open, exactly as in view_arrow_c_array */
  mizu_shm *shm;
  if (mizu_shm_open_view(&shm, mizu_shm_name(sv->owner->shm)) != MIZU_OK) {
    raise_tls();
    return NULL;
  }
  ArrowSchema *schema = (ArrowSchema *) calloc(1, sizeof(ArrowSchema));
  ArrowArray *array = (ArrowArray *) calloc(1, sizeof(ArrowArray));
  const void **buffers = (const void **) calloc(3, sizeof(void *));
  arrow_loan *loan = (arrow_loan *) malloc(sizeof(arrow_loan));
  PyObject *scap = schema != NULL ?
    PyCapsule_New(schema, "arrow_schema", arrow_schema_cap_free) : NULL;
  PyObject *acap = array != NULL ?
    PyCapsule_New(array, "arrow_array", arrow_array_cap_free) : NULL;
  if (scap == NULL || acap == NULL || buffers == NULL || loan == NULL) {
    if (scap == NULL) free(schema);
    if (acap == NULL) free(array);
    Py_XDECREF(scap);
    Py_XDECREF(acap);
    free(buffers);
    free(loan);
    mizu_zc_unref(shm);
    mizu_shm_close(shm, 0);
    if (!PyErr_Occurred()) PyErr_NoMemory();
    return NULL;
  }
  loan->shm = shm;
  loan->pid = mizu_self_pid();
  mizu_mizs_geom g = mizu_mizs_geometry(sv->n);
  const uint8_t *block =
    (const uint8_t *) mizu_shm_addr(shm) + MIZU_HEADER_SIZE;
  schema->format = "U";   /* large_utf8 */
  schema->release = arrow_schema_release;
  array->length = sv->n;
  array->null_count = null_count;
  array->n_buffers = 3;
  array->buffers = buffers;
  buffers[0] = block + g.validity;
  buffers[1] = block + g.offsets;
  buffers[2] = block + g.data;
  array->private_data = loan;
  array->release = arrow_array_release;
  PyObject *out = PyTuple_New(2);
  if (out == NULL) {
    Py_DECREF(scap);
    Py_DECREF(acap);
    return NULL;
  }
  PyTuple_SET_ITEM(out, 0, scap);
  PyTuple_SET_ITEM(out, 1, acap);
  return out;
}

static PyMethodDef strview_methods[] = {
  {"to_list", (PyCFunction)(void (*)(void)) strview_to_list, METH_NOARGS,
   strview_to_list_doc},
  {"__arrow_c_array__", (PyCFunction)(void (*)(void)) strview_arrow_c_array,
   METH_VARARGS | METH_KEYWORDS, strview_arrow_c_array_doc},
  {NULL}
};

static PyTypeObject MizuShmStrViewType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._ShmStrView",
  .tp_basicsize = sizeof(MizuShmStrView),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A zero-copy string view over a shared-memory region (MIZS).",
  .tp_dealloc = (destructor) strview_dealloc,
  .tp_methods = strview_methods,
};

/* Validate the MIZS header at the region base and wrap it as a string
   view. aux != 0 (the SHM_VEC wire form) cross-checks the staged type and
   exact byte count against the header. Attributes (names/class) decline —
   the sender's foreign filter keeps an attributed string vector off this
   path. On failure the exception is set here and the caller subs the
   loan. */
static PyObject *strview_wrap(MizuShmOwner *owner, uint64_t aux) {
  mizu_shm *shm = owner->shm;
  const uint8_t *base = (const uint8_t *) mizu_shm_addr(shm);
  size_t size = mizu_shm_size(shm);
  int64_t n = 0, block = 0, attrs = 0;
  if (mizu_mizs_check(base, size, &n, &block, &attrs) != 0)
    goto corrupt;
  if (aux != 0 &&
      ((uint32_t) mizu_aux_type(aux) != (uint32_t) MIZU_TYPE_STR ||
       mizu_aux_hi(aux) !=
         (uint64_t) ((int64_t) MIZU_HEADER_SIZE + block + attrs)))
    goto corrupt;
  if (attrs != 0) {
    PyErr_SetString(MizuError,
                    "pymizu: R shared-string payload carries attributes "
                    "(names/class), which do not cross the view tier");
    return NULL;
  }
  {
    mizu_mizs_geom g = mizu_mizs_geometry(n);
    const uint8_t *offs = base + MIZU_HEADER_SIZE + g.offsets;
    int64_t first, last;
    memcpy(&first, offs, 8);
    memcpy(&last, offs + 8 * n, 8);
    if (first != 0 || last < 0 || last > block - g.data)
      goto corrupt;
    MizuShmStrView *sv =
      (MizuShmStrView *) MizuShmStrViewType.tp_alloc(&MizuShmStrViewType, 0);
    if (sv == NULL) return NULL;
    Py_INCREF(owner);
    sv->owner = owner;
    sv->validity = base + MIZU_HEADER_SIZE + g.validity;
    sv->offsets = (const int64_t *) (base + MIZU_HEADER_SIZE + g.offsets);
    sv->encoding = base + MIZU_HEADER_SIZE + g.encoding;
    sv->data = base + MIZU_HEADER_SIZE + g.data;
    sv->n = n;
    sv->str_bytes = block - g.data;
    sv->pid = mizu_self_pid();
    return (PyObject *) sv;
  }
corrupt:
  PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
  return NULL;
}

static PyObject *numpy_module(void) {
  if (!mizu_numpy_probed) {
    mizu_numpy_probed = 1;
    PyObject *np = PyImport_ImportModule("numpy");
    if (np == NULL) {
      PyErr_Clear();
      np = Py_None;
      Py_INCREF(Py_None);
    }
    mizu_numpy = np;
  }
  return mizu_numpy == Py_None ? NULL : mizu_numpy;
}

/* The user-facing object over the exporter: a numpy array from
   np.frombuffer when numpy is present (zero-copy; the array's base pins
   the exporter), else the view itself — a read-only buffer exporter
   (memoryview() and np.frombuffer() accept it) that also carries the
   __arrow_c_array__ dunder. Steals the view reference. */
static PyObject *view_to_object(PyObject *view, int type) {
  PyObject *np = numpy_module();
  if (np != NULL) {
    const char *dt;
    switch (type) {
    case MIZU_TYPE_REAL: dt = "float64"; break;
    case MIZU_TYPE_INT:
    case MIZU_TYPE_LGL: dt = "int32"; break;
    case MIZU_TYPE_INT64: dt = "int64"; break;
    case MIZU_TYPE_CPLX: dt = "complex128"; break;
    case MIZU_TYPE_RAW: dt = "uint8"; break;
    default:
      PyErr_Format(MizuError, "pymizu: unknown wire type %d", type);
      Py_DECREF(view);
      return NULL;
    }
    PyObject *fb = PyObject_GetAttrString(np, "frombuffer");
    if (fb != NULL) {
      PyObject *args = PyTuple_Pack(1, view);
      PyObject *kw = Py_BuildValue("{s:s}", "dtype", dt);
      PyObject *arr = (args != NULL && kw != NULL) ?
        PyObject_Call(fb, args, kw) : NULL;
      Py_XDECREF(args);
      Py_XDECREF(kw);
      Py_DECREF(fb);
      if (arr != NULL) {
        Py_DECREF(view);
        return arr;
      }
      PyErr_Clear();   /* fall back to the view itself */
    } else {
      PyErr_Clear();
    }
  }
  return view;
}

/* Validate the MIZH header at the region base and wrap it as a view. aux
   != 0 (the SHM_VEC wire form) cross-checks the staged type and exact byte
   count against the header. R attributes (names/dim/class) ride the layout
   but cannot cross to a Python buffer — their presence is an informative
   error, never a silent drop. Takes over the zc loan on owner's mapping
   (subbed on failure); the owner reference stays the caller's throughout. */
static PyObject *view_wrap_region(MizuShmOwner *owner, uint64_t aux) {
  mizu_shm *shm = owner->shm;
  uint8_t *base = (uint8_t *) mizu_shm_addr(shm);
  int64_t size = (int64_t) mizu_shm_size(shm);
  int type = 0;
  int64_t length = 0;
  const char *err = NULL;
  if (size >= (int64_t) MIZU_HEADER_SIZE) {
    uint32_t magic;
    memcpy(&magic, base, 4);
    if (magic == MIZU_MAGIC_STR) {
      PyObject *sv = strview_wrap(owner, aux);
      if (sv == NULL) mizu_zc_unref(shm);
      return sv;
    }
    if (magic != MIZU_MAGIC_VEC) {
      err = "pymizu: unsupported shared-payload layout (R list views "
            "cannot cross to Python)";
      goto fail;
    }
  }
  /* size, magic, wire type, the extents, the flags word and the validity
     section ride the core's MIZH check; attribute policy stays here */
  int64_t valid[2];
  if (mizu_mizh_check(base, (size_t) size, &type, &length, valid) != 0)
    goto corrupt;
  size_t elt = mizu_type_elt_size(type);
  {
    int64_t attrs;
    memcpy(&attrs, base + 16, 8);
    if (aux != 0 &&
        ((uint32_t) mizu_aux_type(aux) != (uint32_t) type ||
         mizu_aux_hi(aux) != (uint64_t) ((size_t) MIZU_HEADER_SIZE +
                                        (size_t) length * elt +
                                        (size_t) attrs)))
      goto corrupt;
    if (attrs != 0) {
      err = "pymizu: R shared-vector payload carries attributes "
            "(names/dim/class), which do not cross the view tier";
      goto fail;
    }
  }
  {
    MizuShmView *v = (MizuShmView *) MizuShmViewType.tp_alloc(&MizuShmViewType, 0);
    if (v == NULL) goto fail;
    Py_INCREF(owner);
    v->owner = owner;
    v->data = base + MIZU_HEADER_SIZE;
    v->len = (Py_ssize_t) length * (Py_ssize_t) elt;
    v->type = type;
    v->pid = mizu_self_pid();
    return view_to_object((PyObject *) v, type);
  }
corrupt:
  err = "pymizu: corrupt payload slot";
fail:
  mizu_zc_unref(shm);
  PyErr_SetString(MizuError, err);
  return NULL;
}

/* SHM_VEC: the region name in the payload, the staged type and exact byte
   count in aux. The handle's view cache serves repeat opens of a live
   mapping (a hit's counted add mirrors mizu_shm_open_view's); a miss opens
   fresh and inserts. A vanished region is the realistic open failure — the
   consumer-done protocol guarantees the name outlives the frame, so a miss
   means the producer died; propagate via ctx->gone. */
static PyObject *read_shm_vec(const uint8_t *name, uint32_t name_len,
                              uint64_t aux, mizu_read_ctx *ctx) {
  char buf[MIZU_NAME_MAX];
  memcpy(buf, name, name_len);
  buf[name_len] = '\0';
  MizuViewCache *vc = (MizuViewCache *) ctx->binding_ctx;
  MizuShmOwner *owner = vc != NULL ? view_cache_lookup(vc, buf) : NULL;
  if (owner != NULL) {
    mizu_zc_ref(owner->shm);
    Py_INCREF(owner);
  } else {
    mizu_shm *shm;
    if (mizu_shm_open_view(&shm, buf) != MIZU_OK) {
      ctx->gone = 1;
      return NULL;
    }
    owner = owner_new(shm);
    if (owner == NULL) {
      mizu_zc_unref(shm);
      mizu_shm_close(shm, 0);
      return NULL;
    }
    if (vc != NULL) view_cache_insert(vc, buf, owner);
  }
  PyObject *r = view_wrap_region(owner, aux);
  Py_DECREF(owner);
  return r;
}

/* REF: the /mizu_ identifier of an object already in shm — the region name,
   then an optional [i]... path into a list tree. A path leaf has no buffer
   view (it lives inside an MIZL layout); a bare name resolves to the same
   wrap as SHM_VEC, the counted add riding mizu_shm_open_view. */
static PyObject *read_ref(const mizu_slot_hdr *hdr, const uint8_t *payload,
                          mizu_read_ctx *ctx) {
  uint32_t name_len = 0;
  while (name_len < hdr->len && payload[name_len] != '[') name_len++;
  if (name_len < sizeof(MIZU_PREFIX_LITERAL) - 1 || name_len >= MIZU_NAME_MAX ||
      memcmp(payload, MIZU_PREFIX_LITERAL,
             sizeof(MIZU_PREFIX_LITERAL) - 1) != 0) {
    PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
    return NULL;
  }
  if (name_len < hdr->len) {
    PyErr_SetString(MizuError, "pymizu: a view into an R list tree cannot "
                    "cross to Python");
    return NULL;
  }
  return read_shm_vec(payload, name_len, 0, ctx);
}

/* Codec read side: strict bounds throughout; anything torn or trailing is
   a corrupt slot. */
static uint32_t codec_get32(const uint8_t **p) {
  uint32_t v = (uint32_t) (*p)[0] | ((uint32_t) (*p)[1] << 8) |
    ((uint32_t) (*p)[2] << 16) | ((uint32_t) (*p)[3] << 24);
  *p += 4;
  return v;
}

static uint64_t codec_get64(const uint8_t **p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; i++) v |= (uint64_t) (*p)[i] << (8 * i);
  *p += 8;
  return v;
}

static PyObject *codec_read_scalar(const uint8_t **p, const uint8_t *end) {
  if (*p >= end) return NULL;
  switch (*(*p)++) {
  case PYMIZU_TAG_NONE:
    Py_RETURN_NONE;
  case PYMIZU_TAG_BOOL: {
    if ((size_t) (end - *p) < 1) return NULL;
    int v = *(*p)++;
    if (v > 1) return NULL;
    return PyBool_FromLong(v);
  }
  case PYMIZU_TAG_INT: {
    if ((size_t) (end - *p) < 8) return NULL;
    int64_t v = (int64_t) codec_get64(p);
    return PyLong_FromLongLong(v);
  }
  case PYMIZU_TAG_FLOAT: {
    if ((size_t) (end - *p) < 8) return NULL;
    uint64_t u = codec_get64(p);
    double d;
    memcpy(&d, &u, 8);
    return PyFloat_FromDouble(d);
  }
  case PYMIZU_TAG_STR: {
    if ((size_t) (end - *p) < 4) return NULL;
    uint32_t n = codec_get32(p);
    if ((size_t) (end - *p) < n) return NULL;
    PyObject *s = PyUnicode_DecodeUTF8((const char *) *p, (Py_ssize_t) n,
                                       NULL);
    if (s == NULL) return NULL;
    *p += n;
    return s;
  }
  case PYMIZU_TAG_BYTES: {
    if ((size_t) (end - *p) < 4) return NULL;
    uint32_t n = codec_get32(p);
    if ((size_t) (end - *p) < n) return NULL;
    PyObject *b = PyBytes_FromStringAndSize((const char *) *p,
                                            (Py_ssize_t) n);
    if (b == NULL) return NULL;
    *p += n;
    return b;
  }
  }
  return NULL;
}

/* Frame leaf readers (the task-frame wire tags). NULL with no error set
   is a corrupt stream; NULL with ctx->gone is a vanished BUFREF region —
   both propagate unchanged through the frame readers. */
static PyObject *frame_read_flat(const uint8_t **p, const uint8_t *end,
                                 mizu_read_ctx *ctx) {
  if (*p >= end) return NULL;
  switch (**p) {
  case PYMIZU_TAG_BUFFER: {
    if ((size_t) (end - *p) < 10) return NULL;
    (*p)++;
    int type = *(*p)++;
    uint64_t n = codec_get64(p);
    if (n > UINT32_MAX || (uint64_t) (end - *p) < n) return NULL;
    /* the private codec is same-language only: the identity read, no
       scan gate (foreign = 0) */
    PyObject *r = read_raw(*p, (uint32_t) n, type, 0);
    if (r != NULL) *p += n;
    return r;
  }
  case PYMIZU_TAG_BUFREF: {
    if ((size_t) (end - *p) < 11) return NULL;
    (*p)++;
    uint64_t type = *(*p)++;
    uint64_t n = codec_get64(p);
    uint32_t name_len = *(*p)++;
    if (name_len == 0 || name_len >= MIZU_NAME_MAX ||
        (size_t) (end - *p) < name_len || n > (UINT64_MAX >> 8))
      return NULL;
    /* the SHM_VEC aux shape: the staged type and the exact byte count */
    uint64_t aux = mizu_aux_shm_vec((int) type, MIZU_HEADER_SIZE + n);
    PyObject *r = read_shm_vec(*p, name_len, aux, ctx);
    if (r != NULL) *p += name_len;
    return r;
  }
  default:
    return codec_read_scalar(p, end);
  }
}

static PyObject *frame_read_leaf(const uint8_t **p, const uint8_t *end,
                                 mizu_read_ctx *ctx) {
  if (*p >= end) return NULL;
  int tag = **p;
  if (tag == PYMIZU_TAG_LIST || tag == PYMIZU_TAG_TUPLE) {
    (*p)++;
    if ((size_t) (end - *p) < 4) return NULL;
    uint32_t count = codec_get32(p);
    if (count > PYMIZU_CODEC_CAP) return NULL;
    PyObject *out = tag == PYMIZU_TAG_LIST ? PyList_New((Py_ssize_t) count) :
      PyTuple_New((Py_ssize_t) count);
    if (out == NULL) return NULL;
    for (uint32_t i = 0; i < count; i++) {
      PyObject *it = frame_read_flat(p, end, ctx);
      if (it == NULL) {
        Py_DECREF(out);
        return NULL;
      }
      if (tag == PYMIZU_TAG_LIST)
        PyList_SET_ITEM(out, (Py_ssize_t) i, it);
      else
        PyTuple_SET_ITEM(out, (Py_ssize_t) i, it);
    }
    return out;
  }
  if (tag == PYMIZU_TAG_DICT) {
    (*p)++;
    if ((size_t) (end - *p) < 4) return NULL;
    uint32_t count = codec_get32(p);
    if (count > PYMIZU_CODEC_CAP) return NULL;
    PyObject *out = PyDict_New();
    if (out == NULL) return NULL;
    for (uint32_t i = 0; i < count; i++) {
      PyObject *k = codec_read_scalar(p, end);
      if (k != NULL && !PyUnicode_Check(k)) {
        Py_DECREF(k);
        k = NULL;
      }
      PyObject *v = k != NULL ? frame_read_flat(p, end, ctx) : NULL;
      if (k == NULL || v == NULL) {
        Py_XDECREF(k);
        Py_XDECREF(v);
        Py_DECREF(out);
        return NULL;
      }
      int rc = PyDict_SetItem(out, k, v);
      Py_DECREF(k);
      Py_DECREF(v);
      if (rc < 0) {
        Py_DECREF(out);
        return NULL;
      }
    }
    return out;
  }
  return frame_read_flat(p, end, ctx);
}

/* A by-reference fn: import the module (sys.modules-cached), then walk
   the qualname attribute path. A resolution failure (a missing module on
   the worker) raises normally — inside py_exec's read_impl call it
   becomes the task's ERR envelope via the publish_exc discipline. */
static PyObject *fn_resolve(PyObject *mod, PyObject *qual) {
  const char *name = PyUnicode_AsUTF8(mod);
  if (name == NULL) return NULL;
  PyObject *obj = PyImport_ImportModule(name);
  if (obj == NULL) return NULL;
  Py_ssize_t qi = 0, qn = PyUnicode_GetLength(qual);
  if (qn < 0) {
    Py_DECREF(obj);
    return NULL;
  }
  while (qi < qn) {
    Py_ssize_t dot = qi;
    while (dot < qn && PyUnicode_ReadChar(qual, dot) != '.') dot++;
    if (dot < 0) break;
    PyObject *part = PyUnicode_Substring(qual, qi, dot);
    if (part == NULL) {
      Py_DECREF(obj);
      return NULL;
    }
    PyObject *next = PyObject_GetAttr(obj, part);
    Py_DECREF(part);
    Py_DECREF(obj);
    if (next == NULL) return NULL;
    obj = next;
    qi = dot + 1;
  }
  return obj;
}

/* The task frame: fn kind byte (0 by reference, 1 pickled), the args
   tuple, the kwargs dict. Decodes to a plain exact-type
   (fn, args, kwargs) tuple, so py_exec's shape validation is unchanged. */
static PyObject *frame_read_task(const uint8_t **p, const uint8_t *end,
                                 mizu_read_ctx *ctx) {
  if ((size_t) (end - *p) < 1) return NULL;
  int kind = *(*p)++;
  PyObject *fn = NULL;
  if (kind == 0) {
    PyObject *mod = codec_read_scalar(p, end);
    PyObject *qual = mod != NULL ? codec_read_scalar(p, end) : NULL;
    if (qual == NULL) {
      Py_XDECREF(mod);
      return NULL;
    }
    if (PyUnicode_Check(mod) && PyUnicode_Check(qual))
      fn = fn_resolve(mod, qual);
    Py_DECREF(mod);
    Py_DECREF(qual);
  } else if (kind == 1) {
    PyObject *b = codec_read_scalar(p, end);
    if (b == NULL) return NULL;
    if (!PyBytes_Check(b)) {
      Py_DECREF(b);
      return NULL;
    }
    fn = PyObject_CallFunction(mizu_loads, "y#", PyBytes_AS_STRING(b),
                               PyBytes_GET_SIZE(b));
    Py_DECREF(b);
  } else {
    return NULL;
  }
  if (fn == NULL) return NULL;
  /* args: a tuple of leaves (elements may be one container level);
     kwargs: a dict of str keys to leaves */
  PyObject *args = NULL, *kwargs = NULL;
  if ((size_t) (end - *p) < 5 || *(*p)++ != PYMIZU_TAG_TUPLE) goto fail;
  {
    uint32_t na = codec_get32(p);
    if (na > PYMIZU_CODEC_CAP) goto fail;
    args = PyTuple_New((Py_ssize_t) na);
    if (args == NULL) goto fail;
    for (uint32_t i = 0; i < na; i++) {
      PyObject *it = frame_read_leaf(p, end, ctx);
      if (it == NULL) goto fail;
      PyTuple_SET_ITEM(args, (Py_ssize_t) i, it);
    }
  }
  if ((size_t) (end - *p) < 5 || *(*p)++ != PYMIZU_TAG_DICT) goto fail;
  {
    uint32_t nk = codec_get32(p);
    if (nk > PYMIZU_CODEC_CAP) goto fail;
    kwargs = PyDict_New();
    if (kwargs == NULL) goto fail;
    for (uint32_t i = 0; i < nk; i++) {
      PyObject *k = codec_read_scalar(p, end);
      if (k != NULL && !PyUnicode_Check(k)) {
        Py_DECREF(k);
        k = NULL;
      }
      PyObject *v = k != NULL ? frame_read_leaf(p, end, ctx) : NULL;
      if (k == NULL || v == NULL) {
        Py_XDECREF(k);
        Py_XDECREF(v);
        goto fail;
      }
      int rc = PyDict_SetItem(kwargs, k, v);
      Py_DECREF(k);
      Py_DECREF(v);
      if (rc < 0) goto fail;
    }
  }
  PyObject *out = PyTuple_New(3);
  if (out == NULL) {
    Py_DECREF(fn);
    Py_DECREF(args);
    Py_DECREF(kwargs);
    return NULL;
  }
  PyTuple_SET_ITEM(out, 0, fn);
  PyTuple_SET_ITEM(out, 1, args);
  PyTuple_SET_ITEM(out, 2, kwargs);
  return out;
fail:
  Py_DECREF(fn);
  Py_XDECREF(args);
  Py_XDECREF(kwargs);
  return NULL;
}

static PyObject *codec_read(const uint8_t *src, size_t n, mizu_read_ctx *ctx) {
  const uint8_t *p = src + 1, *end = src + n;
  if (p >= end) goto corrupt;
  int tag = *p++;
  PyObject *out = NULL;
  switch (tag) {
  case PYMIZU_TAG_TASK:
    out = frame_read_task(&p, end, ctx);
    if (out == NULL) {
      if (ctx != NULL && ctx->gone && !PyErr_Occurred()) return NULL;
      if (!PyErr_Occurred()) goto corrupt;
      return NULL;
    }
    break;
  case PYMIZU_TAG_LIST:
  case PYMIZU_TAG_TUPLE: {
    if ((size_t) (end - p) < 4) goto corrupt;
    uint32_t count = codec_get32(&p);
    if (count > PYMIZU_CODEC_CAP) goto corrupt;
    out = tag == PYMIZU_TAG_LIST ? PyList_New((Py_ssize_t) count) :
      PyTuple_New((Py_ssize_t) count);
    if (out == NULL) return NULL;
    for (uint32_t i = 0; i < count; i++) {
      PyObject *it = codec_read_scalar(&p, end);
      if (it == NULL) {
        if (!PyErr_Occurred()) goto corrupt_obj;
        Py_DECREF(out);
        return NULL;
      }
      if (tag == PYMIZU_TAG_LIST)
        PyList_SET_ITEM(out, (Py_ssize_t) i, it);
      else
        PyTuple_SET_ITEM(out, (Py_ssize_t) i, it);
    }
    break;
  }
  case PYMIZU_TAG_DICT: {
    if ((size_t) (end - p) < 4) goto corrupt;
    uint32_t count = codec_get32(&p);
    if (count > PYMIZU_CODEC_CAP) goto corrupt;
    out = PyDict_New();
    if (out == NULL) return NULL;
    for (uint32_t i = 0; i < count; i++) {
      PyObject *k = codec_read_scalar(&p, end);
      PyObject *v = k != NULL ? codec_read_scalar(&p, end) : NULL;
      if (k == NULL || v == NULL) {
        Py_XDECREF(k);
        Py_XDECREF(v);
        if (!PyErr_Occurred()) goto corrupt_obj;
        Py_DECREF(out);
        return NULL;
      }
      int rc = PyDict_SetItem(out, k, v);
      Py_DECREF(k);
      Py_DECREF(v);
      if (rc < 0) {
        Py_DECREF(out);
        return NULL;
      }
    }
    break;
  }
  case PYMIZU_TAG_BOOL:
  case PYMIZU_TAG_INT:
  case PYMIZU_TAG_FLOAT:
  case PYMIZU_TAG_STR:
  case PYMIZU_TAG_BYTES:
    p--;   /* the scalar reader consumes its own tag */
    out = codec_read_scalar(&p, end);
    if (out == NULL) {
      if (!PyErr_Occurred()) goto corrupt;
      return NULL;
    }
    break;
  default:
    goto corrupt;
  }
  if (p != end) goto corrupt_obj;
  return out;
corrupt_obj:
  Py_XDECREF(out);
corrupt:
  PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
  return NULL;
}

/* A serialized-stream frame (INLINE / ARENA / SHM_RAW bytes). Our streams
   are pickle protocol 4 (first byte 0x80) or the compact codec
   (MIZU_PYMIZU_CODEC_MAGIC); the interchange stream (MIZU_INTEROP_MAGIC,
   'I') is every binding's cross-language form. MIZU_CODEC_MAGIC ('R') is
   mizu's compact codec, 'B' / 'X' / 'A' the R serialize formats —
   language-private streams a foreign peer should never have sent (the
   identity exchange tells it so; this is a defense, not a user path). */
static PyObject *read_stream(const uint8_t *src, size_t n,
                             mizu_read_ctx *ctx) {
  if (n == 0) {
    PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
    return NULL;
  }
  switch (src[0]) {
  case 0x80:
    return PyObject_CallFunction(mizu_loads, "y#", (const char *) src,
                                 (Py_ssize_t) n);
  case MIZU_PYMIZU_CODEC_MAGIC:
    return codec_read(src, n, ctx);
  case MIZU_INTEROP_MAGIC:
    return pymizu_ix_read(src, n);
  case MIZU_CODEC_MAGIC: case 'B': case 'X': case 'A':
    /* foreign stream: consume the slot (a plain failure would wedge the
       ring behind it); the verb surfaces MIZU_ERR with this message */
    ctx->flags |= MIZU_READ_CONSUME;
    PyErr_SetString(MizuError, "pymizu: R payload (a language-private "
                    "stream) - send values from the portable interchange "
                    "subset");
    return NULL;
  default:
    ctx->flags |= MIZU_READ_CONSUME;
    PyErr_SetString(MizuError, "pymizu: unrecognized payload");
    return NULL;
  }
}

/* The copied-read scan gate (3.2): is the frame's writer a foreign
   language? A channel reads its peer's identity word off the handshake; a
   pool result's writer is the pool's worker word (re-read through the
   mapping — 0 until the first join reads as same-language, matching the
   ctx's peer_lang 0 convention). */
static int read_foreign(const mizu_read_ctx *ctx) {
  mizu_handle *h = ctx->handle;
  if (h != NULL && mizu_handle_kind(h) == MIZU_HTYPE_POOL) {
    uint32_t lang =
      (uint32_t) (mizu_pool_worker_ident((mizu_pool *) h) & 0xff);
    return lang != 0 && lang != MIZU_LANG_PYTHON;
  }
  MizuHandleCtx *hctx = (MizuHandleCtx *) ctx->binding_ctx;
  return hctx != NULL && hctx->peer_lang != 0 &&
         hctx->peer_lang != MIZU_LANG_PYTHON;
}

static PyObject *read_frame(const mizu_slot_hdr *hdr, const uint8_t *payload,
                            size_t limit, mizu_read_ctx *ctx) {
  switch (hdr->kind) {
  case MIZU_KIND_NIL:
    Py_RETURN_NONE;
  case MIZU_KIND_RAWVEC:
    if (hdr->len > limit) break;
    return read_raw(payload, hdr->len, (int) hdr->aux, read_foreign(ctx));
  case MIZU_KIND_RAWSPILL:
    if (mizu_aux_hi(hdr->aux)) {
      /* pool framing: the region name in the payload, its length and the
         wire type packed in aux (the channel's arena framing of this kind
         is resolved by the transport, never reaching here) */
      uint32_t name_len = (uint32_t) mizu_aux_hi(hdr->aux);
      int type = mizu_aux_type(hdr->aux);
      if (name_len == 0 || name_len >= MIZU_NAME_MAX) break;
      mizu_shm *shm = mizu_read_region(ctx, payload, name_len);
      if (shm == NULL) return NULL;          /* ctx->gone set */
      if ((uint64_t) hdr->len > (uint64_t) shm->size) break;
      return read_raw((const uint8_t *) shm->addr, hdr->len, type,
                      read_foreign(ctx));
    }
    /* the channel's arena framing is resolved to its byte range by the
       transport before the call */
    if (hdr->len > limit) break;
    return read_raw(payload, hdr->len, (int) hdr->aux, read_foreign(ctx));
  case MIZU_KIND_STR1: {
    if (hdr->aux == MIZU_STR1_NA) {
      if (hdr->len != 0) break;
      Py_RETURN_NONE;   /* R's missing string reads as None */
    }
    if (hdr->len > limit || hdr->aux > MIZU_CE_BYTES) break;
    switch ((int) hdr->aux) {
    case MIZU_CE_NATIVE:
    case MIZU_CE_UTF8: {
      PyObject *s = PyUnicode_DecodeUTF8((const char *) payload,
                                         (Py_ssize_t) hdr->len, NULL);
      if (s == NULL) {
        PyErr_Clear();
        PyErr_SetString(MizuError, "pymizu: R string payload is not valid "
                        "UTF-8 (native encoding does not cross)");
      }
      return s;
    }
    case MIZU_CE_LATIN1:
      return PyUnicode_DecodeLatin1((const char *) payload,
                                    (Py_ssize_t) hdr->len, NULL);
    default:   /* MIZU_CE_BYTES */
      return PyBytes_FromStringAndSize((const char *) payload,
                                       (Py_ssize_t) hdr->len);
    }
  }
  case MIZU_KIND_INLINE:
    if (hdr->len > limit) break;
    return read_stream(payload, hdr->len, ctx);
  case MIZU_KIND_ARENA:
    /* resolved stream bytes; limit is the arena-validated length */
    return read_stream(payload, limit, ctx);
  case MIZU_KIND_SHM_RAW: {
    if (hdr->len == 0 || hdr->len >= MIZU_NAME_MAX) break;
    mizu_shm *shm = mizu_read_region(ctx, payload, hdr->len);
    if (shm == NULL) return NULL;          /* ctx->gone set */
    /* aux is the exact stream length: a recycled region is larger than the
       stream it carries, and the slack bytes are a previous payload's */
    size_t n = hdr->aux != 0 && hdr->aux <= (uint64_t) shm->size ?
      (size_t) hdr->aux : shm->size;
    return read_stream((const uint8_t *) shm->addr, n, ctx);
  }
  case MIZU_KIND_SHM_VEC:
    if (hdr->len == 0 || hdr->len >= MIZU_NAME_MAX) break;
    return read_shm_vec(payload, hdr->len, hdr->aux, ctx);
  case MIZU_KIND_REF:
    if (hdr->len == 0 || hdr->len > 1024) break;   /* the identifier cap */
    return read_ref(hdr, payload, ctx);
  }
  PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
  return NULL;
}

/* Build a TaskError from the (type name, message, traceback) fields and
   the optional element index: the exception's text is "type: message";
   the fields ride as remote_type / remote_traceback / index. Borrowed
   references throughout. Serves the ERR envelope (below), the err tag's
   Python home (interop.c's reader, through mizu_py_task_error_build),
   and Phase 4's dual-format publish. */
static PyObject *task_error_build(PyObject *tn, PyObject *ms, PyObject *tbs,
                                  PyObject *eidx) {
  PyObject *exc = NULL;
  PyObject *text = PyUnicode_FromFormat("%U: %U", tn, ms);
  if (text != NULL)
    exc = PyObject_CallFunction(MizuTaskError, "N", text);
  if (exc != NULL &&
      (PyObject_SetAttrString(exc, "remote_type", tn) < 0 ||
       PyObject_SetAttrString(exc, "remote_traceback", tbs) < 0 ||
       (eidx != NULL && PyObject_SetAttrString(exc, "index", eidx) < 0)))
    Py_CLEAR(exc);
  return exc;
}

/* interop.c's err-tag reader builds its home through the one builder. */
PyObject *mizu_py_task_error_build(PyObject *tn, PyObject *ms,
                                   PyObject *tbs, PyObject *eidx) {
  return task_error_build(tn, ms, tbs, eidx);
}

/* The ERR outcome's payload is the worker's constructed envelope: a pickled
   (type name, message, traceback) tuple — never a pickled exception
   instance — plus, for a map runner's annotated error, a fourth element
   carrying the in-flight element index. Rebuild it as a TaskError carrying
   the remote type name and traceback text (and `index` when present). An
   'I' err stream has already been read to its TaskError home (the channel
   value discipline), so it passes through. */
static PyObject *task_error_of(PyObject *env) {
  if (PyObject_TypeCheck(env, (PyTypeObject *) MizuError)) {
    Py_INCREF(env);
    return env;
  }
  PyObject *tn = NULL, *ms = NULL, *tbs = NULL, *eidx = NULL;
  Py_ssize_t arity = PyTuple_Check(env) ? PyTuple_GET_SIZE(env) : 0;
  if ((arity == 3 || arity == 4) &&
      PyUnicode_Check(PyTuple_GET_ITEM(env, 0)) &&
      PyUnicode_Check(PyTuple_GET_ITEM(env, 1)) &&
      PyUnicode_Check(PyTuple_GET_ITEM(env, 2))) {
    tn = PyTuple_GET_ITEM(env, 0);
    ms = PyTuple_GET_ITEM(env, 1);
    tbs = PyTuple_GET_ITEM(env, 2);
    if (arity == 4) {
      PyObject *i = PyTuple_GET_ITEM(env, 3);
      if (PyLong_Check(i)) eidx = i;
    }
  } else {
    tn = PyUnicode_FromString("Exception");
    ms = PyUnicode_FromString("pymizu: task failed (unreadable error envelope)");
    tbs = PyUnicode_FromString("");
    if (tn == NULL || ms == NULL || tbs == NULL) {
      Py_XDECREF(tn);
      Py_XDECREF(ms);
      Py_XDECREF(tbs);
      return NULL;
    }
    PyObject *exc = task_error_build(tn, ms, tbs, NULL);
    Py_DECREF(tn);
    Py_DECREF(ms);
    Py_DECREF(tbs);
    return exc;
  }
  return task_error_build(tn, ms, tbs, eidx);
}

/* The DIED outcome carries no payload (a reap cannot write payload bytes
   without racing a live worker's publish): build the error off the claimant
   record. */
static PyObject *worker_died_of(const mizu_read_ctx *ctx) {
  PyObject *exc = PyObject_CallFunction(
    MizuWorkerDiedError, "s", "pymizu: worker died while executing this task");
  if (exc == NULL) return NULL;
  PyObject *slot = ctx->died_slot >= 0 ?
    PyLong_FromLong((long) ctx->died_slot) : (Py_INCREF(Py_None), Py_None);
  PyObject *pid = ctx->died_pid > 0 ?
    PyLong_FromLongLong((long long) ctx->died_pid) :
    (Py_INCREF(Py_None), Py_None);
  int ok = slot != NULL && pid != NULL &&
           PyObject_SetAttrString(exc, "slot", slot) == 0 &&
           PyObject_SetAttrString(exc, "pid", pid) == 0;
  Py_XDECREF(slot);
  Py_XDECREF(pid);
  if (!ok) {
    Py_DECREF(exc);
    return NULL;
  }
  return exc;
}

/* The read dispatch: a pool collect's non-OK outcomes build the binding's
   error object (boxed for the veneer to raise); everything else is a frame
   read. */
static PyObject *read_impl(const mizu_slot_hdr *hdr, const uint8_t *payload,
                           size_t limit, mizu_read_ctx *ctx) {
  switch (ctx->outcome) {
  case MIZU_RS_OK:
    return read_frame(hdr, payload, limit, ctx);
  case MIZU_RS_ERR: {
    PyObject *env = read_frame(hdr, payload, limit, ctx);
    if (env == NULL) return NULL;      /* ctx->gone, or a read failure */
    PyObject *exc = task_error_of(env);
    Py_DECREF(env);
    return caught_new(exc);
  }
  case MIZU_RS_CANCEL:
    return caught_new(PyObject_CallFunction(
      MizuCancelledError, "s", "pymizu: task cancelled or pool stopped"));
  case MIZU_RS_DIED:
    return caught_new(worker_died_of(ctx));
  }
  PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
  return NULL;
}

/* The binding's read_fn. NULL with a Python error set fails the verb as
   MIZU_ERR ("payload read failed"); NULL via ctx->gone propagates the
   vanished-region verdict. */
static void *py_read(const mizu_slot_hdr *hdr, const uint8_t *payload,
                     size_t limit, mizu_read_ctx *ctx) {
  PyGILState_STATE gil = PyGILState_Ensure();
  PyObject *obj = read_impl(hdr, payload, limit, ctx);
  PyGILState_Release(gil);
  return (void *) obj;
}

/* The channel's read_fn: py_read plus the consume rule, one site for every
   current and future decline — an unreadable slot must not wedge the ring
   behind it. Consume exactly the content failures: any Exception except
   MemoryError (environmental, a retry can succeed, so no message is lost
   to a transient). Matching on Exception excludes the whole BaseException
   control-flow tier — KeyboardInterrupt, SystemExit, GeneratorExit — which
   describes the process, never the message bytes, and can be caught
   up-stack, after which the channel must not have lost the message. The
   ctx->gone guard keeps a vanished region on the core's verdict path; the
   PyErr_Occurred guard never exempts a real decline (every read_impl
   failure sets an exception) — it only keeps a NULL without one (an
   internal bug) retryable rather than silently consumed. Pools keep
   py_read: a freed result slot is a separate decision. */
static void *py_chan_read(const mizu_slot_hdr *hdr, const uint8_t *payload,
                          size_t limit, mizu_read_ctx *ctx) {
  PyGILState_STATE gil = PyGILState_Ensure();
  PyObject *obj = read_impl(hdr, payload, limit, ctx);
  if (obj == NULL && !ctx->gone && PyErr_Occurred() &&
      PyErr_ExceptionMatches(PyExc_Exception) &&
      !PyErr_ExceptionMatches(PyExc_MemoryError))
    ctx->flags |= MIZU_READ_CONSUME;
  PyGILState_Release(gil);
  return (void *) obj;
}

/* The batch-verb sink (recv_batch_fn / collect_all_fn): each read product
   lands in the pre-built list as it is produced, so the partial prefix is
   owned by the list on every exit path. The verb runs with the GIL
   released — reacquire per item, as py_read does. */
static void list_sink(void *ctx, size_t i, void *obj) {
  PyGILState_STATE gil = PyGILState_Ensure();
  PyList_SET_ITEM((PyObject *) ctx, (Py_ssize_t) i, (PyObject *) obj);
  PyGILState_Release(gil);
}

/* The check hook: Ctrl-C becomes KeyboardInterrupt. Runs on the verb-calling
   thread only (the core's contract), which released the GIL around the verb —
   reacquire for PyErr_CheckSignals. A pending signal sets the exception on
   this thread state; the veneer propagates it after the verb unwinds. */
static int py_check(void *Py_UNUSED(ctx)) {
  PyGILState_STATE gil = PyGILState_Ensure();
  int rc = PyErr_CheckSignals();
  PyGILState_Release(gil);
  return rc != 0;
}

/* The worker handle's around-park hook: the worker thread holds the GIL
   through run and collect, so each bounded sleep drops it for the worker
   process's other threads. The hook brackets one sleep at a time on the
   verb-calling thread, so a thread-local carries the state. */
static _Thread_local PyThreadState *mizu_park_tstate;

static void py_park(void *Py_UNUSED(ctx), int entering) {
  if (entering) {
    mizu_park_tstate = PyEval_SaveThread();
  } else {
    PyThreadState *tstate = mizu_park_tstate;
    mizu_park_tstate = NULL;
    PyEval_RestoreThread(tstate);
  }
}

// Task execution (the worker's exec_fn) ----------------------------------------

/* UTF-8-boundary truncation: decoding with "ignore" drops the partial
   sequence at the cut. */
static PyObject *utf8_truncate(PyObject *s, size_t n) {
  PyObject *b = PyUnicode_AsUTF8String(s);
  if (b == NULL) {
    PyErr_Clear();
    return PyUnicode_FromString("");
  }
  Py_ssize_t len = PyBytes_GET_SIZE(b);
  if ((size_t) len <= n) {
    Py_DECREF(b);
    Py_INCREF(s);
    return s;
  }
  PyObject *out = PyUnicode_DecodeUTF8(PyBytes_AS_STRING(b), (Py_ssize_t) n,
                                       "ignore");
  Py_DECREF(b);
  if (out == NULL) {
    PyErr_Clear();
    out = PyUnicode_FromString("");
  }
  return out;
}

/* The 3-arg form with the fetched tb: the single-argument form only reads
   __traceback__ off the instance on 3.11+; on 3.10 it formats no stack. */
static PyObject *traceback_text(PyObject *type, PyObject *value,
                                PyObject *tb) {
  if (mizu_traceback_fmt == NULL) {
    PyObject *mod = PyImport_ImportModule("traceback");
    if (mod == NULL) {
      PyErr_Clear();
      return PyUnicode_FromString("");
    }
    mizu_traceback_fmt = PyObject_GetAttrString(mod, "format_exception");
    Py_DECREF(mod);
    if (mizu_traceback_fmt == NULL) {
      PyErr_Clear();
      return PyUnicode_FromString("");
    }
  }
  if (tb == NULL) {
    tb = Py_None;
  }
  PyObject *parts =
      PyObject_CallFunction(mizu_traceback_fmt, "OOO", type, value, tb);
  if (parts == NULL) {
    PyErr_Clear();
    return PyUnicode_FromString("");
  }
  PyObject *sep = PyUnicode_FromString("");
  PyObject *text = sep != NULL ? PyUnicode_Join(sep, parts) : NULL;
  Py_XDECREF(sep);
  Py_DECREF(parts);
  if (text == NULL) {
    PyErr_Clear();
    text = PyUnicode_FromString("");
  }
  return text;
}

/* Frame an exception as an 'I' err stream INLINE (the _send_error shim
   path): type name, str(), traceback text — pymizu_ix_write_err truncates
   to the slot by construction, so the publish cannot fail. */
static int frame_err_exc(PyObject *exc, uint8_t *payload,
                         uint32_t inline_max, mizu_slot_hdr *hdr) {
  const char *tn = PyExceptionClass_Name(Py_TYPE(exc));
  PyObject *tname = PyUnicode_FromString(tn != NULL ? tn : "Exception");
  PyObject *msg = PyObject_Str(exc);
  if (msg == NULL) {
    PyErr_Clear();
    msg = PyUnicode_FromString("<unprintable exception>");
  }
  PyObject *tb = PyException_GetTraceback(exc);   /* new ref or NULL */
  PyObject *tbs = traceback_text(Py_TYPE(exc), exc, tb);
  Py_XDECREF(tb);
  if (tname == NULL || msg == NULL || tbs == NULL) {
    Py_XDECREF(tname);
    Py_XDECREF(msg);
    Py_XDECREF(tbs);
    return 1;
  }
  Py_ssize_t tn_n = 0, ms_n = 0, tb_n = 0;
  const char *tn_s = PyUnicode_AsUTF8AndSize(tname, &tn_n);
  const char *ms_s = PyUnicode_AsUTF8AndSize(msg, &ms_n);
  const char *tb_s = PyUnicode_AsUTF8AndSize(tbs, &tb_n);
  if (tn_s == NULL || ms_s == NULL || tb_s == NULL) {
    PyErr_Clear();                 /* a lone surrogate is no detail */
    static const char empty[] = "";
    if (tn_s == NULL) { tn_s = empty; tn_n = 0; }
    if (ms_s == NULL) { ms_s = empty; ms_n = 0; }
    if (tb_s == NULL) { tb_s = empty; tb_n = 0; }
  }
  size_t n = pymizu_ix_write_err(payload, inline_max,
                                 tn_s, (size_t) tn_n, ms_s, (size_t) ms_n,
                                 tb_s, (size_t) tb_n, 0, 0);
  hdr->kind = MIZU_KIND_INLINE;
  hdr->len = (uint32_t) n;
  hdr->aux = MIZU_AUX_F_KEEPERLESS;
  Py_DECREF(tname);
  Py_DECREF(msg);
  Py_DECREF(tbs);
  return 0;
}

/* Publish the currently-held exception as the task's ERR result: the
   constructed, bounded (type name, message, traceback) envelope — never a
   pickled exception instance, whose unpicklable attributes or __traceback__
   would fail the publish (fail the task, never the worker). The traceback,
   then the message, truncate at a UTF-8 boundary to fit the slot's inline
   budget; framed INLINE in the sink's buffer wherever the envelope fits, so
   the publish itself cannot fail. Returns 0 on publish (or a cancel beat),
   nonzero on infrastructure failure. */
static int publish_exc(mizu_result_sink *sink) {
  PyObject *type = NULL, *value = NULL, *tb = NULL;
  PyErr_Fetch(&type, &value, &tb);
  PyErr_NormalizeException(&type, &value, &tb);
  const char *tn = type != NULL ? PyExceptionClass_Name(type) : NULL;
  PyObject *tname = PyUnicode_FromString(tn != NULL ? tn : "Exception");
  PyObject *msg = value != NULL ? PyObject_Str(value) : NULL;
  if (msg == NULL) {
    PyErr_Clear();
    msg = PyUnicode_FromString("<unprintable exception>");
  }
  PyObject *tbs =
      value != NULL ? traceback_text(type, value, tb) : NULL;
  /* a map runner annotates its error with the in-flight element index: it
     travels as the envelope's fourth element (ordinary task errors stay
     3-tuples); the truncation ladder below never touches it */
  PyObject *eidx = NULL;
  if (value != NULL) {
    eidx = PyObject_GetAttrString(value, "_pymizu_map_index");
    if (eidx == NULL) {
      PyErr_Clear();
    } else if (!PyLong_Check(eidx)) {
      Py_DECREF(eidx);
      eidx = NULL;
    }
  }
  Py_XDECREF(type);
  Py_XDECREF(value);
  Py_XDECREF(tb);
  if (tname == NULL || msg == NULL || tbs == NULL) {
    Py_XDECREF(tname);
    Py_XDECREF(msg);
    Py_XDECREF(tbs);
    Py_XDECREF(eidx);
    return 1;                    /* allocation failure: the worker goes down */
  }
  uint32_t budget = sink->inline_max;
  PyObject *env = NULL, *stream = NULL;
  for (int attempt = 0; attempt < 5; attempt++) {
    PyObject *cand = eidx != NULL ? PyTuple_Pack(4, tname, msg, tbs, eidx)
                                  : PyTuple_Pack(3, tname, msg, tbs);
    if (cand == NULL) goto infra;
    PyObject *s = PyObject_CallFunction(mizu_dumps, "Oi", cand, 4);
    if (s == NULL) {
      Py_DECREF(cand);
      goto infra;
    }
    if ((uint64_t) PyBytes_GET_SIZE(s) <= (uint64_t) budget) {
      env = cand;
      stream = s;
      break;
    }
    Py_DECREF(s);
    Py_DECREF(cand);
    PyObject *shrunk;
    switch (attempt) {
    case 0:
      shrunk = utf8_truncate(tbs, budget / 2);
      Py_DECREF(tbs);
      tbs = shrunk;
      break;
    case 1:
      shrunk = PyUnicode_FromString("");
      Py_DECREF(tbs);
      tbs = shrunk;
      break;
    case 2:
      shrunk = utf8_truncate(msg, budget / 2);
      Py_DECREF(msg);
      msg = shrunk;
      break;
    default:
      shrunk = PyUnicode_FromString(
        "pymizu: task error (untransportable condition)");
      Py_DECREF(msg);
      msg = shrunk;
      break;
    }
    if (shrunk == NULL) goto infra;
  }
  int rc;
  if (stream != NULL) {
    memcpy(sink->payload, PyBytes_AS_STRING(stream),
           (size_t) PyBytes_GET_SIZE(stream));
    rc = mizu_result_publish_err(sink, NULL,
                                (uint32_t) PyBytes_GET_SIZE(stream));
  } else {
    /* below the inline guarantee (a tiny slot): the tiered stage carries
       the smallest envelope out of line */
    env = eidx != NULL ? PyTuple_Pack(4, tname, msg, tbs, eidx)
                       : PyTuple_Pack(3, tname, msg, tbs);
    if (env == NULL) goto infra;
    rc = mizu_result_publish_err(sink, (void *) env, 0);
  }
  Py_DECREF(env);
  Py_XDECREF(stream);
  Py_DECREF(tname);
  Py_DECREF(msg);
  Py_DECREF(tbs);
  Py_XDECREF(eidx);
  return rc < 0;
infra:
  Py_XDECREF(tname);
  Py_XDECREF(msg);
  Py_XDECREF(tbs);
  Py_XDECREF(eidx);
  return 1;
}

/* The worker's task: decode the (callable, args, kwargs) frame, call, and
   publish through the sink. Two error disciplines, both honored without a
   longjmp: a task's own error (any Exception) is caught into the
   constructed envelope at every reentry depth — `catching` needs no
   distinction — and KeyboardInterrupt / SystemExit escape as infrastructure
   failure (the hard-crash semantics: the worker goes down and the reaper's
   DIED verdict fails the task). Runs with the GIL held: the worker veneer
   never releases it. */
static int py_exec(const mizu_slot_hdr *hdr, const uint8_t *payload,
                   size_t limit, mizu_result_sink *sink, int catching,
                   mizu_read_ctx *ctx) {
  (void) catching;
  PyObject *task = read_impl(hdr, payload, limit, ctx);
  if (task == NULL) {
    if (ctx->gone) {
      /* the enqueuer died and its region went along: the task can never
         run anywhere — it fails as DIED, and the drain continues */
      mizu_result_publish_died(sink);
      return 0;
    }
    return publish_exc(sink);    /* the decode failure is the task's error */
  }
  PyObject *fn = NULL, *args = NULL, *kwargs = NULL;
  int shape_ok = PyTuple_Check(task) && PyTuple_GET_SIZE(task) == 3;
  if (shape_ok) {
    fn = PyTuple_GET_ITEM(task, 0);
    args = PyTuple_GET_ITEM(task, 1);
    kwargs = PyTuple_GET_ITEM(task, 2);
    shape_ok = PyCallable_Check(fn) && PyTuple_Check(args) &&
               (kwargs == Py_None || PyDict_Check(kwargs));
  }
  if (!shape_ok) {
    Py_DECREF(task);
    PyErr_SetString(MizuError, "pymizu: corrupt task payload");
    return publish_exc(sink);
  }
  PyObject *value =
    PyObject_Call(fn, args, kwargs == Py_None ? NULL : kwargs);
  Py_DECREF(task);
  if (value == NULL) {
    if (!PyErr_ExceptionMatches(PyExc_Exception))
      return 1;      /* BaseException: the worker goes down; the exception
                        stays set for the worker entry to report */
    return publish_exc(sink);
  }
  int rc = mizu_result_publish(sink, (void *) value);
  Py_DECREF(value);
  if (rc < 0 && PyErr_Occurred()) {
    /* staging the result failed (an unpicklable object): recover as the
       task's ERR result. The handle's recorded stage error is stale-only —
       nothing reads it while exec keeps returning 0. */
    rc = publish_exc(sink) != 0 ? -1 : 0;
  }
  return rc < 0;
}

// The channel handle ---------------------------------------------------------------

typedef struct {
  PyObject_HEAD
  mizu_channel *core;
  long self_pid;
  MizuViewCache *vcache;   /* the binding.ctx view cache, for teardown */
} MizuChannel;

static void chan_binding(mizu_binding *b) {
  mizu_binding_init(b);
  b->stage = py_stage;
  b->read = py_chan_read;   /* the consume-on-decline read_fn; pools keep
                               py_read */
  b->check = py_check;
  b->ident = MIZU_IDENT(MIZU_LANG_PYTHON, MIZU_CAP_MIZS);
  /* exec/park/sweep/drop NULL: a channel never evals; submitter handles
     release the GIL around the whole verb, so no park hook; staging pins
     nothing, so no drop hook. */
}

static mizu_channel *chan_get(MizuChannel *self) {
  mizu_channel *c = self->core;
  if (c == NULL) {
    PyErr_SetString(PyExc_ValueError, "pymizu: channel handle is closed");
    return NULL;
  }
  if (self->self_pid != mizu_self_pid()) {
    PyErr_SetString(MizuError, "pymizu: channel handles do not survive fork()");
    return NULL;
  }
  return c;
}

/* Raise a MIZU_ERR from a handle verb. A callback that already set a Python
   error (the check hook's KeyboardInterrupt, a stage/read failure) wins. */
static PyObject *raise_handle(MizuChannel *self) {
  if (PyErr_Occurred()) return NULL;
  mizu_errcat cat = mizu_channel_errcat(self->core);
  const char *msg = mizu_channel_error(self->core);
  switch (cat) {
  case MIZU_ERRCAT_INTERRUPTED:
    PyErr_SetNone(PyExc_KeyboardInterrupt);
    break;
  case MIZU_ERRCAT_NOSPACE:
  case MIZU_ERRCAT_NOMEMORY:
  case MIZU_ERRCAT_EXISTS:
    PyErr_Format(MizuShmError, "pymizu: %s", msg);
    break;
  default:
    PyErr_Format(MizuError, "pymizu: %s", msg);
    break;
  }
  return NULL;
}

static PyObject *status_or_raise(MizuChannel *self, mizu_status st,
                                 PyObject *on_ok) {
  switch (st) {
  case MIZU_OK:        Py_INCREF(on_ok); return on_ok;
  case MIZU_FULL:      Py_INCREF(SentFull); return SentFull;
  case MIZU_TIMEOUT:   Py_INCREF(SentTimeout); return SentTimeout;
  case MIZU_CLOSED:    Py_INCREF(SentClosed); return SentClosed;
  case MIZU_PEER_GONE: Py_INCREF(SentGone); return SentGone;
  default:            return raise_handle(self);
  }
}

/* seconds (None waits indefinitely) to the core's timeout_ms; the
   double conversion itself is the core's mizu_timeout_ms. */
static int timeout_ms_of(PyObject *arg, double *out) {
  if (arg == Py_None) {
    *out = -1;
    return 0;
  }
  double t = PyFloat_AsDouble(arg);
  if (t == -1 && PyErr_Occurred()) return -1;
  *out = mizu_timeout_ms(t);
  return 0;
}

static MizuChannel *chan_wrap(mizu_channel *c, MizuViewCache *vc) {
  MizuChannel *self = (MizuChannel *) MizuChannelType.tp_alloc(&MizuChannelType, 0);
  if (self == NULL) return NULL;
  self->core = c;
  self->self_pid = mizu_self_pid();
  self->vcache = vc;
  return self;
}

// Verbs ----------------------------------------------------------------------------

PyDoc_STRVAR(send_doc,
"send(x) -> True | sentinel\n\n\
Publish a message to the peer. Never blocks for ring space: returns\n\
pymizu.FULL when the ring is full, pymizu.CLOSED / pymizu.PEER_GONE on a\n\
closed or dead peer.");

static PyObject *Channel_send(MizuChannel *self, PyObject *arg) {
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_send(c, (void *) arg);
  Py_END_ALLOW_THREADS
  return status_or_raise(self, st, Py_True);
}

PyDoc_STRVAR(send_error_doc,
"_send_error(exc) -> bool\n\n\
The peer shim's uncaught-error send: point the handle's err field at the\n\
exception and send it — the stage hook pointer-matches and frames the 'I'\n\
err stream INLINE in place of a value, whatever the peer's language.\n\
Bounded: the ordinary send never blocks for ring space, so a full ring\n\
drops the stream (the stderr traceback stands either way). True on\n\
publish, False on a full ring or an already-closed channel.");

static PyObject *Channel_send_error(MizuChannel *self, PyObject *arg) {
  if (!PyExceptionInstance_Check(arg)) {
    PyErr_SetString(PyExc_TypeError,
                    "pymizu: expected an exception instance");
    return NULL;
  }
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  MizuHandleCtx *hctx = (MizuHandleCtx *) self->vcache;
  hctx->err_exc = arg;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_send(c, (void *) arg);
  Py_END_ALLOW_THREADS
  hctx->err_exc = NULL;
  if (st == MIZU_ERR) return raise_handle(self);
  return PyBool_FromLong(st == MIZU_OK);
}

PyDoc_STRVAR(send_batch_doc,
"send_batch(xs) -> int\n\n\
Publish a sequence of messages in one batched tail store. Returns the\n\
count accepted — short of len(xs) when the ring filled or the channel\n\
closed midway; send the next element singly to learn which.");

static PyObject *Channel_send_batch(MizuChannel *self, PyObject *arg) {
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  PyObject *seq = PySequence_Fast(arg, "pymizu: expected a sequence of payloads");
  if (seq == NULL) return NULL;
  Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
  void **objs = PyMem_Malloc((size_t) (n != 0 ? n : 1) * sizeof(void *));
  if (objs == NULL) {
    Py_DECREF(seq);
    return PyErr_NoMemory();
  }
  PyObject **items = PySequence_Fast_ITEMS(seq);
  for (Py_ssize_t i = 0; i < n; i++)
    objs[i] = (void *) items[i];
  size_t accepted = 0;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_send_batch(c, objs, (size_t) n, &accepted);
  Py_END_ALLOW_THREADS
  PyMem_Free(objs);
  Py_DECREF(seq);
  if (st == MIZU_ERR) return raise_handle(self);
  return PyLong_FromSize_t(accepted);
}

PyDoc_STRVAR(recv_doc,
"recv(timeout=None) -> object | sentinel\n\n\
Return the next message, waiting up to `timeout` seconds (None waits\n\
indefinitely, 0 polls). A terminal state is reported only once the ring\n\
is drained. Ctrl-C stays responsive during the wait.");

static PyObject *Channel_recv(MizuChannel *self, PyObject *args,
                              PyObject *kw) {
  static char *kwlist[] = {"timeout", NULL};
  PyObject *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:recv", kwlist, &tmo))
    return NULL;
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  void *obj = NULL;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_recv(c, &obj, ms);
  Py_END_ALLOW_THREADS
  if (st == MIZU_OK) return (PyObject *) obj;
  return status_or_raise(self, st, Py_None);
}

PyDoc_STRVAR(recv_batch_doc,
"recv_batch(n=256, timeout=None) -> list | sentinel\n\n\
Wait for the first message exactly like recv(), then drain up to `n`\n\
already-published messages without waiting further. A batch that reaches\n\
a message it cannot read returns what it has; the failure surfaces on\n\
the next receive.");

static PyObject *Channel_recv_batch(MizuChannel *self, PyObject *args,
                                    PyObject *kw) {
  static char *kwlist[] = {"n", "timeout", NULL};
  int n = 256;
  PyObject *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|iO:recv_batch", kwlist,
                                   &n, &tmo))
    return NULL;
  if (n < 1) {
    PyErr_SetString(PyExc_ValueError, "pymizu: n must be at least 1");
    return NULL;
  }
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  PyObject *out = PyList_New((Py_ssize_t) n);
  if (out == NULL) return NULL;
  size_t count = 0;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_recv_batch_fn(c, (size_t) n, &count, list_sink, out, ms);
  Py_END_ALLOW_THREADS
  if (st == MIZU_OK) {
    /* the batch ended early at a declining read: the prefix is real, and
       the slot stays for the next receive to reproduce the failure — so
       clear its pending exception (MemoryError included: the retry
       reproduces it, and clearing loses nothing). A BaseException-only
       one — an interrupt out of the unpickle — propagates instead, the
       prefix lost; swallowing a Ctrl-C would be a regression on today.
       This runs before SetSlice, so a genuine SetSlice failure is never
       masked, and no return carries a stale exception. */
    if (PyErr_Occurred()) {
      if (PyErr_ExceptionMatches(PyExc_Exception)) {
        PyErr_Clear();
      } else {
        Py_DECREF(out);
        return NULL;
      }
    }
    if (count < (size_t) n &&
        PyList_SetSlice(out, (Py_ssize_t) count, (Py_ssize_t) n, NULL) < 0) {
      Py_DECREF(out);
      return NULL;
    }
    return out;
  }
  Py_DECREF(out);
  return status_or_raise(self, st, Py_None);
}

PyDoc_STRVAR(close_doc,
"close(timeout=5.0) -> bool\n\n\
Orderly shutdown: signal close, then wait up to `timeout` seconds for the\n\
peer's close or its death. On rendezvous all resources are released and\n\
the handle is dead (closing again is a no-op). On timeout the handle\n\
stays usable and the rendezvous is retried at destroy.");

static PyObject *Channel_close(MizuChannel *self, PyObject *args,
                               PyObject *kw) {
  static char *kwlist[] = {"timeout", NULL};
  PyObject *tmo = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:close", kwlist, &tmo))
    return NULL;
  if (self->core == NULL || self->self_pid != mizu_self_pid()) {
    /* idempotent; a forked child's copy never touches the shared region */
    self->core = NULL;
    Py_RETURN_TRUE;
  }
  double ms;
  if (tmo == NULL) {
    ms = 5000.0;
  } else if (timeout_ms_of(tmo, &ms) < 0) {
    return NULL;
  }
  mizu_channel *c = self->core;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_close(c, ms);
  Py_END_ALLOW_THREADS
  if (st == MIZU_ERR) return raise_handle(self);
  if (st == MIZU_OK) {
    mizu_channel_destroy(c);
    self->core = NULL;
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;   /* MIZU_TIMEOUT: destroy retries the rendezvous */
}

PyDoc_STRVAR(close_signal_doc,
"close_signal() -> None\n\n\
The peer half of the close protocol: set this side's close bit and wake\n\
the host, with no rendezvous. No-op on a released handle.");

static PyObject *Channel_close_signal(MizuChannel *self,
                                      PyObject *Py_UNUSED(args)) {
  if (self->core != NULL && self->self_pid == mizu_self_pid())
    mizu_channel_close_signal(self->core);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(destroy_doc,
"destroy() -> None\n\n\
Idempotent, never blocks: signals close and releases what the\n\
non-blocking rendezvous check completes. The finalizer target.");

static PyObject *Channel_destroy(MizuChannel *self,
                                 PyObject *Py_UNUSED(args)) {
  if (self->core != NULL && self->self_pid == mizu_self_pid())
    mizu_channel_destroy(self->core);
  self->core = NULL;
  /* destroy first: the core's teardown read paths may consult the binding */
  view_cache_free(self->vcache);
  self->vcache = NULL;
  Py_RETURN_NONE;
}

PyDoc_STRVAR(alive_doc,
"alive() -> bool\n\n\
Peer-process liveness (the lock probe, not a listener flag): a peer that\n\
closed but still runs reads as alive.");

static PyObject *Channel_alive(MizuChannel *self, PyObject *Py_UNUSED(args)) {
  if (self->core == NULL || self->self_pid != mizu_self_pid())
    Py_RETURN_FALSE;
  return PyBool_FromLong(mizu_channel_alive(self->core));
}

PyDoc_STRVAR(ready_set_doc,
"ready_set() -> None\n\n\
The peer's startup signal, sent after it has consumed the drop.");

static PyObject *Channel_ready_set(MizuChannel *self,
                                   PyObject *Py_UNUSED(args)) {
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  if (mizu_channel_ready_set(c) != MIZU_OK) return raise_handle(self);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(ready_wait_doc,
"ready_wait(timeout) -> bool\n\n\
The host's startup rendezvous: wait up to `timeout` seconds for the\n\
peer's ready signal. False on expiry.");

static PyObject *Channel_ready_wait(MizuChannel *self, PyObject *tmo) {
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_ready_wait(c, ms);
  Py_END_ALLOW_THREADS
  if (st == MIZU_ERR) return raise_handle(self);
  if (st == MIZU_OK) {
    /* the peer's identity word is on the region once it attached: the
       reader-language policy's input, cached on the handle */
    uint64_t ident = mizu_channel_peer_ident(c);
    MizuHandleCtx *hc = (MizuHandleCtx *) self->vcache;
    hc->peer_lang = (uint32_t) (ident & 0xff);
    hc->peer_caps = (uint32_t) (ident >> 32);
  }
  return PyBool_FromLong(st == MIZU_OK);
}

PyDoc_STRVAR(peer_ident_doc,
"_peer_ident() -> (int, int)\n\n\
The peer's identity word as a (language, capabilities) pair (0, 0 until\n\
the word is read: the host fills it at ready_wait, the peer at attach).");

static PyObject *Channel_peer_ident(MizuChannel *self,
                                    PyObject *Py_UNUSED(args)) {
  const MizuHandleCtx *hc = (const MizuHandleCtx *) self->vcache;
  return Py_BuildValue("(II)", (unsigned int) hc->peer_lang,
                       (unsigned int) hc->peer_caps);
}

PyDoc_STRVAR(info_doc,
"info() -> dict\n\n\
Handle-local and wire-state snapshot (counters are process-local where\n\
noted).");

static int dict_set(PyObject *d, const char *key, unsigned long long v) {
  PyObject *o = PyLong_FromUnsignedLongLong(v);
  if (o == NULL) return -1;
  int rc = PyDict_SetItemString(d, key, o);
  Py_DECREF(o);
  return rc;
}

static PyObject *Channel_info(MizuChannel *self, PyObject *Py_UNUSED(args)) {
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  mizu_channel_info info;
  if (mizu_channel_info_get(c, &info) != MIZU_OK) return raise_handle(self);
  PyObject *name = PyUnicode_FromString(info.name);
  PyObject *side = PyUnicode_FromString(info.side == MIZU_ENTITY_HOST ?
                                        "host" : "peer");
  PyObject *d = PyDict_New();
  if (name == NULL || side == NULL || d == NULL)
    goto fail;
  if (PyDict_SetItemString(d, "name", name) < 0 ||
      PyDict_SetItemString(d, "side", side) < 0 ||
      dict_set(d, "capacity", info.capacity) < 0 ||
      dict_set(d, "slot_size", info.slot_size) < 0 ||
      dict_set(d, "arena_size", info.arena_size) < 0 ||
      dict_set(d, "inline_max", info.inline_max) < 0 ||
      dict_set(d, "spin", (unsigned long long) info.spin) < 0 ||
      dict_set(d, "ready", (unsigned long long) info.ready) < 0 ||
      dict_set(d, "closed", (unsigned long long) info.closed) < 0 ||
      dict_set(d, "peer_pid", (unsigned long long) info.peer_pid) < 0 ||
      dict_set(d, "tx_sent", info.tx_sent) < 0 ||
      dict_set(d, "tx_published", info.tx_published) < 0 ||
      dict_set(d, "tx_consumed", info.tx_consumed) < 0 ||
      dict_set(d, "rx_consumed", info.rx_consumed) < 0 ||
      dict_set(d, "rx_published", info.rx_published) < 0 ||
      dict_set(d, "fl_entries", info.fl_entries) < 0 ||
      dict_set(d, "fl_hits", info.fl_hits) < 0 ||
      dict_set(d, "open_hits", info.open_hits) < 0 ||
      dict_set(d, "open_misses", info.open_misses) < 0 ||
      dict_set(d, "ledger_entries", info.ledger_entries) < 0)
    goto fail;
  Py_DECREF(name);
  Py_DECREF(side);
  return d;
fail:
  Py_XDECREF(name);
  Py_XDECREF(side);
  Py_XDECREF(d);
  return NULL;
}

static PyObject *Channel_token_get(MizuChannel *self,
                                   void *Py_UNUSED(closure)) {
  mizu_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  char buf[64];
  if (mizu_channel_token(c, buf, sizeof(buf)) != MIZU_OK)
    return raise_handle(self);
  return PyUnicode_FromString(buf);
}

static PyObject *Channel_repr(MizuChannel *self) {
  if (self->core == NULL)
    return PyUnicode_FromString("<pymizu.Channel (closed)>");
  char buf[64];
  if (mizu_channel_token(self->core, buf, sizeof(buf)) != MIZU_OK)
    buf[0] = '\0';
  return PyUnicode_FromFormat("<pymizu.Channel %s>", buf);
}

static void Channel_dealloc(MizuChannel *self) {
  /* destroy signals close and runs the non-blocking rendezvous check; a
     forked child's copy never touches the shared region */
  if (self->core != NULL && self->self_pid == mizu_self_pid())
    mizu_channel_destroy(self->core);
  view_cache_free(self->vcache);
  MizuChannelType.tp_free((PyObject *) self);
}

static PyMethodDef Channel_methods[] = {
  {"send", (PyCFunction) Channel_send, METH_O, send_doc},
  {"_send_error", (PyCFunction) Channel_send_error, METH_O, send_error_doc},
  {"send_batch", (PyCFunction) Channel_send_batch, METH_O, send_batch_doc},
  {"recv", (PyCFunction)(void (*)(void)) Channel_recv,
   METH_VARARGS | METH_KEYWORDS, recv_doc},
  {"recv_batch", (PyCFunction)(void (*)(void)) Channel_recv_batch,
   METH_VARARGS | METH_KEYWORDS, recv_batch_doc},
  {"close", (PyCFunction)(void (*)(void)) Channel_close,
   METH_VARARGS | METH_KEYWORDS, close_doc},
  {"close_signal", (PyCFunction) Channel_close_signal, METH_NOARGS,
   close_signal_doc},
  {"destroy", (PyCFunction) Channel_destroy, METH_NOARGS, destroy_doc},
  {"alive", (PyCFunction) Channel_alive, METH_NOARGS, alive_doc},
  {"ready_set", (PyCFunction) Channel_ready_set, METH_NOARGS, ready_set_doc},
  {"ready_wait", (PyCFunction) Channel_ready_wait, METH_O, ready_wait_doc},
  {"_peer_ident", (PyCFunction) Channel_peer_ident, METH_NOARGS,
   peer_ident_doc},
  {"info", (PyCFunction) Channel_info, METH_NOARGS, info_doc},
  {NULL, NULL, 0, NULL}
};

static PyGetSetDef Channel_getset[] = {
  {"token", (getter) Channel_token_get, NULL,
   "The join token (\"<pid hex>_<counter hex>\") for the peer's attach.",
   NULL},
  {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject MizuChannelType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._Channel",
  .tp_basicsize = sizeof(MizuChannel),
  .tp_repr = (reprfunc) Channel_repr,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A channel handle (process-private; does not survive fork()). "
            "Construct through pymizu.Channel.create() / Channel.attach().",
  .tp_methods = Channel_methods,
  .tp_getset = Channel_getset,
  .tp_dealloc = (destructor) Channel_dealloc,
};


// The pool handle ----------------------------------------------------------------

typedef struct {
  PyObject_HEAD
  mizu_pool *core;
  long self_pid;
  int role;            /* MIZU_ROLE_* */
  MizuViewCache *vcache;
  PyObject *trace_fn;  /* the registered trace callable, or NULL */
} MizuPool;

typedef struct {
  PyObject_HEAD
  uint64_t word;       /* the core's 8-byte mizu_task, by value */
  PyObject *pool;      /* keeps the pool handle alive */
} MizuTask;

/* Submitter/controller handles release the GIL around every verb; worker
   handles hold it (exec_fn reentry) and drop it only around bounded sleeps
   via the park hook. */
#define POOL_ALLOW_THREADS(self) \
  PyThreadState *_save = \
    (self)->role == MIZU_ROLE_WORKER ? NULL : PyEval_SaveThread()
#define POOL_RESUME() \
  if (_save != NULL) PyEval_RestoreThread(_save)

static void pool_binding(mizu_binding *b, int worker) {
  mizu_binding_init(b);
  b->stage = py_stage;
  b->read = py_read;
  b->check = py_check;
  b->exec = worker ? py_exec : NULL;
  b->park = worker ? py_park : NULL;
  b->ident = MIZU_IDENT(MIZU_LANG_PYTHON, MIZU_CAP_MIZS);
  /* sweep/drop NULL: no per-handle caches, and staging pins nothing */
}

static mizu_pool *pool_peek(MizuPool *self) {
  mizu_pool *p = self->core;
  if (p == NULL) return NULL;
  if (self->self_pid != mizu_self_pid()) {
    PyErr_SetString(MizuError, "pymizu: pool handles do not survive fork()");
    return NULL;
  }
  return p;
}

static mizu_pool *pool_get(MizuPool *self) {
  mizu_pool *p = pool_peek(self);
  if (p == NULL && !PyErr_Occurred())
    PyErr_SetString(PyExc_ValueError, "pymizu: pool handle is closed");
  return p;
}

/* Raise a pool verb's MIZU_ERR. A callback that already set a Python error
   (the check hook's KeyboardInterrupt, a stage/read failure) wins. */
static PyObject *pool_raise(MizuPool *self) {
  if (PyErr_Occurred()) return NULL;
  mizu_errcat cat = mizu_pool_errcat(self->core);
  const char *msg = mizu_pool_error(self->core);
  switch (cat) {
  case MIZU_ERRCAT_INTERRUPTED:
    PyErr_SetNone(PyExc_KeyboardInterrupt);
    break;
  case MIZU_ERRCAT_STOPPED:
    PyErr_Format(MizuStoppedError, "pymizu: %s", msg);
    break;
  case MIZU_ERRCAT_EXHAUSTED:
    PyErr_Format(MizuSlotsExhaustedError, "pymizu: %s", msg);
    break;
  case MIZU_ERRCAT_NOSPACE:
  case MIZU_ERRCAT_NOMEMORY:
  case MIZU_ERRCAT_EXISTS:
    PyErr_Format(MizuShmError, "pymizu: %s", msg);
    break;
  default:
    PyErr_Format(MizuError, "pymizu: %s", msg);
    break;
  }
  return NULL;
}

static MizuPool *pool_wrap(mizu_pool *p, int role, MizuViewCache *vc) {
  MizuPool *self = (MizuPool *) MizuPoolType.tp_alloc(&MizuPoolType, 0);
  if (self == NULL) return NULL;
  self->core = p;
  self->self_pid = mizu_self_pid();
  self->role = role;
  self->vcache = vc;
  self->trace_fn = NULL;
  return self;
}

/* Task-lifecycle trace trampoline. Emit sites run on worker threads and on
   hot paths with the GIL released (submitter verbs), so the GIL is
   reacquired per call; a hook exception can never propagate into the
   core. */
static void pool_trace_cb(mizu_trace_event event, uint64_t task_id,
                          void *ctx) {
  static const char *const events[] = {
    "submit", "start", "done", "error", "drop", "rehome"
  };
  MizuPool *self = (MizuPool *) ctx;
  PyGILState_STATE gs = PyGILState_Ensure();
  PyObject *fn = self->trace_fn;
  if (fn != NULL) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%u:%llu", (unsigned) (task_id >> 48),
             (unsigned long long) (task_id & ((1ull << 48) - 1)));
    PyObject *r = PyObject_CallFunction(fn, "ss", events[event], buf);
    if (r == NULL)
      PyErr_WriteUnraisable(fn);
    else
      Py_DECREF(r);
  }
  PyGILState_Release(gs);
}

/* A collected value is either the task's result or a _Caught box carrying
   the outcome's exception. Unwrap and raise; with_index attributes the
   0-based position (collect_any / collect_all). */
static PyObject *caught_or_value(PyObject *v, size_t index, int with_index) {
  if (Py_TYPE(v) != &MizuCaughtType) return v;
  PyObject *exc = ((MizuCaught *) v)->exc;
  Py_INCREF(exc);
  Py_DECREF(v);
  if (with_index) {
    PyObject *i = PyLong_FromSize_t(index);
    if (i == NULL || PyObject_SetAttrString(exc, "index", i) < 0) {
      Py_XDECREF(i);
      Py_DECREF(exc);
      return NULL;
    }
    Py_DECREF(i);
  }
  PyErr_SetObject((PyObject *) Py_TYPE(exc), exc);
  Py_DECREF(exc);
  return NULL;
}

// Task handles -------------------------------------------------------------------

static MizuTask *task_wrap(MizuPool *pool, const mizu_task *t) {
  MizuTask *self = (MizuTask *) MizuTaskType.tp_alloc(&MizuTaskType, 0);
  if (self == NULL) return NULL;
  self->word = t->word;
  Py_INCREF(pool);
  self->pool = (PyObject *) pool;
  return self;
}

/* Unpack a task handle and its pool (NULL on error, with the exception
   set). Errors on a foreign or finalized handle; the core detects a stale
   (collected/released) sequence. */
static mizu_task task_get(MizuTask *self, MizuPool **pool_out) {
  MizuPool *pool = (MizuPool *) self->pool;
  mizu_task t = { self->word };
  if (self->word == 0) {
    PyErr_SetString(PyExc_ValueError, "pymizu: task handle is closed");
    pool = NULL;
  } else if (pool_get(pool) == NULL) {
    pool = NULL;
  }
  *pool_out = pool;
  return t;
}

/* Extract a task sequence's handles and their shared pool. */
static MizuPool *tasks_get(PyObject *arg, mizu_task **ts_out, size_t *n_out) {
  PyObject *seq = PySequence_Fast(
    arg, "pymizu: tasks must be a non-empty sequence of task handles");
  if (seq == NULL) return NULL;
  Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
  if (n < 1) {
    Py_DECREF(seq);
    PyErr_SetString(PyExc_ValueError,
                    "pymizu: tasks must be a non-empty sequence of task handles");
    return NULL;
  }
  mizu_task *ts = PyMem_Malloc((size_t) n * sizeof(mizu_task));
  if (ts == NULL) {
    Py_DECREF(seq);
    PyErr_NoMemory();
    return NULL;
  }
  MizuPool *pool = NULL;
  PyObject **items = PySequence_Fast_ITEMS(seq);
  for (Py_ssize_t i = 0; i < n; i++) {
    if (Py_TYPE(items[i]) != &MizuTaskType) {
      PyErr_SetString(PyExc_TypeError, "pymizu: not a task handle");
      goto fail;
    }
    MizuPool *pi;
    ts[i] = task_get((MizuTask *) items[i], &pi);
    if (pi == NULL) goto fail;
    if (pool == NULL) {
      pool = pi;
    } else if (pool != pi) {
      PyErr_SetString(PyExc_ValueError,
                      "pymizu: task handles must belong to the same pool handle");
      goto fail;
    }
  }
  Py_DECREF(seq);
  *ts_out = ts;
  *n_out = (size_t) n;
  return pool;
fail:
  PyMem_Free(ts);
  Py_DECREF(seq);
  return NULL;
}

// Pool verbs ---------------------------------------------------------------------

PyDoc_STRVAR(pool_submit_doc,
"submit(payload, timeout=None) -> _Task\n\n\
Stage and submit one task payload (the facade's (fn, args, kwargs) tuple).\n\
Blocks only for injection-ring space, up to `timeout` seconds (None waits\n\
indefinitely, 0 polls): pymizu.SubmitTimeoutError on expiry,\n\
pymizu.SlotsExhaustedError / pymizu.StoppedError on the fatal outcomes.");

static PyObject *Pool_submit(MizuPool *self, PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"payload", "timeout", NULL};
  PyObject *payload, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:submit", kwlist, &payload,
                                   &tmo))
    return NULL;
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  mizu_task t;
  mizu_status st;
  POOL_ALLOW_THREADS(self);
  st = mizu_pool_submit(p, (void *) payload, &t, ms);
  POOL_RESUME();
  if (st == MIZU_OK) return (PyObject *) task_wrap(self, &t);
  if (st == MIZU_FULL) {
    PyErr_SetString(MizuSubmitTimeoutError,
                    "pymizu: submission timed out (injection ring full)");
    return NULL;
  }
  return pool_raise(self);
}

PyDoc_STRVAR(pool_submit_batch_doc,
"submit_batch(payloads, timeout=None) -> list of _Task\n\n\
One task per payload in a single crossing. Ring-full past `timeout` ends\n\
the batch short — the accepted handles stay valid and collectible; the\n\
fatal outcomes still raise.");

static PyObject *Pool_submit_batch(MizuPool *self, PyObject *args,
                                   PyObject *kw) {
  static char *kwlist[] = {"payloads", "timeout", NULL};
  PyObject *seq, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:submit_batch", kwlist,
                                   &seq, &tmo))
    return NULL;
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  PyObject *fast =
    PySequence_Fast(seq, "pymizu: expected a sequence of task payloads");
  if (fast == NULL) return NULL;
  Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
  void **objs = PyMem_Malloc((size_t) (n != 0 ? n : 1) * sizeof(void *));
  mizu_task *ts = PyMem_Malloc((size_t) (n != 0 ? n : 1) * sizeof(mizu_task));
  if (objs == NULL || ts == NULL) {
    PyMem_Free(objs);
    PyMem_Free(ts);
    Py_DECREF(fast);
    return PyErr_NoMemory();
  }
  PyObject **items = PySequence_Fast_ITEMS(fast);
  for (Py_ssize_t i = 0; i < n; i++)
    objs[i] = (void *) items[i];
  size_t done = 0;
  mizu_status st;
  POOL_ALLOW_THREADS(self);
  st = mizu_pool_submit_batch(p, objs, (size_t) n, ts, &done, ms);
  POOL_RESUME();
  PyMem_Free(objs);
  Py_DECREF(fast);
  if (st == MIZU_ERR) {
    PyMem_Free(ts);
    return pool_raise(self);
  }
  PyObject *out = PyList_New((Py_ssize_t) done);
  if (out == NULL) {
    PyMem_Free(ts);
    return NULL;
  }
  for (size_t i = 0; i < done; i++) {
    MizuTask *t = task_wrap(self, &ts[i]);
    if (t == NULL) {
      PyMem_Free(ts);
      Py_DECREF(out);
      return NULL;
    }
    PyList_SET_ITEM(out, (Py_ssize_t) i, (PyObject *) t);
  }
  PyMem_Free(ts);
  return out;
}

PyDoc_STRVAR(pool_collect_any_doc,
"collect_any(tasks, timeout=None) -> (index, value) | sentinel\n\n\
Wait on several of this handle's tasks at once; return the first terminal\n\
one as its 0-based position and value (ties among already-terminal handles\n\
break to the earliest position). A non-OK outcome raises with an `index`\n\
attribute. pymizu.TIMEOUT on expiry; the reported handle is consumed, the\n\
rest stay collectible.");

static PyObject *Pool_collect_any(MizuPool *self, PyObject *args,
                                  PyObject *kw) {
  static char *kwlist[] = {"tasks", "timeout", NULL};
  PyObject *seq, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:collect_any", kwlist,
                                   &seq, &tmo))
    return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  mizu_task *ts;
  size_t n;
  MizuPool *pool = tasks_get(seq, &ts, &n);
  if (pool == NULL) return NULL;
  if (pool != self) {
    PyMem_Free(ts);
    PyErr_SetString(PyExc_ValueError,
                    "pymizu: task handles must belong to this pool handle");
    return NULL;
  }
  void *v = NULL;
  size_t idx = 0;
  mizu_status st;
  POOL_ALLOW_THREADS(self);
  st = mizu_pool_collect_any(self->core, ts, n, &idx, &v, ms);
  POOL_RESUME();
  PyMem_Free(ts);
  if (st == MIZU_TIMEOUT) {
    Py_INCREF(SentTimeout);
    return SentTimeout;
  }
  if (st == MIZU_ERR) return pool_raise(self);
  PyObject *val = caught_or_value((PyObject *) v, idx, 1);
  if (val == NULL) return NULL;
  PyObject *i = PyLong_FromSize_t(idx);
  PyObject *out = i != NULL ? PyTuple_New(2) : NULL;
  if (out != NULL) {
    PyTuple_SET_ITEM(out, 0, i);
    PyTuple_SET_ITEM(out, 1, val);
  } else {
    Py_XDECREF(i);
    Py_DECREF(val);
  }
  return out;
}

PyDoc_STRVAR(pool_collect_all_doc,
"collect_all(tasks, timeout=None) -> list | sentinel\n\n\
Wait until every task is terminal; return all values in input order. On\n\
the first non-OK outcome by position it raises with an `index` attribute\n\
— only the reported handle is consumed; every other, the results ahead\n\
of it included, stays collectible. pymizu.TIMEOUT consumes nothing.");

static PyObject *Pool_collect_all(MizuPool *self, PyObject *args,
                                  PyObject *kw) {
  static char *kwlist[] = {"tasks", "timeout", NULL};
  PyObject *seq, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:collect_all", kwlist,
                                   &seq, &tmo))
    return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  mizu_task *ts;
  size_t n;
  MizuPool *pool = tasks_get(seq, &ts, &n);
  if (pool == NULL) return NULL;
  if (pool != self) {
    PyMem_Free(ts);
    PyErr_SetString(PyExc_ValueError,
                    "pymizu: task handles must belong to this pool handle");
    return NULL;
  }
  PyObject *out = PyList_New((Py_ssize_t) n);
  if (out == NULL) {
    PyMem_Free(ts);
    return NULL;
  }
  size_t err_idx = 0;
  mizu_status st;
  POOL_ALLOW_THREADS(self);
  st = mizu_pool_collect_all_fn(self->core, ts, n, list_sink, out, &err_idx,
                               ms);
  POOL_RESUME();
  PyMem_Free(ts);
  if (st == MIZU_TIMEOUT) {
    Py_DECREF(out);
    Py_INCREF(SentTimeout);
    return SentTimeout;
  }
  if (st == MIZU_ERR) {
    Py_DECREF(out);
    return pool_raise(self);
  }
  if (err_idx < n) {
    /* the first non-OK by position: its box, with the 0-based index */
    PyObject *box = PyList_GET_ITEM(out, (Py_ssize_t) err_idx);
    Py_INCREF(box);
    Py_DECREF(out);
    return caught_or_value(box, err_idx, 1);
  }
  return out;
}

PyDoc_STRVAR(pool_ready_wait_doc,
"ready_wait(slots, timeout) -> bool\n\n\
The startup / elastic-spawn rendezvous: wait up to `timeout` seconds for\n\
the given worker slots to join. False on expiry. Controller only.");

static PyObject *Pool_ready_wait(MizuPool *self, PyObject *args,
                                 PyObject *kw) {
  static char *kwlist[] = {"slots", "timeout", NULL};
  PyObject *seq, *tmo;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "OO:ready_wait", kwlist,
                                   &seq, &tmo))
    return NULL;
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  if (self->role != MIZU_ROLE_CONTROLLER) {
    PyErr_SetString(MizuError, "pymizu: only the controller can wait for workers");
    return NULL;
  }
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  PyObject *fast = PySequence_Fast(seq, "pymizu: expected worker slot indices");
  if (fast == NULL) return NULL;
  Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
  uint32_t *slots = PyMem_Malloc((size_t) (n != 0 ? n : 1) * sizeof(uint32_t));
  if (slots == NULL) {
    Py_DECREF(fast);
    return PyErr_NoMemory();
  }
  PyObject **items = PySequence_Fast_ITEMS(fast);
  for (Py_ssize_t i = 0; i < n; i++) {
    long s = PyLong_AsLong(items[i]);
    if (s == -1 && PyErr_Occurred()) {
      PyMem_Free(slots);
      Py_DECREF(fast);
      return NULL;
    }
    if (s < 0 || s >= MIZU_MAX_WORKERS) {
      PyMem_Free(slots);
      Py_DECREF(fast);
      PyErr_SetString(PyExc_ValueError, "pymizu: worker slot out of range");
      return NULL;
    }
    slots[i] = (uint32_t) s;
  }
  Py_DECREF(fast);
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_pool_ready_wait(p, slots, (size_t) n, ms);
  Py_END_ALLOW_THREADS
  PyMem_Free(slots);
  if (st == MIZU_ERR) return pool_raise(self);
  return PyBool_FromLong(st == MIZU_OK);
}

PyDoc_STRVAR(pool_retire_doc,
"retire(slot) -> None\n\n\
Ask one worker to exit cleanly: non-blocking, never preemptive. The worker\n\
observes between tasks and releases its slot; the remaining workers\n\
consume its queued work in place. Controller only.");

static PyObject *Pool_retire(MizuPool *self, PyObject *arg) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  if (self->role != MIZU_ROLE_CONTROLLER) {
    PyErr_SetString(MizuError, "pymizu: only the controller can retire a worker");
    return NULL;
  }
  long slot = PyLong_AsLong(arg);
  if (slot == -1 && PyErr_Occurred()) return NULL;
  if (slot < 0 || slot >= MIZU_MAX_WORKERS) {
    PyErr_SetString(PyExc_ValueError, "pymizu: worker slot out of range");
    return NULL;
  }
  if (mizu_pool_retire(p, (uint32_t) slot) != MIZU_OK)
    return pool_raise(self);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(pool_stop_doc,
"stop(timeout=5.0) -> bool\n\n\
Orderly shutdown, controller only: broadcast shutdown, wake every parked\n\
participant, cancel all pending tasks (blocked collectors raise\n\
pymizu.CancelledError), then wait up to `timeout` seconds for clean worker\n\
exits and unlink. False on expiry — the workers still exit on their own.\n\
Idempotent; the handle is dead afterwards.");

static PyObject *Pool_stop(MizuPool *self, PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"timeout", NULL};
  PyObject *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:stop", kwlist, &tmo))
    return NULL;
  if (self->core == NULL) Py_RETURN_TRUE;    /* idempotent */
  if (self->self_pid != mizu_self_pid()) {
    self->core = NULL;
    Py_RETURN_TRUE;
  }
  if (self->role != MIZU_ROLE_CONTROLLER) {
    PyErr_SetString(MizuError, "pymizu: only the controller can stop a pool");
    return NULL;
  }
  double ms;
  if (tmo == Py_None) {
    ms = 5000.0;
  } else if (timeout_ms_of(tmo, &ms) < 0) {
    return NULL;
  }
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_pool_stop(self->core, ms);
  Py_END_ALLOW_THREADS
  if (st == MIZU_ERR) return pool_raise(self);
  /* the handle stays alive (a post-stop submit reads the shutdown flag as
     StoppedError); the finalizer's destroy is a no-op after */
  return PyBool_FromLong(st == MIZU_OK);
}

PyDoc_STRVAR(pool_destroy_doc,
"destroy() -> None\n\n\
Idempotent, never blocks: a controller broadcasts shutdown without the\n\
wait; a participant releases its slot. The finalizer target.");

static PyObject *Pool_destroy(MizuPool *self, PyObject *Py_UNUSED(args)) {
  if (self->core != NULL && self->self_pid == mizu_self_pid())
    mizu_pool_destroy(self->core);
  self->core = NULL;
  view_cache_free(self->vcache);
  self->vcache = NULL;
  Py_CLEAR(self->trace_fn);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(pool_run_doc,
"run() -> int\n\n\
The worker loop: claim/steal tasks and execute them on this thread,\n\
blocking until an exit reason — 0 shutdown, 1 owner gone, 2 retired.\n\
Raises on infrastructure failure. Worker handles only. The GIL stays held\n\
(the park hook drops it around each bounded sleep).");

static PyObject *Pool_run(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  if (self->role != MIZU_ROLE_WORKER) {
    PyErr_SetString(MizuError, "pymizu: not a worker handle");
    return NULL;
  }
  mizu_worker_exit ex = mizu_pool_worker_run(p);
  if (PyErr_Occurred()) return NULL;   /* a BaseException escaped a task */
  if (ex == MIZU_EXIT_ERROR) return pool_raise(self);
  return PyLong_FromLong((long) ex);
}

PyDoc_STRVAR(pool_leave_doc,
"leave() -> None\n\n\
The clean-exit handshake of a worker. No-op on a released handle.");

static PyObject *Pool_leave(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = self->core;
  if (p == NULL || self->self_pid != mizu_self_pid()) Py_RETURN_NONE;
  if (self->role != MIZU_ROLE_WORKER) {
    PyErr_SetString(MizuError, "pymizu: not a worker handle");
    return NULL;
  }
  if (mizu_pool_leave(p) != MIZU_OK) return pool_raise(self);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(pool_lame_duck_doc,
"lame_duck() -> bool\n\n\
One linger beat for a retired worker anchoring its uncollected results:\n\
True when the anchor may drop (shutdown or owner death ends the linger).");

static PyObject *Pool_lame_duck(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = self->core;
  if (p == NULL || self->self_pid != mizu_self_pid()) Py_RETURN_TRUE;
  return PyBool_FromLong(mizu_pool_lame_duck(p));
}

// Pool introspection --------------------------------------------------------------

static const char *const wk_states[] =
  {"free", "claiming", "live", "leaving", "reaping"};
static const char *const sub_states[] = {"free", "live", "reaping"};
static const char *const rs_states[] =
  {"free", "pending", "ok", "err", "cancel", "died"};

static int dict_set_sll(PyObject *d, const char *key, long long v) {
  PyObject *o = PyLong_FromLongLong(v);
  if (o == NULL) return -1;
  int rc = PyDict_SetItemString(d, key, o);
  Py_DECREF(o);
  return rc;
}

static int dict_set_str(PyObject *d, const char *key, const char *s) {
  PyObject *o = PyUnicode_FromString(s);
  if (o == NULL) return -1;
  int rc = PyDict_SetItemString(d, key, o);
  Py_DECREF(o);
  return rc;
}

static PyObject *state_list(const uint8_t *states, uint32_t n,
                            const char *const *names, int nnames) {
  PyObject *out = PyList_New((Py_ssize_t) n);
  if (out == NULL) return NULL;
  for (uint32_t i = 0; i < n; i++) {
    int s = states[i];
    PyObject *o = PyUnicode_FromString(
      s >= 0 && s < nnames ? names[s] : "unknown");
    if (o == NULL) {
      Py_DECREF(out);
      return NULL;
    }
    PyList_SET_ITEM(out, (Py_ssize_t) i, o);
  }
  return out;
}

PyDoc_STRVAR(pool_status_doc,
"status() -> dict\n\n\
A read-only wire-state snapshot: registry states, parked-worker count,\n\
queued injection entries, and result-slot occupancy.");

static PyObject *Pool_status(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  mizu_pool_status st;
  if (mizu_pool_status_get(p, &st) != MIZU_OK) return pool_raise(self);
  PyObject *d = PyDict_New();
  if (d == NULL) return NULL;
  const char *role = st.role == MIZU_ROLE_CONTROLLER ? "controller" :
    st.role == MIZU_ROLE_WORKER ? "worker" : "submitter";
  PyObject *workers = NULL, *submitters = NULL, *tasks = NULL, *deque = NULL;
  int nparked = 0;
  for (uint32_t i = 0; i < 64; i++)
    nparked += (int) (st.parked_mask >> i) & 1;
  uint64_t queued = 0;
  for (uint32_t j = 0; j < st.max_submitters; j++)
    queued += st.inj_queued[j];
  workers = state_list(st.worker_state, st.max_workers, wk_states, 5);
  submitters = state_list(st.sub_state, st.max_submitters, sub_states, 3);
  tasks = PyDict_New();
  deque = PyList_New((Py_ssize_t) st.max_workers);
  if (workers == NULL || submitters == NULL || tasks == NULL || deque == NULL)
    goto fail;
  {
    static const char *const tnames[] =
      {"pending", "ok", "err", "cancel", "died"};
    for (int s = 0; s < 5; s++)
      if (dict_set(tasks, tnames[s],
                   (unsigned long long) st.tasks_by_state[s + MIZU_RS_PENDING]) < 0)
        goto fail;
    for (uint32_t i = 0; i < st.max_workers; i++) {
      PyObject *o = PyLong_FromLongLong((long long) st.deque_depth[i]);
      if (o == NULL) goto fail;
      PyList_SET_ITEM(deque, (Py_ssize_t) i, o);
    }
  }
  if (dict_set_str(d, "name", st.name) < 0 ||
      dict_set_str(d, "role", role) < 0 ||
      dict_set(d, "max_workers", st.max_workers) < 0 ||
      dict_set(d, "max_submitters", st.max_submitters) < 0 ||
      dict_set(d, "injection_cap", st.injection_cap) < 0 ||
      dict_set(d, "result_slots", st.result_slots) < 0 ||
      dict_set(d, "slot_size", st.slot_size) < 0 ||
      dict_set(d, "parked", (unsigned long long) nparked) < 0 ||
      dict_set(d, "injection", queued) < 0 ||
      dict_set(d, "shutdown", (unsigned long long) (st.shutdown != 0)) < 0 ||
      PyDict_SetItemString(d, "workers", workers) < 0 ||
      PyDict_SetItemString(d, "submitters", submitters) < 0 ||
      PyDict_SetItemString(d, "tasks", tasks) < 0 ||
      PyDict_SetItemString(d, "deque", deque) < 0)
    goto fail;
  Py_DECREF(workers);
  Py_DECREF(submitters);
  Py_DECREF(tasks);
  Py_DECREF(deque);
  return d;
fail:
  Py_XDECREF(workers);
  Py_XDECREF(submitters);
  Py_XDECREF(tasks);
  Py_XDECREF(deque);
  Py_DECREF(d);
  return NULL;
}

PyDoc_STRVAR(pool_dump_doc,
"dump() -> dict\n\n\
A read-only debugging snapshot of the whole pool region, one level deeper\n\
than status(): per-slot registry detail, every occupied result slot, and\n\
the handle-local machinery under `local`. States can move mid-fill.");

static PyObject *dump_worker(const mizu_pool_dump *d, uint32_t i) {
  const mizu_worker_stat *w = &d->workers[i];
  PyObject *r = PyDict_New();
  if (r == NULL) return NULL;
  int s = w->status, ps = w->park_state;
  if (dict_set(r, "slot", i) < 0 ||
      dict_set_str(r, "status",
                   s >= 0 && s < 5 ? wk_states[s] : "unknown") < 0 ||
      dict_set_sll(r, "pid", (long long) w->pid) < 0 ||
      dict_set_str(r, "park_state",
                   ps >= 0 && ps < 4 ?
                   (const char *[]) {"running", "idle", "parked",
                                     "waking"}[ps] : "unknown") < 0 ||
      dict_set(r, "parked", (unsigned long long)
               ((d->status.parked_mask >> i) & 1)) < 0 ||
      dict_set_sll(r, "top", (long long) w->deque_top) < 0 ||
      dict_set_sll(r, "bottom", (long long) w->deque_bottom) < 0 ||
      dict_set_sll(r, "in_flight", (long long) w->in_flight_rs) < 0 ||
      dict_set(r, "tasks", (unsigned long long) w->tasks) < 0 ||
      dict_set(r, "steals", (unsigned long long) w->steals) < 0 ||
      dict_set(r, "injections", (unsigned long long) w->injections) < 0 ||
      dict_set(r, "parks", (unsigned long long) w->parks) < 0 ||
      dict_set(r, "helps", (unsigned long long) w->helps) < 0) {
    Py_DECREF(r);
    return NULL;
  }
  return r;
}

static PyObject *dump_submitter(const mizu_pool_dump *d, uint32_t j) {
  const mizu_sub_stat *s = &d->submitters[j];
  PyObject *r = PyDict_New();
  if (r == NULL) return NULL;
  int st = s->status;
  if (dict_set(r, "slot", j) < 0 ||
      dict_set_str(r, "status",
                   st >= 0 && st < 3 ? sub_states[st] : "unknown") < 0 ||
      dict_set_sll(r, "pid", (long long) s->pid) < 0 ||
      dict_set(r, "rs_start", s->rs_start) < 0 ||
      dict_set(r, "rs_count", s->rs_count) < 0 ||
      dict_set(r, "queued",
               (unsigned long long) (s->injected - s->claimed)) < 0 ||
      dict_set(r, "injected", (unsigned long long) s->injected) < 0 ||
      dict_set(r, "claimed", (unsigned long long) s->claimed) < 0 ||
      dict_set(r, "spills", (unsigned long long) s->spills) < 0 ||
      dict_set(r, "spill_reuse", (unsigned long long) s->spill_reuse) < 0 ||
      dict_set(r, "ready", (unsigned long long) (s->ready != 0)) < 0 ||
      dict_set(r, "full_waiter",
               (unsigned long long) (s->full_waiter != 0)) < 0) {
    Py_DECREF(r);
    return NULL;
  }
  return r;
}

static PyObject *Pool_dump(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  mizu_pool_dump d;
  if (mizu_pool_dump_get(p, &d) != MIZU_OK) return pool_raise(self);
  PyObject *out = PyDict_New();
  PyObject *workers = NULL, *submitters = NULL, *tasks = NULL, *local = NULL;
  if (out == NULL) return NULL;
  workers = PyList_New((Py_ssize_t) d.n_workers);
  submitters = PyList_New((Py_ssize_t) d.n_submitters);
  tasks = PyList_New(0);
  local = PyDict_New();
  if (workers == NULL || submitters == NULL || tasks == NULL || local == NULL)
    goto fail;
  for (uint32_t i = 0; i < d.n_workers; i++) {
    PyObject *r = dump_worker(&d, i);
    if (r == NULL) goto fail;
    PyList_SET_ITEM(workers, (Py_ssize_t) i, r);
  }
  for (uint32_t j = 0; j < d.n_submitters; j++) {
    PyObject *r = dump_submitter(&d, j);
    if (r == NULL) goto fail;
    PyList_SET_ITEM(submitters, (Py_ssize_t) j, r);
  }
  {
    /* every non-FREE result slot, paged in one call (result_slots rows is
       always enough) */
    uint32_t cap = d.status.result_slots;
    mizu_rs_row *rows = PyMem_Malloc((size_t) (cap != 0 ? cap : 1) *
                                    sizeof(mizu_rs_row));
    if (rows == NULL) {
      PyErr_NoMemory();
      goto fail;
    }
    uint32_t n = 0;
    mizu_status st = mizu_pool_tasks_get(p, rows, cap, &n);
    if (st != MIZU_OK) {
      PyMem_Free(rows);
      pool_raise(self);
      goto fail_no_raise;
    }
    for (uint32_t m = 0; m < n; m++) {
      PyObject *r = PyDict_New();
      int s = rows[m].status;
      int ok = r != NULL &&
               dict_set(r, "slot", rows[m].slot) == 0 &&
               dict_set_str(r, "status",
                            s >= 0 && s < 6 ? rs_states[s] : "unknown") == 0 &&
               dict_set(r, "sequence",
                        (unsigned long long) rows[m].sequence) == 0 &&
               dict_set_sll(r, "worker", (long long) rows[m].worker_slot) == 0 &&
               dict_set_sll(r, "waiter", (long long) rows[m].waiter_slot) == 0 &&
               PyList_Append(tasks, r) == 0;
      Py_XDECREF(r);
      if (!ok) {
        PyMem_Free(rows);
        goto fail;
      }
    }
    PyMem_Free(rows);
  }
  if (dict_set(local, "fl_entries", (unsigned long long) d.fl_entries) < 0 ||
      dict_set(local, "fl_bytes", (unsigned long long) d.fl_bytes) < 0 ||
      dict_set(local, "fl_hits", (unsigned long long) d.fl_hits) < 0 ||
      dict_set(local, "open_hits", (unsigned long long) d.open_hits) < 0 ||
      dict_set(local, "open_misses", (unsigned long long) d.open_misses) < 0 ||
      dict_set(local, "collect_parks",
               (unsigned long long) d.collect_parks) < 0 ||
      dict_set_str(out, "name", d.status.name) < 0 ||
      dict_set(out, "shutdown",
               (unsigned long long) (d.status.shutdown != 0)) < 0 ||
      dict_set(out, "help", (unsigned long long) (d.help_wanted != 0)) < 0 ||
      PyDict_SetItemString(out, "workers", workers) < 0 ||
      PyDict_SetItemString(out, "submitters", submitters) < 0 ||
      PyDict_SetItemString(out, "tasks", tasks) < 0 ||
      PyDict_SetItemString(out, "local", local) < 0)
    goto fail;
  Py_DECREF(workers);
  Py_DECREF(submitters);
  Py_DECREF(tasks);
  Py_DECREF(local);
  return out;
fail:
fail_no_raise:
  Py_XDECREF(workers);
  Py_XDECREF(submitters);
  Py_XDECREF(tasks);
  Py_XDECREF(local);
  Py_DECREF(out);
  return NULL;
}

PyDoc_STRVAR(pool_stats_doc,
"stats() -> dict\n\n\
Cumulative per-worker and per-submitter counters since each participant\n\
joined: tasks, steals, injections, parks, helps, and deque depth\n\
(workers); injected, claimed, spills, spill_reuse, and queued\n\
(submitters). Read-only; states can move mid-fill.");

static PyObject *Pool_stats(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  mizu_pool_dump d;
  if (mizu_pool_dump_get(p, &d) != MIZU_OK) return pool_raise(self);
  PyObject *out = PyDict_New();
  PyObject *workers = NULL, *submitters = NULL;
  if (out == NULL) return NULL;
  workers = PyList_New((Py_ssize_t) d.n_workers);
  submitters = PyList_New((Py_ssize_t) d.n_submitters);
  if (workers == NULL || submitters == NULL) goto fail;
  for (uint32_t i = 0; i < d.n_workers; i++) {
    const mizu_worker_stat *w = &d.workers[i];
    int s = w->status;
    int64_t dep = w->deque_bottom - w->deque_top;
    PyObject *r = PyDict_New();
    int ok = r != NULL &&
             dict_set(r, "slot", i) == 0 &&
             dict_set_str(r, "status",
                          s >= 0 && s < 5 ? wk_states[s] : "unknown") == 0 &&
             dict_set_sll(r, "pid", (long long) w->pid) == 0 &&
             dict_set(r, "tasks", (unsigned long long) w->tasks) == 0 &&
             dict_set(r, "steals", (unsigned long long) w->steals) == 0 &&
             dict_set(r, "injections",
                      (unsigned long long) w->injections) == 0 &&
             dict_set(r, "parks", (unsigned long long) w->parks) == 0 &&
             dict_set(r, "helps", (unsigned long long) w->helps) == 0 &&
             dict_set(r, "deque",
                      (unsigned long long) (dep > 0 ? dep : 0)) == 0;
    if (!ok) {
      Py_XDECREF(r);
      goto fail;
    }
    PyList_SET_ITEM(workers, (Py_ssize_t) i, r);
  }
  for (uint32_t j = 0; j < d.n_submitters; j++) {
    const mizu_sub_stat *s = &d.submitters[j];
    int st = s->status;
    PyObject *r = PyDict_New();
    int ok = r != NULL &&
             dict_set(r, "slot", j) == 0 &&
             dict_set_str(r, "status",
                          st >= 0 && st < 3 ? sub_states[st] :
                          "unknown") == 0 &&
             dict_set_sll(r, "pid", (long long) s->pid) == 0 &&
             dict_set(r, "injected",
                      (unsigned long long) s->injected) == 0 &&
             dict_set(r, "claimed", (unsigned long long) s->claimed) == 0 &&
             dict_set(r, "spills", (unsigned long long) s->spills) == 0 &&
             dict_set(r, "spill_reuse",
                      (unsigned long long) s->spill_reuse) == 0 &&
             dict_set(r, "queued",
                      (unsigned long long) (s->injected - s->claimed)) == 0;
    if (!ok) {
      Py_XDECREF(r);
      goto fail;
    }
    PyList_SET_ITEM(submitters, (Py_ssize_t) j, r);
  }
  if (PyDict_SetItemString(out, "workers", workers) < 0 ||
      PyDict_SetItemString(out, "submitters", submitters) < 0)
    goto fail;
  Py_DECREF(workers);
  Py_DECREF(submitters);
  return out;
fail:
  Py_XDECREF(workers);
  Py_XDECREF(submitters);
  Py_DECREF(out);
  return NULL;
}

PyDoc_STRVAR(pool_set_trace_doc,
"set_trace(fn) -> None\n\n\
Register a per-handle task-lifecycle hook called as fn(event, id) at each\n\
event this process observes: 'submit' on the submitting thread; 'start',\n\
'done', 'error', 'drop', 'rehome' on worker handles. id is\n\
'<submitter slot>:<counter>'. None removes the hook.");

static PyObject *Pool_set_trace(MizuPool *self, PyObject *fn) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  if (fn == Py_None) {
    mizu_pool_set_trace(p, NULL, NULL);
    Py_CLEAR(self->trace_fn);
    Py_RETURN_NONE;
  }
  if (!PyCallable_Check(fn)) {
    PyErr_SetString(PyExc_TypeError, "pymizu: expected a callable or None");
    return NULL;
  }
  Py_INCREF(fn);
  Py_XSETREF(self->trace_fn, fn);
  if (mizu_pool_set_trace(p, pool_trace_cb, self) != MIZU_OK) {
    mizu_pool_set_trace(p, NULL, NULL);
    Py_CLEAR(self->trace_fn);
    return pool_raise(self);
  }
  Py_RETURN_NONE;
}

static PyObject *Pool_token_get(MizuPool *self, void *Py_UNUSED(closure)) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  char buf[64];
  if (mizu_pool_token(p, buf, sizeof(buf)) != MIZU_OK)
    return pool_raise(self);
  return PyUnicode_FromString(buf);
}

static PyObject *Pool_repr(MizuPool *self) {
  if (self->core == NULL)
    return PyUnicode_FromString("<pymizu.Pool (closed)>");
  char buf[64];
  if (mizu_pool_token(self->core, buf, sizeof(buf)) != MIZU_OK)
    buf[0] = '\0';
  const char *role = self->role == MIZU_ROLE_CONTROLLER ? "controller" :
    self->role == MIZU_ROLE_WORKER ? "worker" : "submitter";
  return PyUnicode_FromFormat("<pymizu.Pool %s (%s)>", buf, role);
}

static void Pool_dealloc(MizuPool *self) {
  /* a controller destroy broadcasts shutdown (no wait); a participant
     releases its slot; a forked child's copy never touches the region */
  if (self->core != NULL && self->self_pid == mizu_self_pid())
    mizu_pool_destroy(self->core);
  view_cache_free(self->vcache);
  Py_XDECREF(self->trace_fn);
  MizuPoolType.tp_free((PyObject *) self);
}

// Pool map support (the Pool.map veneers) -----------------------------------

PyDoc_STRVAR(pool_map_caps_doc,
"_map_caps() -> (live_workers, free_result_slots, injection_cap, \\\n\
inline_entry_budget)\n\n\
A map's batch-sizing inputs in one read pass. A worker's first nested map\n\
claims its submitter slot here, exactly as a first nested submit does.");

static PyObject *Pool_map_caps(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  uint32_t free_rs, inj_cap, inline_entry;
  if (mizu_pool_map_caps(p, &free_rs, &inj_cap, &inline_entry) != 0)
    return pool_raise(self);
  mizu_pool_status st;
  if (mizu_pool_status_get(p, &st) != MIZU_OK) return pool_raise(self);
  uint32_t live = 0;
  for (uint32_t i = 0; i < st.max_workers; i++)
    live += st.worker_state[i] == MIZU_WK_LIVE;
  return Py_BuildValue("(IIII)", live, free_rs, inj_cap, inline_entry);
}

static void pool_sig_capsule_free(PyObject *caps) {
  void *p = PyCapsule_GetPointer(caps, MIZU_PY_SIG_CAPSULE);
  if (p == NULL) {
    PyErr_Clear();   /* NULL payload tolerated */
    return;
  }
  free(p);
}

PyDoc_STRVAR(pool_signals_doc,
"_signals() -> capsule\n\n\
The opaque pool-signal trio a map runner loads once per batch transition\n\
(the help_wanted doorbell, the shared shutdown word, the owner-dead\n\
flag). Worker-local only: the capsule wraps raw addresses, valid only in\n\
the process that created it — a runner calls this on its own attached\n\
pool handle, never on one received from a submitter.");

static PyObject *Pool_signals(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  mizu_pool_sig *s = mizu_pool_signals(p);
  if (s == NULL) return PyErr_NoMemory();
  return PyCapsule_New(s, MIZU_PY_SIG_CAPSULE, pool_sig_capsule_free);
}

PyDoc_STRVAR(pool_help_once_doc,
"_help_once() -> bool\n\n\
One doorbell help beat at a runner's batch boundary: claims one foreign\n\
injection task (a runner-flagged one is re-homed onto this worker's own\n\
deque, not executed). True when it claimed. The worker keeps holding the\n\
GIL around the call: a claimed task's Python callable needs it.");

static PyObject *Pool_help_once(MizuPool *self, PyObject *Py_UNUSED(args)) {
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  int rc = mizu_pool_help_once(p);
  if (rc < 0) return pool_raise(self);
  return PyBool_FromLong(rc);
}

PyDoc_STRVAR(pool_submit_runner_doc,
"_submit_runner(payload, timeout=None) -> _Task\n\n\
A map runner's submit: submit plus the MIZU_ENTRY_RUNNER wire flag, so a\n\
doorbell help beat re-homes it onto the helper's own deque instead of\n\
executing a join ticket nested. Same error taxonomy as submit().");

static PyObject *Pool_submit_runner(MizuPool *self, PyObject *args,
                                    PyObject *kw) {
  static char *kwlist[] = {"payload", "timeout", NULL};
  PyObject *payload, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:_submit_runner", kwlist,
                                   &payload, &tmo))
    return NULL;
  mizu_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  mizu_task t;
  mizu_status st;
  POOL_ALLOW_THREADS(self);
  st = mizu_pool_submit_flags(p, (void *) payload, MIZU_ENTRY_RUNNER, &t, ms);
  POOL_RESUME();
  if (st == MIZU_OK) return (PyObject *) task_wrap(self, &t);
  if (st == MIZU_FULL) {
    PyErr_SetString(MizuSubmitTimeoutError,
                    "pymizu: submission timed out (injection ring full)");
    return NULL;
  }
  return pool_raise(self);
}

static PyMethodDef Pool_methods[] = {
  {"submit", (PyCFunction)(void (*)(void)) Pool_submit,
   METH_VARARGS | METH_KEYWORDS, pool_submit_doc},
  {"submit_batch", (PyCFunction)(void (*)(void)) Pool_submit_batch,
   METH_VARARGS | METH_KEYWORDS, pool_submit_batch_doc},
  {"collect_any", (PyCFunction)(void (*)(void)) Pool_collect_any,
   METH_VARARGS | METH_KEYWORDS, pool_collect_any_doc},
  {"collect_all", (PyCFunction)(void (*)(void)) Pool_collect_all,
   METH_VARARGS | METH_KEYWORDS, pool_collect_all_doc},
  {"ready_wait", (PyCFunction)(void (*)(void)) Pool_ready_wait,
   METH_VARARGS | METH_KEYWORDS, pool_ready_wait_doc},
  {"retire", (PyCFunction) Pool_retire, METH_O, pool_retire_doc},
  {"stop", (PyCFunction)(void (*)(void)) Pool_stop,
   METH_VARARGS | METH_KEYWORDS, pool_stop_doc},
  {"destroy", (PyCFunction) Pool_destroy, METH_NOARGS, pool_destroy_doc},
  {"run", (PyCFunction) Pool_run, METH_NOARGS, pool_run_doc},
  {"leave", (PyCFunction) Pool_leave, METH_NOARGS, pool_leave_doc},
  {"lame_duck", (PyCFunction) Pool_lame_duck, METH_NOARGS, pool_lame_duck_doc},
  {"status", (PyCFunction) Pool_status, METH_NOARGS, pool_status_doc},
  {"dump", (PyCFunction) Pool_dump, METH_NOARGS, pool_dump_doc},
  {"stats", (PyCFunction) Pool_stats, METH_NOARGS, pool_stats_doc},
  {"set_trace", (PyCFunction) Pool_set_trace, METH_O, pool_set_trace_doc},
  {"_map_caps", (PyCFunction) Pool_map_caps, METH_NOARGS, pool_map_caps_doc},
  {"_signals", (PyCFunction) Pool_signals, METH_NOARGS, pool_signals_doc},
  {"_help_once", (PyCFunction) Pool_help_once, METH_NOARGS,
   pool_help_once_doc},
  {"_submit_runner", (PyCFunction)(void (*)(void)) Pool_submit_runner,
   METH_VARARGS | METH_KEYWORDS, pool_submit_runner_doc},
  {NULL, NULL, 0, NULL}
};

static PyGetSetDef Pool_getset[] = {
  {"token", (getter) Pool_token_get, NULL,
   "The join token (\"<pid hex>_<counter hex>\") for worker/submitter attach.",
   NULL},
  {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject MizuPoolType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._Pool",
  .tp_basicsize = sizeof(MizuPool),
  .tp_repr = (reprfunc) Pool_repr,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A pool handle (process-private; does not survive fork()). "
            "Construct through pymizu.Pool.create() / Pool.attach().",
  .tp_methods = Pool_methods,
  .tp_getset = Pool_getset,
  .tp_dealloc = (destructor) Pool_dealloc,
};

// Task verbs ---------------------------------------------------------------------

PyDoc_STRVAR(task_collect_doc,
"collect(timeout=None) -> value | sentinel\n\n\
Wait up to `timeout` seconds (None indefinitely, 0 polls) for the task's\n\
terminal state and return its result. A task error re-raises as\n\
pymizu.TaskError (carrying remote_type / remote_traceback), a cancellation\n\
as pymizu.CancelledError, a dead worker as pymizu.WorkerDiedError.\n\
pymizu.TIMEOUT on expiry; a task is collected exactly once.");

static PyObject *Task_collect(MizuTask *self, PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"timeout", NULL};
  PyObject *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:collect", kwlist, &tmo))
    return NULL;
  MizuPool *pool;
  mizu_task t = task_get(self, &pool);
  if (pool == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  void *v = NULL;
  mizu_status st;
  POOL_ALLOW_THREADS(pool);
  st = mizu_pool_collect(pool->core, &t, &v, ms);
  POOL_RESUME();
  if (st == MIZU_TIMEOUT) {
    Py_INCREF(SentTimeout);
    return SentTimeout;
  }
  if (st == MIZU_ERR) return pool_raise(pool);
  return caught_or_value((PyObject *) v, 0, 0);
}

PyDoc_STRVAR(task_cancel_doc,
"cancel() -> bool\n\n\
Advisory and discard-only, never preemptive: a still-queued task is\n\
skipped; an executing one runs to completion and its result is dropped.\n\
True when this call cancelled the task; every other edge folds to False.");

static PyObject *Task_cancel(MizuTask *self, PyObject *Py_UNUSED(args)) {
  MizuPool *pool = (MizuPool *) self->pool;
  int cancelled = 0;
  if (self->word != 0 && pool->core != NULL &&
      pool->self_pid == mizu_self_pid()) {
    mizu_task t = { self->word };
    cancelled = mizu_pool_cancel(pool->core, &t);
  }
  return PyBool_FromLong(cancelled);
}

static PyObject *Task_state_get(MizuTask *self, void *Py_UNUSED(closure)) {
  MizuPool *pool = (MizuPool *) self->pool;
  const char *state = "dropped";
  if (self->word != 0 && pool->core != NULL &&
      pool->self_pid == mizu_self_pid()) {
    mizu_task t = { self->word };
    switch (mizu_pool_task_state(pool->core, &t)) {
    case MIZU_RS_PENDING: state = "pending"; break;
    case MIZU_RS_OK:      state = "ok"; break;
    case MIZU_RS_ERR:     state = "err"; break;
    case MIZU_RS_CANCEL:  state = "cancel"; break;
    case MIZU_RS_DIED:    state = "died"; break;
    default:             state = "collected"; break;   /* FREE / stale */
    }
  }
  return PyUnicode_FromString(state);
}

static PyObject *Task_pool_get(MizuTask *self, void *Py_UNUSED(closure)) {
  Py_INCREF(self->pool);
  return self->pool;
}

static PyObject *Task_repr(MizuTask *self) {
  return PyUnicode_FromFormat("<pymizu.Task seq=%llu slot=%u>",
                              (unsigned long long) (self->word & ((1ULL << 40) - 1)),
                              (unsigned int) (self->word >> 40));
}

static void Task_dealloc(MizuTask *self) {
  MizuPool *pool = (MizuPool *) self->pool;
  /* the finalizer release for a handle that was never collected: cancels a
     pending task, frees a terminal one — advisory and total, so safe for a
     stale handle, a released pool, or a forked child (guarded) alike */
  if (self->word != 0 && pool->core != NULL &&
      pool->self_pid == mizu_self_pid()) {
    mizu_task t = { self->word };
    mizu_pool_task_release(pool->core, &t);
  }
  Py_DECREF(self->pool);
  MizuTaskType.tp_free((PyObject *) self);
}

static PyMethodDef Task_methods[] = {
  {"collect", (PyCFunction)(void (*)(void)) Task_collect,
   METH_VARARGS | METH_KEYWORDS, task_collect_doc},
  {"cancel", (PyCFunction) Task_cancel, METH_NOARGS, task_cancel_doc},
  {NULL, NULL, 0, NULL}
};

static PyGetSetDef Task_getset[] = {
  {"state", (getter) Task_state_get, NULL,
   "The task's state (\"pending\" / \"ok\" / \"err\" / \"cancel\" / \"died\" "
   "/ \"collected\" / \"dropped\"); informational, racy against slot reuse.",
   NULL},
  {"pool", (getter) Task_pool_get, NULL,
   "The pool handle this task was submitted on.", NULL},
  {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject MizuTaskType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._Task",
  .tp_basicsize = sizeof(MizuTask),
  .tp_repr = (reprfunc) Task_repr,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A task handle from _Pool.submit(). Collected exactly once; "
            "an uncollected handle's finalizer releases its slot.",
  .tp_methods = Task_methods,
  .tp_getset = Task_getset,
  .tp_dealloc = (destructor) Task_dealloc,
};

// Create / attach (module functions; the policy layer lives in pymizu.Channel) ------

static int mizu_pow2(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

PyDoc_STRVAR(channel_new_doc,
"_channel_new(capacity, slot_size, arena_size, spin, drop) -> _Channel\n\n\
Host side: write the preamble and return the handle. The caller spawns\n\
the peer (with the token) and completes the startup rendezvous.");

static PyObject *pymizu_channel_new(PyObject *Py_UNUSED(module),
                                   PyObject *args, PyObject *kw) {
  static char *kwlist[] =
    {"capacity", "slot_size", "arena_size", "spin", "drop", NULL};
  unsigned int cap, slot;
  unsigned long long arena;
  int spin;
  Py_buffer drop;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "IIKpy*:_channel_new", kwlist,
                                   &cap, &slot, &arena, &spin, &drop))
    return NULL;
  if (!mizu_pow2(cap) || cap < 2 || cap > (1u << 24)) {
    PyBuffer_Release(&drop);
    PyErr_SetString(PyExc_ValueError,
                    "pymizu: capacity must be a power of two between 2 and 2^24");
    return NULL;
  }
  if (!mizu_pow2(slot) || slot < 64 || slot > (1u << 20)) {
    PyBuffer_Release(&drop);
    PyErr_SetString(PyExc_ValueError,
              "pymizu: slot_size must be a power of two between 64 and 2^20");
    return NULL;
  }
  if (arena % 64 != 0) {
    PyBuffer_Release(&drop);
    PyErr_SetString(PyExc_ValueError,
                    "pymizu: arena_size must be a non-negative multiple of 64");
    return NULL;
  }
  mizu_channel_opts opts;
  mizu_channel_opts_init(&opts);
  opts.capacity = (uint32_t) cap;
  opts.slot_size = (uint32_t) slot;
  opts.arena_size = (uint64_t) arena;
  opts.flags = spin ? MIZU_FLAG_SPIN : 0;
  opts.drop = drop.len > 0 ? (const uint8_t *) drop.buf : NULL;
  opts.drop_size = (uint64_t) drop.len;
  MizuViewCache *vc = &((MizuHandleCtx *) PyMem_Calloc(1, sizeof(MizuHandleCtx)))->vc;
  if (vc == NULL) {
    PyBuffer_Release(&drop);
    return PyErr_NoMemory();
  }
  mizu_binding b;
  chan_binding(&b);
  b.ctx = vc;
  mizu_channel *c;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_create(&c, &opts, &b);
  Py_END_ALLOW_THREADS
  PyBuffer_Release(&drop);
  if (st != MIZU_OK) {
    view_cache_free(vc);
    raise_tls();
    return NULL;
  }
  MizuChannel *self = chan_wrap(c, vc);
  if (self == NULL) {
    mizu_channel_destroy(c);
    view_cache_free(vc);
  }
  return (PyObject *) self;
}

PyDoc_STRVAR(channel_attach_doc,
"_channel_attach(token, *, _ident=None) -> (_Channel, bytes)\n\n\
Peer side: attach to the channel named by the join token and return the\n\
handle with the drop (the peer bootstrap bytes). The caller consumes the\n\
drop, then signals ready_set. _ident is the test-only identity-word\n\
override: a (lang, caps) pair standing in for this build's word.");

static PyObject *pymizu_channel_attach(PyObject *Py_UNUSED(module),
                                      PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"token", "_ident", NULL};
  const char *token;
  PyObject *ident = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "s|O:_channel_attach", kwlist,
                                   &token, &ident))
    return NULL;
  const char *us = strchr(token, '_');
  int ok = us != NULL && us != token && us[1] != '\0' &&
           strchr(us + 1, '_') == NULL;
  for (const char *p = token; ok && *p != '\0'; p++)
    ok = (*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f') || *p == '_';
  if (!ok) {
    PyErr_SetString(PyExc_ValueError, "pymizu: malformed join token");
    return NULL;
  }
  long lang = -1, caps = 0;
  if (ident != Py_None) {
    if (!PyTuple_Check(ident) || PyTuple_GET_SIZE(ident) != 2)
      goto bad_ident;
    lang = PyLong_AsLong(PyTuple_GET_ITEM(ident, 0));
    caps = PyLong_AsLong(PyTuple_GET_ITEM(ident, 1));
    if ((lang == -1 || caps == -1) && PyErr_Occurred()) return NULL;
    if (lang < 0 || lang > 255 || caps < 0) goto bad_ident;
  }
  MizuViewCache *vc = &((MizuHandleCtx *) PyMem_Calloc(1, sizeof(MizuHandleCtx)))->vc;
  if (vc == NULL) return PyErr_NoMemory();
  mizu_binding b;
  chan_binding(&b);
  if (lang >= 0)
    b.ident = MIZU_IDENT((uint32_t) lang, (uint32_t) caps);
  b.ctx = vc;
  mizu_channel *c;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_channel_attach(&c, token, &b);
  Py_END_ALLOW_THREADS
  if (st != MIZU_OK) {
    view_cache_free(vc);
    raise_tls();
    return NULL;
  }
  MizuChannel *self = chan_wrap(c, vc);
  if (self == NULL) {
    mizu_channel_destroy(c);
    view_cache_free(vc);
    return NULL;
  }
  {
    /* the host's word is on the region at attach */
    uint64_t word = mizu_channel_peer_ident(c);
    MizuHandleCtx *hc = (MizuHandleCtx *) self->vcache;
    hc->peer_lang = (uint32_t) (word & 0xff);
    hc->peer_caps = (uint32_t) (word >> 32);
  }
  goto attached;
bad_ident:
  PyErr_SetString(PyExc_ValueError,
                  "pymizu: _ident must be a (lang, caps) pair of ints");
  return NULL;
attached:;
  /* borrowed drop bytes, valid until destroy — copy out for the caller */
  const uint8_t *bytes;
  uint64_t n;
  mizu_channel_drop(c, &bytes, &n);
  PyObject *drop = PyBytes_FromStringAndSize((const char *) bytes,
                                             (Py_ssize_t) n);
  if (drop == NULL) {
    Py_DECREF(self);
    return NULL;
  }
  PyObject *out = PyTuple_Pack(2, (PyObject *) self, drop);
  Py_DECREF(self);
  Py_DECREF(drop);
  return out;
}


// Pool create / attach / join -----------------------------------------------------

/* The join-token shape: "<pid hex>_<counter hex>". */
static int token_valid(const char *token) {
  const char *us = strchr(token, '_');
  int ok = us != NULL && us != token && us[1] != '\0' &&
           strchr(us + 1, '_') == NULL;
  for (const char *p = token; ok && *p != '\0'; p++)
    ok = (*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f') || *p == '_';
  return ok;
}

PyDoc_STRVAR(pool_new_doc,
"_pool_new(max_workers, max_submitters, injection_cap, per_worker_cap, "
"result_slots, slot_size) -> _Pool\n\n\
Controller side: create the pool region and return the handle (holding\n\
submitter slot 0). The caller spawns the workers (with the token) and\n\
completes the startup rendezvous.");

static PyObject *pymizu_pool_new(PyObject *Py_UNUSED(module),
                                PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"max_workers", "max_submitters", "injection_cap",
                           "per_worker_cap", "result_slots", "slot_size",
                           NULL};
  unsigned int maxw, maxs, inj, deq, rslots, slot;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "IIIIII:_pool_new", kwlist,
                                   &maxw, &maxs, &inj, &deq, &rslots, &slot))
    return NULL;
  if (maxw < 1 || maxw > MIZU_MAX_WORKERS) {
    PyErr_Format(PyExc_ValueError,
                 "pymizu: max_workers must be between 1 and %d",
                 MIZU_MAX_WORKERS);
    return NULL;
  }
  if (maxs < 1 || maxs > 64) {
    PyErr_SetString(PyExc_ValueError,
                    "pymizu: max_submitters must be between 1 and 64");
    return NULL;
  }
  if (!mizu_pow2(inj) || inj < 2 || inj > (1u << 24)) {
    PyErr_SetString(PyExc_ValueError,
                "pymizu: injection_cap must be a power of two between 2 and 2^24");
    return NULL;
  }
  if (!mizu_pow2(deq) || deq < 2 || deq > (1u << 24)) {
    PyErr_SetString(PyExc_ValueError,
              "pymizu: per_worker_cap must be a power of two between 2 and 2^24");
    return NULL;
  }
  /* floor 128: a result slot's inline budget (slot - 40) must hold a
     region name (up to 27 bytes on Windows) for an SHM_RAW spill */
  if (!mizu_pow2(slot) || slot < 128 || slot > (1u << 20)) {
    PyErr_SetString(PyExc_ValueError,
              "pymizu: slot_size must be a power of two between 128 and 2^20");
    return NULL;
  }
  if (rslots < maxs || rslots > (1u << 24)) {
    PyErr_SetString(PyExc_ValueError,
              "pymizu: result_slots must be between max_submitters and 2^24");
    return NULL;
  }
  rslots = (rslots + maxs - 1) / maxs * maxs;   /* per-submitter partition */
  mizu_pool_opts opts;
  mizu_pool_opts_init(&opts);
  opts.max_workers = (uint32_t) maxw;
  opts.max_submitters = (uint32_t) maxs;
  opts.injection_cap = (uint32_t) inj;
  opts.per_worker_cap = (uint32_t) deq;
  opts.result_slots = (uint32_t) rslots;
  opts.slot_size = (uint32_t) slot;
  MizuViewCache *vc = &((MizuHandleCtx *) PyMem_Calloc(1, sizeof(MizuHandleCtx)))->vc;
  if (vc == NULL) return PyErr_NoMemory();
  mizu_binding b;
  pool_binding(&b, 0);
  b.ctx = vc;
  mizu_pool *p;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_pool_create(&p, &opts, &b);
  Py_END_ALLOW_THREADS
  if (st != MIZU_OK) {
    view_cache_free(vc);
    raise_tls();
    return NULL;
  }
  MizuPool *self = pool_wrap(p, MIZU_ROLE_CONTROLLER, vc);
  if (self == NULL) {
    mizu_pool_destroy(p);
    view_cache_free(vc);
  }
  return (PyObject *) self;
}

PyDoc_STRVAR(pool_attach_doc,
"_pool_attach(token) -> _Pool\n\n\
Submitter side: join a live pool from another process, claiming a free\n\
submitter slot with its own injection ring and result-slot subrange.");

static PyObject *pymizu_pool_attach(PyObject *Py_UNUSED(module),
                                   PyObject *arg) {
  const char *token = PyUnicode_AsUTF8(arg);
  if (token == NULL) return NULL;
  if (!token_valid(token)) {
    PyErr_SetString(PyExc_ValueError, "pymizu: malformed join token");
    return NULL;
  }
  MizuViewCache *vc = &((MizuHandleCtx *) PyMem_Calloc(1, sizeof(MizuHandleCtx)))->vc;
  if (vc == NULL) return PyErr_NoMemory();
  mizu_binding b;
  pool_binding(&b, 0);
  b.ctx = vc;
  mizu_pool *p;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_pool_attach(&p, token, &b);
  Py_END_ALLOW_THREADS
  if (st != MIZU_OK) {
    view_cache_free(vc);
    raise_tls();
    return NULL;
  }
  MizuPool *self = pool_wrap(p, MIZU_ROLE_SUBMITTER, vc);
  if (self == NULL) {
    mizu_pool_destroy(p);
    view_cache_free(vc);
  }
  return (PyObject *) self;
}

PyDoc_STRVAR(pool_worker_join_doc,
"_pool_worker_join(token, slot) -> _Pool\n\n\
Worker side: attach to the pool named by the join token as worker `slot`\n\
(the liveness lock before the status CAS), registering the exec callback.\n\
The entry point is python -m pymizu.worker.");

static PyObject *pymizu_pool_worker_join(PyObject *Py_UNUSED(module),
                                        PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"token", "slot", "_ident", NULL};
  const char *token;
  unsigned int slot;
  PyObject *ident = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "sI|O:_pool_worker_join",
                                   kwlist, &token, &slot, &ident))
    return NULL;
  if (!token_valid(token)) {
    PyErr_SetString(PyExc_ValueError, "pymizu: malformed join token");
    return NULL;
  }
  long lang = -1, caps = 0;
  if (ident != Py_None) {
    if (!PyTuple_Check(ident) || PyTuple_GET_SIZE(ident) != 2) {
      PyErr_SetString(PyExc_ValueError,
                      "pymizu: _ident must be a (lang, caps) pair of ints");
      return NULL;
    }
    lang = PyLong_AsLong(PyTuple_GET_ITEM(ident, 0));
    caps = PyLong_AsLong(PyTuple_GET_ITEM(ident, 1));
    if ((lang == -1 || caps == -1) && PyErr_Occurred()) return NULL;
    if (lang < 0 || lang > 255 || caps < 0) {
      PyErr_SetString(PyExc_ValueError,
                      "pymizu: _ident must be a (lang, caps) pair of ints");
      return NULL;
    }
  }
  MizuViewCache *vc = &((MizuHandleCtx *) PyMem_Calloc(1, sizeof(MizuHandleCtx)))->vc;
  if (vc == NULL) return PyErr_NoMemory();
  mizu_binding b;
  pool_binding(&b, 1);
  if (lang >= 0)
    b.ident = MIZU_IDENT((uint32_t) lang, (uint32_t) caps);
  b.ctx = vc;
  mizu_pool *p;
  mizu_status st;
  Py_BEGIN_ALLOW_THREADS
  st = mizu_pool_worker_join(&p, token, (uint32_t) slot, &b);
  Py_END_ALLOW_THREADS
  if (st != MIZU_OK) {
    view_cache_free(vc);
    raise_tls();
    return NULL;
  }
  MizuPool *self = pool_wrap(p, MIZU_ROLE_WORKER, vc);
  if (self == NULL) {
    mizu_pool_destroy(p);
    view_cache_free(vc);
  }
  return (PyObject *) self;
}

PyDoc_STRVAR(task_frame_doc,
"_task_frame(fn, args, kwargs) -> tuple\n\n\
The Pool.submit payload marker: a (fn, args, kwargs) tuple tagged for\n\
the structured frame codec. Facade use only.");

static PyObject *pymizu_task_frame(PyObject *Py_UNUSED(module),
                                  PyObject *const *args,
                                  Py_ssize_t nargs) {
  if (nargs != 3 || !PyTuple_Check(args[1]) || !PyDict_Check(args[2])) {
    PyErr_SetString(PyExc_TypeError,
                    "pymizu: _task_frame expects (fn, args tuple, kwargs dict)");
    return NULL;
  }
  return task_frame_new(args[0], args[1], args[2]);
}

PyDoc_STRVAR(read_stream_doc,
"_read_stream(stream) -> object\n\n\
Decode a serialized-stream frame (the read side's parser, exposed for\n\
the test suite: corrupt-stream and wire-dispatch coverage).");

static PyObject *pymizu_read_stream(PyObject *Py_UNUSED(module),
                                   PyObject *arg) {
  if (!PyBytes_Check(arg)) {
    PyErr_SetString(PyExc_TypeError, "pymizu: expected a bytes stream");
    return NULL;
  }
  mizu_read_ctx ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.size = (uint32_t) sizeof(ctx);
  ctx.outcome = MIZU_RS_OK;
  ctx.died_slot = -1;
  PyObject *r = read_stream((const uint8_t *) PyBytes_AS_STRING(arg),
                            (size_t) PyBytes_GET_SIZE(arg), &ctx);
  if (r == NULL && !PyErr_Occurred()) {
    if (ctx.gone)
      PyErr_SetString(MizuError, "pymizu: referenced region is gone");
    else
      PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
  }
  return r;
}

PyDoc_STRVAR(write_stream_doc,
"_write_stream(obj) -> bytes\n\n\
Encode a value as an 'I' interchange stream (the write side's walk,\n\
exposed for the test suite: the golden corpus drives it). Raises\n\
DeclinedError for a value outside the portable subset.");

static PyObject *pymizu_write_stream(PyObject *Py_UNUSED(module),
                                     PyObject *arg) {
  return pymizu_ix_write_stream(arg);
}

PyDoc_STRVAR(write_err_doc,
"_write_err(type, message, detail, index=None, budget=240) -> bytes\n\n\
Frame an err stream (tag 0x11) from its fields, truncated to the inline\n\
budget (240 is the default slot's) so it fits by construction. Exposed\n\
for the test suite: the golden corpus drives it.");

static PyObject *pymizu_write_err(PyObject *Py_UNUSED(module),
                                  PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"type", "message", "detail", "index", "budget",
                           NULL};
  const char *type, *message, *detail;
  Py_ssize_t tn, mn, dn;
  PyObject *index = Py_None;
  unsigned int budget = 240;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "s#s#s#|OI:_write_err", kwlist,
                                   &type, &tn, &message, &mn, &detail, &dn,
                                   &index, &budget))
    return NULL;
  int has_index = index != Py_None;
  unsigned long long idx = 0;
  if (has_index) {
    idx = PyLong_AsUnsignedLongLong(index);
    if (idx == (unsigned long long) -1 && PyErr_Occurred()) return NULL;
  }
  uint8_t stackbuf[512];
  uint8_t *dst = stackbuf;
  if (budget > sizeof stackbuf) {
    dst = PyMem_Malloc(budget);
    if (dst == NULL) return PyErr_NoMemory();
  }
  size_t n = pymizu_ix_write_err(dst, (uint32_t) budget,
                                 type, (size_t) tn, message, (size_t) mn,
                                 detail, (size_t) dn, has_index, idx);
  PyObject *out = PyBytes_FromStringAndSize((const char *) dst,
                                            (Py_ssize_t) n);
  if (dst != stackbuf) PyMem_Free(dst);
  return out;
}

PyDoc_STRVAR(is_sentinel_doc,
"is_sentinel(x) -> bool\n\n\
Provenance, not shape: True only for the exact sentinel singletons this\n\
process's verbs return, so a look-alike payload never passes.");

static PyObject *pymizu_is_sentinel(PyObject *Py_UNUSED(module), PyObject *x) {
  return PyBool_FromLong(x == SentFull || x == SentTimeout ||
                         x == SentClosed || x == SentGone);
}

PyDoc_STRVAR(abi_version_doc,
"abi_version() -> int\n\n\
Wire-format ABI version of the compiled-in libmizu core.");

static PyObject *pymizu_abi_version(PyObject *Py_UNUSED(module),
                                   PyObject *Py_UNUSED(args)) {
  return PyLong_FromUnsignedLong((unsigned long) MIZU_ABI_VERSION);
}

PyDoc_STRVAR(prune_doc,
"prune() -> list[str]\n\n\
Reap the /mizu_ shared-memory regions dead processes leave behind.\n\
Returns the region names removed; empty when none — always empty on\n\
Windows, where a mapping cannot outlive its creator.");

static PyObject *pymizu_prune(PyObject *Py_UNUSED(module),
                             PyObject *Py_UNUSED(args)) {
  int n = 0;
  char **list = mizu_shm_reap(&n);
  PyObject *out = PyList_New(n);
  if (out == NULL) {
    for (int i = 0; i < n; i++) free(list[i]);
    free(list);
    return NULL;
  }
  for (int i = 0; i < n; i++) {
    PyObject *s = PyUnicode_FromString(list[i]);
    free(list[i]);
    if (s == NULL) {
      for (i++; i < n; i++) free(list[i]);
      free(list);
      Py_DECREF(out);
      return NULL;
    }
    PyList_SET_ITEM(out, i, s);
  }
  free(list);
  return out;
}

PyDoc_STRVAR(tune_malloc_doc,
"_tune_malloc() -> None\n\n\
Apply the core's glibc malloc tuning (larger mmap/trim thresholds) to\n\
this process. Called by the spawned child/worker entry points; a\n\
process that set its own GLIBC_TUNABLES is left untouched. No-op off\n\
glibc.");

static PyObject *pymizu_tune_malloc(PyObject *Py_UNUSED(module),
                                   PyObject *Py_UNUSED(args)) {
  mizu_tune();
  Py_RETURN_NONE;
}

static PyMethodDef pymizu_methods[] = {
  {"_channel_new", (PyCFunction)(void (*)(void)) pymizu_channel_new,
   METH_VARARGS | METH_KEYWORDS, channel_new_doc},
  {"_channel_attach", (PyCFunction)(void (*)(void)) pymizu_channel_attach,
   METH_VARARGS | METH_KEYWORDS, channel_attach_doc},
  {"_pool_new", (PyCFunction)(void (*)(void)) pymizu_pool_new,
   METH_VARARGS | METH_KEYWORDS, pool_new_doc},
  {"_pool_attach", (PyCFunction) pymizu_pool_attach, METH_O, pool_attach_doc},
  {"_pool_worker_join", (PyCFunction)(void (*)(void))
   pymizu_pool_worker_join, METH_VARARGS | METH_KEYWORDS,
   pool_worker_join_doc},
  {"_task_frame", (PyCFunction)(void (*)(void)) pymizu_task_frame,
   METH_FASTCALL, task_frame_doc},
  {"_read_stream", pymizu_read_stream, METH_O, read_stream_doc},
  {"_write_stream", pymizu_write_stream, METH_O, write_stream_doc},
  {"_write_err", (PyCFunction)(void (*)(void)) pymizu_write_err,
   METH_VARARGS | METH_KEYWORDS, write_err_doc},
  {"is_sentinel", pymizu_is_sentinel, METH_O, is_sentinel_doc},
  {"abi_version", (PyCFunction) pymizu_abi_version, METH_NOARGS, abi_version_doc},
  {"prune", (PyCFunction) pymizu_prune, METH_NOARGS, prune_doc},
  {"_tune_malloc", (PyCFunction) pymizu_tune_malloc, METH_NOARGS,
   tune_malloc_doc},
  {NULL, NULL, 0, NULL}
};

PyDoc_STRVAR(pymizu_module_doc,
"Lock-free shared-memory IPC: SPSC channels and work-stealing task pools\n\
(the libmizu core, compiled in).");

static struct PyModuleDef pymizu_module = {
  PyModuleDef_HEAD_INIT, "_pymizu", pymizu_module_doc, -1, pymizu_methods,
  NULL, NULL, NULL, NULL
};

static int add_exception(PyObject *m, PyObject **slot, const char *name,
                         PyObject *base, const char *doc) {
  PyObject *dict = PyDict_New();
  PyObject *docstr = PyUnicode_FromString(doc);
  if (dict == NULL || docstr == NULL) {
    Py_XDECREF(dict);
    Py_XDECREF(docstr);
    return -1;
  }
  int rc = PyDict_SetItemString(dict, "__doc__", docstr);
  Py_DECREF(docstr);
  if (rc == 0)
    *slot = PyErr_NewException(name, base, dict);
  Py_DECREF(dict);
  if (rc != 0 || *slot == NULL) return -1;
  const char *dot = strchr(name, '.');
  return PyModule_AddObject(m, dot != NULL ? dot + 1 : name, *slot);
}

PyMODINIT_FUNC
PyInit__pymizu(void)
{
  if (PyType_Ready(&MizuSentinelType) < 0) return NULL;
  if (PyType_Ready(&MizuCaughtType) < 0) return NULL;
  if (PyType_Ready(&MizuChannelType) < 0) return NULL;
  if (PyType_Ready(&MizuPoolType) < 0) return NULL;
  if (PyType_Ready(&MizuTaskType) < 0) return NULL;
  if (PyType_Ready(&MizuShmViewType) < 0) return NULL;
  if (PyType_Ready(&MizuShmStrViewType) < 0) return NULL;
  if (PyType_Ready(&MizuShmOwnerType) < 0) return NULL;
  MizuTaskFrameType.tp_base = &PyTuple_Type;
  if (PyType_Ready(&MizuTaskFrameType) < 0) return NULL;

  /* cloudpickle when installed, stock pickle otherwise; both read each
     other's protocol-4 streams */
  PyObject *pickler = PyImport_ImportModule("cloudpickle");
  if (pickler == NULL) {
    PyErr_Clear();
    pickler = PyImport_ImportModule("pickle");
    if (pickler == NULL) return NULL;
  }
  mizu_dumps = PyObject_GetAttrString(pickler, "dumps");
  mizu_loads = PyObject_GetAttrString(pickler, "loads");
  Py_DECREF(pickler);
  if (mizu_dumps == NULL || mizu_loads == NULL) return NULL;

  PyObject *m = PyModule_Create(&pymizu_module);
  if (m == NULL) return NULL;

  SentFull = sentinel_new("FULL");
  SentTimeout = sentinel_new("TIMEOUT");
  SentClosed = sentinel_new("CLOSED");
  SentGone = sentinel_new("PEER_GONE");
  if (SentFull == NULL || SentTimeout == NULL || SentClosed == NULL ||
      SentGone == NULL ||
      PyModule_AddObject(m, "FULL", SentFull) < 0 ||
      PyModule_AddObject(m, "TIMEOUT", SentTimeout) < 0 ||
      PyModule_AddObject(m, "CLOSED", SentClosed) < 0 ||
      PyModule_AddObject(m, "PEER_GONE", SentGone) < 0) {
    Py_DECREF(m);
    return NULL;
  }

  if (add_exception(m, &MizuError, "pymizu.MizuError", PyExc_Exception,
                    "Base class for all pymizu errors.") < 0 ||
      add_exception(m, &MizuStartupError, "pymizu.StartupError", MizuError,
                    "A channel peer or pool worker failed to attach within "
                    "the startup timeout.") < 0 ||
      add_exception(m, &MizuShmError, "pymizu.ShmError", MizuError,
                    "A shared-memory region operation failed.") < 0 ||
      add_exception(m, &MizuSubmitTimeoutError, "pymizu.SubmitTimeoutError",
                    MizuError,
                    "Pool.submit() timed out waiting for injection-ring "
                    "space.") < 0 ||
      add_exception(m, &MizuSlotsExhaustedError, "pymizu.SlotsExhaustedError",
                    MizuError,
                    "Pool.submit() found no free result slot: too many "
                    "outstanding (uncollected) tasks.") < 0 ||
      add_exception(m, &MizuStoppedError, "pymizu.StoppedError", MizuError,
                    "The pool is stopped; no further submission is "
                    "possible.") < 0 ||
      add_exception(m, &MizuCancelledError, "pymizu.CancelledError",
                    MizuError,
                    "The task was cancelled before it ran.") < 0 ||
      add_exception(m, &MizuWorkerDiedError, "pymizu.WorkerDiedError",
                    MizuError,
                    "The executing worker died mid-task. Carries 'slot' "
                    "and 'pid' attributes identifying the worker.") < 0 ||
      add_exception(m, &MizuTaskError, "pymizu.TaskError", MizuError,
                    "The task callable raised. Carries 'remote_type' and "
                    "'remote_traceback' attributes describing the "
                    "worker-side exception.") < 0 ||
      add_exception(m, &MizuDeclinedError, "pymizu.DeclinedError",
                    PyExc_TypeError,
                    "A send on a foreign-language channel of a value "
                    "outside the portable interchange subset. Carries "
                    "'path' and 'reason' attributes.") < 0) {
    Py_DECREF(m);
    return NULL;
  }

  if (PyModule_AddObject(m, "_Channel", (PyObject *) &MizuChannelType) < 0 ||
      PyModule_AddObject(m, "_Pool", (PyObject *) &MizuPoolType) < 0 ||
      PyModule_AddObject(m, "_Task", (PyObject *) &MizuTaskType) < 0 ||
      PyModule_AddStringConstant(m, "__core_version__",
                                 MIZU_VERSION_STRING) < 0 ||
      mizu_py_map_register(m, MizuError, MizuShmError) < 0 ||
      mizu_py_interop_register(m, MizuError, MizuDeclinedError) < 0) {
    Py_DECREF(m);
    return NULL;
  }
  return m;
}
