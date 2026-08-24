/* pyrei — Python binding for librei (raw CPython C API).
 *
 * The extension compiles the vendored core sources directly, so the module
 * is self-contained: no system librei is required or consulted.
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

#include <math.h>
#include <string.h>

#include "rei.h"
#include "internal.h"
#include "pymap.h"

#define REI_STR_(x) #x
#define REI_STR(x) REI_STR_(x)
#define REI_VERSION_STRING                                                \
  REI_STR(REI_VERSION_MAJOR) "." REI_STR(REI_VERSION_MINOR) "."           \
  REI_STR(REI_VERSION_PATCH)

/* R's cetype_t marks (the STR1 aux values), fixed by the wire format. */
enum { REI_CE_NATIVE = 0, REI_CE_UTF8 = 1, REI_CE_LATIN1 = 2, REI_CE_BYTES = 3 };

static PyTypeObject ReiChannelType;
static PyTypeObject ReiPoolType;
static PyTypeObject ReiTaskType;
static PyTypeObject ReiCaughtType;

// Module state -------------------------------------------------------------------

static PyObject *ReiError;           /* base */
static PyObject *ReiStartupError;    /* child failed to attach in time */
static PyObject *ReiShmError;        /* region create/open failure */
static PyObject *ReiSubmitTimeoutError;   /* ring full past the deadline */
static PyObject *ReiSlotsExhaustedError;
static PyObject *ReiStoppedError;
static PyObject *ReiCancelledError;
static PyObject *ReiWorkerDiedError;
static PyObject *ReiTaskError;       /* a task's own error, re-raised */

/* traceback.format_exception, resolved lazily on the first task error (the
   error path is cold; importing it at module init is not). */
static PyObject *rei_traceback_fmt;

/* pickle, or cloudpickle when installed (used transparently — its output is
   a standard protocol stream). Resolved at module init; protocol pinned to 4. */
static PyObject *rei_dumps, *rei_loads;

/* numpy, probed lazily on the first raw-vector read: the module, or Py_None
   when absent. Never a build-time dependency. */
static PyObject *rei_numpy;
static int rei_numpy_probed;

// Sentinels ----------------------------------------------------------------------

/* The four terminal-state singletons. Identity is the contract
   (`x is pyrei.TIMEOUT`); they are falsy so a bare `if ch.recv():` loop
   exits on any terminal state. */
typedef struct {
  PyObject_HEAD
  const char *name;
} ReiSentinel;

static PyObject *SentFull, *SentTimeout, *SentClosed, *SentGone;

static PyObject *sent_repr(PyObject *self) {
  return PyUnicode_FromFormat("pyrei.%s", ((ReiSentinel *) self)->name);
}

static int sent_bool(PyObject *Py_UNUSED(self)) {
  return 0;
}

static PyNumberMethods sent_as_number = {
  .nb_bool = sent_bool,
};

static PyTypeObject ReiSentinelType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "pyrei.Sentinel",
  .tp_basicsize = sizeof(ReiSentinel),
  .tp_repr = sent_repr,
  .tp_as_number = &sent_as_number,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "Terminal-state sentinel returned by pyrei verbs. "
            "Identity-tested against pyrei.FULL / TIMEOUT / CLOSED / PEER_GONE.",
};

static PyObject *sentinel_new(const char *name) {
  ReiSentinel *s = (ReiSentinel *) ReiSentinelType.tp_alloc(&ReiSentinelType, 0);
  if (s == NULL) return NULL;
  s->name = name;
  return (PyObject *) s;
}

// Outcome boxes (the rei_caught mirror) --------------------------------------

/* A collect's non-OK outcome comes back boxed: the exception instance rides
   inside, so a task value that is itself an exception object stays bare.
   The collect veneer unwraps the box and raises. */
typedef struct {
  PyObject_HEAD
  PyObject *exc;
} ReiCaught;

static void Caught_dealloc(ReiCaught *self) {
  Py_DECREF(self->exc);
  ReiCaughtType.tp_free((PyObject *) self);
}

static PyTypeObject ReiCaughtType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pyrei._Caught",
  .tp_basicsize = sizeof(ReiCaught),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A collected task outcome, boxed for the veneer to raise.",
  .tp_dealloc = (destructor) Caught_dealloc,
};

static PyObject *caught_new(PyObject *exc) {   /* steals exc */
  if (exc == NULL) return NULL;
  ReiCaught *c = (ReiCaught *) ReiCaughtType.tp_alloc(&ReiCaughtType, 0);
  if (c == NULL) {
    Py_DECREF(exc);
    return NULL;
  }
  c->exc = exc;
  return (PyObject *) c;
}

// Errors -------------------------------------------------------------------------

/* create/attach failure: the core composed the message (size + hint) in the
   thread-local slot. Space/existence failures carry the shm class. */
static int raise_tls(void) {
  rei_errcat cat = rei_last_error_category();
  const char *msg = rei_last_error_message();
  PyObject *exc = (cat == REI_ERRCAT_NOSPACE || cat == REI_ERRCAT_NOMEMORY ||
                   cat == REI_ERRCAT_EXISTS) ? ReiShmError : ReiError;
  PyErr_Format(exc, "pyrei: %s", msg);
  return -1;
}

// Staging ------------------------------------------------------------------------

/* Frame n bytes over the INLINE / ARENA / SHM_RAW tiers — the reference
   stager's (bytes.c) discipline: one arena chunk past the inline budget,
   else a reap and a spill region retained SPILL (surrendered to the free
   list at consumer-done). */
static int stage_bytes(const uint8_t *src, size_t n, rei_slot_hdr *hdr,
                       uint8_t *payload, uint32_t inline_max, rei_handle *h) {
  if (n == 0) {
    hdr->kind = REI_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return 0;
  }
  if (n <= (size_t) inline_max) {
    memcpy(payload, src, n);
    hdr->kind = REI_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = 0;
    return 0;
  }
  uint64_t off;
  uint8_t *chunk = rei_stage_arena_alloc(h, REI_ALIGN64(n), &off);
  if (chunk != NULL) {
    memcpy(chunk, src, n);
    hdr->kind = REI_KIND_ARENA;
    hdr->len = 0;
    hdr->aux = off;
    uint64_t n64 = (uint64_t) n;
    memcpy(payload, &n64, sizeof(n64));
    return 0;
  }
  rei_stage_reap(h);
  rei_shm *shm;
  if (rei_stage_spill_get(h, n, &shm) != REI_OK) {
    PyErr_Format(ReiShmError, "pyrei: cannot create payload region "
                 "(%zu bytes): %s", n, rei_last_error_message());
    return 1;
  }
  memcpy(shm->addr, src, n);
  hdr->kind = REI_KIND_SHM_RAW;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = (uint64_t) n;   /* exact length: a recycled region carries slack */
  memcpy(payload, shm->name, shm->name_len);
  rei_stage_retain(h, shm);
  return 0;
}

/* The O(1) raw-tier gate (the mirror of R's attribute-free/non-ALTREP gate):
   C-contiguous, native byte order, at most 1-D, and a dtype that maps
   width-exactly onto a wire type. No bool: numpy bool is 1 byte/elt where
   R logical is 4 — the mapping would misread at 4x stride. Everything else
   falls to pickle. */
static int wire_type_of(const Py_buffer *v) {
  if (v->ndim > 1) return 0;
  const char *f = v->format;
  if (f == NULL) return v->itemsize == 1 ? REI_TYPE_RAW : 0;
  if (f[1] == '\0') {
    switch (f[0]) {
    case 'B': return v->itemsize == 1 ? REI_TYPE_RAW : 0;
    case 'd': return v->itemsize == 8 ? REI_TYPE_REAL : 0;
    case 'i': return v->itemsize == 4 ? REI_TYPE_INT : 0;
    }
    return 0;
  }
  if (f[0] == 'Z' && f[1] == 'd' && f[2] == '\0')
    return v->itemsize == 16 ? REI_TYPE_CPLX : 0;
  return 0;
}

/* map.c's raw-x gate rides the same dtype map. */
int rei_py_wire_type_of(const Py_buffer *v) {
  return wire_type_of(v);
}

/* Bare vector bytes: RAWVEC inline within the budget, else one arena chunk
   as RAWSPILL. There is no SHM_VEC producer in v1, so the arena serves at
   any size (R caps it at REI_ZC_FLOOR_RAW only because its next tier is
   zero-copy); a miss falls to pickle. Returns 0 staged, 1 error, -1 pickle. */
static int stage_raw(const Py_buffer *v, int type, rei_slot_hdr *hdr,
                     uint8_t *payload, uint32_t inline_max, rei_handle *h) {
  size_t n = (size_t) v->len;
  if (n <= (size_t) inline_max) {
    memcpy(payload, v->buf, n);
    hdr->kind = REI_KIND_RAWVEC;
    hdr->len = (uint32_t) n;
    hdr->aux = (uint64_t) type;
    return 0;
  }
  uint64_t off;
  uint8_t *chunk = rei_stage_arena_alloc(h, REI_ALIGN64(n), &off);
  if (chunk == NULL && h->htype == REI_HTYPE_POOL && n <= UINT32_MAX) {
    /* a pool has no arena: out-of-line frames are always named regions (the
       mirror of R's pool RAWSPILL framing — aux packs the wire type and the
       region name length). A region failure falls to pickle. */
    rei_shm *shm;
    if (rei_stage_spill_get(h, n, &shm) != REI_OK) return -1;
    memcpy(shm->addr, v->buf, n);
    hdr->kind = REI_KIND_RAWSPILL;
    hdr->len = (uint32_t) n;
    hdr->aux = (uint64_t) type | ((uint64_t) shm->name_len << 8);
    memcpy(payload, shm->name, shm->name_len);
    rei_stage_retain(h, shm);   /* bare bytes carry no identifier: no pin */
    return 0;
  }
  if (chunk == NULL) return -1;
  memcpy(chunk, v->buf, n);
  hdr->kind = REI_KIND_RAWSPILL;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) type;
  memcpy(payload, &off, sizeof(off));
  return 0;
}

// Fast-path codec --------------------------------------------------------------

/* The compact codec: the hot subset (bool, int, float, str, bytes, and one
   flat container level of those, capped) framed as a tiny binary stream —
   the analogue of R's compact codec. Anything outside the subset falls
   back to pickle, the same discipline as R's codec rejecting ALTREP.
   Streams carry a magic first byte so readers dispatch on it; the
   first-byte namespace is shared (pickle protocol 4 is 0x80, R's codec is
   'S', R native streams 'B'/'X'/'A'). Integers are int64, little-endian;
   all supported platforms are little-endian. */
#define PYREI_CODEC_MAGIC 0x50   /* 'P' */
#define PYREI_CODEC_CAP 64       /* container element cap: staging stays bounded */

enum {
  PYREI_TAG_BOOL = 'b', PYREI_TAG_INT = 'i', PYREI_TAG_FLOAT = 'f',
  PYREI_TAG_STR = 's', PYREI_TAG_BYTES = 'y',
  PYREI_TAG_LIST = 'l', PYREI_TAG_TUPLE = 't', PYREI_TAG_DICT = 'd'
};

/* Exact-type checks throughout: a subclass (IntEnum, a str subclass) keeps
   its semantics on the pickle path. */
static int codec_tag_of(PyObject *o) {
  if (PyBool_Check(o)) return PYREI_TAG_BOOL;
  if (PyLong_CheckExact(o)) return PYREI_TAG_INT;
  if (PyFloat_CheckExact(o)) return PYREI_TAG_FLOAT;
  if (PyUnicode_CheckExact(o)) return PYREI_TAG_STR;
  if (PyBytes_CheckExact(o)) return PYREI_TAG_BYTES;
  return 0;
}

/* Size pass, 0 ok / -1 reject (not in the subset, an int past int64, a
   lone-surrogate str, over the cap). Never sets an error. */
static int codec_scalar_size(PyObject *o, uint64_t *sz) {
  switch (codec_tag_of(o)) {
  case PYREI_TAG_BOOL: *sz += 2; return 0;
  case PYREI_TAG_INT: {
    long long v = PyLong_AsLongLong(o);
    if (v == -1 && PyErr_Occurred()) {
      PyErr_Clear();   /* OverflowError: an exotic int rides pickle */
      return -1;
    }
    *sz += 9;
    return 0;
  }
  case PYREI_TAG_FLOAT: *sz += 9; return 0;
  case PYREI_TAG_STR: {
    Py_ssize_t n;
    if (PyUnicode_AsUTF8AndSize(o, &n) == NULL) {
      PyErr_Clear();   /* lone surrogates ride pickle */
      return -1;
    }
    *sz += 5 + (uint64_t) n;
    return 0;
  }
  case PYREI_TAG_BYTES:
    *sz += 5 + (uint64_t) PyBytes_GET_SIZE(o);
    return 0;
  }
  return -1;
}

static int codec_size(PyObject *o, uint64_t *sz) {
  if (PyList_CheckExact(o) || PyTuple_CheckExact(o)) {
    Py_ssize_t n = PyList_CheckExact(o) ? PyList_GET_SIZE(o) :
      PyTuple_GET_SIZE(o);
    if (n > PYREI_CODEC_CAP) return -1;
    *sz += 5;
    for (Py_ssize_t i = 0; i < n; i++) {
      PyObject *it = PyList_CheckExact(o) ? PyList_GET_ITEM(o, i) :
        PyTuple_GET_ITEM(o, i);
      if (codec_scalar_size(it, sz) < 0) return -1;
    }
    return 0;
  }
  if (PyDict_CheckExact(o)) {
    if (PyDict_Size(o) > PYREI_CODEC_CAP) return -1;
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
  switch (codec_tag_of(o)) {
  case PYREI_TAG_BOOL:
    *(*p)++ = PYREI_TAG_BOOL;
    *(*p)++ = (uint8_t) (o == Py_True);
    return;
  case PYREI_TAG_INT: {
    *(*p)++ = PYREI_TAG_INT;
    long long v = PyLong_AsLongLong(o);
    codec_put64(p, (uint64_t) v);
    return;
  }
  case PYREI_TAG_FLOAT: {
    *(*p)++ = PYREI_TAG_FLOAT;
    double d = PyFloat_AS_DOUBLE(o);
    uint64_t u;
    memcpy(&u, &d, 8);
    codec_put64(p, u);
    return;
  }
  case PYREI_TAG_STR: {
    *(*p)++ = PYREI_TAG_STR;
    Py_ssize_t n;
    const char *s = PyUnicode_AsUTF8AndSize(o, &n);
    codec_put32(p, (uint32_t) n);
    memcpy(*p, s, (size_t) n);
    *p += n;
    return;
  }
  default: {   /* PYREI_TAG_BYTES */
    *(*p)++ = PYREI_TAG_BYTES;
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
    *(*p)++ = (uint8_t) (is_list ? PYREI_TAG_LIST : PYREI_TAG_TUPLE);
    codec_put32(p, (uint32_t) n);
    for (Py_ssize_t i = 0; i < n; i++)
      codec_put_scalar(p, is_list ? PyList_GET_ITEM(o, i) :
                       PyTuple_GET_ITEM(o, i));
    return;
  }
  if (PyDict_CheckExact(o)) {
    *(*p)++ = PYREI_TAG_DICT;
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
static int stage_codec(PyObject *obj, rei_slot_hdr *hdr, uint8_t *payload,
                       uint32_t inline_max, rei_handle *h) {
  uint64_t sz = 1;   /* the magic byte */
  if (codec_size(obj, &sz) < 0) return -1;
  if (sz <= (uint64_t) inline_max) {
    payload[0] = PYREI_CODEC_MAGIC;
    uint8_t *p = payload + 1;
    codec_put(&p, obj);
    hdr->kind = REI_KIND_INLINE;
    hdr->len = (uint32_t) sz;
    hdr->aux = 0;
    return 0;
  }
  uint8_t *buf = (uint8_t *) malloc((size_t) sz);
  if (buf == NULL) return -1;   /* pickle's own allocation failure reports */
  buf[0] = PYREI_CODEC_MAGIC;
  uint8_t *p = buf + 1;
  codec_put(&p, obj);
  int rc = stage_bytes(buf, (size_t) sz, hdr, payload, inline_max, h);
  free(buf);
  return rc;
}

static int stage_impl(PyObject *obj, rei_slot_hdr *hdr, uint8_t *payload,
                      uint32_t inline_max, rei_handle *h) {
  if (obj == Py_None) {
    hdr->kind = REI_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return 0;
  }
  if (PyUnicode_CheckExact(obj)) {
    /* STR1, the mirror of R's length-1 string tier: UTF-8 bytes in the
       payload, aux the cetype mark (Python str has no encoding of its own;
       UTF-8 is the canonical crossing). Inline-budget gate, matching rei's
       cap. Exact-type check: a str subclass keeps its pickle semantics. */
    Py_ssize_t n;
    const char *s = PyUnicode_AsUTF8AndSize(obj, &n);
    if (s == NULL)
      PyErr_Clear();   /* lone surrogates fall through to codec/pickle */
    else if (n <= (Py_ssize_t) inline_max) {
      memcpy(payload, s, (size_t) n);
      hdr->kind = REI_KIND_STR1;
      hdr->len = (uint32_t) n;
      hdr->aux = REI_CE_UTF8;
      return 0;
    }
  }
  if (PyObject_CheckBuffer(obj)) {
    Py_buffer v;
    if (PyObject_GetBuffer(obj, &v, PyBUF_C_CONTIGUOUS | PyBUF_FORMAT) == 0) {
      int type = wire_type_of(&v);
      int rc = -1;
      if (type != 0)
        rc = stage_raw(&v, type, hdr, payload, inline_max, h);
      PyBuffer_Release(&v);
      if (rc >= 0) return rc;
    } else {
      PyErr_Clear();
    }
  }
  int crc = stage_codec(obj, hdr, payload, inline_max, h);
  if (crc >= 0) return crc;
  PyObject *stream =
    PyObject_CallFunction(rei_dumps, "Oi", obj, 4);   /* protocol pinned */
  if (stream == NULL) return 1;
  int rc = stage_bytes((const uint8_t *) PyBytes_AS_STRING(stream),
                       (size_t) PyBytes_GET_SIZE(stream),
                       hdr, payload, inline_max, h);
  Py_DECREF(stream);
  return rc;
}

/* The binding's stage_fn, registered on every channel handle. The veneer
   released the GIL around the verb; reacquire. */
static int py_stage(void *obj, rei_slot_hdr *hdr, uint8_t *payload,
                    uint32_t inline_max, rei_handle *h) {
  PyGILState_STATE gil = PyGILState_Ensure();
  int rc = stage_impl((PyObject *) obj, hdr, payload, inline_max, h);
  PyGILState_Release(gil);
  return rc;
}

// Reading ------------------------------------------------------------------------

static PyObject *numpy_empty(void) {
  if (!rei_numpy_probed) {
    rei_numpy_probed = 1;
    PyObject *np = PyImport_ImportModule("numpy");
    if (np == NULL) {
      PyErr_Clear();
      np = Py_None;
      Py_INCREF(Py_None);
    }
    rei_numpy = np;
  }
  if (rei_numpy == Py_None) return NULL;
  return PyObject_GetAttrString(rei_numpy, "empty");   /* new ref */
}

/* RAWVEC/RAWSPILL materialize: one memcpy into a fresh numpy array (or a
   memoryview copy) before consumer-done — ring slots are reused and arena
   chunks FIFO-reclaim, so a view over them dangles. LGL reads as int32
   (width-compatible; there is no bool mapping). */
static PyObject *read_raw(const uint8_t *src, uint32_t len, int type) {
  size_t elt = rei_type_elt_size(type);
  if (elt == 0 || len % elt != 0) {
    PyErr_SetString(ReiError, "pyrei: corrupt payload slot");
    return NULL;
  }
  size_t nelts = len / elt;
  PyObject *empty = numpy_empty();
  if (empty != NULL) {
    const char *dt;
    switch (type) {
    case REI_TYPE_REAL: dt = "float64"; break;
    case REI_TYPE_INT:
    case REI_TYPE_LGL: dt = "int32"; break;
    case REI_TYPE_CPLX: dt = "complex128"; break;
    default: dt = "uint8"; break;
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
  case PYREI_TAG_BOOL: {
    if ((size_t) (end - *p) < 1) return NULL;
    int v = *(*p)++;
    if (v > 1) return NULL;
    return PyBool_FromLong(v);
  }
  case PYREI_TAG_INT: {
    if ((size_t) (end - *p) < 8) return NULL;
    int64_t v = (int64_t) codec_get64(p);
    return PyLong_FromLongLong(v);
  }
  case PYREI_TAG_FLOAT: {
    if ((size_t) (end - *p) < 8) return NULL;
    uint64_t u = codec_get64(p);
    double d;
    memcpy(&d, &u, 8);
    return PyFloat_FromDouble(d);
  }
  case PYREI_TAG_STR: {
    if ((size_t) (end - *p) < 4) return NULL;
    uint32_t n = codec_get32(p);
    if ((size_t) (end - *p) < n) return NULL;
    PyObject *s = PyUnicode_DecodeUTF8((const char *) *p, (Py_ssize_t) n,
                                       NULL);
    if (s == NULL) return NULL;
    *p += n;
    return s;
  }
  case PYREI_TAG_BYTES: {
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

static PyObject *codec_read(const uint8_t *src, size_t n) {
  const uint8_t *p = src + 1, *end = src + n;
  if (p >= end) goto corrupt;
  int tag = *p++;
  PyObject *out = NULL;
  switch (tag) {
  case PYREI_TAG_LIST:
  case PYREI_TAG_TUPLE: {
    if ((size_t) (end - p) < 4) goto corrupt;
    uint32_t count = codec_get32(&p);
    if (count > PYREI_CODEC_CAP) goto corrupt;
    out = tag == PYREI_TAG_LIST ? PyList_New((Py_ssize_t) count) :
      PyTuple_New((Py_ssize_t) count);
    if (out == NULL) return NULL;
    for (uint32_t i = 0; i < count; i++) {
      PyObject *it = codec_read_scalar(&p, end);
      if (it == NULL) {
        if (!PyErr_Occurred()) goto corrupt_obj;
        Py_DECREF(out);
        return NULL;
      }
      if (tag == PYREI_TAG_LIST)
        PyList_SET_ITEM(out, (Py_ssize_t) i, it);
      else
        PyTuple_SET_ITEM(out, (Py_ssize_t) i, it);
    }
    break;
  }
  case PYREI_TAG_DICT: {
    if ((size_t) (end - p) < 4) goto corrupt;
    uint32_t count = codec_get32(&p);
    if (count > PYREI_CODEC_CAP) goto corrupt;
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
  case PYREI_TAG_BOOL:
  case PYREI_TAG_INT:
  case PYREI_TAG_FLOAT:
  case PYREI_TAG_STR:
  case PYREI_TAG_BYTES:
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
  PyErr_SetString(ReiError, "pyrei: corrupt payload slot");
  return NULL;
}

/* A serialized-stream frame (INLINE / ARENA / SHM_RAW bytes). Our streams
   are pickle protocol 4 (first byte 0x80) or the compact codec
   (PYREI_CODEC_MAGIC). 'S' is the R compact codec, 'B' / 'X' / 'A' the R
   serialize formats — no codec interop in v1. */
static PyObject *read_stream(const uint8_t *src, size_t n) {
  if (n == 0) {
    PyErr_SetString(ReiError, "pyrei: corrupt payload slot");
    return NULL;
  }
  switch (src[0]) {
  case 0x80:
    return PyObject_CallFunction(rei_loads, "y#", (const char *) src,
                                 (Py_ssize_t) n);
  case PYREI_CODEC_MAGIC:
    return codec_read(src, n);
  case 'S': case 'B': case 'X': case 'A':
    PyErr_SetString(ReiError, "pyrei: R payload (no codec interop) - "
                    "send Python values from a pyrei peer");
    return NULL;
  default:
    PyErr_SetString(ReiError, "pyrei: unrecognized payload");
    return NULL;
  }
}

static PyObject *read_frame(const rei_slot_hdr *hdr, const uint8_t *payload,
                            size_t limit, rei_read_ctx *ctx) {
  switch (hdr->kind) {
  case REI_KIND_NIL:
    Py_RETURN_NONE;
  case REI_KIND_RAWVEC:
    if (hdr->len > limit) break;
    return read_raw(payload, hdr->len, (int) hdr->aux);
  case REI_KIND_RAWSPILL:
    if (hdr->aux >> 8) {
      /* pool framing: the region name in the payload, its length and the
         wire type packed in aux (the channel's arena framing of this kind
         is resolved by the transport, never reaching here) */
      uint32_t name_len = (uint32_t) (hdr->aux >> 8);
      int type = (int) (hdr->aux & 0xff);
      if (name_len == 0 || name_len >= REI_NAME_MAX) break;
      rei_shm *shm = rei_read_region(ctx, payload, name_len);
      if (shm == NULL) return NULL;          /* ctx->gone set */
      if ((uint64_t) hdr->len > (uint64_t) shm->size) break;
      return read_raw((const uint8_t *) shm->addr, hdr->len, type);
    }
    /* the channel's arena framing is resolved to its byte range by the
       transport before the call */
    if (hdr->len > limit) break;
    return read_raw(payload, hdr->len, (int) hdr->aux);
  case REI_KIND_STR1: {
    if (hdr->aux == REI_STR1_NA) {
      if (hdr->len != 0) break;
      Py_RETURN_NONE;   /* R's missing string reads as None */
    }
    if (hdr->len > limit || hdr->aux > REI_CE_BYTES) break;
    switch ((int) hdr->aux) {
    case REI_CE_NATIVE:
    case REI_CE_UTF8: {
      PyObject *s = PyUnicode_DecodeUTF8((const char *) payload,
                                         (Py_ssize_t) hdr->len, NULL);
      if (s == NULL) {
        PyErr_Clear();
        PyErr_SetString(ReiError, "pyrei: R string payload is not valid "
                        "UTF-8 (native encoding does not cross)");
      }
      return s;
    }
    case REI_CE_LATIN1:
      return PyUnicode_DecodeLatin1((const char *) payload,
                                    (Py_ssize_t) hdr->len, NULL);
    default:   /* REI_CE_BYTES */
      return PyBytes_FromStringAndSize((const char *) payload,
                                       (Py_ssize_t) hdr->len);
    }
  }
  case REI_KIND_INLINE:
    if (hdr->len > limit) break;
    return read_stream(payload, hdr->len);
  case REI_KIND_ARENA:
    /* resolved stream bytes; limit is the arena-validated length */
    return read_stream(payload, limit);
  case REI_KIND_SHM_RAW: {
    if (hdr->len == 0 || hdr->len >= REI_NAME_MAX) break;
    rei_shm *shm = rei_read_region(ctx, payload, hdr->len);
    if (shm == NULL) return NULL;          /* ctx->gone set */
    /* aux is the exact stream length: a recycled region is larger than the
       stream it carries, and the slack bytes are a previous payload's */
    size_t n = hdr->aux != 0 && hdr->aux <= (uint64_t) shm->size ?
      (size_t) hdr->aux : shm->size;
    return read_stream((const uint8_t *) shm->addr, n);
  }
  case REI_KIND_SHM_VEC:
  case REI_KIND_REF:
    PyErr_SetString(ReiError, "pyrei: R shared-vector payload (the zero-copy "
                    "view tier is not implemented yet)");
    return NULL;
  }
  PyErr_SetString(ReiError, "pyrei: corrupt payload slot");
  return NULL;
}

/* The ERR outcome's payload is the worker's constructed envelope: a pickled
   (type name, message, traceback) tuple — never a pickled exception
   instance — plus, for a map runner's annotated error, a fourth element
   carrying the in-flight element index. Rebuild it as a TaskError carrying
   the remote type name and traceback text (and `index` when present). */
static PyObject *task_error_of(PyObject *env) {
  PyObject *tn = NULL, *ms = NULL, *tbs = NULL, *eidx = NULL;
  Py_ssize_t arity = PyTuple_Check(env) ? PyTuple_GET_SIZE(env) : 0;
  if ((arity == 3 || arity == 4) &&
      PyUnicode_Check(PyTuple_GET_ITEM(env, 0)) &&
      PyUnicode_Check(PyTuple_GET_ITEM(env, 1)) &&
      PyUnicode_Check(PyTuple_GET_ITEM(env, 2))) {
    tn = PyTuple_GET_ITEM(env, 0);
    ms = PyTuple_GET_ITEM(env, 1);
    tbs = PyTuple_GET_ITEM(env, 2);
    Py_INCREF(tn);
    Py_INCREF(ms);
    Py_INCREF(tbs);
    if (arity == 4) {
      PyObject *i = PyTuple_GET_ITEM(env, 3);
      if (PyLong_Check(i)) {
        eidx = i;
        Py_INCREF(eidx);
      }
    }
  } else {
    tn = PyUnicode_FromString("Exception");
    ms = PyUnicode_FromString("pyrei: task failed (unreadable error envelope)");
    tbs = PyUnicode_FromString("");
  }
  PyObject *exc = NULL;
  PyObject *text = (tn != NULL && ms != NULL) ?
    PyUnicode_FromFormat("%U: %U", tn, ms) : NULL;
  if (text != NULL)
    exc = PyObject_CallFunction(ReiTaskError, "N", text);
  if (exc != NULL && tn != NULL && tbs != NULL &&
      (PyObject_SetAttrString(exc, "remote_type", tn) < 0 ||
       PyObject_SetAttrString(exc, "remote_traceback", tbs) < 0 ||
       (eidx != NULL && PyObject_SetAttrString(exc, "index", eidx) < 0)))
    Py_CLEAR(exc);
  Py_XDECREF(tn);
  Py_XDECREF(ms);
  Py_XDECREF(tbs);
  Py_XDECREF(eidx);
  return exc;
}

/* The DIED outcome carries no payload (a reap cannot write payload bytes
   without racing a live worker's publish): build the error off the claimant
   record. */
static PyObject *worker_died_of(const rei_read_ctx *ctx) {
  PyObject *exc = PyObject_CallFunction(
    ReiWorkerDiedError, "s", "pyrei: worker died while executing this task");
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
static PyObject *read_impl(const rei_slot_hdr *hdr, const uint8_t *payload,
                           size_t limit, rei_read_ctx *ctx) {
  switch (ctx->outcome) {
  case REI_RS_OK:
    return read_frame(hdr, payload, limit, ctx);
  case REI_RS_ERR: {
    PyObject *env = read_frame(hdr, payload, limit, ctx);
    if (env == NULL) return NULL;      /* ctx->gone, or a read failure */
    PyObject *exc = task_error_of(env);
    Py_DECREF(env);
    return caught_new(exc);
  }
  case REI_RS_CANCEL:
    return caught_new(PyObject_CallFunction(
      ReiCancelledError, "s", "pyrei: task cancelled or pool stopped"));
  case REI_RS_DIED:
    return caught_new(worker_died_of(ctx));
  }
  PyErr_SetString(ReiError, "pyrei: corrupt payload slot");
  return NULL;
}

/* The binding's read_fn. NULL with a Python error set fails the verb as
   REI_ERR ("payload read failed"); NULL via ctx->gone propagates the
   vanished-region verdict. */
static void *py_read(const rei_slot_hdr *hdr, const uint8_t *payload,
                     size_t limit, rei_read_ctx *ctx) {
  PyGILState_STATE gil = PyGILState_Ensure();
  PyObject *obj = read_impl(hdr, payload, limit, ctx);
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
static _Thread_local PyThreadState *rei_park_tstate;

static void py_park(void *Py_UNUSED(ctx), int entering) {
  if (entering) {
    rei_park_tstate = PyEval_SaveThread();
  } else {
    PyThreadState *tstate = rei_park_tstate;
    rei_park_tstate = NULL;
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
  if (rei_traceback_fmt == NULL) {
    PyObject *mod = PyImport_ImportModule("traceback");
    if (mod == NULL) {
      PyErr_Clear();
      return PyUnicode_FromString("");
    }
    rei_traceback_fmt = PyObject_GetAttrString(mod, "format_exception");
    Py_DECREF(mod);
    if (rei_traceback_fmt == NULL) {
      PyErr_Clear();
      return PyUnicode_FromString("");
    }
  }
  if (tb == NULL) {
    tb = Py_None;
  }
  PyObject *parts =
      PyObject_CallFunction(rei_traceback_fmt, "OOO", type, value, tb);
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

/* Publish the currently-held exception as the task's ERR result: the
   constructed, bounded (type name, message, traceback) envelope — never a
   pickled exception instance, whose unpicklable attributes or __traceback__
   would fail the publish (fail the task, never the worker). The traceback,
   then the message, truncate at a UTF-8 boundary to fit the slot's inline
   budget; framed INLINE in the sink's buffer wherever the envelope fits, so
   the publish itself cannot fail. Returns 0 on publish (or a cancel beat),
   nonzero on infrastructure failure. */
static int publish_exc(rei_result_sink *sink) {
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
    eidx = PyObject_GetAttrString(value, "_pyrei_map_index");
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
    PyObject *s = PyObject_CallFunction(rei_dumps, "Oi", cand, 4);
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
        "pyrei: task error (untransportable condition)");
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
    rc = rei_result_publish_err(sink, NULL,
                                (uint32_t) PyBytes_GET_SIZE(stream));
  } else {
    /* below the inline guarantee (a tiny slot): the tiered stage carries
       the smallest envelope out of line */
    env = eidx != NULL ? PyTuple_Pack(4, tname, msg, tbs, eidx)
                       : PyTuple_Pack(3, tname, msg, tbs);
    if (env == NULL) goto infra;
    rc = rei_result_publish_err(sink, (void *) env, 0);
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
static int py_exec(const rei_slot_hdr *hdr, const uint8_t *payload,
                   size_t limit, rei_result_sink *sink, int catching,
                   void *ctx) {
  (void) catching;
  rei_read_ctx rctx;
  memset(&rctx, 0, sizeof(rctx));
  rctx.size = (uint32_t) sizeof(rctx);
  rctx.outcome = REI_RS_OK;
  rctx.died_slot = -1;
  rctx.handle = (rei_handle *) sink->p;
  rctx.binding_ctx = ctx;
  PyObject *task = read_impl(hdr, payload, limit, &rctx);
  if (task == NULL) {
    if (rctx.gone) {
      /* the enqueuer died and its region went along: the task can never
         run anywhere — it fails as DIED, and the drain continues */
      rei_result_publish_died(sink);
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
    PyErr_SetString(ReiError, "pyrei: corrupt task payload");
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
  int rc = rei_result_publish(sink, (void *) value);
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
  rei_channel *core;
  long self_pid;
} ReiChannel;

static void chan_binding(rei_binding *b) {
  rei_binding_init(b);
  b->stage = py_stage;
  b->read = py_read;
  b->check = py_check;
  /* exec/park/sweep/drop NULL: a channel never evals; submitter handles
     release the GIL around the whole verb, so no park hook; staging pins
     nothing, so no drop hook. */
}

static rei_channel *chan_get(ReiChannel *self) {
  rei_channel *c = self->core;
  if (c == NULL) {
    PyErr_SetString(PyExc_ValueError, "pyrei: channel handle is closed");
    return NULL;
  }
  if (self->self_pid != rei_self_pid()) {
    PyErr_SetString(ReiError, "pyrei: channel handles do not survive fork()");
    return NULL;
  }
  return c;
}

/* Raise a REI_ERR from a handle verb. A callback that already set a Python
   error (the check hook's KeyboardInterrupt, a stage/read failure) wins. */
static PyObject *raise_handle(ReiChannel *self) {
  if (PyErr_Occurred()) return NULL;
  rei_errcat cat = rei_channel_errcat(self->core);
  const char *msg = rei_channel_error(self->core);
  switch (cat) {
  case REI_ERRCAT_INTERRUPTED:
    PyErr_SetNone(PyExc_KeyboardInterrupt);
    break;
  case REI_ERRCAT_NOSPACE:
  case REI_ERRCAT_NOMEMORY:
  case REI_ERRCAT_EXISTS:
    PyErr_Format(ReiShmError, "pyrei: %s", msg);
    break;
  default:
    PyErr_Format(ReiError, "pyrei: %s", msg);
    break;
  }
  return NULL;
}

static PyObject *status_or_raise(ReiChannel *self, rei_status st,
                                 PyObject *on_ok) {
  switch (st) {
  case REI_OK:        Py_INCREF(on_ok); return on_ok;
  case REI_FULL:      Py_INCREF(SentFull); return SentFull;
  case REI_TIMEOUT:   Py_INCREF(SentTimeout); return SentTimeout;
  case REI_CLOSED:    Py_INCREF(SentClosed); return SentClosed;
  case REI_PEER_GONE: Py_INCREF(SentGone); return SentGone;
  default:            return raise_handle(self);
  }
}

/* seconds (None / non-finite waits indefinitely, <= 0 polls) to the core's
   timeout_ms (0 polls, < 0 indefinite). */
static int timeout_ms_of(PyObject *arg, double *out) {
  if (arg == Py_None) {
    *out = -1;
    return 0;
  }
  double t = PyFloat_AsDouble(arg);
  if (t == -1 && PyErr_Occurred()) return -1;
  if (!isfinite(t)) {
    *out = -1;
    return 0;
  }
  *out = t <= 0 ? 0 : t * 1000;
  return 0;
}

static ReiChannel *chan_wrap(rei_channel *c) {
  ReiChannel *self = (ReiChannel *) ReiChannelType.tp_alloc(&ReiChannelType, 0);
  if (self == NULL) return NULL;
  self->core = c;
  self->self_pid = rei_self_pid();
  return self;
}

// Verbs ----------------------------------------------------------------------------

PyDoc_STRVAR(send_doc,
"send(x) -> True | sentinel\n\n\
Publish a message to the peer. Never blocks for ring space: returns\n\
pyrei.FULL when the ring is full, pyrei.CLOSED / pyrei.PEER_GONE on a\n\
closed or dead peer.");

static PyObject *Channel_send(ReiChannel *self, PyObject *arg) {
  rei_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_send(c, (void *) arg);
  Py_END_ALLOW_THREADS
  return status_or_raise(self, st, Py_True);
}

PyDoc_STRVAR(send_batch_doc,
"send_batch(xs) -> int\n\n\
Publish a sequence of messages in one batched tail store. Returns the\n\
count accepted — short of len(xs) when the ring filled or the channel\n\
closed midway; send the next element singly to learn which.");

static PyObject *Channel_send_batch(ReiChannel *self, PyObject *arg) {
  rei_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  PyObject *seq = PySequence_Fast(arg, "pyrei: expected a sequence of payloads");
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
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_send_batch(c, objs, (size_t) n, &accepted);
  Py_END_ALLOW_THREADS
  PyMem_Free(objs);
  Py_DECREF(seq);
  if (st == REI_ERR) return raise_handle(self);
  return PyLong_FromSize_t(accepted);
}

PyDoc_STRVAR(recv_doc,
"recv(timeout=None) -> object | sentinel\n\n\
Return the next message, waiting up to `timeout` seconds (None waits\n\
indefinitely, 0 polls). A terminal state is reported only once the ring\n\
is drained. Ctrl-C stays responsive during the wait.");

static PyObject *Channel_recv(ReiChannel *self, PyObject *args,
                              PyObject *kw) {
  static char *kwlist[] = {"timeout", NULL};
  PyObject *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:recv", kwlist, &tmo))
    return NULL;
  rei_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  void *obj = NULL;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_recv(c, &obj, ms);
  Py_END_ALLOW_THREADS
  if (st == REI_OK) return (PyObject *) obj;
  return status_or_raise(self, st, Py_None);
}

PyDoc_STRVAR(recv_batch_doc,
"recv_batch(n=256, timeout=None) -> list | sentinel\n\n\
Wait for the first message exactly like recv(), then drain up to `n`\n\
already-published messages without waiting further.");

static PyObject *Channel_recv_batch(ReiChannel *self, PyObject *args,
                                    PyObject *kw) {
  static char *kwlist[] = {"n", "timeout", NULL};
  int n = 256;
  PyObject *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|iO:recv_batch", kwlist,
                                   &n, &tmo))
    return NULL;
  if (n < 1) {
    PyErr_SetString(PyExc_ValueError, "pyrei: n must be at least 1");
    return NULL;
  }
  rei_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  PyObject *out = PyList_New((Py_ssize_t) n);
  if (out == NULL) return NULL;
  size_t count = 0;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_recv_batch_fn(c, (size_t) n, &count, list_sink, out, ms);
  Py_END_ALLOW_THREADS
  if (st == REI_OK) {
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

static PyObject *Channel_close(ReiChannel *self, PyObject *args,
                               PyObject *kw) {
  static char *kwlist[] = {"timeout", NULL};
  PyObject *tmo = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:close", kwlist, &tmo))
    return NULL;
  if (self->core == NULL || self->self_pid != rei_self_pid()) {
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
  rei_channel *c = self->core;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_close(c, ms);
  Py_END_ALLOW_THREADS
  if (st == REI_ERR) return raise_handle(self);
  if (st == REI_OK) {
    rei_channel_destroy(c);
    self->core = NULL;
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;   /* REI_TIMEOUT: destroy retries the rendezvous */
}

PyDoc_STRVAR(close_signal_doc,
"close_signal() -> None\n\n\
The peer half of the close protocol: set this side's close bit and wake\n\
the host, with no rendezvous. No-op on a released handle.");

static PyObject *Channel_close_signal(ReiChannel *self,
                                      PyObject *Py_UNUSED(args)) {
  if (self->core != NULL && self->self_pid == rei_self_pid())
    rei_channel_close_signal(self->core);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(destroy_doc,
"destroy() -> None\n\n\
Idempotent, never blocks: signals close and releases what the\n\
non-blocking rendezvous check completes. The finalizer target.");

static PyObject *Channel_destroy(ReiChannel *self,
                                 PyObject *Py_UNUSED(args)) {
  if (self->core != NULL && self->self_pid == rei_self_pid())
    rei_channel_destroy(self->core);
  self->core = NULL;
  Py_RETURN_NONE;
}

PyDoc_STRVAR(alive_doc,
"alive() -> bool\n\n\
Peer-process liveness (the lock probe, not a listener flag): a peer that\n\
closed but still runs reads as alive.");

static PyObject *Channel_alive(ReiChannel *self, PyObject *Py_UNUSED(args)) {
  if (self->core == NULL || self->self_pid != rei_self_pid())
    Py_RETURN_FALSE;
  return PyBool_FromLong(rei_channel_alive(self->core));
}

PyDoc_STRVAR(ready_set_doc,
"ready_set() -> None\n\n\
The peer's startup signal, sent after it has consumed the drop.");

static PyObject *Channel_ready_set(ReiChannel *self,
                                   PyObject *Py_UNUSED(args)) {
  rei_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  if (rei_channel_ready_set(c) != REI_OK) return raise_handle(self);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(ready_wait_doc,
"ready_wait(timeout) -> bool\n\n\
The host's startup rendezvous: wait up to `timeout` seconds for the\n\
peer's ready signal. False on expiry.");

static PyObject *Channel_ready_wait(ReiChannel *self, PyObject *tmo) {
  rei_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_ready_wait(c, ms);
  Py_END_ALLOW_THREADS
  if (st == REI_ERR) return raise_handle(self);
  return PyBool_FromLong(st == REI_OK);
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

static PyObject *Channel_info(ReiChannel *self, PyObject *Py_UNUSED(args)) {
  rei_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  rei_channel_info info;
  if (rei_channel_info_get(c, &info) != REI_OK) return raise_handle(self);
  PyObject *name = PyUnicode_FromString(info.name);
  PyObject *side = PyUnicode_FromString(info.side == REI_ENTITY_HOST ?
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

static PyObject *Channel_token_get(ReiChannel *self,
                                   void *Py_UNUSED(closure)) {
  rei_channel *c = chan_get(self);
  if (c == NULL) return NULL;
  char buf[64];
  if (rei_channel_token(c, buf, sizeof(buf)) != REI_OK)
    return raise_handle(self);
  return PyUnicode_FromString(buf);
}

static PyObject *Channel_repr(ReiChannel *self) {
  if (self->core == NULL)
    return PyUnicode_FromString("<pyrei.Channel (closed)>");
  char buf[64];
  if (rei_channel_token(self->core, buf, sizeof(buf)) != REI_OK)
    buf[0] = '\0';
  return PyUnicode_FromFormat("<pyrei.Channel %s>", buf);
}

static void Channel_dealloc(ReiChannel *self) {
  /* destroy signals close and runs the non-blocking rendezvous check; a
     forked child's copy never touches the shared region */
  if (self->core != NULL && self->self_pid == rei_self_pid())
    rei_channel_destroy(self->core);
  ReiChannelType.tp_free((PyObject *) self);
}

static PyMethodDef Channel_methods[] = {
  {"send", (PyCFunction) Channel_send, METH_O, send_doc},
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
  {"info", (PyCFunction) Channel_info, METH_NOARGS, info_doc},
  {NULL, NULL, 0, NULL}
};

static PyGetSetDef Channel_getset[] = {
  {"token", (getter) Channel_token_get, NULL,
   "The join token (\"<pid hex>_<counter hex>\") for the peer's attach.",
   NULL},
  {NULL, NULL, NULL, NULL, NULL}
};

static PyTypeObject ReiChannelType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pyrei._Channel",
  .tp_basicsize = sizeof(ReiChannel),
  .tp_repr = (reprfunc) Channel_repr,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A channel handle (process-private; does not survive fork()). "
            "Construct through pyrei.Channel.create() / Channel.attach().",
  .tp_methods = Channel_methods,
  .tp_getset = Channel_getset,
  .tp_dealloc = (destructor) Channel_dealloc,
};


// The pool handle ----------------------------------------------------------------

typedef struct {
  PyObject_HEAD
  rei_pool *core;
  long self_pid;
  int role;            /* REI_ROLE_* */
} ReiPool;

typedef struct {
  PyObject_HEAD
  uint64_t word;       /* the core's 8-byte rei_task, by value */
  PyObject *pool;      /* keeps the pool handle alive */
} ReiTask;

/* Submitter/controller handles release the GIL around every verb; worker
   handles hold it (exec_fn reentry) and drop it only around bounded sleeps
   via the park hook. */
#define POOL_ALLOW_THREADS(self) \
  PyThreadState *_save = \
    (self)->role == REI_ROLE_WORKER ? NULL : PyEval_SaveThread()
#define POOL_RESUME() \
  if (_save != NULL) PyEval_RestoreThread(_save)

static void pool_binding(rei_binding *b, int worker) {
  rei_binding_init(b);
  b->stage = py_stage;
  b->read = py_read;
  b->check = py_check;
  b->exec = worker ? py_exec : NULL;
  b->park = worker ? py_park : NULL;
  /* sweep/drop NULL: no per-handle caches, and staging pins nothing */
}

static rei_pool *pool_peek(ReiPool *self) {
  rei_pool *p = self->core;
  if (p == NULL) return NULL;
  if (self->self_pid != rei_self_pid()) {
    PyErr_SetString(ReiError, "pyrei: pool handles do not survive fork()");
    return NULL;
  }
  return p;
}

static rei_pool *pool_get(ReiPool *self) {
  rei_pool *p = pool_peek(self);
  if (p == NULL && !PyErr_Occurred())
    PyErr_SetString(PyExc_ValueError, "pyrei: pool handle is closed");
  return p;
}

/* Raise a pool verb's REI_ERR. A callback that already set a Python error
   (the check hook's KeyboardInterrupt, a stage/read failure) wins. */
static PyObject *pool_raise(ReiPool *self) {
  if (PyErr_Occurred()) return NULL;
  rei_errcat cat = rei_pool_errcat(self->core);
  const char *msg = rei_pool_error(self->core);
  switch (cat) {
  case REI_ERRCAT_INTERRUPTED:
    PyErr_SetNone(PyExc_KeyboardInterrupt);
    break;
  case REI_ERRCAT_STOPPED:
    PyErr_Format(ReiStoppedError, "pyrei: %s", msg);
    break;
  case REI_ERRCAT_EXHAUSTED:
    PyErr_Format(ReiSlotsExhaustedError, "pyrei: %s", msg);
    break;
  case REI_ERRCAT_NOSPACE:
  case REI_ERRCAT_NOMEMORY:
  case REI_ERRCAT_EXISTS:
    PyErr_Format(ReiShmError, "pyrei: %s", msg);
    break;
  default:
    PyErr_Format(ReiError, "pyrei: %s", msg);
    break;
  }
  return NULL;
}

static ReiPool *pool_wrap(rei_pool *p, int role) {
  ReiPool *self = (ReiPool *) ReiPoolType.tp_alloc(&ReiPoolType, 0);
  if (self == NULL) return NULL;
  self->core = p;
  self->self_pid = rei_self_pid();
  self->role = role;
  return self;
}

/* A collected value is either the task's result or a _Caught box carrying
   the outcome's exception. Unwrap and raise; with_index attributes the
   0-based position (collect_any / collect_all). */
static PyObject *caught_or_value(PyObject *v, size_t index, int with_index) {
  if (Py_TYPE(v) != &ReiCaughtType) return v;
  PyObject *exc = ((ReiCaught *) v)->exc;
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

static ReiTask *task_wrap(ReiPool *pool, const rei_task *t) {
  ReiTask *self = (ReiTask *) ReiTaskType.tp_alloc(&ReiTaskType, 0);
  if (self == NULL) return NULL;
  self->word = t->word;
  Py_INCREF(pool);
  self->pool = (PyObject *) pool;
  return self;
}

/* Unpack a task handle and its pool (NULL on error, with the exception
   set). Errors on a foreign or finalized handle; the core detects a stale
   (collected/released) sequence. */
static rei_task task_get(ReiTask *self, ReiPool **pool_out) {
  ReiPool *pool = (ReiPool *) self->pool;
  rei_task t = { self->word };
  if (self->word == 0) {
    PyErr_SetString(PyExc_ValueError, "pyrei: task handle is closed");
    pool = NULL;
  } else if (pool_get(pool) == NULL) {
    pool = NULL;
  }
  *pool_out = pool;
  return t;
}

/* Extract a task sequence's handles and their shared pool. */
static ReiPool *tasks_get(PyObject *arg, rei_task **ts_out, size_t *n_out) {
  PyObject *seq = PySequence_Fast(
    arg, "pyrei: tasks must be a non-empty sequence of task handles");
  if (seq == NULL) return NULL;
  Py_ssize_t n = PySequence_Fast_GET_SIZE(seq);
  if (n < 1) {
    Py_DECREF(seq);
    PyErr_SetString(PyExc_ValueError,
                    "pyrei: tasks must be a non-empty sequence of task handles");
    return NULL;
  }
  rei_task *ts = PyMem_Malloc((size_t) n * sizeof(rei_task));
  if (ts == NULL) {
    Py_DECREF(seq);
    PyErr_NoMemory();
    return NULL;
  }
  ReiPool *pool = NULL;
  PyObject **items = PySequence_Fast_ITEMS(seq);
  for (Py_ssize_t i = 0; i < n; i++) {
    if (Py_TYPE(items[i]) != &ReiTaskType) {
      PyErr_SetString(PyExc_TypeError, "pyrei: not a task handle");
      goto fail;
    }
    ReiPool *pi;
    ts[i] = task_get((ReiTask *) items[i], &pi);
    if (pi == NULL) goto fail;
    if (pool == NULL) {
      pool = pi;
    } else if (pool != pi) {
      PyErr_SetString(PyExc_ValueError,
                      "pyrei: task handles must belong to the same pool handle");
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
indefinitely, 0 polls): pyrei.SubmitTimeoutError on expiry,\n\
pyrei.SlotsExhaustedError / pyrei.StoppedError on the fatal outcomes.");

static PyObject *Pool_submit(ReiPool *self, PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"payload", "timeout", NULL};
  PyObject *payload, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:submit", kwlist, &payload,
                                   &tmo))
    return NULL;
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  rei_task t;
  rei_status st;
  POOL_ALLOW_THREADS(self);
  st = rei_pool_submit(p, (void *) payload, &t, ms);
  POOL_RESUME();
  if (st == REI_OK) return (PyObject *) task_wrap(self, &t);
  if (st == REI_FULL) {
    PyErr_SetString(ReiSubmitTimeoutError,
                    "pyrei: submission timed out (injection ring full)");
    return NULL;
  }
  return pool_raise(self);
}

PyDoc_STRVAR(pool_submit_batch_doc,
"submit_batch(payloads, timeout=None) -> list of _Task\n\n\
One task per payload in a single crossing. Ring-full past `timeout` ends\n\
the batch short — the accepted handles stay valid and collectible; the\n\
fatal outcomes still raise.");

static PyObject *Pool_submit_batch(ReiPool *self, PyObject *args,
                                   PyObject *kw) {
  static char *kwlist[] = {"payloads", "timeout", NULL};
  PyObject *seq, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:submit_batch", kwlist,
                                   &seq, &tmo))
    return NULL;
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  PyObject *fast =
    PySequence_Fast(seq, "pyrei: expected a sequence of task payloads");
  if (fast == NULL) return NULL;
  Py_ssize_t n = PySequence_Fast_GET_SIZE(fast);
  void **objs = PyMem_Malloc((size_t) (n != 0 ? n : 1) * sizeof(void *));
  rei_task *ts = PyMem_Malloc((size_t) (n != 0 ? n : 1) * sizeof(rei_task));
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
  rei_status st;
  POOL_ALLOW_THREADS(self);
  st = rei_pool_submit_batch(p, objs, (size_t) n, ts, &done, ms);
  POOL_RESUME();
  PyMem_Free(objs);
  Py_DECREF(fast);
  if (st == REI_ERR) {
    PyMem_Free(ts);
    return pool_raise(self);
  }
  PyObject *out = PyList_New((Py_ssize_t) done);
  if (out == NULL) {
    PyMem_Free(ts);
    return NULL;
  }
  for (size_t i = 0; i < done; i++) {
    ReiTask *t = task_wrap(self, &ts[i]);
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
attribute. pyrei.TIMEOUT on expiry; the reported handle is consumed, the\n\
rest stay collectible.");

static PyObject *Pool_collect_any(ReiPool *self, PyObject *args,
                                  PyObject *kw) {
  static char *kwlist[] = {"tasks", "timeout", NULL};
  PyObject *seq, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:collect_any", kwlist,
                                   &seq, &tmo))
    return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  rei_task *ts;
  size_t n;
  ReiPool *pool = tasks_get(seq, &ts, &n);
  if (pool == NULL) return NULL;
  if (pool != self) {
    PyMem_Free(ts);
    PyErr_SetString(PyExc_ValueError,
                    "pyrei: task handles must belong to this pool handle");
    return NULL;
  }
  void *v = NULL;
  size_t idx = 0;
  rei_status st;
  POOL_ALLOW_THREADS(self);
  st = rei_pool_collect_any(self->core, ts, n, &idx, &v, ms);
  POOL_RESUME();
  PyMem_Free(ts);
  if (st == REI_TIMEOUT) {
    Py_INCREF(SentTimeout);
    return SentTimeout;
  }
  if (st == REI_ERR) return pool_raise(self);
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
— handles up to it inclusive are consumed, the rest stay collectible.\n\
pyrei.TIMEOUT consumes nothing.");

static PyObject *Pool_collect_all(ReiPool *self, PyObject *args,
                                  PyObject *kw) {
  static char *kwlist[] = {"tasks", "timeout", NULL};
  PyObject *seq, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:collect_all", kwlist,
                                   &seq, &tmo))
    return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  rei_task *ts;
  size_t n;
  ReiPool *pool = tasks_get(seq, &ts, &n);
  if (pool == NULL) return NULL;
  if (pool != self) {
    PyMem_Free(ts);
    PyErr_SetString(PyExc_ValueError,
                    "pyrei: task handles must belong to this pool handle");
    return NULL;
  }
  PyObject *out = PyList_New((Py_ssize_t) n);
  if (out == NULL) {
    PyMem_Free(ts);
    return NULL;
  }
  size_t err_idx = 0;
  rei_status st;
  POOL_ALLOW_THREADS(self);
  st = rei_pool_collect_all_fn(self->core, ts, n, list_sink, out, &err_idx,
                               ms);
  POOL_RESUME();
  PyMem_Free(ts);
  if (st == REI_TIMEOUT) {
    Py_DECREF(out);
    Py_INCREF(SentTimeout);
    return SentTimeout;
  }
  if (st == REI_ERR) {
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

static PyObject *Pool_ready_wait(ReiPool *self, PyObject *args,
                                 PyObject *kw) {
  static char *kwlist[] = {"slots", "timeout", NULL};
  PyObject *seq, *tmo;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "OO:ready_wait", kwlist,
                                   &seq, &tmo))
    return NULL;
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  if (self->role != REI_ROLE_CONTROLLER) {
    PyErr_SetString(ReiError, "pyrei: only the controller can wait for workers");
    return NULL;
  }
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  PyObject *fast = PySequence_Fast(seq, "pyrei: expected worker slot indices");
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
    if (s < 0 || s >= REI_MAX_WORKERS) {
      PyMem_Free(slots);
      Py_DECREF(fast);
      PyErr_SetString(PyExc_ValueError, "pyrei: worker slot out of range");
      return NULL;
    }
    slots[i] = (uint32_t) s;
  }
  Py_DECREF(fast);
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_pool_ready_wait(p, slots, (size_t) n, ms);
  Py_END_ALLOW_THREADS
  PyMem_Free(slots);
  if (st == REI_ERR) return pool_raise(self);
  return PyBool_FromLong(st == REI_OK);
}

PyDoc_STRVAR(pool_retire_doc,
"retire(slot) -> None\n\n\
Ask one worker to exit cleanly: non-blocking, never preemptive. The worker\n\
observes between tasks and releases its slot; the remaining workers\n\
consume its queued work in place. Controller only.");

static PyObject *Pool_retire(ReiPool *self, PyObject *arg) {
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  if (self->role != REI_ROLE_CONTROLLER) {
    PyErr_SetString(ReiError, "pyrei: only the controller can retire a worker");
    return NULL;
  }
  long slot = PyLong_AsLong(arg);
  if (slot == -1 && PyErr_Occurred()) return NULL;
  if (slot < 0 || slot >= REI_MAX_WORKERS) {
    PyErr_SetString(PyExc_ValueError, "pyrei: worker slot out of range");
    return NULL;
  }
  if (rei_pool_retire(p, (uint32_t) slot) != REI_OK)
    return pool_raise(self);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(pool_stop_doc,
"stop(timeout=5.0) -> bool\n\n\
Orderly shutdown, controller only: broadcast shutdown, wake every parked\n\
participant, cancel all pending tasks (blocked collectors raise\n\
pyrei.CancelledError), then wait up to `timeout` seconds for clean worker\n\
exits and unlink. False on expiry — the workers still exit on their own.\n\
Idempotent; the handle is dead afterwards.");

static PyObject *Pool_stop(ReiPool *self, PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"timeout", NULL};
  PyObject *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:stop", kwlist, &tmo))
    return NULL;
  if (self->core == NULL) Py_RETURN_TRUE;    /* idempotent */
  if (self->self_pid != rei_self_pid()) {
    self->core = NULL;
    Py_RETURN_TRUE;
  }
  if (self->role != REI_ROLE_CONTROLLER) {
    PyErr_SetString(ReiError, "pyrei: only the controller can stop a pool");
    return NULL;
  }
  double ms;
  if (tmo == Py_None) {
    ms = 5000.0;
  } else if (timeout_ms_of(tmo, &ms) < 0) {
    return NULL;
  }
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_pool_stop(self->core, ms);
  Py_END_ALLOW_THREADS
  if (st == REI_ERR) return pool_raise(self);
  /* the handle stays alive (a post-stop submit reads the shutdown flag as
     StoppedError); the finalizer's destroy is a no-op after */
  return PyBool_FromLong(st == REI_OK);
}

PyDoc_STRVAR(pool_destroy_doc,
"destroy() -> None\n\n\
Idempotent, never blocks: a controller broadcasts shutdown without the\n\
wait; a participant releases its slot. The finalizer target.");

static PyObject *Pool_destroy(ReiPool *self, PyObject *Py_UNUSED(args)) {
  if (self->core != NULL && self->self_pid == rei_self_pid())
    rei_pool_destroy(self->core);
  self->core = NULL;
  Py_RETURN_NONE;
}

PyDoc_STRVAR(pool_run_doc,
"run() -> int\n\n\
The worker loop: claim/steal tasks and execute them on this thread,\n\
blocking until an exit reason — 0 shutdown, 1 owner gone, 2 retired.\n\
Raises on infrastructure failure. Worker handles only. The GIL stays held\n\
(the park hook drops it around each bounded sleep).");

static PyObject *Pool_run(ReiPool *self, PyObject *Py_UNUSED(args)) {
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  if (self->role != REI_ROLE_WORKER) {
    PyErr_SetString(ReiError, "pyrei: not a worker handle");
    return NULL;
  }
  rei_worker_exit ex = rei_pool_worker_run(p);
  if (PyErr_Occurred()) return NULL;   /* a BaseException escaped a task */
  if (ex == REI_EXIT_ERROR) return pool_raise(self);
  return PyLong_FromLong((long) ex);
}

PyDoc_STRVAR(pool_leave_doc,
"leave() -> None\n\n\
The clean-exit handshake of a worker. No-op on a released handle.");

static PyObject *Pool_leave(ReiPool *self, PyObject *Py_UNUSED(args)) {
  rei_pool *p = self->core;
  if (p == NULL || self->self_pid != rei_self_pid()) Py_RETURN_NONE;
  if (self->role != REI_ROLE_WORKER) {
    PyErr_SetString(ReiError, "pyrei: not a worker handle");
    return NULL;
  }
  if (rei_pool_leave(p) != REI_OK) return pool_raise(self);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(pool_lame_duck_doc,
"lame_duck() -> bool\n\n\
One linger beat for a retired worker anchoring its uncollected results:\n\
True when the anchor may drop (shutdown or owner death ends the linger).");

static PyObject *Pool_lame_duck(ReiPool *self, PyObject *Py_UNUSED(args)) {
  rei_pool *p = self->core;
  if (p == NULL || self->self_pid != rei_self_pid()) Py_RETURN_TRUE;
  return PyBool_FromLong(rei_pool_lame_duck(p));
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

static PyObject *Pool_status(ReiPool *self, PyObject *Py_UNUSED(args)) {
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  rei_pool_status st;
  if (rei_pool_status_get(p, &st) != REI_OK) return pool_raise(self);
  PyObject *d = PyDict_New();
  if (d == NULL) return NULL;
  const char *role = st.role == REI_ROLE_CONTROLLER ? "controller" :
    st.role == REI_ROLE_WORKER ? "worker" : "submitter";
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
                   (unsigned long long) st.tasks_by_state[s + REI_RS_PENDING]) < 0)
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

static PyObject *dump_worker(const rei_pool_dump *d, uint32_t i) {
  const rei_worker_stat *w = &d->workers[i];
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

static PyObject *dump_submitter(const rei_pool_dump *d, uint32_t j) {
  const rei_sub_stat *s = &d->submitters[j];
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

static PyObject *Pool_dump(ReiPool *self, PyObject *Py_UNUSED(args)) {
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  rei_pool_dump d;
  if (rei_pool_dump_get(p, &d) != REI_OK) return pool_raise(self);
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
    rei_rs_row *rows = PyMem_Malloc((size_t) (cap != 0 ? cap : 1) *
                                    sizeof(rei_rs_row));
    if (rows == NULL) {
      PyErr_NoMemory();
      goto fail;
    }
    uint32_t n = 0;
    rei_status st = rei_pool_tasks_get(p, rows, cap, &n);
    if (st != REI_OK) {
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

static PyObject *Pool_token_get(ReiPool *self, void *Py_UNUSED(closure)) {
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  char buf[64];
  if (rei_pool_token(p, buf, sizeof(buf)) != REI_OK)
    return pool_raise(self);
  return PyUnicode_FromString(buf);
}

static PyObject *Pool_repr(ReiPool *self) {
  if (self->core == NULL)
    return PyUnicode_FromString("<pyrei.Pool (closed)>");
  char buf[64];
  if (rei_pool_token(self->core, buf, sizeof(buf)) != REI_OK)
    buf[0] = '\0';
  const char *role = self->role == REI_ROLE_CONTROLLER ? "controller" :
    self->role == REI_ROLE_WORKER ? "worker" : "submitter";
  return PyUnicode_FromFormat("<pyrei.Pool %s (%s)>", buf, role);
}

static void Pool_dealloc(ReiPool *self) {
  /* a controller destroy broadcasts shutdown (no wait); a participant
     releases its slot; a forked child's copy never touches the region */
  if (self->core != NULL && self->self_pid == rei_self_pid())
    rei_pool_destroy(self->core);
  ReiPoolType.tp_free((PyObject *) self);
}

// Pool map support (the Pool.map veneers) -----------------------------------

PyDoc_STRVAR(pool_map_caps_doc,
"_map_caps() -> (live_workers, free_result_slots, injection_cap, \\\n\
inline_entry_budget)\n\n\
A map's batch-sizing inputs in one read pass. A worker's first nested map\n\
claims its submitter slot here, exactly as a first nested submit does.");

static PyObject *Pool_map_caps(ReiPool *self, PyObject *Py_UNUSED(args)) {
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  uint32_t free_rs, inj_cap, inline_entry;
  if (rei_pool_map_caps(p, &free_rs, &inj_cap, &inline_entry) != 0)
    return pool_raise(self);
  rei_pool_status st;
  if (rei_pool_status_get(p, &st) != REI_OK) return pool_raise(self);
  uint32_t live = 0;
  for (uint32_t i = 0; i < st.max_workers; i++)
    live += st.worker_state[i] == REI_WK_LIVE;
  return Py_BuildValue("(IIII)", live, free_rs, inj_cap, inline_entry);
}

static void pool_sig_capsule_free(PyObject *caps) {
  void *p = PyCapsule_GetPointer(caps, REI_PY_SIG_CAPSULE);
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

static PyObject *Pool_signals(ReiPool *self, PyObject *Py_UNUSED(args)) {
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  rei_pool_sig *s = rei_pool_signals(p);
  if (s == NULL) return PyErr_NoMemory();
  return PyCapsule_New(s, REI_PY_SIG_CAPSULE, pool_sig_capsule_free);
}

PyDoc_STRVAR(pool_help_once_doc,
"_help_once() -> bool\n\n\
One doorbell help beat at a runner's batch boundary: claims one foreign\n\
injection task (a runner-flagged one is re-homed onto this worker's own\n\
deque, not executed). True when it claimed. The worker keeps holding the\n\
GIL around the call: a claimed task's Python callable needs it.");

static PyObject *Pool_help_once(ReiPool *self, PyObject *Py_UNUSED(args)) {
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  int rc = rei_pool_help_once(p);
  if (rc < 0) return pool_raise(self);
  return PyBool_FromLong(rc);
}

PyDoc_STRVAR(pool_submit_runner_doc,
"_submit_runner(payload, timeout=None) -> _Task\n\n\
A map runner's submit: submit plus the REI_ENTRY_RUNNER wire flag, so a\n\
doorbell help beat re-homes it onto the helper's own deque instead of\n\
executing a join ticket nested. Same error taxonomy as submit().");

static PyObject *Pool_submit_runner(ReiPool *self, PyObject *args,
                                    PyObject *kw) {
  static char *kwlist[] = {"payload", "timeout", NULL};
  PyObject *payload, *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O:_submit_runner", kwlist,
                                   &payload, &tmo))
    return NULL;
  rei_pool *p = pool_get(self);
  if (p == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  rei_task t;
  rei_status st;
  POOL_ALLOW_THREADS(self);
  st = rei_pool_submit_flags(p, (void *) payload, REI_ENTRY_RUNNER, &t, ms);
  POOL_RESUME();
  if (st == REI_OK) return (PyObject *) task_wrap(self, &t);
  if (st == REI_FULL) {
    PyErr_SetString(ReiSubmitTimeoutError,
                    "pyrei: submission timed out (injection ring full)");
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

static PyTypeObject ReiPoolType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pyrei._Pool",
  .tp_basicsize = sizeof(ReiPool),
  .tp_repr = (reprfunc) Pool_repr,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A pool handle (process-private; does not survive fork()). "
            "Construct through pyrei.Pool.create() / Pool.attach().",
  .tp_methods = Pool_methods,
  .tp_getset = Pool_getset,
  .tp_dealloc = (destructor) Pool_dealloc,
};

// Task verbs ---------------------------------------------------------------------

PyDoc_STRVAR(task_collect_doc,
"collect(timeout=None) -> value | sentinel\n\n\
Wait up to `timeout` seconds (None indefinitely, 0 polls) for the task's\n\
terminal state and return its result. A task error re-raises as\n\
pyrei.TaskError (carrying remote_type / remote_traceback), a cancellation\n\
as pyrei.CancelledError, a dead worker as pyrei.WorkerDiedError.\n\
pyrei.TIMEOUT on expiry; a task is collected exactly once.");

static PyObject *Task_collect(ReiTask *self, PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"timeout", NULL};
  PyObject *tmo = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:collect", kwlist, &tmo))
    return NULL;
  ReiPool *pool;
  rei_task t = task_get(self, &pool);
  if (pool == NULL) return NULL;
  double ms;
  if (timeout_ms_of(tmo, &ms) < 0) return NULL;
  void *v = NULL;
  rei_status st;
  POOL_ALLOW_THREADS(pool);
  st = rei_pool_collect(pool->core, &t, &v, ms);
  POOL_RESUME();
  if (st == REI_TIMEOUT) {
    Py_INCREF(SentTimeout);
    return SentTimeout;
  }
  if (st == REI_ERR) return pool_raise(pool);
  return caught_or_value((PyObject *) v, 0, 0);
}

PyDoc_STRVAR(task_cancel_doc,
"cancel() -> bool\n\n\
Advisory and discard-only, never preemptive: a still-queued task is\n\
skipped; an executing one runs to completion and its result is dropped.\n\
True when this call cancelled the task; every other edge folds to False.");

static PyObject *Task_cancel(ReiTask *self, PyObject *Py_UNUSED(args)) {
  ReiPool *pool = (ReiPool *) self->pool;
  int cancelled = 0;
  if (self->word != 0 && pool->core != NULL &&
      pool->self_pid == rei_self_pid()) {
    rei_task t = { self->word };
    cancelled = rei_pool_cancel(pool->core, &t);
  }
  return PyBool_FromLong(cancelled);
}

static PyObject *Task_state_get(ReiTask *self, void *Py_UNUSED(closure)) {
  ReiPool *pool = (ReiPool *) self->pool;
  const char *state = "dropped";
  if (self->word != 0 && pool->core != NULL &&
      pool->self_pid == rei_self_pid()) {
    rei_task t = { self->word };
    switch (rei_pool_task_state(pool->core, &t)) {
    case REI_RS_PENDING: state = "pending"; break;
    case REI_RS_OK:      state = "ok"; break;
    case REI_RS_ERR:     state = "err"; break;
    case REI_RS_CANCEL:  state = "cancel"; break;
    case REI_RS_DIED:    state = "died"; break;
    default:             state = "collected"; break;   /* FREE / stale */
    }
  }
  return PyUnicode_FromString(state);
}

static PyObject *Task_pool_get(ReiTask *self, void *Py_UNUSED(closure)) {
  Py_INCREF(self->pool);
  return self->pool;
}

static PyObject *Task_repr(ReiTask *self) {
  return PyUnicode_FromFormat("<pyrei.Task seq=%llu slot=%u>",
                              (unsigned long long) (self->word & ((1ULL << 40) - 1)),
                              (unsigned int) (self->word >> 40));
}

static void Task_dealloc(ReiTask *self) {
  ReiPool *pool = (ReiPool *) self->pool;
  /* the finalizer release for a handle that was never collected: cancels a
     pending task, frees a terminal one — advisory and total, so safe for a
     stale handle, a released pool, or a forked child (guarded) alike */
  if (self->word != 0 && pool->core != NULL &&
      pool->self_pid == rei_self_pid()) {
    rei_task t = { self->word };
    rei_pool_task_release(pool->core, &t);
  }
  Py_DECREF(self->pool);
  ReiTaskType.tp_free((PyObject *) self);
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

static PyTypeObject ReiTaskType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pyrei._Task",
  .tp_basicsize = sizeof(ReiTask),
  .tp_repr = (reprfunc) Task_repr,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A task handle from _Pool.submit(). Collected exactly once; "
            "an uncollected handle's finalizer releases its slot.",
  .tp_methods = Task_methods,
  .tp_getset = Task_getset,
  .tp_dealloc = (destructor) Task_dealloc,
};

// Create / attach (module functions; the policy layer lives in pyrei.Channel) ------

static int rei_pow2(uint64_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

PyDoc_STRVAR(channel_new_doc,
"_channel_new(capacity, slot_size, arena_size, spin, drop) -> _Channel\n\n\
Host side: write the preamble and return the handle. The caller spawns\n\
the peer (with the token) and completes the startup rendezvous.");

static PyObject *pyrei_channel_new(PyObject *Py_UNUSED(module),
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
  if (!rei_pow2(cap) || cap < 2 || cap > (1u << 24)) {
    PyBuffer_Release(&drop);
    PyErr_SetString(PyExc_ValueError,
                    "pyrei: capacity must be a power of two between 2 and 2^24");
    return NULL;
  }
  if (!rei_pow2(slot) || slot < 64 || slot > (1u << 20)) {
    PyBuffer_Release(&drop);
    PyErr_SetString(PyExc_ValueError,
              "pyrei: slot_size must be a power of two between 64 and 2^20");
    return NULL;
  }
  if (arena % 64 != 0) {
    PyBuffer_Release(&drop);
    PyErr_SetString(PyExc_ValueError,
                    "pyrei: arena_size must be a non-negative multiple of 64");
    return NULL;
  }
  rei_channel_opts opts;
  rei_channel_opts_init(&opts);
  opts.capacity = (uint32_t) cap;
  opts.slot_size = (uint32_t) slot;
  opts.arena_size = (uint64_t) arena;
  opts.flags = spin ? REI_FLAG_SPIN : 0;
  opts.drop = drop.len > 0 ? (const uint8_t *) drop.buf : NULL;
  opts.drop_size = (uint64_t) drop.len;
  rei_binding b;
  chan_binding(&b);
  rei_channel *c;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_create(&c, &opts, &b);
  Py_END_ALLOW_THREADS
  PyBuffer_Release(&drop);
  if (st != REI_OK) {
    raise_tls();
    return NULL;
  }
  return (PyObject *) chan_wrap(c);
}

PyDoc_STRVAR(channel_attach_doc,
"_channel_attach(token) -> (_Channel, bytes)\n\n\
Peer side: attach to the channel named by the join token and return the\n\
handle with the drop (the peer bootstrap bytes). The caller consumes the\n\
drop, then signals ready_set.");

static PyObject *pyrei_channel_attach(PyObject *Py_UNUSED(module),
                                      PyObject *arg) {
  const char *token = PyUnicode_AsUTF8(arg);
  if (token == NULL) return NULL;
  const char *us = strchr(token, '_');
  int ok = us != NULL && us != token && us[1] != '\0' &&
           strchr(us + 1, '_') == NULL;
  for (const char *p = token; ok && *p != '\0'; p++)
    ok = (*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f') || *p == '_';
  if (!ok) {
    PyErr_SetString(PyExc_ValueError, "pyrei: malformed join token");
    return NULL;
  }
  rei_binding b;
  chan_binding(&b);
  rei_channel *c;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_attach(&c, token, &b);
  Py_END_ALLOW_THREADS
  if (st != REI_OK) {
    raise_tls();
    return NULL;
  }
  ReiChannel *self = chan_wrap(c);
  if (self == NULL) {
    rei_channel_destroy(c);
    return NULL;
  }
  /* borrowed drop bytes, valid until destroy — copy out for the caller */
  const uint8_t *bytes;
  uint64_t n;
  rei_channel_drop(c, &bytes, &n);
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

static PyObject *pyrei_pool_new(PyObject *Py_UNUSED(module),
                                PyObject *args, PyObject *kw) {
  static char *kwlist[] = {"max_workers", "max_submitters", "injection_cap",
                           "per_worker_cap", "result_slots", "slot_size",
                           NULL};
  unsigned int maxw, maxs, inj, deq, rslots, slot;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "IIIIII:_pool_new", kwlist,
                                   &maxw, &maxs, &inj, &deq, &rslots, &slot))
    return NULL;
  if (maxw < 1 || maxw > REI_MAX_WORKERS) {
    PyErr_Format(PyExc_ValueError,
                 "pyrei: max_workers must be between 1 and %d",
                 REI_MAX_WORKERS);
    return NULL;
  }
  if (maxs < 1 || maxs > 64) {
    PyErr_SetString(PyExc_ValueError,
                    "pyrei: max_submitters must be between 1 and 64");
    return NULL;
  }
  if (!rei_pow2(inj) || inj < 2 || inj > (1u << 24)) {
    PyErr_SetString(PyExc_ValueError,
                "pyrei: injection_cap must be a power of two between 2 and 2^24");
    return NULL;
  }
  if (!rei_pow2(deq) || deq < 2 || deq > (1u << 24)) {
    PyErr_SetString(PyExc_ValueError,
              "pyrei: per_worker_cap must be a power of two between 2 and 2^24");
    return NULL;
  }
  /* floor 128: a result slot's inline budget (slot - 40) must hold a
     region name (up to 27 bytes on Windows) for an SHM_RAW spill */
  if (!rei_pow2(slot) || slot < 128 || slot > (1u << 20)) {
    PyErr_SetString(PyExc_ValueError,
              "pyrei: slot_size must be a power of two between 128 and 2^20");
    return NULL;
  }
  if (rslots < maxs || rslots > (1u << 24)) {
    PyErr_SetString(PyExc_ValueError,
              "pyrei: result_slots must be between max_submitters and 2^24");
    return NULL;
  }
  rslots = (rslots + maxs - 1) / maxs * maxs;   /* per-submitter partition */
  rei_pool_opts opts;
  rei_pool_opts_init(&opts);
  opts.max_workers = (uint32_t) maxw;
  opts.max_submitters = (uint32_t) maxs;
  opts.injection_cap = (uint32_t) inj;
  opts.per_worker_cap = (uint32_t) deq;
  opts.result_slots = (uint32_t) rslots;
  opts.slot_size = (uint32_t) slot;
  rei_binding b;
  pool_binding(&b, 0);
  rei_pool *p;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_pool_create(&p, &opts, &b);
  Py_END_ALLOW_THREADS
  if (st != REI_OK) {
    raise_tls();
    return NULL;
  }
  return (PyObject *) pool_wrap(p, REI_ROLE_CONTROLLER);
}

PyDoc_STRVAR(pool_attach_doc,
"_pool_attach(token) -> _Pool\n\n\
Submitter side: join a live pool from another process, claiming a free\n\
submitter slot with its own injection ring and result-slot subrange.");

static PyObject *pyrei_pool_attach(PyObject *Py_UNUSED(module),
                                   PyObject *arg) {
  const char *token = PyUnicode_AsUTF8(arg);
  if (token == NULL) return NULL;
  if (!token_valid(token)) {
    PyErr_SetString(PyExc_ValueError, "pyrei: malformed join token");
    return NULL;
  }
  rei_binding b;
  pool_binding(&b, 0);
  rei_pool *p;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_pool_attach(&p, token, &b);
  Py_END_ALLOW_THREADS
  if (st != REI_OK) {
    raise_tls();
    return NULL;
  }
  return (PyObject *) pool_wrap(p, REI_ROLE_SUBMITTER);
}

PyDoc_STRVAR(pool_worker_join_doc,
"_pool_worker_join(token, slot) -> _Pool\n\n\
Worker side: attach to the pool named by the join token as worker `slot`\n\
(the liveness lock before the status CAS), registering the exec callback.\n\
The entry point is python -m pyrei.worker.");

static PyObject *pyrei_pool_worker_join(PyObject *Py_UNUSED(module),
                                        PyObject *args) {
  const char *token;
  unsigned int slot;
  if (!PyArg_ParseTuple(args, "sI:_pool_worker_join", &token, &slot))
    return NULL;
  if (!token_valid(token)) {
    PyErr_SetString(PyExc_ValueError, "pyrei: malformed join token");
    return NULL;
  }
  rei_binding b;
  pool_binding(&b, 1);
  rei_pool *p;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_pool_worker_join(&p, token, (uint32_t) slot, &b);
  Py_END_ALLOW_THREADS
  if (st != REI_OK) {
    raise_tls();
    return NULL;
  }
  return (PyObject *) pool_wrap(p, REI_ROLE_WORKER);
}

PyDoc_STRVAR(is_sentinel_doc,
"is_sentinel(x) -> bool\n\n\
Provenance, not shape: True only for the exact sentinel singletons this\n\
process's verbs return, so a look-alike payload never passes.");

static PyObject *pyrei_is_sentinel(PyObject *Py_UNUSED(module), PyObject *x) {
  return PyBool_FromLong(x == SentFull || x == SentTimeout ||
                         x == SentClosed || x == SentGone);
}

PyDoc_STRVAR(abi_version_doc,
"abi_version() -> int\n\n\
Wire-format ABI version of the compiled-in librei core.");

static PyObject *pyrei_abi_version(PyObject *Py_UNUSED(module),
                                   PyObject *Py_UNUSED(args)) {
  return PyLong_FromUnsignedLong((unsigned long) REI_ABI_VERSION);
}

static PyMethodDef pyrei_methods[] = {
  {"_channel_new", (PyCFunction)(void (*)(void)) pyrei_channel_new,
   METH_VARARGS | METH_KEYWORDS, channel_new_doc},
  {"_channel_attach", (PyCFunction) pyrei_channel_attach, METH_O,
   channel_attach_doc},
  {"_pool_new", (PyCFunction)(void (*)(void)) pyrei_pool_new,
   METH_VARARGS | METH_KEYWORDS, pool_new_doc},
  {"_pool_attach", (PyCFunction) pyrei_pool_attach, METH_O, pool_attach_doc},
  {"_pool_worker_join", (PyCFunction) pyrei_pool_worker_join, METH_VARARGS,
   pool_worker_join_doc},
  {"is_sentinel", pyrei_is_sentinel, METH_O, is_sentinel_doc},
  {"abi_version", (PyCFunction) pyrei_abi_version, METH_NOARGS, abi_version_doc},
  {NULL, NULL, 0, NULL}
};

PyDoc_STRVAR(pyrei_module_doc,
"Lock-free shared-memory IPC: SPSC channels and work-stealing task pools\n\
(the librei core, compiled in).");

static struct PyModuleDef pyrei_module = {
  PyModuleDef_HEAD_INIT, "_pyrei", pyrei_module_doc, -1, pyrei_methods,
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
PyInit__pyrei(void)
{
  if (PyType_Ready(&ReiSentinelType) < 0) return NULL;
  if (PyType_Ready(&ReiCaughtType) < 0) return NULL;
  if (PyType_Ready(&ReiChannelType) < 0) return NULL;
  if (PyType_Ready(&ReiPoolType) < 0) return NULL;
  if (PyType_Ready(&ReiTaskType) < 0) return NULL;

  /* cloudpickle when installed, stock pickle otherwise; both read each
     other's protocol-4 streams */
  PyObject *pickler = PyImport_ImportModule("cloudpickle");
  if (pickler == NULL) {
    PyErr_Clear();
    pickler = PyImport_ImportModule("pickle");
    if (pickler == NULL) return NULL;
  }
  rei_dumps = PyObject_GetAttrString(pickler, "dumps");
  rei_loads = PyObject_GetAttrString(pickler, "loads");
  Py_DECREF(pickler);
  if (rei_dumps == NULL || rei_loads == NULL) return NULL;

  PyObject *m = PyModule_Create(&pyrei_module);
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

  if (add_exception(m, &ReiError, "pyrei.ReiError", PyExc_Exception,
                    "Base class for all pyrei errors.") < 0 ||
      add_exception(m, &ReiStartupError, "pyrei.StartupError", ReiError,
                    "A channel peer or pool worker failed to attach within "
                    "the startup timeout.") < 0 ||
      add_exception(m, &ReiShmError, "pyrei.ShmError", ReiError,
                    "A shared-memory region operation failed.") < 0 ||
      add_exception(m, &ReiSubmitTimeoutError, "pyrei.SubmitTimeoutError",
                    ReiError,
                    "Pool.submit() timed out waiting for injection-ring "
                    "space.") < 0 ||
      add_exception(m, &ReiSlotsExhaustedError, "pyrei.SlotsExhaustedError",
                    ReiError,
                    "Pool.submit() found no free result slot: too many "
                    "outstanding (uncollected) tasks.") < 0 ||
      add_exception(m, &ReiStoppedError, "pyrei.StoppedError", ReiError,
                    "The pool is stopped; no further submission is "
                    "possible.") < 0 ||
      add_exception(m, &ReiCancelledError, "pyrei.CancelledError",
                    ReiError,
                    "The task was cancelled before it ran.") < 0 ||
      add_exception(m, &ReiWorkerDiedError, "pyrei.WorkerDiedError",
                    ReiError,
                    "The executing worker died mid-task. Carries 'slot' "
                    "and 'pid' attributes identifying the worker.") < 0 ||
      add_exception(m, &ReiTaskError, "pyrei.TaskError", ReiError,
                    "The task callable raised. Carries 'remote_type' and "
                    "'remote_traceback' attributes describing the "
                    "worker-side exception.") < 0) {
    Py_DECREF(m);
    return NULL;
  }

  if (PyModule_AddObject(m, "_Channel", (PyObject *) &ReiChannelType) < 0 ||
      PyModule_AddObject(m, "_Pool", (PyObject *) &ReiPoolType) < 0 ||
      PyModule_AddObject(m, "_Task", (PyObject *) &ReiTaskType) < 0 ||
      PyModule_AddStringConstant(m, "__core_version__",
                                 REI_VERSION_STRING) < 0 ||
      rei_py_map_register(m, ReiError, ReiShmError) < 0) {
    Py_DECREF(m);
    return NULL;
  }
  return m;
}
