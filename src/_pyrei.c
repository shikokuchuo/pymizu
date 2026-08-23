/* pyrei — Python binding for librei (raw CPython C API).
 *
 * The extension compiles the vendored core sources directly, so the module
 * is self-contained: no system librei is required or consulted.
 *
 * GIL policy (channel handles are submitter handles — exec is NULL): the
 * veneer releases the GIL around every verb; the stage/read/check callbacks
 * reacquire it with PyGILState_Ensure. The park hook stays NULL — it exists
 * for exec-capable worker handles, which hold the GIL through collect.
 * Staging pins nothing (pickle streams and bare vector bytes are
 * self-contained; out-of-line regions ride the core's retain table), so the
 * drop hook is NULL too.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <math.h>
#include <string.h>

#include "rei.h"
#include "internal.h"

#define REI_STR_(x) #x
#define REI_STR(x) REI_STR_(x)
#define REI_VERSION_STRING                                                \
  REI_STR(REI_VERSION_MAJOR) "." REI_STR(REI_VERSION_MINOR) "."           \
  REI_STR(REI_VERSION_PATCH)

/* R's cetype_t marks (the STR1 aux values), fixed by the wire format. */
enum { REI_CE_NATIVE = 0, REI_CE_UTF8 = 1, REI_CE_LATIN1 = 2, REI_CE_BYTES = 3 };

static PyTypeObject ReiChannelType;

// Module state -------------------------------------------------------------------

static PyObject *ReiError;           /* base */
static PyObject *ReiStartupError;    /* child failed to attach in time */
static PyObject *ReiShmError;        /* region create/open failure */
static PyObject *ReiSubmitTimeoutError;   /* pool (verb surface lands later) */
static PyObject *ReiSlotsExhaustedError;
static PyObject *ReiStoppedError;
static PyObject *ReiCancelledError;
static PyObject *ReiWorkerDiedError;

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
  if (chunk == NULL) return -1;
  memcpy(chunk, v->buf, n);
  hdr->kind = REI_KIND_RAWSPILL;
  hdr->len = (uint32_t) n;
  hdr->aux = (uint64_t) type;
  memcpy(payload, &off, sizeof(off));
  return 0;
}

static int stage_impl(PyObject *obj, rei_slot_hdr *hdr, uint8_t *payload,
                      uint32_t inline_max, rei_handle *h) {
  if (obj == Py_None) {
    hdr->kind = REI_KIND_NIL;
    hdr->len = 0;
    hdr->aux = 0;
    return 0;
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

/* A serialized-stream frame (INLINE / ARENA / SHM_RAW bytes). Our streams
   are pickle protocol 4, first byte 0x80. 'S' is the R compact codec, 'B' /
   'X' / 'A' the R serialize formats — no codec interop in v1. */
static PyObject *read_stream(const uint8_t *src, size_t n) {
  if (n == 0) {
    PyErr_SetString(ReiError, "pyrei: corrupt payload slot");
    return NULL;
  }
  switch (src[0]) {
  case 0x80:
    return PyObject_CallFunction(rei_loads, "y#", (const char *) src,
                                 (Py_ssize_t) n);
  case 'S': case 'B': case 'X': case 'A':
    PyErr_SetString(ReiError, "pyrei: R payload (no codec interop) - "
                    "send Python values from a pyrei peer");
    return NULL;
  default:
    PyErr_SetString(ReiError, "pyrei: unrecognized payload");
    return NULL;
  }
}

static PyObject *read_impl(const rei_slot_hdr *hdr, const uint8_t *payload,
                           size_t limit, rei_read_ctx *ctx) {
  switch (hdr->kind) {
  case REI_KIND_NIL:
    Py_RETURN_NONE;
  case REI_KIND_RAWVEC:
    if (hdr->len > limit) break;
    return read_raw(payload, hdr->len, (int) hdr->aux);
  case REI_KIND_RAWSPILL:
    /* the channel's arena framing is resolved to its byte range by the
       transport before the call (the pool's region framing is pool-side) */
    if (hdr->len > limit) break;
    return read_raw(payload, hdr->len, (int) (hdr->aux & 0xff));
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
  void **objs = PyMem_Malloc((size_t) n * sizeof(void *));
  if (objs == NULL) return PyErr_NoMemory();
  size_t count = 0;
  rei_status st;
  Py_BEGIN_ALLOW_THREADS
  st = rei_channel_recv_batch(c, objs, (size_t) n, &count, ms);
  Py_END_ALLOW_THREADS
  if (st == REI_OK) {
    PyObject *out = PyList_New((Py_ssize_t) count);
    if (out != NULL)
      for (size_t i = 0; i < count; i++)
        PyList_SET_ITEM(out, (Py_ssize_t) i, (PyObject *) objs[i]);
    PyMem_Free(objs);
    return out;
  }
  PyMem_Free(objs);
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
                         PyObject *base) {
  *slot = PyErr_NewException(name, base, NULL);
  if (*slot == NULL) return -1;
  const char *dot = strchr(name, '.');
  return PyModule_AddObject(m, dot != NULL ? dot + 1 : name, *slot);
}

PyMODINIT_FUNC
PyInit__pyrei(void)
{
  if (PyType_Ready(&ReiSentinelType) < 0) return NULL;
  if (PyType_Ready(&ReiChannelType) < 0) return NULL;

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

  if (add_exception(m, &ReiError, "pyrei.ReiError", PyExc_Exception) < 0 ||
      add_exception(m, &ReiStartupError, "pyrei.StartupError", ReiError) < 0 ||
      add_exception(m, &ReiShmError, "pyrei.ShmError", ReiError) < 0 ||
      add_exception(m, &ReiSubmitTimeoutError, "pyrei.SubmitTimeoutError",
                    ReiError) < 0 ||
      add_exception(m, &ReiSlotsExhaustedError, "pyrei.SlotsExhaustedError",
                    ReiError) < 0 ||
      add_exception(m, &ReiStoppedError, "pyrei.StoppedError", ReiError) < 0 ||
      add_exception(m, &ReiCancelledError, "pyrei.CancelledError",
                    ReiError) < 0 ||
      add_exception(m, &ReiWorkerDiedError, "pyrei.WorkerDiedError",
                    ReiError) < 0) {
    Py_DECREF(m);
    return NULL;
  }

  if (PyModule_AddObject(m, "_Channel", (PyObject *) &ReiChannelType) < 0 ||
      PyModule_AddStringConstant(m, "__core_version__",
                                 REI_VERSION_STRING) < 0) {
    Py_DECREF(m);
    return NULL;
  }
  return m;
}
