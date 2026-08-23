/* pyrei Pool.map staging and worker-side context — one fresh rei region
   per map call, holding a 128-byte header, ONE pickled descriptor stream
   ((fn, args, kwargs), or (fn, args, kwargs, x) when x rides the
   descriptor), an optional bare-bytes x section (a C-contiguous buffer of
   a supported dtype, wrapped once per worker and indexed per element), and
   the morsel state: cancel word + generation on one cache line, the shared
   cursor alone on the next, then the CLAIM array of (generation << 2) |
   state words. Runner tasks are ordinary pool tasks and this file touches
   no pool internals: the signal words arrive as an opaque capsule the
   caller created in-process. The layout mirrors the R package's map.c;
   the generation machinery is kept for layout parity (pyrei v1 has no
   prepared maps, so the generation is always 0). */

#include <stdlib.h>
#include <string.h>

#include "pymap.h"
#include "rei.h"
#include "internal.h"

#define REI_PYMAP_MAGIC 0x4D525950u   /* "PYRM" */

static PyObject *ReiErr;   /* pyrei.ReiError (borrowed at registration) */
static PyObject *ReiShmErr;   /* pyrei.ShmError */

enum { REI_PYMAP_X_DESC = 0, REI_PYMAP_X_RAWBUF };

/* Map-descriptor region header. Not pool wire format — it rides its own
   region, keyed by the same ABI version — but the same rules apply: the
   struct is the layout, 64-byte-aligned sections follow it. */
typedef struct rei_pymap_hdr_s {
  uint32_t magic;
  uint32_t version;
  uint32_t flags;            /* reserved, 0 */
  uint32_t x_kind;           /* REI_PYMAP_X_DESC / REI_PYMAP_X_RAWBUF */
  uint32_t x_tag;            /* RAWBUF section wire type (REI_TYPE_*) */
  uint32_t pad0;
  uint64_t n;                /* map elements */
  uint64_t desc_off, desc_len;
  uint64_t x_off, x_len;
  uint64_t morsel_size;      /* elements per morsel */
  uint64_t n_morsels;        /* ceiling(n / morsel_size) */
  uint64_t state_off;        /* morsel state section offset */
  uint32_t claim_n;          /* CLAIM word count (runner ordinal bound) */
  uint8_t  pad[36];
} rei_pymap_hdr;

typedef char rei_pymap_hdr_assert[(sizeof(rei_pymap_hdr) == 128) ? 1 : -1];

/* Morsel state section, identical to R's: one cache line for the cancel
   word and run generation counter (read-mostly), one for the shared cursor
   (the ticket dispenser, alone so runner RMW traffic never touches the
   cancel line), then the CLAIM array — one word per runner *ordinal*,
   packing (generation << 2) | state so the lane claim and the generation
   fence are one atomic. Issue is a plain relaxed fetch_add; ordering rides
   the task claim/publish chain. Completion is never recorded here: runners
   publish their batch histories through their ordinary results, and the
   lost set on death is arithmetic over them. */
#define REI_PYMAP_CANCEL_OFF ((uint64_t) 0)
#define REI_PYMAP_GEN_OFF    ((uint64_t) 4)
#define REI_PYMAP_CURSOR_OFF ((uint64_t) 64)
#define REI_PYMAP_CLAIM_OFF  ((uint64_t) 128)
#define REI_PYMAP_GEN_MASK   ((uint32_t) 0x3FFFFFFF)

enum { REI_PY_MORSEL_IDLE = 0, REI_PY_MORSEL_RUNNING, REI_PY_MORSEL_ABANDONED };

/* Batch sizing policy constants (frozen by the R package's gate sweep):
   k targets a 200 us batch duration, growing at most 2x per step and
   shrinking immediately on overshoot, clamped to the 64-morsel cap. */
#define REI_PYMAP_T_TARGET  200e-6
#define REI_PYMAP_BATCH_CAP 64

// Map handle -------------------------------------------------------------------

/* The header is validated exactly once per mapping — at open, or authored
   at stage — and cached process-local behind the capsule, so the per-call
   primitives pay a bounds check instead of a full re-validation. The local
   copy is immune to concurrent scribbling over shm that per-call re-reads
   would re-trust. The capsule pins the context, so the mapping outlives
   any view handed out over it as long as the Python side caches them
   together. */
typedef struct rei_pymap_s {
  rei_shm *shm;
  rei_pymap_hdr h;
  int owner;             /* stage side: the destructor unlinks */
  /* Batch sizing state (map_next), process-private and never wire state,
     reset at each run's first-call CLAIM CAS. */
  int32_t  run_r;        /* ordinal whose ramp this is (-1 = none) */
  uint64_t k;            /* current batch size, morsels */
  uint64_t k_last;       /* morsels issued last transition */
  double   t_last;       /* rei_now() at the last issue */
  double   cost;         /* est. seconds per morsel (0 = unknown) */
  int      skip;         /* last interval contained a help: no update */
} rei_pymap;

static void pymap_free(rei_pymap *mh) {
  if (mh->shm != NULL)
    rei_shm_close(mh->shm, mh->owner);   /* owner: unlink + unmap + free */
  free(mh);
}

/* _map_close's tombstone: PyCapsule_SetPointer rejects NULL, so a closed
   context is marked with a sentinel address the accessors refuse. */
static int pymap_closed_marker;
#define REI_PYMAP_CLOSED ((void *) &pymap_closed_marker)

static void pymap_capsule_free(PyObject *caps) {
  void *p = PyCapsule_GetPointer(caps, REI_PY_MAP_CAPSULE);
  if (p == NULL) {
    PyErr_Clear();   /* a NULL payload is not produced, but tolerate it */
    return;
  }
  if (p != REI_PYMAP_CLOSED) pymap_free((rei_pymap *) p);
}

static rei_pymap *pymap_get(PyObject *caps) {
  rei_pymap *mh = (rei_pymap *) PyCapsule_GetPointer(caps, REI_PY_MAP_CAPSULE);
  if (mh == NULL || mh == (rei_pymap *) REI_PYMAP_CLOSED) {
    /* a foreign capsule or a closed context land alike */
    PyErr_SetString(ReiErr, "pyrei: not a map context (or it is closed)");
    return NULL;
  }
  return mh;
}

static _Atomic uint32_t *pymap_cancel_word(rei_pymap *mh) {
  return (_Atomic uint32_t *)
    ((unsigned char *) mh->shm->addr + mh->h.state_off + REI_PYMAP_CANCEL_OFF);
}

static _Atomic uint64_t *pymap_cursor_word(rei_pymap *mh) {
  return (_Atomic uint64_t *)
    ((unsigned char *) mh->shm->addr + mh->h.state_off + REI_PYMAP_CURSOR_OFF);
}

static _Atomic uint32_t *pymap_claim_word(rei_pymap *mh, uint32_t r) {
  return (_Atomic uint32_t *)
    ((unsigned char *) mh->shm->addr + mh->h.state_off + REI_PYMAP_CLAIM_OFF +
     (uint64_t) r * 4);
}

static int pymap_ordinal(rei_pymap *mh, PyObject *ord, uint32_t *out) {
  long r = PyLong_AsLong(ord);
  if (r == -1 && PyErr_Occurred()) return -1;
  if (r < 0 || (uint32_t) r >= mh->h.claim_n) {
    PyErr_SetString(ReiErr, "pyrei: runner ordinal out of range");
    return -1;
  }
  *out = (uint32_t) r;
  return 0;
}

// Staging (submitter side) -------------------------------------------------------

PyDoc_STRVAR(map_stage_doc,
"_map_stage(desc_bytes, x, n, morsel_size) -> (region_name, capsule)\n\n\
Stage one map call into a fresh region. `desc_bytes` is the pickled\n\
descriptor stream; `x` a C-contiguous buffer of a supported dtype for the\n\
bare-bytes x section, or None when x rides the descriptor. The returned\n\
capsule pins the region: its destructor unlinks (the GC backstop);\n\
_map_close unlinks explicitly. The capsule must outlive all runner\n\
completion — workers attach by name.");

static PyObject *py_map_stage(PyObject *Py_UNUSED(module), PyObject *args) {
  Py_buffer desc, xbuf;
  unsigned long long n_ll, ms_ll;
  PyObject *x_obj;
  int have_x;
  if (!PyArg_ParseTuple(args, "y*OKK:_map_stage", &desc, &x_obj, &n_ll,
                        &ms_ll))
    return NULL;
  if (n_ll < 1 || n_ll > ((uint64_t) 1 << 48)) {
    PyBuffer_Release(&desc);
    PyErr_SetString(ReiErr, "pyrei: invalid map length");
    return NULL;
  }
  if (ms_ll < 1 || ms_ll > n_ll) {
    PyBuffer_Release(&desc);
    PyErr_SetString(ReiErr, "pyrei: invalid map morsel size");
    return NULL;
  }
  if (desc.len < 1) {
    PyBuffer_Release(&desc);
    PyErr_SetString(ReiErr, "pyrei: invalid map descriptor size");
    return NULL;
  }
  have_x = x_obj != Py_None;
  int x_tag = 0;
  if (have_x) {
    if (PyObject_GetBuffer(x_obj, &xbuf,
                           PyBUF_C_CONTIGUOUS | PyBUF_FORMAT) < 0)
      goto fail_desc;
    x_tag = rei_py_wire_type_of(&xbuf);
    size_t elt = rei_type_elt_size(x_tag);
    if (x_tag == 0 || elt == 0 || (uint64_t) xbuf.len != n_ll * elt) {
      PyBuffer_Release(&xbuf);
      PyBuffer_Release(&desc);
      PyErr_SetString(ReiErr,
                      "pyrei: x is not eligible for the map raw section");
      return NULL;
    }
  }

  rei_pymap_hdr h;
  memset(&h, 0, sizeof(h));
  h.magic = REI_PYMAP_MAGIC;
  h.version = REI_ABI_VERSION;
  h.n = (uint64_t) n_ll;
  h.desc_off = sizeof(rei_pymap_hdr);
  h.desc_len = (uint64_t) desc.len;
  h.morsel_size = (uint64_t) ms_ll;
  h.n_morsels = (h.n + h.morsel_size - 1) / h.morsel_size;
  h.claim_n = REI_MAX_WORKERS;
  uint64_t off = REI_ALIGN64(sizeof(rei_pymap_hdr) + h.desc_len);
  if (have_x) {
    h.x_kind = REI_PYMAP_X_RAWBUF;
    h.x_tag = (uint32_t) x_tag;
    h.x_off = off;
    h.x_len = (uint64_t) xbuf.len;
    off = REI_ALIGN64(off + h.x_len);
  }
  /* morsel state after the descriptor / x sections; a fresh region is
     zero-filled, so cancel, generation, cursor and every CLAIM word
     ((0 << 2) | IDLE) start armed for generation 0 */
  h.state_off = off;
  off = REI_ALIGN64(off + REI_PYMAP_CLAIM_OFF + (uint64_t) h.claim_n * 4);
  if (off > ((uint64_t) 1 << 46)) {
    PyErr_SetString(ReiShmErr, "pyrei: map region too large");
    goto fail_bufs;
  }

  rei_shm *shm;
  int rc = rei_shm_create_heap(&shm, (size_t) off);
  if (rc != REI_ERRCAT_NONE) {
    const char *summary, *hint;
    rei_err_describe(rc, &summary, &hint);
    PyErr_Format(ReiShmErr, "pyrei: cannot create map region (%llu bytes): "
                 "%s%s%s", (unsigned long long) off, summary,
                 hint[0] != '\0' ? ". " : "", hint);
    goto fail_bufs;
  }
  unsigned char *b = (unsigned char *) shm->addr;
  memcpy(b, &h, sizeof(h));
  memcpy(b + h.desc_off, desc.buf, (size_t) h.desc_len);
  if (have_x)
    memcpy(b + h.x_off, xbuf.buf, (size_t) h.x_len);
  PyBuffer_Release(&desc);
  if (have_x) PyBuffer_Release(&xbuf);

  rei_pymap *mh = calloc(1, sizeof(*mh));
  if (mh == NULL) {
    rei_shm_close(shm, 1);
    return PyErr_NoMemory();
  }
  mh->shm = shm;
  mh->h = h;
  mh->owner = 1;
  mh->run_r = -1;
  mh->k = 1;
  PyObject *caps = PyCapsule_New(mh, REI_PY_MAP_CAPSULE, pymap_capsule_free);
  if (caps == NULL) {
    pymap_free(mh);
    return NULL;
  }
  PyObject *name = PyUnicode_FromStringAndSize(shm->name, shm->name_len);
  if (name == NULL) {
    Py_DECREF(caps);
    return NULL;
  }
  PyObject *out = PyTuple_New(2);
  if (out == NULL) {
    Py_DECREF(caps);
    Py_DECREF(name);
    return NULL;
  }
  PyTuple_SET_ITEM(out, 0, name);
  PyTuple_SET_ITEM(out, 1, caps);
  return out;

fail_bufs:
  if (have_x) PyBuffer_Release(&xbuf);
fail_desc:
  PyBuffer_Release(&desc);
  return NULL;
}

PyDoc_STRVAR(map_close_doc,
"_map_close(capsule) -> None\n\n\
Explicitly release a map context: the stage side unlinks the region (the\n\
clean-collect / timeout path; the capsule destructor is only the GC\n\
backstop), the worker side drops its mapping. Idempotent.");

static PyObject *py_map_close(PyObject *Py_UNUSED(module), PyObject *caps) {
  if (!PyCapsule_CheckExact(caps)) {
    PyErr_SetString(ReiErr, "pyrei: not a map context");
    return NULL;
  }
  void *p = PyCapsule_GetPointer(caps, REI_PY_MAP_CAPSULE);
  if (p == NULL) {
    PyErr_Clear();   /* tolerate a NULL payload */
    Py_RETURN_NONE;
  }
  if (p == REI_PYMAP_CLOSED) Py_RETURN_NONE;   /* idempotent */
  if (PyCapsule_SetPointer(caps, REI_PYMAP_CLOSED) < 0) return NULL;
  pymap_free((rei_pymap *) p);
  Py_RETURN_NONE;
}

// Worker-side context -------------------------------------------------------------

static const char *pymap_hdr_validate(const rei_shm *shm, rei_pymap_hdr *out) {
  if (shm->size < sizeof(rei_pymap_hdr))
    return "region is smaller than a map header";
  rei_pymap_hdr h;
  memcpy(&h, shm->addr, sizeof(h));
  if (h.magic != REI_PYMAP_MAGIC)
    return "bad magic: not a pyrei map region";
  if (h.version != REI_ABI_VERSION)
    return "ABI version mismatch: worker and submitter were built against "
           "different rei wire formats";
  if (h.n == 0 || h.n > ((uint64_t) 1 << 48))
    return "element count out of range";
  if (h.desc_off < sizeof(rei_pymap_hdr) || h.desc_off > shm->size ||
      h.desc_len == 0 || h.desc_len > shm->size - h.desc_off)
    return "descriptor lies outside the region";
  if (h.morsel_size == 0 ||
      h.n_morsels != (h.n + h.morsel_size - 1) / h.morsel_size)
    return "morsel geometry is inconsistent";
  if (h.claim_n == 0 || h.claim_n > (1u << 16) ||
      h.state_off < sizeof(rei_pymap_hdr) || (h.state_off & 63) != 0 ||
      h.state_off > shm->size ||
      REI_PYMAP_CLAIM_OFF + (uint64_t) h.claim_n * 4 > shm->size - h.state_off)
    return "morsel state section lies outside the region";
  if (h.x_kind == REI_PYMAP_X_RAWBUF) {
    size_t elt = rei_type_elt_size((int) h.x_tag);
    if (elt == 0 || h.x_off > shm->size || h.x_len > shm->size - h.x_off ||
        h.x_len != h.n * elt)
      return "x section lies outside the region";
  } else if (h.x_kind != REI_PYMAP_X_DESC) {
    return "unknown x section kind";
  }
  if (out != NULL) *out = h;
  return NULL;
}

PyDoc_STRVAR(map_open_doc,
"_map_open(region_name) -> capsule\n\n\
Attach a map region (worker side): writable always — every runner CASes\n\
the shared morsel state — and no-populate, so a large raw x demand-pages\n\
per worker. The header is validated once here. Failure raises an ordinary\n\
error: it happens inside the task eval, so it publishes as the runner's\n\
ERR result (or the cancel drop absorbs it on the timeout path).");

static PyObject *py_map_open(PyObject *Py_UNUSED(module), PyObject *arg) {
  const char *name = PyUnicode_AsUTF8(arg);
  if (name == NULL) return NULL;
  rei_shm *shm = rei_shm_open_rw_heap(name, 0);
  if (shm == NULL) {
    PyErr_Format(ReiShmErr, "pyrei: cannot open map region '%s' — its "
                 "submitter died or the map ended", name);
    return NULL;
  }
  rei_pymap_hdr h;
  const char *err = pymap_hdr_validate(shm, &h);
  if (err != NULL) {
    rei_shm_close(shm, 0);
    PyErr_Format(ReiErr, "pyrei: invalid map region: %s", err);
    return NULL;
  }
  rei_pymap *mh = calloc(1, sizeof(*mh));
  if (mh == NULL) {
    rei_shm_close(shm, 0);
    return PyErr_NoMemory();
  }
  mh->shm = shm;
  mh->h = h;
  mh->run_r = -1;
  mh->k = 1;
  return PyCapsule_New(mh, REI_PY_MAP_CAPSULE, pymap_capsule_free);
}

PyDoc_STRVAR(map_header_doc,
"_map_header(capsule) -> dict\n\n\
The validated stage-time constants: n, x_kind, x_tag, morsel_size,\n\
n_morsels, claim_n.");

static PyObject *py_map_header(PyObject *Py_UNUSED(module), PyObject *caps) {
  rei_pymap *mh = pymap_get(caps);
  if (mh == NULL) return NULL;
  return Py_BuildValue("{s:K,s:I,s:I,s:K,s:K,s:I}",
                       "n", (unsigned long long) mh->h.n,
                       "x_kind", mh->h.x_kind,
                       "x_tag", mh->h.x_tag,
                       "morsel_size", (unsigned long long) mh->h.morsel_size,
                       "n_morsels", (unsigned long long) mh->h.n_morsels,
                       "claim_n", mh->h.claim_n);
}

PyDoc_STRVAR(map_desc_doc,
"_map_desc(capsule) -> bytes\n\n\
The one descriptor stream, copied out of the mapping (the Python side\n\
unpickles it once per worker and caches).");

static PyObject *py_map_desc(PyObject *Py_UNUSED(module), PyObject *caps) {
  rei_pymap *mh = pymap_get(caps);
  if (mh == NULL) return NULL;
  return PyBytes_FromStringAndSize(
    (const char *) mh->shm->addr + mh->h.desc_off, (Py_ssize_t) mh->h.desc_len);
}

PyDoc_STRVAR(map_x_view_doc,
"_map_x_view(capsule) -> memoryview\n\n\
A read-only view over the raw x section. The view owns no reference: it\n\
must not outlive the context capsule (the Python cache holds them\n\
together).");

static PyObject *py_map_x_view(PyObject *Py_UNUSED(module), PyObject *caps) {
  rei_pymap *mh = pymap_get(caps);
  if (mh == NULL) return NULL;
  if (mh->h.x_kind != REI_PYMAP_X_RAWBUF) {
    PyErr_SetString(ReiErr, "pyrei: map region has no x section");
    return NULL;
  }
  return PyMemoryView_FromMemory(
    (char *) mh->shm->addr + mh->h.x_off, (Py_ssize_t) mh->h.x_len,
    PyBUF_READ);
}

// Morsel protocol -----------------------------------------------------------------

PyDoc_STRVAR(map_next_doc,
"_map_next(capsule, signals, ordinal) -> (lo, hi, help) | None\n\n\
One whole batch transition: generation-fenced lane claim (generation 0 in\n\
v1), cancel and pool-signal checks, adaptive sizing, cursor issue. None\n\
means stop: the lane was lost to the trim, the cancel word fired, the\n\
pool is stopping, the owner died, or the cursor is exhausted. [lo, hi) is\n\
the 0-based element range; `help` flags the doorbell. `signals` is a\n\
worker-local pool-signal capsule (never one received from a submitter),\n\
or None to skip the loads. Atomics only, no park: the GIL is held\n\
throughout.");

static PyObject *py_map_next(PyObject *Py_UNUSED(module), PyObject *args) {
  PyObject *caps, *sig_caps;
  uint32_t r;
  if (!PyArg_ParseTuple(args, "OOI:_map_next", &caps, &sig_caps, &r))
    return NULL;
  rei_pymap *mh = pymap_get(caps);
  if (mh == NULL) return NULL;
  if (r >= mh->h.claim_n) {
    PyErr_SetString(ReiErr, "pyrei: runner ordinal out of range");
    return NULL;
  }
  rei_pool_sig *s = NULL;
  if (sig_caps != Py_None) {
    s = (rei_pool_sig *) PyCapsule_GetPointer(sig_caps, REI_PY_SIG_CAPSULE);
    if (s == NULL) {
      PyErr_SetString(ReiErr, "pyrei: not a pool-signal handle");
      return NULL;
    }
  }
  const uint32_t gen = 0;   /* v1: a fresh region per map, no re-arm */

  /* first transition: CAS (gen << 2)|IDLE -> RUNNING — the one atomic
     that both claims the lane and fences the generation. It fails alike
     against ABANDONED (lost to the trim); RUNNING at our generation means
     this very task already claimed it, so later transitions fall straight
     through. */
  _Atomic uint32_t *cw = pymap_claim_word(mh, r);
  uint32_t running = (gen << 2) | REI_PY_MORSEL_RUNNING;
  uint32_t w = atomic_load_explicit(cw, memory_order_acquire);
  if (w == ((gen << 2) | REI_PY_MORSEL_IDLE) &&
      atomic_compare_exchange_strong_explicit(cw, &w, running,
                                              memory_order_seq_cst,
                                              memory_order_acquire))
    w = running;
  if (w != running) Py_RETURN_NONE;

  if (mh->run_r != (int32_t) r) {
    /* run boundary through this ctx: relearn over a fresh ramp */
    mh->run_r = (int32_t) r;
    mh->k = 1;
    mh->k_last = 0;
    mh->cost = 0;
    mh->skip = 0;
  }

  if (atomic_load_explicit(pymap_cancel_word(mh), memory_order_acquire) != 0)
    Py_RETURN_NONE;

  int help = 0;
  if (s != NULL) {
    /* a runner is the one place a worker sits for a whole map without
       touching its step loop, where these words are consumed: None
       unwinds it there within ~a batch instead of at cursor exhaustion */
    if (atomic_load_explicit(s->shutdown, memory_order_relaxed) != 0 ||
        atomic_load_explicit(s->owner_dead, memory_order_relaxed) != 0)
      Py_RETURN_NONE;
    help = atomic_load_explicit(s->help_wanted, memory_order_relaxed) != 0;
  }

  double now = rei_now();
  if (mh->k_last > 0) {
    if (mh->skip) {
      mh->skip = 0;   /* interval contained a helped foreign task */
    } else {
      double per = (now - mh->t_last) / (double) mh->k_last;
      mh->cost = per > 1e-9 ? per : 1e-9;   /* clock-floor trivial f */
    }
    if (mh->cost > 0) {
      double want = REI_PYMAP_T_TARGET / mh->cost;
      uint64_t wk = want >= 1 ? (uint64_t) want : 1;
      /* grow at most 2x per step toward the target; shrink immediately on
         overshoot; clamp to the batch cap */
      mh->k = wk >= mh->k * 2 ? mh->k * 2 : wk;
      if (mh->k > REI_PYMAP_BATCH_CAP) mh->k = REI_PYMAP_BATCH_CAP;
    }
  }
  uint64_t k = mh->k;

  /* relaxed issue: atomicity (unique claim) is all the shared state
     provides; ordering rides the task claim/publish chain. Overshoot of
     up to k is harmless — a runner stops at its first exhausted issue. */
  uint64_t m = atomic_fetch_add_explicit(pymap_cursor_word(mh), k,
                                         memory_order_relaxed);
  if (m >= mh->h.n_morsels) Py_RETURN_NONE;
  if (k > mh->h.n_morsels - m) k = mh->h.n_morsels - m;   /* final grant */
  mh->k_last = k;
  mh->t_last = now;
  if (help) mh->skip = 1;

  uint64_t lo = m * mh->h.morsel_size;
  uint64_t hi = (m + k) * mh->h.morsel_size;
  if (hi > mh->h.n) hi = mh->h.n;
  return Py_BuildValue("(KKi)", (unsigned long long) lo,
                       (unsigned long long) hi, help);
}

PyDoc_STRVAR(map_abandon_doc,
"_map_abandon(capsule, ordinal) -> int\n\n\
The exhausted-runner trim's CAS, folding its own trigger: a no-op unless\n\
the cursor is exhausted or the cancel word is set. Returns the morsel-\n\
state verdict: 2 abandoned (never started, never will — cancel and drop),\n\
1 running (collect it), 0 idle (trigger unarmed: defer).");

static PyObject *py_map_abandon(PyObject *Py_UNUSED(module), PyObject *args) {
  PyObject *caps, *ord;
  if (!PyArg_ParseTuple(args, "OO:_map_abandon", &caps, &ord)) return NULL;
  rei_pymap *mh = pymap_get(caps);
  if (mh == NULL) return NULL;
  uint32_t r;
  if (pymap_ordinal(mh, ord, &r) < 0) return NULL;
  _Atomic uint32_t *cw = pymap_claim_word(mh, r);
  const uint32_t gen = 0;
  int armed =
    atomic_load_explicit(pymap_cursor_word(mh), memory_order_acquire) >=
      mh->h.n_morsels ||
    atomic_load_explicit(pymap_cancel_word(mh), memory_order_acquire) != 0;
  uint32_t w = atomic_load_explicit(cw, memory_order_acquire);
  if (armed)
    while (w == ((gen << 2) | REI_PY_MORSEL_IDLE))
      if (atomic_compare_exchange_strong_explicit(
            cw, &w, (gen << 2) | REI_PY_MORSEL_ABANDONED,
            memory_order_seq_cst, memory_order_acquire))
        return PyLong_FromLong(REI_PY_MORSEL_ABANDONED);
  return PyLong_FromLong((long) (w & 3u));
}

PyDoc_STRVAR(map_cancel_set_doc,
"_map_cancel_set(capsule) -> None\n\n\
Set the cancel word: the submitter on timeout / interrupt / death, or an\n\
erroring runner itself before its ERR publish — the fail-fast store that\n\
stops every peer within ~a batch. Idempotent.");

static PyObject *py_map_cancel_set(PyObject *Py_UNUSED(module),
                                   PyObject *caps) {
  void *p = PyCapsule_GetPointer(caps, REI_PY_MAP_CAPSULE);
  if (p == NULL) PyErr_Clear();   /* foreign capsule: no-op */
  if (p == NULL || p == REI_PYMAP_CLOSED)
    Py_RETURN_NONE;   /* closed or foreign: no-op (runs from unwind paths) */
  atomic_store_explicit(pymap_cancel_word((rei_pymap *) p), 1u,
                        memory_order_seq_cst);
  Py_RETURN_NONE;
}

PyDoc_STRVAR(map_cancel_get_doc,
"_map_cancel_get(capsule) -> bool");

static PyObject *py_map_cancel_get(PyObject *Py_UNUSED(module),
                                   PyObject *caps) {
  rei_pymap *mh = pymap_get(caps);
  if (mh == NULL) return NULL;
  return PyBool_FromLong(
    atomic_load_explicit(pymap_cancel_word(mh), memory_order_acquire) != 0);
}

/* Worker-death lost set, in element space: issued = [0, cursor) clamped to
   n, lost = issued minus the union of the collected batch histories — a
   batch in no history was issued but never completed (its claimant died,
   or fn errored mid-batch); a dead runner's whole history lands here too —
   it publishes only at exhaustion. histories is a list of per-runner
   lists of (lo, hi) 0-based half-open element ranges. Returns a list of
   (lo, hi) tuples. */
typedef struct rei_pyrange_s { uint64_t lo, hi; } rei_pyrange;

static int rei_pyrange_cmp(const void *a, const void *b) {
  uint64_t x = ((const rei_pyrange *) a)->lo;
  uint64_t y = ((const rei_pyrange *) b)->lo;
  return (x > y) - (x < y);
}

PyDoc_STRVAR(map_lost_doc,
"_map_lost(capsule, histories) -> list of (lo, hi)\n\n\
The worker-death lost set: the element ranges issued off the cursor but\n\
covered by no collected batch history.");

static PyObject *py_map_lost(PyObject *Py_UNUSED(module), PyObject *args) {
  PyObject *caps, *runs;
  if (!PyArg_ParseTuple(args, "OO:_map_lost", &caps, &runs)) return NULL;
  rei_pymap *mh = pymap_get(caps);
  if (mh == NULL) return NULL;
  PyObject *seq = PySequence_Fast(
    runs, "pyrei: histories must be a list of range lists");
  if (seq == NULL) return NULL;
  Py_ssize_t nh = PySequence_Fast_GET_SIZE(seq);
  Py_ssize_t total = 0;
  for (Py_ssize_t i = 0; i < nh; i++) {
    PyObject *hist = PySequence_Fast(PySequence_Fast_GET_ITEM(seq, i),
                                     "pyrei: invalid map batch history");
    if (hist == NULL) goto fail_seq;
    total += PySequence_Fast_GET_SIZE(hist);
    Py_DECREF(hist);
  }
  rei_pyrange *b = PyMem_Malloc((size_t) (total > 0 ? total : 1) *
                                sizeof(*b));
  if (b == NULL) {
    PyErr_NoMemory();
    goto fail_seq;
  }
  Py_ssize_t at = 0;
  for (Py_ssize_t i = 0; i < nh; i++) {
    PyObject *hist = PySequence_Fast(PySequence_Fast_GET_ITEM(seq, i),
                                     "pyrei: invalid map batch history");
    if (hist == NULL) goto fail_b;
    Py_ssize_t nb = PySequence_Fast_GET_SIZE(hist);
    for (Py_ssize_t j = 0; j < nb; j++, at++) {
      unsigned long long lo, hi;
      if (!PyArg_ParseTuple(PySequence_Fast_GET_ITEM(hist, j), "KK",
                            &lo, &hi) || hi < lo || hi > mh->h.n) {
        PyErr_SetString(ReiErr, "pyrei: invalid map batch history");
        Py_DECREF(hist);
        goto fail_b;
      }
      b[at].lo = (uint64_t) lo;
      b[at].hi = (uint64_t) hi;
    }
    Py_DECREF(hist);
  }
  qsort(b, (size_t) total, sizeof(*b), rei_pyrange_cmp);
  uint64_t cur = atomic_load_explicit(pymap_cursor_word(mh),
                                      memory_order_acquire);
  if (cur > mh->h.n_morsels) cur = mh->h.n_morsels;
  uint64_t issued = cur * mh->h.morsel_size;
  if (issued > mh->h.n) issued = mh->h.n;

  PyObject *out = PyList_New(0);
  if (out == NULL) goto fail_b;
  uint64_t pos = 0;
  for (Py_ssize_t i = 0; i <= total; i++) {
    /* one gap per step: before range i, then the tail before `issued` */
    uint64_t next = i < total ? b[i].lo : issued;
    if (next > pos) {
      uint64_t glo = pos, ghi = next;
      if (ghi > issued) ghi = issued;
      if (ghi > glo) {
        PyObject *r = Py_BuildValue("(KK)", (unsigned long long) glo,
                                    (unsigned long long) ghi);
        if (r == NULL || PyList_Append(out, r) < 0) {
          Py_XDECREF(r);
          Py_DECREF(out);
          goto fail_b;
        }
        Py_DECREF(r);
      }
    }
    if (i < total && b[i].hi > pos) pos = b[i].hi;
  }
  PyMem_Free(b);
  Py_DECREF(seq);
  return out;

fail_b:
  PyMem_Free(b);
fail_seq:
  Py_DECREF(seq);
  return NULL;
}

PyDoc_STRVAR(map_probe_x_doc,
"_map_probe_x(x) -> (tag, n_elts, n_bytes) | None\n\n\
The raw-x gate: a C-contiguous buffer of a supported dtype (the channel's\n\
dtype map) rides the region as bare bytes; anything else returns None and\n\
x rides the descriptor.");

static PyObject *py_map_probe_x(PyObject *Py_UNUSED(module), PyObject *arg) {
  if (!PyObject_CheckBuffer(arg)) Py_RETURN_NONE;
  Py_buffer v;
  if (PyObject_GetBuffer(arg, &v, PyBUF_C_CONTIGUOUS | PyBUF_FORMAT) < 0) {
    PyErr_Clear();
    Py_RETURN_NONE;
  }
  int type = rei_py_wire_type_of(&v);
  if (type == 0) {
    PyBuffer_Release(&v);
    Py_RETURN_NONE;
  }
  size_t elt = rei_type_elt_size(type);
  PyObject *out = Py_BuildValue("(iKK)", type,
                                (unsigned long long) ((size_t) v.len / elt),
                                (unsigned long long) v.len);
  PyBuffer_Release(&v);
  return out;
}

// Registration -----------------------------------------------------------------

static PyMethodDef pymap_methods[] = {
  {"_map_stage", (PyCFunction) py_map_stage, METH_VARARGS, map_stage_doc},
  {"_map_close", (PyCFunction) py_map_close, METH_O, map_close_doc},
  {"_map_open", (PyCFunction) py_map_open, METH_O, map_open_doc},
  {"_map_header", (PyCFunction) py_map_header, METH_O, map_header_doc},
  {"_map_desc", (PyCFunction) py_map_desc, METH_O, map_desc_doc},
  {"_map_x_view", (PyCFunction) py_map_x_view, METH_O, map_x_view_doc},
  {"_map_next", (PyCFunction) py_map_next, METH_VARARGS, map_next_doc},
  {"_map_abandon", (PyCFunction) py_map_abandon, METH_VARARGS,
   map_abandon_doc},
  {"_map_cancel_set", (PyCFunction) py_map_cancel_set, METH_O,
   map_cancel_set_doc},
  {"_map_cancel_get", (PyCFunction) py_map_cancel_get, METH_O,
   map_cancel_get_doc},
  {"_map_lost", (PyCFunction) py_map_lost, METH_VARARGS, map_lost_doc},
  {"_map_probe_x", (PyCFunction) py_map_probe_x, METH_O, map_probe_x_doc},
  {NULL, NULL, 0, NULL}
};

int rei_py_map_register(PyObject *m, PyObject *rei_error,
                        PyObject *shm_error) {
  ReiErr = rei_error;
  ReiShmErr = shm_error;
  Py_INCREF(ReiErr);
  Py_INCREF(ReiShmErr);
  for (PyMethodDef *def = pymap_methods; def->ml_name != NULL; def++) {
    PyObject *fn = PyCFunction_New(def, NULL);
    if (fn == NULL || PyModule_AddObject(m, def->ml_name, fn) < 0) {
      Py_XDECREF(fn);
      return -1;
    }
  }
  return 0;
}
