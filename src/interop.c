/* interop.c — the 'I' interchange codec: the value walk (what qualifies,
 * in what order), the builder over the core's validating pull cursor, the
 * Frame columnar home's wire side, and the __arrow_c_stream__ stage
 * front-end (the Frame object itself is interop_frame.c's).
 *
 * The vendored core owns the byte grammar (bounds, the depth cap, UTF-8
 * validity, the informative unknown-tag / unknown-version declines) through
 * the mizu_ix_* cursor and emit helpers; this file owns the native-object
 * walk (with its reference counting), the decline record, and the Arrow
 * shape decisions. DESIGN.md's Interchange codec section is the spec.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <math.h>
#include <stdatomic.h>
#include <string.h>

#include "pyinterop.h"
#include <structmember.h>
#include "pyshmframe.h"
#include "pyframe.h"

static PyObject *MizuError;
static PyObject *MizuDeclinedError;

/* The cursor failure's exception: the core's thread-local message. void
   so the return-NULL / return -1 / goto fail sites share the one call. */
static void ixr_cursor_err(void) {
  PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
}

static void store_na_r(uint8_t *dst) {
  const uint64_t bits = PYMIZU_NA_REAL_BITS;
  memcpy(dst, &bits, 8);
}

/* The UTF-8 rule single-sources through the core's mizu_ix_utf8_valid
   (the byte-shape helper registry; the vendored mizu_ext.h inline). */

/* Days-from-civil (Howard Hinnant's algorithm): days since 1970-01-01. */
static int64_t days_from_civil(int64_t y, int64_t m, int64_t d) {
  y -= m <= 2;
  int64_t era = (y >= 0 ? y : y - 399) / 400;
  uint64_t yoe = (uint64_t) (y - era * 400);
  uint64_t doy = (153 * (uint64_t) (m + (m > 2 ? -3 : 9)) + 2) / 5 +
    (uint64_t) (d - 1);
  uint64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + (int64_t) doe - 719468;
}

/* The datetime module's date/datetime types, probed through sys.modules
   (never imported here: a date object exists only when the user imported
   datetime — the buffer-subclass probe's discipline). */
static PyTypeObject *mizu_dt_date, *mizu_dt_datetime, *mizu_dt_timedelta;
static int mizu_dt_probed;

static void datetime_probe(void) {
  if (mizu_dt_probed) return;
  mizu_dt_probed = 1;
  PyObject *mods = PyImport_GetModuleDict();
  PyObject *dt = PyMapping_GetItemString(mods, "datetime");
  if (dt == NULL) {
    PyErr_Clear();
    return;
  }
  if (dt != Py_None) {
    PyObject *d = PyObject_GetAttrString(dt, "date");
    if (d != NULL && PyType_Check(d)) mizu_dt_date = (PyTypeObject *) d;
    else Py_XDECREF(d);
    PyObject *t = PyObject_GetAttrString(dt, "datetime");
    if (t != NULL && PyType_Check(t)) mizu_dt_datetime = (PyTypeObject *) t;
    else Py_XDECREF(t);
    PyObject *td = PyObject_GetAttrString(dt, "timedelta");
    if (td != NULL && PyType_Check(td))
      mizu_dt_timedelta = (PyTypeObject *) td;
    else Py_XDECREF(td);
    if (mizu_dt_date == NULL || mizu_dt_datetime == NULL ||
        mizu_dt_timedelta == NULL)
      PyErr_Clear();
  }
  Py_DECREF(dt);
}

/* ok NULL: the exact-typed stdlib objects — the C-level getsets cannot
   fail (the error clear is unconditional insurance). A live ok: the
   utcoffset result — PyDelta_Check admits a timedelta subclass, which can
   shadow the attributes. */
static int64_t attr_as_long(PyObject *obj, const char *name, int *ok) {
  PyObject *v = PyObject_GetAttrString(obj, name);
  if (v == NULL) {
    if (ok != NULL) *ok = 0;
    else PyErr_Clear();
    return 0;
  }
  long long x = PyLong_AsLongLong(v);
  Py_DECREF(v);
  if (x == -1 && PyErr_Occurred()) {
    if (ok != NULL) *ok = 0;
    else PyErr_Clear();
    return 0;
  }
  return (int64_t) x;
}

// The writer ------------------------------------------------------------------------

/* The W2 plan record: the task walk's record mode captures the first 16
   SHM_VEC candidates in document order, each with its by-value subtree
   size (a recorded candidate's subtree walks with detection suppressed —
   the never-nest rule). The selection is arithmetic: the first candidate
   with total - size[i] + the conservative ref-leaf reservation <=
   inline_max is the zc node. */
typedef struct {
  PyObject *cand[16];
  size_t size[16];
  int ncand;
} ixp_plan;

/* The walk state: dst NULL sizes (and validates — every decline is found
   in count mode); a real dst writes behind limit, counting past it — the
   first walk may write into the slot payload while sizing, a decline or
   the overflow unwinding with nothing committed (transactional staging).
   The decline record (path + reason) becomes the DeclinedError; warn
   accumulates the conversion warnings through the write pass (raised
   after it completes, never mid-write).

   The F1 refs state: with refs, a re-sendable view emits a 0x13 leaf
   (the caps filter read off the peer word, skipped when h is NULL) and
   the plan's zc_node stages the stream's one SHM_VEC checkout on its
   first encounter — a mid-write checkout failure sets abandon and
   unwinds (the caller re-runs with no_zc = 1). ref_emitted reports a
   ref went out: the frame pins the spec and never claims keeperless. */
typedef struct {
  uint8_t *dst;
  size_t total;
  size_t limit;
  int overflow;          /* the write exceeded limit; the count runs on */
  int depth;
  int decline;
  char path[128];
  size_t path_len;
  char reason[192];
  cvt_warn warn;
  mizu_handle *h;
  uint32_t caps;
  uint32_t inline_max;
  int churn;
  int no_zc;
  int refs;
  PyObject *zc_node;
  int zc_spent;
  int abandon;
  int ref_emitted;
  /* non-NULL runs the walk in record mode (W2): the optimistic write
     flips to count-only at the first SHM_VEC candidate, the candidates
     recorded for the caller's arithmetic selection */
  ixp_plan *plan;
  int plan_suppress;
} ixw;

// The corpus's ref marker ---------------------------------------------------------

/* A region identifier standing in for a view, so the conformance loop
   round-trips synthetic identifiers without a region: the hook writer
   (h == NULL) emits it as a 0x13 leaf; the hook decode represents one. */
typedef struct {
  PyObject_HEAD
  PyObject *id;
} MizuIxRef;

static void ixref_dealloc(PyObject *self) {
  Py_XDECREF(((MizuIxRef *) self)->id);
  Py_TYPE(self)->tp_free(self);
}

static int ixref_init(PyObject *self, PyObject *args,
                      PyObject *Py_UNUSED(kwargs)) {
  PyObject *id = NULL;
  if (!PyArg_ParseTuple(args, "U", &id)) return -1;
  Py_INCREF(id);
  Py_XSETREF(((MizuIxRef *) self)->id, id);
  return 0;
}

static PyMemberDef ixref_members[] = {
  { "id", T_OBJECT_EX, offsetof(MizuIxRef, id), READONLY,
    "the region identifier" },
  { NULL, 0, 0, 0, NULL }
};

static PyTypeObject MizuIxRefType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "_pymizu._IxRef",
  .tp_basicsize = sizeof(MizuIxRef),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "The corpus's ref marker: a region identifier standing in "
            "for a view.",
  .tp_dealloc = ixref_dealloc,
  .tp_init = ixref_init,
  .tp_new = PyType_GenericNew,
  .tp_members = ixref_members,
};

static PyObject *ixr_ref_marker(const unsigned char *ptr, uint64_t len) {
  MizuIxRef *self =
    (MizuIxRef *) MizuIxRefType.tp_alloc(&MizuIxRefType, 0);
  if (self == NULL) return NULL;
  self->id =
    PyUnicode_FromStringAndSize((const char *) ptr, (Py_ssize_t) len);
  if (self->id == NULL) {
    Py_DECREF(self);
    return NULL;
  }
  return (PyObject *) self;
}

#define IXW_DST(w) ((w)->dst != NULL ? (w)->dst + (w)->total : NULL)
/* One emit: the put helpers count with a NULL dst; past the limit the
   count runs on but the writes stop (the codec.c overflow discipline).
   The call evaluates twice with plain-value arguments — the size query
   first (dst masked to NULL), then the write when it fits. */
#define IXW_PUT(w, call) do { \
    if (!(w)->decline) { \
      if ((w)->dst == NULL) { (w)->total += (call); break; } \
      uint8_t *save_ = (w)->dst; (w)->dst = NULL; \
      size_t n_ = (call); \
      (w)->dst = save_; \
      if ((w)->total + n_ <= (w)->limit) { \
        (void) (call); \
      } else { \
        (w)->overflow = 1; (w)->dst = NULL; \
      } \
      (w)->total += n_; \
    } \
  } while (0)

static void ixw_decline(ixw *w, const char *reason) {
  if (!w->decline) {
    w->decline = 1;
    snprintf(w->reason, sizeof(w->reason), "%s", reason);
  }
}

static void ixw_decline_type(ixw *w, PyObject *obj, const char *what) {
  if (!w->decline) {
    w->decline = 1;
    snprintf(w->reason, sizeof(w->reason), "%s (type '%s')", what,
             Py_TYPE(obj)->tp_name);
  }
}

static void ixw_path_index(ixw *w, Py_ssize_t i) {
  size_t save = w->path_len;
  int n = snprintf(w->path + w->path_len, sizeof(w->path) - w->path_len,
                   "[%zd]", i);
  w->path_len += (n > 0 && (size_t) n < sizeof(w->path) - save) ?
    (size_t) n : 0;
}

static void ixw_path_pop(ixw *w, size_t save) {
  w->path[save] = '\0';
  w->path_len = save;
}

static void ixw_path_key(ixw *w, PyObject *key) {
  PyObject *r = PyObject_Repr(key);
  if (r != NULL) {
    snprintf(w->path + w->path_len, sizeof(w->path) - w->path_len, "[%s]",
             PyUnicode_AsUTF8(r));
    Py_DECREF(r);
  }
  w->path_len = strnlen(w->path, sizeof(w->path));
}

static void ixw_node(ixw *w, PyObject *obj);

/* The vector-tag wire header (tag + u64 count) for the emit-bytes forms;
   the memcpy forms use mizu_ix_put_vec directly. */
static size_t ixe_vec_begin(uint8_t *dst, int tag, uint64_t n) {
  if (dst != NULL) {
    dst[0] = (uint8_t) tag;
    memcpy(dst + 1, &n, 8);
  }
  return 9;
}

/* The wire-type -> vector-tag table is the core's mizu_ix_tag_of (the
   byte-shape helper registry), used via ixe_vec_begin below. */

/* A strv value, or the 0x04 scalar at length 1 (the corpus's scalar-form
   pin — attr dicts included). A len of -1 is NA (strs[i] then unused). */
static void ixe_strv(ixw *w, Py_ssize_t n, const char **strs,
                     const int64_t *lens) {
  if (n == 1) {
    IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), strs[0], (int32_t) lens[0]));
    return;
  }
  IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) n));
  for (Py_ssize_t i = 0; i < n; i++)
    IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), strs[i], (int32_t) lens[i]));
}

/* The list[str | None] predicate, the one home of the rule: an exact
   list, every element an exact str or None with at least one str, every
   str UTF-8 encodable (a lone surrogate fails the probe). An all-None
   list is not a string vector: [None] crosses as a 0x0c list of nil (R's
   list(NULL), the depth-64 corpus pin) at every size. The 'I' writer's
   list branch and the MIZS tiers (stage_mizs_list, layout.c's leaf) gate
   on it, so tier choice by size never changes the type the value crosses
   as. 1 hit (str_bytes takes the UTF-8 byte sum), 0 not. */
int mizu_py_strlist_probe(PyObject *obj, int64_t *str_bytes) {
  if (!PyList_CheckExact(obj)) return 0;
  const Py_ssize_t n = PyList_GET_SIZE(obj);
  int64_t bytes = 0;
  int any_str = 0;
  for (Py_ssize_t i = 0; i < n; i++) {
    PyObject *elt = PyList_GET_ITEM(obj, i);
    if (elt == Py_None) continue;
    if (!PyUnicode_CheckExact(elt)) return 0;
    Py_ssize_t len;
    if (PyUnicode_AsUTF8AndSize(elt, &len) == NULL) {
      PyErr_Clear();   /* lone surrogates decline */
      return 0;
    }
    any_str = 1;
    /* a str past the wire's int32 length is not a string vector: the
       generic walk declines it with the top-level path's reason */
    if (len > INT32_MAX) return 0;
    bytes += (int64_t) len;
  }
  if (!any_str) return 0;
  *str_bytes = bytes;
  return 1;
}

/* A probed list[str | None] as the strv value (the ixe_strv shape: the
   0x04 scalar at length 1, None the NA element). The probe cached the
   UTF-8 forms, so the re-encode cannot fail — check anyway: a decline is
   transactional, a write past NULL is not. */
static void ixe_strlist(ixw *w, PyObject *obj) {
  const Py_ssize_t n = PyList_GET_SIZE(obj);
  if (n == 1) {
    /* the probe's any_str rule: the single element is always a string */
    PyObject *elt = PyList_GET_ITEM(obj, 0);
    Py_ssize_t len;
    const char *s = PyUnicode_AsUTF8AndSize(elt, &len);
    if (s == NULL) {
      PyErr_Clear();
      ixw_decline(w, "a string is not writable as UTF-8 (a lone surrogate)");
      return;
    }
    IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), s, (int32_t) len));
    return;
  }
  IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) n));
  for (Py_ssize_t i = 0; i < n && !w->decline; i++) {
    PyObject *elt = PyList_GET_ITEM(obj, i);
    if (elt == Py_None) {
      IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), NULL, -1));
      continue;
    }
    Py_ssize_t len;
    const char *s = PyUnicode_AsUTF8AndSize(elt, &len);
    if (s == NULL) {
      PyErr_Clear();
      ixw_decline(w, "a string is not writable as UTF-8 (a lone surrogate)");
      return;
    }
    IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), s, (int32_t) len));
  }
}

/* An intv value (the callers pass n >= 2). Vector elements must fit int32
   (the callers' contract). */
static void ixe_intv(ixw *w, Py_ssize_t n, const int64_t *vals,
                     const int *na) {
  IXW_PUT(w, ixe_vec_begin(IXW_DST(w), MIZU_IX_TAG_INTV, (uint64_t) n));
  if (w->decline) return;
  if (w->dst != NULL && w->total + (size_t) n * 4 <= w->limit) {
    uint8_t *dst = w->dst + w->total;
    for (Py_ssize_t i = 0; i < n; i++) {
      int32_t v = na != NULL && na[i] ? MIZU_NA_INT32 : (int32_t) vals[i];
      memcpy(dst + 4 * i, &v, 4);
    }
  } else if (w->dst != NULL) {
    w->overflow = 1;
    w->dst = NULL;
  }
  w->total += (size_t) n * 4;
}

/* An intv from an i32 span (row.names reads). */
static void ixe_intv_i32(ixw *w, const int32_t *vals, Py_ssize_t n) {
  if (n == 1) {
    int64_t v = vals[0] == MIZU_NA_INT32 ? INT64_MIN : vals[0];
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), v));
    return;
  }
  IXW_PUT(w, ixe_vec_begin(IXW_DST(w), MIZU_IX_TAG_INTV, (uint64_t) n));
  if (w->decline) return;
  if (w->dst != NULL && w->total + (size_t) n * 4 <= w->limit) {
    memcpy(w->dst + w->total, vals, (size_t) n * 4);
  } else if (w->dst != NULL) {
    w->overflow = 1;
    w->dst = NULL;
  }
  w->total += (size_t) n * 4;
}

static void ixe_key(ixw *w, const char *key) {
  IXW_PUT(w, mizu_ix_put_key(IXW_DST(w), key, (uint32_t) strlen(key)));
}

/* The class attribute: strv form, scalar at length 1. */
static void ixe_class(ixw *w, const char *const *names, Py_ssize_t n) {
  const char *strs[2] = { NULL, NULL };
  int64_t lens[2] = { 0, 0 };
  for (Py_ssize_t i = 0; i < n; i++) {
    strs[i] = names[i];
    lens[i] = (int64_t) strlen(names[i]);
  }
  ixe_strv(w, n, strs, lens);
}

/* A realv value from an int64-count source with a per-unit scale (the
   temporal shapes' value half): out = count * scale, NaT (INT64_MIN) ->
   the NA payload. */
/* The REALV body off int64 counts: valid NULL keys on the owned sentinel
   (INT64_MIN); an adopted column's nulls ride the bitmap instead. */
static void ixe_realv_counts(ixw *w, const int64_t *counts,
                             const uint8_t *valid, uint64_t n,
                             double scale) {
  IXW_PUT(w, ixe_vec_begin(IXW_DST(w), MIZU_IX_TAG_REALV, n));
  if (w->decline) return;
  if (w->dst != NULL && w->total + (size_t) n * 8 <= w->limit) {
    uint8_t *dst = w->dst + w->total;
    for (uint64_t i = 0; i < n; i++) {
      if ((valid != NULL && !bitmap_at(valid, i)) ||
          counts[i] == INT64_MIN) {
        store_na_r(dst + 8 * i);
      } else {
        double v = (double) counts[i] * scale;
        memcpy(dst + 8 * i, &v, 8);
      }
    }
  } else if (w->dst != NULL) {
    w->overflow = 1;
    w->dst = NULL;
  }
  w->total += (size_t) n * 8;
}

/* The attr shapes emit frames the value walk's depth accounting never
   counted: the cursor pushes the attr frame, the value container, the
   attrs dict, and a vector value inside the dict. k is the worst-case
   extra pushes past w->depth (ix_push fails at 64); decline at the send —
   the depth discipline — rather than ship a stream every reader
   rejects. */
static int ixw_attr_depth(ixw *w, int k) {
  if (w->depth + k <= MIZU_IX_DEPTH_MAX) return 1;
  ixw_decline(w, "the value nests past the depth cap (64)");
  return 0;
}

/* The attr shapes (the corpus-pinned forms):
   factor:  attr(intv codes, {levels: strv, class: "factor"})
   Date:    attr(realv days, {class: "Date"})
   POSIXct: attr(realv seconds, {class: ["POSIXct", "POSIXt"], tzone: tz})
*/
static void ixe_date(ixw *w, const int64_t *days, uint64_t n, double scale) {
  static const char *cls[1] = { MIZU_IX_CLASS_DATE };
  if (!ixw_attr_depth(w, 2)) return;   /* attr + vec (scalar class) */
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_realv_counts(w, days, NULL, n, scale);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 1));
  ixe_key(w, MIZU_IX_ATTR_CLASS);
  ixe_class(w, cls, 1);
}

static void ixe_posixct(ixw *w, const int64_t *counts, const uint8_t *valid,
                        uint64_t n, const char *tz, double scale) {
  static const char *cls[2] = { MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT };
  if (!ixw_attr_depth(w, 3)) return;   /* attr + vec + dict's class strv */
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_realv_counts(w, counts, valid, n, scale);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 2));
  ixe_key(w, MIZU_IX_ATTR_CLASS);
  ixe_class(w, cls, 2);
  ixe_key(w, MIZU_IX_ATTR_TZONE);
  const char *one[1] = { tz };
  int64_t len[1] = { (int64_t) strlen(tz) };
  ixe_strv(w, 1, one, len);
}

/* The difftime shape: attr(realv seconds, {class: "difftime", units:
   "secs"}) — counts an int64 span of some time unit, scale the unit in
   seconds. R's five units normalize to seconds on the wire; the R reader
   stamps the value with units = "secs". */
static void ixe_difftime(ixw *w, const int64_t *counts,
                         const uint8_t *valid, uint64_t n, double scale) {
  static const char *cls[1] = { MIZU_IX_CLASS_DIFFTIME };
  if (!ixw_attr_depth(w, 2)) return;   /* attr + vec (scalar units) */
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_realv_counts(w, counts, valid, n, scale);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 2));
  ixe_key(w, MIZU_IX_ATTR_CLASS);
  ixe_class(w, cls, 1);
  ixe_key(w, MIZU_IX_ATTR_UNITS);
  const char *one[1] = { MIZU_IX_UNIT_SECS };
  int64_t len[1] = { 4 };
  ixe_strv(w, 1, one, len);
}

// The layout attribute blobs (pyshmframe.h's; complete 'I' streams) ----------------

/* The labeled shapes' writer helpers, defined with the writer below:
   the caps gate, the stashed class vector, and the extras' raw splice. */
static int ixw_labeled_gate(ixw *w);
static void ixe_class_tuple(ixw *w, PyObject *kt);
static void ixe_span(ixw *w, const void *src, size_t n);

/* The frame dict's names value: the corpus's scalar form at length 1. */
static void ixe_names(ixw *w, char **names, int n) {
  if (n == 1) {
    IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), names[0],
                               (int32_t) strlen(names[0])));
    return;
  }
  IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) n));
  for (int i = 0; i < n; i++)
    IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), names[i],
                                  (int32_t) strlen(names[i])));
}

/* The frame dict {names, class, row.names[, extras...]} with a custom
   row_names (a same-language Frame's int32 or character form; the
   automatic c(NA, -n) form is the core writer's) and, when r_attrs is
   set, the stashed class vector verbatim plus the extras spliced raw in
   stored order — the extended frame's blob form (MIZU_CAP_LABELED;
   caps must carry the bit). 0 with an exception set on a malformed
   row_names, a malformed stash, or a caps-less peer (never on the size
   pass's own account — the caller clears and falls to the next tier).
   The attribute-blob emitters for the layout leaves (factor levels,
   Date, POSIXct, difftime) moved upstream to libmizu's arrow.c. */
size_t mizu_py_blob_frame(uint8_t *dst, char **names, int ncols, int64_t rows,
                          PyObject *row_names, PyObject *r_attrs,
                          uint32_t caps) {
  ixw w;
  memset(&w, 0, sizeof(w));
  w.dst = dst;
  w.caps = caps;
  w.limit = SIZE_MAX;   /* the caller's buffer: unbounded (size pass NULL) */
  if (r_attrs != NULL && !ixw_labeled_gate(&w)) {
    PyErr_SetString(MizuDeclinedError, w.reason);
    return 0;
  }
  const int nex = r_attrs != NULL ?
    (int) PyList_GET_SIZE(PyTuple_GET_ITEM(r_attrs, 1)) : 0;
  IXW_PUT(&w, mizu_ix_put_header(IXW_DST(&w)));
  IXW_PUT(&w, mizu_ix_put_dict_begin(IXW_DST(&w), (uint64_t) (3 + nex)));
  ixe_key(&w, MIZU_IX_ATTR_NAMES);
  ixe_names(&w, names, ncols);
  ixe_key(&w, MIZU_IX_ATTR_CLASS);
  if (r_attrs != NULL) {
    ixe_class_tuple(&w, PyTuple_GET_ITEM(r_attrs, 0));
    if (w.decline) {
      PyErr_SetString(MizuError, w.reason);
      return 0;
    }
  } else {
    static const char *cls[1] = { MIZU_IX_CLASS_DATAFRAME };
    ixe_class(&w, cls, 1);
  }
  ixe_key(&w, MIZU_IX_ATTR_ROWNAMES);
  if (row_names == Py_None) {
    /* the automatic c(NA, -n) form (the stash triggers the blob on its
       own) */
    int32_t v[2];
    v[0] = MIZU_NA_INT32;
    v[1] = rows != 0 ? (int32_t) -rows : 0;
    IXW_PUT(&w, mizu_ix_put_vec(IXW_DST(&w), MIZU_TYPE_INT, v, 2));
    goto extras;
  }
  if (PyList_Check(row_names)) {
    Py_ssize_t nr = PyList_GET_SIZE(row_names);
    if (nr != (Py_ssize_t) rows) goto malformed;
    if (nr == 1) {
      Py_ssize_t len;
      const char *u =
        PyUnicode_AsUTF8AndSize(PyList_GET_ITEM(row_names, 0), &len);
      if (u == NULL) return 0;
      if (len > INT32_MAX) goto malformed;
      IXW_PUT(&w, mizu_ix_put_str(IXW_DST(&w), u, (int32_t) len));
    } else {
      IXW_PUT(&w, mizu_ix_put_strv_begin(IXW_DST(&w), (uint64_t) nr));
      for (Py_ssize_t i = 0; i < nr; i++) {
        Py_ssize_t len;
        const char *u =
          PyUnicode_AsUTF8AndSize(PyList_GET_ITEM(row_names, i), &len);
        if (u == NULL) return 0;
        if (len > INT32_MAX) goto malformed;
        IXW_PUT(&w, mizu_ix_put_strelt(IXW_DST(&w), u, (int32_t) len));
      }
    }
    goto extras;
  }
  if (PyObject_CheckBuffer(row_names)) {
    Py_buffer v;
    if (PyObject_GetBuffer(row_names, &v, PyBUF_ND | PyBUF_FORMAT) < 0)
      return 0;
    int ok = v.strides == NULL && v.ndim == 1 &&
      v.len == (Py_ssize_t) rows * 4 &&
      mizu_py_wire_type_of(&v) == MIZU_TYPE_INT;
    if (ok) ixe_intv_i32(&w, (const int32_t *) v.buf, (Py_ssize_t) rows);
    PyBuffer_Release(&v);
    if (!ok) goto malformed;
    goto extras;
  }
malformed:
  PyErr_SetString(MizuError, "pymizu: a Frame's row_names are not int32 "
                  "or character of the row count");
  return 0;
extras:
  if (w.decline) {
    PyErr_SetString(MizuError, w.reason);
    return 0;
  }
  /* the extras: (key, raw span) pairs in stored order, spliced verbatim
     — never decoded */
  for (int i = 0; i < nex && !w.decline; i++) {
    PyObject *t = PyList_GET_ITEM(PyTuple_GET_ITEM(r_attrs, 1), i);
    PyObject *ks = PyTuple_GET_ITEM(t, 0);
    PyObject *bs = PyTuple_GET_ITEM(t, 1);
    Py_ssize_t kn;
    const char *k = PyUnicode_AsUTF8AndSize(ks, &kn);
    if (k == NULL) {
      PyErr_Clear();
      PyErr_SetString(MizuError, "pymizu: a stashed frame extra's key "
                      "is not writable as UTF-8");
      return 0;
    }
    IXW_PUT(&w, mizu_ix_put_key(IXW_DST(&w), k, (uint32_t) kn));
    ixe_span(&w, PyBytes_AS_STRING(bs), (size_t) PyBytes_GET_SIZE(bs));
  }
  if (w.decline) {
    PyErr_SetString(MizuError, w.reason);
    return 0;
  }
  return w.total;
}

// The builder -----------------------------------------------------------------------

/* np.empty(dims, dtype=dt[, order="F"]); NULL with an exception set. */
/* Every caller has already gated on numpy's presence. */
static PyObject *ixr_np_alloc(const Py_ssize_t *dims, int nd, const char *dt,
                              int fortran) {
  PyObject *empty = PyObject_GetAttrString(mizu_py_numpy_module(), "empty");
  if (empty == NULL) return NULL;
  PyObject *shape = PyTuple_New(nd);
  PyObject *arr = NULL;
  if (shape != NULL) {
    for (int i = 0; i < nd; i++) {
      PyObject *d = PyLong_FromSsize_t(dims[i]);
      if (d == NULL) {
        Py_CLEAR(shape);
        break;
      }
      PyTuple_SET_ITEM(shape, i, d);
    }
  }
  PyObject *dts = shape != NULL ? PyUnicode_FromString(dt) : NULL;
  PyObject *kw = PyDict_New();
  PyObject *ord = NULL;
  if (kw != NULL && dts != NULL &&
      PyDict_SetItemString(kw, "dtype", dts) == 0) {
    if (fortran) {
      ord = PyUnicode_FromString("F");
      if (ord == NULL || PyDict_SetItemString(kw, "order", ord) < 0)
        Py_CLEAR(kw);
    }
  } else {
    Py_CLEAR(kw);
  }
  Py_XDECREF(dts);
  Py_XDECREF(ord);
  if (kw != NULL) {
    PyObject *args = PyTuple_Pack(1, shape);
    if (args != NULL) {
      arr = PyObject_Call(empty, args, kw);
      Py_DECREF(args);
    }
  }
  Py_XDECREF(shape);
  Py_XDECREF(kw);
  Py_DECREF(empty);
  return arr;
}

/* Fill arr's buffer (conv(dst, ctx), or a plain memcpy when src is given);
   the size matches n by construction (the fresh allocation above). An
   F-order array's buffer refuses the C-contiguous request — it IS the
   wire's layout, so either contiguity fills by one memcpy. */
static int ixr_np_fill(PyObject *arr, void (*conv)(uint8_t *, const void *),
                       const void *ctx, const void *src, size_t n) {
  Py_buffer v;
  if (PyObject_GetBuffer(arr, &v, PyBUF_ND | PyBUF_WRITABLE) < 0) {
    PyErr_Clear();
    if (PyObject_GetBuffer(arr, &v, PyBUF_F_CONTIGUOUS | PyBUF_WRITABLE) < 0)
      return -1;
  }
  if (src != NULL) memcpy(v.buf, src, n);
  else conv((uint8_t *) v.buf, ctx);
  PyBuffer_Release(&v);
  return 0;
}

/* The no-numpy vector home: a memoryview over a fresh bytearray copy
   (read_raw's fallback — sentinels in place, documented). */
PyObject *ixr_memoryview(const void *src, size_t n) {
  PyObject *ba = PyByteArray_FromStringAndSize((const char *) src,
                                               (Py_ssize_t) n);
  if (ba == NULL) return NULL;
  PyObject *mv = PyMemoryView_FromObject(ba);
  Py_DECREF(ba);
  return mv;
}

void conv_lgl_bool(uint8_t *dst, const void *ctx) {
  const i32_span *s = (const i32_span *) ctx;
  for (uint64_t i = 0; i < s->n; i++) dst[i] = (uint8_t) (s->p[i] != 0);
}

/* Date days -> i64 days, the NA sentinel shifted (to_dict's
   datetime64[D] conversion). */
void conv_days_i64(uint8_t *dst, const void *ctx) {
  const i32_span *s = (const i32_span *) ctx;
  for (uint64_t i = 0; i < s->n; i++) {
    int64_t v = s->p[i] == MIZU_NA_INT32 ? INT64_MIN : (int64_t) s->p[i];
    memcpy(dst + 8 * i, &v, 8);
  }
}

static void conv_int_f64(uint8_t *dst, const void *ctx) {
  const i32_span *s = (const i32_span *) ctx;
  for (uint64_t i = 0; i < s->n; i++) {
    if (s->p[i] == MIZU_NA_INT32) {
      store_na_r(dst + 8 * i);
    } else {
      double v = (double) s->p[i];
      memcpy(dst + 8 * i, &v, 8);
    }
  }
}

/* A vector read to a 1-D numpy array of dtype dt (memcpy, or conv when not
   NULL). */
PyObject *ixr_vec_conv(const char *dt, const uint8_t *ptr,
                              uint64_t count, size_t elt,
                              void (*conv)(uint8_t *, const void *),
                              const void *ctx) {
  Py_ssize_t dims[1] = { (Py_ssize_t) count };
  PyObject *arr = ixr_np_alloc(dims, 1, dt, 0);
  if (arr == NULL) return NULL;
  if (ixr_np_fill(arr, conv, ctx, conv == NULL ? ptr : NULL,
                  (size_t) count * elt) < 0) {
    Py_DECREF(arr);
    return NULL;
  }
  return arr;
}

PyObject *ixr_vec_raw(const char *dt, const uint8_t *ptr,
                             uint64_t count, size_t elt) {
  PyObject *np = mizu_py_numpy_module();
  if (np == NULL) return ixr_memoryview(ptr, (size_t) count * elt);
  return ixr_vec_conv(dt, ptr, count, elt, NULL, NULL);
}

/* The fused lglv/intv read rule (the copy-tier half of the dtype matrix):
   lglv -> bool_ when no INT_MIN, else int32 with the sentinel; intv ->
   int32 when no INT_MIN, else float64 with R's NA_real_ payload (every
   int32 exact there). i64v keeps its dtype and warns on the sentinel.
   Without numpy: a memoryview copy, sentinels in place (documented). */
static PyObject *ixr_vec(int wire_type, const uint8_t *ptr, uint64_t count) {
  size_t n = (size_t) count;
  switch (wire_type) {
  case MIZU_TYPE_LGL: {
    int na = span_has_na32((const int32_t *) ptr, n);
    PyObject *np = mizu_py_numpy_module();
    if (np == NULL) return ixr_memoryview(ptr, n * 4);
    if (na) return ixr_vec_conv("int32", ptr, count, 4, NULL, NULL);
    i32_span s = { (const int32_t *) ptr, count };
    return ixr_vec_conv("bool", ptr, count, 1, conv_lgl_bool, &s);
  }
  case MIZU_TYPE_INT: {
    int na = span_has_na32((const int32_t *) ptr, n);
    if (!na) return ixr_vec_raw("int32", ptr, count, 4);
    PyObject *np = mizu_py_numpy_module();
    if (np == NULL) return ixr_memoryview(ptr, n * 4);
    i32_span s = { (const int32_t *) ptr, count };
    return ixr_vec_conv("float64", ptr, count, 8, conv_int_f64, &s);
  }
  case MIZU_TYPE_REAL:
    return ixr_vec_raw("float64", ptr, count, 8);
  case MIZU_TYPE_CPLX:
    return ixr_vec_raw("complex128", ptr, count, 16);
  case MIZU_TYPE_RAW:
    return ixr_vec_raw("uint8", ptr, count, 1);
  case MIZU_TYPE_INT64: {
    const int64_t *p = (const int64_t *) ptr;
    uint64_t n_na = 0;
    for (size_t i = 0; i < n; i++) n_na += p[i] == MIZU_NA_INT64;
    if (n_na != 0 &&
        PyErr_WarnFormat(PyExc_RuntimeWarning, 1,
                         "pymizu: %llu int64 value(s) of -9223372036854775808 "
                         "read as NA_integer64_ in R",
                         (unsigned long long) n_na) < 0)
      return NULL;
    return ixr_vec_raw("int64", ptr, count, 8);
  }
  }
  Py_UNREACHABLE();   /* the cursor's tag->type mapping is the closed six */
}

/* The reader's 0x13 mode, threaded through the builder: 0 declines (the
   channel value reader and the layout blobs — refs never cross there),
   1 resolves through rctx's view cache (the task decode, the map
   descriptor reader, the collect-side result reader), 2 represents the
   identifier as a marker for the corpus's conformance loop (the hook
   decode — never resolves, so synthetic identifiers round-trip). */
typedef struct {
  int refs;
  mizu_read_ctx *rctx;
} ixr_mode;

// to_dict's null-fidelity reads (pyframe.h) -------------------------------------

/* The masked-arm workhorse: fill a fresh buffer of n elements of elt
   bytes, valid lanes verbatim from values, masked lanes the kind's
   missing form. NULL on OOM. */
static uint8_t *masked_buf(const uint8_t *values, const uint8_t *valid,
                           uint64_t n, size_t elt, int kind) {
  uint8_t *buf = mizu_py_ix_alloc(n, elt);
  if (buf == NULL) return NULL;
  memcpy(buf, values, (size_t) n * elt);
  for (uint64_t i = 0; i < n; i++) {
    if (bitmap_at(valid, i)) continue;
    switch (kind) {
    case FCOL_F64: store_na_r(buf + 8 * i); break;
    case FCOL_I64: case FCOL_TS: case FCOL_TD: case FCOL_U64: {
      const int64_t na = MIZU_NA_INT64;
      memcpy(buf + 8 * i, &na, 8);
      break;
    }
    default: {   /* I32 / DATE */
      const int32_t na = MIZU_NA_INT32;
      memcpy(buf + 4 * i, &na, 4);
      break;
    }
    }
  }
  return buf;
}

/* The bitmap's clear-bit count (0 short-circuits every masked arm to the
   owned forms). */
static uint64_t masked_nulls(const uint8_t *valid, uint64_t n) {
  uint64_t nulls = 0;
  for (uint64_t i = 0; i < n; i++) nulls += !bitmap_at(valid, i);
  return nulls;
}

PyObject *ixr_masked_f64(const uint8_t *values, const uint8_t *valid,
                         uint64_t n) {
  if (valid == NULL || masked_nulls(valid, n) == 0)
    return ixr_vec_raw("float64", values, n, 8);
  uint8_t *buf = masked_buf(values, valid, n, 8, FCOL_F64);
  if (buf == NULL) return NULL;
  PyObject *np = mizu_py_numpy_module();
  PyObject *out = np == NULL ? ixr_memoryview(buf, (size_t) n * 8) :
    ixr_vec_conv("float64", buf, n, 8, NULL, NULL);
  free(buf);
  return out;
}

PyObject *ixr_masked_i32(const uint8_t *values, const uint8_t *valid,
                         uint64_t n) {
  if (valid == NULL) return ixr_vec(MIZU_TYPE_INT, values, n);
  if (masked_nulls(valid, n) == 0) return ixr_vec_raw("int32", values, n, 4);
  PyObject *np = mizu_py_numpy_module();
  if (np == NULL) {
    uint8_t *buf = masked_buf(values, valid, n, 4, FCOL_I32);
    if (buf == NULL) return NULL;
    PyObject *out = ixr_memoryview(buf, (size_t) n * 4);
    free(buf);
    return out;
  }
  /* the fused read rule: int32 with nulls crosses to float64, R's NA_real_
     payload at the masked lanes (every int32 exact there) */
  Py_ssize_t dims[1] = { (Py_ssize_t) n };
  PyObject *arr = ixr_np_alloc(dims, 1, "float64", 0);
  if (arr == NULL) return NULL;
  Py_buffer v;
  /* a fresh 1-D array always exports PyBUF_ND | PyBUF_WRITABLE */
  (void) PyObject_GetBuffer(arr, &v, PyBUF_ND | PyBUF_WRITABLE);
  const int32_t *p = (const int32_t *) values;
  for (uint64_t i = 0; i < n; i++) {
    if (bitmap_at(valid, i)) {
      double x = (double) p[i];
      memcpy((uint8_t *) v.buf + 8 * i, &x, 8);
    } else {
      store_na_r((uint8_t *) v.buf + 8 * i);
    }
  }
  PyBuffer_Release(&v);
  return arr;
}

PyObject *ixr_masked_i64(const uint8_t *values, const uint8_t *valid,
                         uint64_t n) {
  if (valid == NULL) return ixr_vec(MIZU_TYPE_INT64, values, n);
  if (masked_nulls(valid, n) == 0) return ixr_vec(MIZU_TYPE_INT64, values, n);
  uint8_t *buf = masked_buf(values, valid, n, 8, FCOL_I64);
  if (buf == NULL) return NULL;
  /* a genuine INT64_MIN on a valid lane is now indistinguishable from a
     null — the fused read rule's warning, counted on valid lanes only */
  uint64_t amb = 0;
  const int64_t *p = (const int64_t *) values;
  for (uint64_t i = 0; i < n; i++)
    if (bitmap_at(valid, i)) amb += p[i] == MIZU_NA_INT64;
  if (amb != 0 &&
      PyErr_WarnFormat(PyExc_RuntimeWarning, 1,
                       "pymizu: %llu int64 value(s) of -9223372036854775808 "
                       "read as NA_integer64_ in R",
                       (unsigned long long) amb) < 0) {
    free(buf);
    return NULL;
  }
  PyObject *np = mizu_py_numpy_module();
  PyObject *out = np == NULL ? ixr_memoryview(buf, (size_t) n * 8) :
    ixr_vec_conv("int64", buf, n, 8, NULL, NULL);
  free(buf);
  return out;
}

PyObject *ixr_masked_u64(const uint8_t *values, const uint8_t *valid,
                         uint64_t n) {
  if (valid == NULL || masked_nulls(valid, n) == 0)
    return ixr_vec_raw("uint64", values, n, 8);
  PyObject *np = mizu_py_numpy_module();
  if (np == NULL) {
    uint8_t *buf = masked_buf(values, valid, n, 8, FCOL_U64);
    if (buf == NULL) return NULL;
    PyObject *out = ixr_memoryview(buf, (size_t) n * 8);
    free(buf);
    return out;
  }
  /* masked: float64 + NaN (parity with masked int32 — the Arrow export
     is the exact path) */
  Py_ssize_t dims[1] = { (Py_ssize_t) n };
  PyObject *arr = ixr_np_alloc(dims, 1, "float64", 0);
  if (arr == NULL) return NULL;
  Py_buffer v;
  /* a fresh 1-D array always exports PyBUF_ND | PyBUF_WRITABLE */
  (void) PyObject_GetBuffer(arr, &v, PyBUF_ND | PyBUF_WRITABLE);
  const uint64_t *p = (const uint64_t *) values;
  for (uint64_t i = 0; i < n; i++) {
    if (bitmap_at(valid, i)) {
      double x = (double) p[i];
      memcpy((uint8_t *) v.buf + 8 * i, &x, 8);
    } else {
      store_na_r((uint8_t *) v.buf + 8 * i);
    }
  }
  PyBuffer_Release(&v);
  return arr;
}

PyObject *ixr_masked_days(const uint8_t *values, const uint8_t *valid,
                          uint64_t n) {
  if (valid == NULL || masked_nulls(valid, n) == 0) {
    i32_span s = { (const int32_t *) values, n };
    return ixr_vec_conv("datetime64[D]", values, n, 8, conv_days_i64, &s);
  }
  /* masked lanes -> datetime64[D] NaT (the INT32_MIN payload) */
  uint8_t *buf = masked_buf(values, valid, n, 4, FCOL_DATE);
  if (buf == NULL) return NULL;
  i32_span s = { (const int32_t *) buf, n };
  PyObject *out = ixr_vec_conv("datetime64[D]", buf, n, 8, conv_days_i64,
                               &s);
  free(buf);
  return out;
}

PyObject *ixr_masked_us(const char *dt, const uint8_t *values,
                        const uint8_t *valid, uint64_t n) {
  if (valid == NULL || masked_nulls(valid, n) == 0)
    return ixr_vec_conv(dt, values, n, 8, NULL, NULL);
  /* masked lanes -> NaT (INT64_MIN is datetime64/timedelta64's own) */
  uint8_t *buf = masked_buf(values, valid, n, 8, FCOL_TS);
  if (buf == NULL) return NULL;
  PyObject *out = ixr_vec_conv(dt, buf, n, 8, NULL, NULL);
  free(buf);
  return out;
}

static PyObject *ixr_value(mizu_ix *cur, const ixr_mode *mode);
static PyObject *ixr_frame(mizu_ix *cur, uint64_t ncols,
                           const ixr_mode *mode);

static PyObject *ixr_str(const mizu_ix_item *it) {
  if (it->na) Py_RETURN_NONE;
  return PyUnicode_DecodeUTF8((const char *) it->ptr, (Py_ssize_t) it->len,
                              NULL);
}

static PyObject *ixr_strv(mizu_ix *cur, uint64_t count,
                          const ixr_mode *Py_UNUSED(mode)) {
  PyObject *out = PyList_New((Py_ssize_t) count);
  if (out == NULL) return NULL;
  for (uint64_t i = 0; i < count; i++) {
    mizu_ix_item it;
    if (mizu_ix_next(cur, &it) != MIZU_OK || it.kind != MIZU_IX_STR) {
      Py_DECREF(out);
      ixr_cursor_err();
      return NULL;
    }
    PyObject *s = ixr_str(&it);
    if (s == NULL) {
      Py_DECREF(out);
      return NULL;
    }
    PyList_SET_ITEM(out, (Py_ssize_t) i, s);
  }
  return out;
}

static PyObject *ixr_list(mizu_ix *cur, uint64_t count, const ixr_mode *mode) {
  PyObject *out = PyList_New((Py_ssize_t) count);
  if (out == NULL) return NULL;
  for (uint64_t i = 0; i < count; i++) {
    PyObject *v = ixr_value(cur, mode);
    if (v == NULL) {
      Py_DECREF(out);
      return NULL;
    }
    PyList_SET_ITEM(out, (Py_ssize_t) i, v);
  }
  return out;
}

static PyObject *ixr_dict(mizu_ix *cur, uint64_t count, const ixr_mode *mode) {
  PyObject *out = PyDict_New();
  if (out == NULL) return NULL;
  for (uint64_t i = 0; i < count; i++) {
    mizu_ix_item kit;
    if (mizu_ix_next(cur, &kit) != MIZU_OK || kit.kind != MIZU_IX_STR) {
      Py_DECREF(out);
      ixr_cursor_err();
      return NULL;
    }
    PyObject *k = PyUnicode_DecodeUTF8((const char *) kit.ptr,
                                       (Py_ssize_t) kit.len, NULL);
    if (k == NULL) {
      Py_DECREF(out);
      return NULL;
    }
    PyObject *v = ixr_value(cur, mode);
    if (v == NULL) {
      Py_DECREF(k);
      Py_DECREF(out);
      return NULL;
    }
    int dup = PyDict_Contains(out, k);
    if (dup < 0 || dup || PyDict_SetItem(out, k, v) < 0) {
      Py_DECREF(k);
      Py_DECREF(v);
      Py_DECREF(out);
      if (dup > 0)
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "a duplicate dict key");
      return NULL;
    }
    Py_DECREF(k);
    Py_DECREF(v);
  }
  return out;
}

/* The attr dict's raw entries: keys as spans; values a string span (the
   0x04 form), a string list (0x0b), an int span (0x07), an int scalar
   (0x02), a built object (the dimnames list, or its (entries,
   axis_names) tuple for the attr(list, {names}) carrier), or a raw span
   (an extended frame's extra — skipped whole, never decoded: the
   pristine echo splices it verbatim). The shape readers consume the
   spans without an intermediate conversion (a compact row.names intv
   carries an NA the float64 home would hide). */
enum { AV_OBJ, AV_STR, AV_STRLIST, AV_INTSPAN, AV_INT, AV_SPAN };

typedef struct {
  PyObject *obj;             /* AV_OBJ / AV_STRLIST */
  const uint8_t *ptr;        /* AV_STR / AV_INTSPAN / AV_SPAN */
  uint64_t count;            /* AV_STR / AV_SPAN (bytes) /
                                AV_INTSPAN (elements) */
  int64_t v;                 /* AV_INT */
} attr_val;

typedef struct {
  const uint8_t *key;
  uint64_t key_len;
  int kind;
  attr_val val;
} attr_ent;

static void attr_vals_free(attr_ent *ents, int n) {
  for (int i = 0; i < n; i++)
    if (ents[i].kind == AV_OBJ || ents[i].kind == AV_STRLIST)
      Py_XDECREF(ents[i].val.obj);
  free(ents);
}

static const attr_ent *attr_find(const attr_ent *ents, int n,
                                 const char *key);

/* Read the attr's dict (the cursor's next item is the DICT begin). */
static attr_ent *ixr_attr_dict(mizu_ix *cur, int *n_out,
                               const ixr_mode *mode) {
  mizu_ix_item it;
  if (mizu_ix_next(cur, &it) != MIZU_OK || it.kind != MIZU_IX_DICT) {
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "the attr attributes are not a dict");
    return NULL;
  }
  uint64_t n = it.count;
  /* the bound: the legitimate maximum is the reserved vocabulary plus a
     frame's extras — it guards the calloc and the O(n^2) duplicate-key
     scan against a hostile count */
  if (n > 1024) {
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "an attribute dict past 1024 entries");
    return NULL;
  }
  attr_ent *ents = calloc(n != 0 ? (size_t) n : 1, sizeof(attr_ent));
  if (ents == NULL) {
    PyErr_NoMemory();
    return NULL;
  }
  for (uint64_t i = 0; i < n; i++) {
    mizu_ix_item kit;
    if (mizu_ix_next(cur, &kit) != MIZU_OK || kit.kind != MIZU_IX_STR) {
      ixr_cursor_err();
      goto fail;
    }
    for (uint64_t j = 0; j < i; j++)
      if (ents[j].key_len == kit.len &&
          memcmp(ents[j].key, kit.ptr, kit.len) == 0) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "a duplicate dict key");
        goto fail;
      }
    ents[i].key = kit.ptr;
    ents[i].key_len = kit.len;
    if (!mizu_ix_attr_reserved((const char *) kit.ptr,
                               (size_t) kit.len)) {
      /* an extended frame's extra: the raw span, skipped whole with the
         core's verb — never decoded (the pristine echo splices it
         verbatim); the reserved keys below carry the vocabulary's
         values */
      const uint8_t *s;
      size_t slen;
      if (mizu_ix_skip(cur, &s, &slen) != MIZU_OK) {
        ixr_cursor_err();
        goto fail;
      }
      ents[i].kind = AV_SPAN;
      ents[i].val.ptr = s;
      ents[i].val.count = (uint64_t) slen;
      continue;
    }
    mizu_ix_item vit;
    if (mizu_ix_next(cur, &vit) != MIZU_OK) {
      ixr_cursor_err();
      goto fail;
    }
    switch (vit.kind) {
    case MIZU_IX_STR1:
      if (vit.na) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "an NA attribute string");
        goto fail;
      }
      ents[i].kind = AV_STR;
      ents[i].val.ptr = vit.ptr;
      ents[i].val.count = vit.len;
      break;
    case MIZU_IX_STRV:
      ents[i].kind = AV_STRLIST;
      ents[i].val.obj = ixr_strv(cur, vit.count, mode);
      if (ents[i].val.obj == NULL) goto fail;
      break;
    case MIZU_IX_VEC:
      if (vit.type != MIZU_TYPE_INT) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "an attribute vector of an unexpected type");
        goto fail;
      }
      ents[i].kind = AV_INTSPAN;
      ents[i].val.ptr = vit.ptr;
      ents[i].val.count = vit.count;
      break;
    case MIZU_IX_INT:
      ents[i].kind = AV_INT;
      memcpy(&ents[i].val.v, &vit.u64[0], 8);
      break;
    case MIZU_IX_LGL:
      /* a logical attr rides AV_INT (0/1, NA 2 — the u64 marker's form) */
      ents[i].kind = AV_INT;
      ents[i].val.v = (int64_t) vit.u64[0];
      break;
    case MIZU_IX_LIST:
      /* the dimnames list form (nil / strv entries) */
      ents[i].kind = AV_OBJ;
      ents[i].val.obj = ixr_list(cur, vit.count, mode);
      if (ents[i].val.obj == NULL) goto fail;
      break;
    case MIZU_IX_ATTR: {
      /* the dimnames carrier attr(list, {names}) — the one nested attr,
         admitted under the dimnames key alone: an (entries, axis_names)
         tuple, the dict exactly {names} */
      if (ents[i].key_len != IX_KEYLEN(MIZU_IX_ATTR_DIMNAMES) ||
          memcmp(ents[i].key, MIZU_IX_ATTR_DIMNAMES,
                 IX_KEYLEN(MIZU_IX_ATTR_DIMNAMES)) != 0) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "a nested attr outside the dimnames value");
        goto fail;
      }
      mizu_ix_item v2;
      if (mizu_ix_next(cur, &v2) != MIZU_OK) {
        ixr_cursor_err();
        goto fail;
      }
      if (v2.kind != MIZU_IX_LIST) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "a nested attr's value is not a list");
        goto fail;
      }
      PyObject *entries = ixr_list(cur, v2.count, mode);
      if (entries == NULL) goto fail;
      int nn = 0;
      attr_ent *nents = ixr_attr_dict(cur, &nn, mode);
      if (nents == NULL) {
        Py_DECREF(entries);
        goto fail;
      }
      const attr_ent *ax = attr_find(nents, nn, MIZU_IX_ATTR_NAMES);
      if (nn != 1 || ax == NULL ||
          (ax->kind != AV_STRLIST && ax->kind != AV_STR)) {
        attr_vals_free(nents, nn);
        Py_DECREF(entries);
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "the dimnames carrier's attributes are not "
                        "{names}");
        goto fail;
      }
      PyObject *axn;
      if (ax->kind == AV_STR) {
        axn = PyList_New(1);
        PyObject *s = PyUnicode_DecodeUTF8((const char *) ax->val.ptr,
                                           (Py_ssize_t) ax->val.count,
                                           NULL);
        if (axn != NULL && s != NULL) PyList_SET_ITEM(axn, 0, s);
        else {
          Py_XDECREF(axn);
          axn = NULL;
          Py_XDECREF(s);
        }
      } else {
        axn = ax->val.obj;
        Py_INCREF(axn);
      }
      attr_vals_free(nents, nn);
      if (axn == NULL) {
        Py_DECREF(entries);
        goto fail;
      }
      ents[i].kind = AV_OBJ;
      ents[i].val.obj = PyTuple_Pack(2, entries, axn);
      Py_DECREF(entries);
      Py_DECREF(axn);
      if (ents[i].val.obj == NULL) goto fail;
      break;
    }
    default:
      /* vit was already consumed by the mizu_ix_next above; anything
         outside the attribute vocabulary is a decline, never a re-read
         (pulling the next item here once desynced the whole dict) */
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "an attribute value of an unsupported kind");
      goto fail;
    }
  }
  *n_out = (int) n;
  return ents;
fail:
  attr_vals_free(ents, (int) n);
  return NULL;
}

static const attr_ent *attr_find(const attr_ent *ents, int n,
                                 const char *key) {
  size_t klen = strlen(key);
  for (int i = 0; i < n; i++)
    if (ents[i].key_len == klen && memcmp(ents[i].key, key, klen) == 0)
      return &ents[i];
  return NULL;
}

/* A layout attribute blob (§3.5): a complete 'I' stream whose value is
   the attribute dict, read with the finishing check. A non-'I' blob (an
   R_Serialize stream from a peer that sent one despite the mask) declines
   as a corrupt or newer region. */
static attr_ent *blob_attrs(const uint8_t *buf, size_t size, int *n_out) {
  if (size == 0 || buf[0] != MIZU_INTEROP_MAGIC) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                    "(a serialized attribute blob)");
    return NULL;
  }
  mizu_ix cur;
  if (mizu_ix_open(&cur, buf, size) != MIZU_OK) {
    ixr_cursor_err();
    return NULL;
  }
  attr_ent *ents = ixr_attr_dict(&cur, n_out, NULL);
  if (ents == NULL) return NULL;
  if (mizu_ix_end(&cur) != MIZU_OK) {
    ixr_cursor_err();
    attr_vals_free(ents, *n_out);
    return NULL;
  }
  return ents;
}

static int str_obj_eq(PyObject *s, const char *c) {
  return PyUnicode_Check(s) && PyUnicode_CompareWithASCIIString(s, c) == 0;
}

/* class == [a] (AV_STR is the length-1 form) or [a, b]. */
static int class_is(const attr_ent *e, const char *a, const char *b) {
  if (e == NULL) return 0;
  if (e->kind == AV_STR) {
    size_t alen = strlen(a);
    return b == NULL && e->val.count == alen &&
      memcmp(e->val.ptr, a, alen) == 0;
  }
  if (e->kind != AV_STRLIST) return 0;
  PyObject *lst = e->val.obj;
  Py_ssize_t n = PyList_GET_SIZE(lst);
  if (b == NULL)
    return n == 1 && str_obj_eq(PyList_GET_ITEM(lst, 0), a);
  return n == 2 && str_obj_eq(PyList_GET_ITEM(lst, 0), a) &&
    str_obj_eq(PyList_GET_ITEM(lst, 1), b);
}

/* The tscale attr's verbatim-ns verdict (the MIZU_CAP_MIZL_TSN leaf form). */
static int tscale_ns(const attr_ent *e) {
  return e != NULL && e->kind == AV_STR && e->val.count == 2 &&
    memcmp(e->val.ptr, "ns", 2) == 0;
}

// The labeled shapes (MIZU_CAP_LABELED) --------------------------------------------

/* The DimArray type from sys.modules only (the fast gates' probe:
   pymizu._dimarray imports numpy, and the import stays deferred — a
   DimArray instance cannot exist before its module is imported, so a
   miss means "no DimArray", never an import). */
static PyTypeObject *dimarray_type_soft(void) {
  static PyTypeObject *t;
  static int probed;
  if (probed) return t;
  PyObject *mods = PyImport_GetModuleDict();
  PyObject *mod = PyMapping_GetItemString(mods, "pymizu._dimarray");
  if (mod == NULL) {
    PyErr_Clear();
    return NULL;
  }
  probed = 1;
  PyObject *o = PyObject_GetAttrString(mod, "DimArray");
  if (o == NULL || !PyType_Check(o)) {
    Py_XDECREF(o);
    PyErr_Clear();
    return NULL;
  }
  t = (PyTypeObject *) o;
  return t;
}

/* The DimArray type, imported lazily at need (the read-side wrap:
   pymizu._dimarray imports numpy — without numpy this fails where any
   array read already fails today, so the labeled shapes inherit the
   no-numpy decline path unchanged). */
static PyTypeObject *dimarray_type(void) {
  PyTypeObject *t = dimarray_type_soft();
  if (t != NULL) return t;
  PyObject *mod = PyImport_ImportModule("pymizu._dimarray");
  if (mod == NULL) {
    PyErr_Clear();
    return NULL;
  }
  PyObject *o = PyObject_GetAttrString(mod, "DimArray");
  Py_DECREF(mod);
  if (o == NULL || !PyType_Check(o)) {
    Py_XDECREF(o);
    PyErr_Clear();
    return NULL;
  }
  return (PyTypeObject *) o;
}

/* Wrap an array as a DimArray and attach the labels: the .view shares
   the data, so the base chain (and with it the REF echo's region
   identity) is preserved. NULL with an exception set. */
static PyObject *dimarray_attach(PyObject *arr, PyObject *names,
                                 PyObject *dimnames, PyObject *axis_names,
                                 int table) {
  PyTypeObject *t = dimarray_type();
  if (t == NULL) {
    PyErr_SetString(MizuError, "pymizu: a labeled array read needs numpy "
                    "(install it)");
    return NULL;
  }
  PyObject *view = PyObject_CallMethod(arr, "view", "O", t);
  if (view == NULL) return NULL;
  /* __array_finalize__ nulled the four slots; attach what crossed */
  if (names != NULL && PyObject_SetAttrString(view, "names", names) < 0)
    goto fail;
  if (dimnames != NULL &&
      PyObject_SetAttrString(view, "dimnames", dimnames) < 0)
    goto fail;
  if (axis_names != NULL &&
      PyObject_SetAttrString(view, "axis_names", axis_names) < 0)
    goto fail;
  if (table) {
    PyObject *tc = PyUnicode_FromString(MIZU_IX_CLASS_TABLE);
    if (tc == NULL) goto fail;
    const int rc = PyObject_SetAttrString(view, "r_class", tc);
    Py_DECREF(tc);
    if (rc < 0) goto fail;
  }
  return view;
fail:
  Py_DECREF(view);
  return NULL;
}

/* A names strv as a Python tuple of str | None (AV_STR the length-1
   form). */
static PyObject *str_tuple_of(const attr_ent *e) {
  if (e->kind == AV_STR) {
    PyObject *s = PyUnicode_DecodeUTF8((const char *) e->val.ptr,
                                       (Py_ssize_t) e->val.count, NULL);
    if (s == NULL) return NULL;
    PyObject *t = PyTuple_Pack(1, s);
    Py_DECREF(s);
    return t;
  }
  return PyList_AsTuple(e->val.obj);
}

/* The dimnames value as (entries, axis_names-or-NULL): the AV_OBJ list
   (nil / strv entries crossing as None / str lists) or the carrier's
   (entries, axis_names) tuple — every entry a tuple of str | None. */
static int dimnames_tuples(PyObject *obj, PyObject **out_entries,
                           PyObject **out_axis) {
  PyObject *entries = obj;
  PyObject *axis = NULL;
  if (PyTuple_Check(obj) && PyTuple_GET_SIZE(obj) == 2) {
    entries = PyTuple_GET_ITEM(obj, 0);
    axis = PyTuple_GET_ITEM(obj, 1);
  }
  if (!PyList_Check(entries)) return -1;
  const Py_ssize_t nd = PyList_GET_SIZE(entries);
  PyObject *et = PyTuple_New(nd);
  if (et == NULL) return -1;
  for (Py_ssize_t i = 0; i < nd; i++) {
    PyObject *e = PyList_GET_ITEM(entries, i);
    if (e == Py_None) {
      Py_INCREF(Py_None);
      PyTuple_SET_ITEM(et, i, Py_None);
      continue;
    }
    if (!PyList_Check(e)) {
      Py_DECREF(et);
      return -1;
    }
    PyObject *t = PyList_AsTuple(e);
    if (t == NULL) {
      Py_DECREF(et);
      return -1;
    }
    PyTuple_SET_ITEM(et, i, t);
  }
  if (axis != NULL) {
    axis = PyList_AsTuple(axis);
    if (axis == NULL) {
      Py_DECREF(et);
      return -1;
    }
  }
  *out_entries = et;
  *out_axis = axis;
  return 0;
}

/* The label stash (pyinterop.h's ix_labels): the validated sequences
   are owned references, cleared by mizu_py_labels_clear. */
void mizu_py_labels_clear(ix_labels *lb) {
  Py_CLEAR(lb->names);
  Py_CLEAR(lb->dimnames);
  Py_CLEAR(lb->axis_names);
  lb->table = 0;
}

static int labels_str_ok(PyObject *o) {
  return o == Py_None ||
    (PyUnicode_CheckExact(o) && PyUnicode_AsUTF8AndSize(o, NULL) != NULL);
}

/* A labels sequence (a list or tuple of str | None elements) of the
   wanted length, or the decline. */
static int labels_seq_ok(PyObject *s, Py_ssize_t n, const char *what) {
  if ((!PyList_Check(s) && !PyTuple_Check(s)) || PySequence_Size(s) != n) {
    PyErr_Format(MizuDeclinedError,
                 "pymizu: %s must have %zd entries of str | None", what, n);
    return 0;
  }
  for (Py_ssize_t i = 0; i < n; i++) {
    PyObject *e = PySequence_GetItem(s, i);
    const int ok = e != NULL && labels_str_ok(e);
    Py_XDECREF(e);
    if (!ok) {
      PyErr_Format(MizuDeclinedError,
                   "pymizu: %s must have %zd entries of str | None", what,
                   n);
      return 0;
    }
  }
  return 1;
}

/* Read and validate the stash off a DimArray: 0 the object carries no
   labels, 1 a validated stash, -1 with DeclinedError set (names and
   dimnames mutually exclusive, axis_names requiring dimnames, the
   lengths against (nd, shape), r_class "table" or absent). The three
   sequence slots are owned references — the caller clears them
   (labels_clear). */
int mizu_py_labels_of(PyObject *obj, int nd, const int64_t *shape,
                     ix_labels *out) {
  memset(out, 0, sizeof(*out));
  PyTypeObject *t = dimarray_type_soft();
  if (t == NULL || !PyObject_TypeCheck(obj, t)) return 0;
  if (nd < 1 || nd > 32) {
    PyErr_SetString(MizuDeclinedError,
                    "pymizu: labels ride a 1..32-dimensional array");
    return -1;
  }
  PyObject *names = PyObject_GetAttrString(obj, "names");
  PyObject *dimnames = PyObject_GetAttrString(obj, "dimnames");
  PyObject *axis_names = PyObject_GetAttrString(obj, "axis_names");
  PyObject *r_class = PyObject_GetAttrString(obj, "r_class");
  if (names == NULL || dimnames == NULL || axis_names == NULL ||
      r_class == NULL)
    goto fail;
  int rc = 0;
  if (r_class != Py_None) {
    if (!str_obj_eq(r_class, MIZU_IX_CLASS_TABLE)) {
      PyErr_SetString(MizuDeclinedError,
                      "pymizu: r_class must be \"table\" or None");
      goto fail;
    }
    out->table = 1;
  }
  if (names != Py_None && dimnames != Py_None) {
    PyErr_SetString(MizuDeclinedError,
                    "pymizu: names and dimnames are mutually exclusive");
    goto fail;
  }
  if (axis_names != Py_None && dimnames == Py_None) {
    PyErr_SetString(MizuDeclinedError,
                    "pymizu: axis_names requires dimnames");
    goto fail;
  }
  if (names != Py_None) {
    if (nd != 1 || !labels_seq_ok(names, shape[0], "names")) {
      if (!PyErr_Occurred())
        PyErr_SetString(MizuDeclinedError,
                        "pymizu: names rides a 1-D array only");
      goto fail;
    }
    out->names = names;
    Py_INCREF(names);
    rc = 1;
  }
  if (dimnames != Py_None) {
    if ((!PyList_Check(dimnames) && !PyTuple_Check(dimnames)) ||
        PySequence_Size(dimnames) != nd) {
      PyErr_Format(MizuDeclinedError,
                   "pymizu: dimnames must have %d entries (one per axis)",
                   nd);
      goto fail;
    }
    for (int i = 0; i < nd; i++) {
      PyObject *e = PySequence_GetItem(dimnames, i);
      if (e == NULL) goto fail;
      int ok = e == Py_None || labels_seq_ok(e, shape[i], "a dimnames entry");
      const int had = PyErr_Occurred() != NULL;
      Py_DECREF(e);
      if (!ok) {
        if (!had)
          PyErr_Format(MizuDeclinedError,
                       "pymizu: a dimnames entry must be None or %lld "
                       "entries of str | None", (long long) shape[i]);
        goto fail;
      }
    }
    out->dimnames = dimnames;
    Py_INCREF(dimnames);
    rc = 1;
  }
  if (axis_names != Py_None) {
    if (!labels_seq_ok(axis_names, nd, "axis_names")) goto fail;
    out->axis_names = axis_names;
    Py_INCREF(axis_names);
    rc = 1;
  }
  if (out->table && out->dimnames == NULL) {
    PyErr_SetString(MizuDeclinedError,
                    "pymizu: r_class \"table\" requires dimnames");
    goto fail;
  }
  Py_DECREF(names);
  Py_DECREF(dimnames);
  Py_DECREF(axis_names);
  Py_DECREF(r_class);
  return rc;
fail:
  Py_XDECREF(names);
  Py_XDECREF(dimnames);
  Py_XDECREF(axis_names);
  Py_XDECREF(r_class);
  mizu_py_labels_clear(out);
  return -1;
}

/* The DimArray type check alone: the buffer gates' exemption from the
   subclass reject (its buffer export is the plain ndarray one — the
   MaskedArray doctrine does not apply to our own type). */
int mizu_py_dimarray_check(PyObject *obj) {
  PyTypeObject *t = dimarray_type_soft();
  return t != NULL && PyObject_TypeCheck(obj, t);
}

/* The fast gate for the stage order (skipping the raw tiers): a
   DimArray carrying any label. */
int mizu_py_dimarray_labeled(PyObject *obj) {
  PyTypeObject *t = dimarray_type_soft();
  if (t == NULL || !PyObject_TypeCheck(obj, t)) return 0;
  PyObject *a = PyObject_GetAttrString(obj, "names");
  int yes = a != NULL && a != Py_None;
  Py_XDECREF(a);
  if (yes) return 1;
  a = PyObject_GetAttrString(obj, "dimnames");
  yes = a != NULL && a != Py_None;
  Py_XDECREF(a);
  if (yes) return 1;
  a = PyObject_GetAttrString(obj, "axis_names");
  yes = a != NULL && a != Py_None;
  Py_XDECREF(a);
  if (yes) return 1;
  a = PyObject_GetAttrString(obj, "r_class");
  yes = a != NULL && a != Py_None;
  Py_XDECREF(a);
  return yes;
}

/* A user str | None sequence against the blob's parsed strv (the
   elements are str or None by construction). */
static int labels_eq_strseq(PyObject *seq, PyObject *list) {
  if (!PyList_Check(seq) && !PyTuple_Check(seq)) return 0;
  const Py_ssize_t n = PySequence_Size(seq);
  if (n != PyList_GET_SIZE(list)) return 0;
  for (Py_ssize_t i = 0; i < n; i++) {
    PyObject *u = PySequence_GetItem(seq, i);
    PyObject *b = PyList_GET_ITEM(list, i);
    const int eq = (u == Py_None && b == Py_None) ||
      (u != Py_None && b != Py_None && PyUnicode_Check(u) &&
       PyUnicode_Compare(u, b) == 0);
    Py_XDECREF(u);
    if (!eq) return 0;
  }
  return 1;
}

/* The {names} value against a user sequence: AV_STR the length-1 form,
   AV_STRLIST otherwise. */
static int labels_eq_names(PyObject *seq, const attr_ent *nm) {
  if (nm->kind == AV_STR) {
    if (!PyList_Check(seq) && !PyTuple_Check(seq)) return 0;
    if (PySequence_Size(seq) != 1) return 0;
    PyObject *u = PySequence_GetItem(seq, 0);
    int eq = 0;
    if (u != NULL && u != Py_None && PyUnicode_Check(u)) {
      Py_ssize_t l;
      const char *s = PyUnicode_AsUTF8AndSize(u, &l);
      eq = s != NULL && (uint64_t) l == nm->val.count &&
        memcmp(s, nm->val.ptr, (size_t) l) == 0;
    }
    Py_XDECREF(u);
    return eq;
  }
  if (nm->kind != AV_STRLIST) return 0;
  return labels_eq_strseq(seq, nm->val.obj);
}

/* The dimnames value against a user's (dimnames, axis_names): the list
   form (nil entries) or the carrier's (entries, axis_names) tuple. */
static int labels_eq_dnval(PyObject *dn_user, PyObject *ax_user,
                           PyObject *blob_dn) {
  PyObject *entries = blob_dn, *axis = NULL;
  if (PyTuple_Check(blob_dn) && PyTuple_GET_SIZE(blob_dn) == 2) {
    entries = PyTuple_GET_ITEM(blob_dn, 0);
    axis = PyTuple_GET_ITEM(blob_dn, 1);
  }
  if (!PyList_Check(entries) ||
      (!PyList_Check(dn_user) && !PyTuple_Check(dn_user)))
    return 0;
  const Py_ssize_t nd = PyList_GET_SIZE(entries);
  if (PySequence_Size(dn_user) != nd) return 0;
  for (Py_ssize_t i = 0; i < nd; i++) {
    PyObject *u = PySequence_GetItem(dn_user, i);
    PyObject *b = PyList_GET_ITEM(entries, i);
    const int eq = b == Py_None ? u == Py_None :
      u != Py_None && labels_eq_strseq(u, b);
    Py_XDECREF(u);
    if (!eq) return 0;
  }
  if (axis == NULL) return ax_user == NULL;
  return ax_user != NULL && labels_eq_strseq(ax_user, axis);
}

/* The REF gates' label compare (pyinterop.h): the region blob's labels
   against the candidate's stash — a by-reference send requires them
   equal, else the candidate crosses by value (a relabeled or
   label-stripped array never resurrects the region's labels; a plain
   array over a labeled region copies: honest loss). 1 equal, 0 differ
   or malformed (the by-value paths re-validate and decline there). */
int mizu_py_labels_match(const uint8_t *blob, size_t blob_size,
                         PyObject *obj, int nd, const int64_t *shape) {
  int nent = 0;
  attr_ent *ents =
    blob_size != 0 ? blob_attrs(blob, blob_size, &nent) : NULL;
  if (blob_size != 0 && ents == NULL) {
    PyErr_Clear();
    return 0;
  }
  const attr_ent *nm = NULL, *dn = NULL, *cls = NULL;
  if (ents != NULL) {
    nm = attr_find(ents, nent, MIZU_IX_ATTR_NAMES);
    dn = attr_find(ents, nent, MIZU_IX_ATTR_DIMNAMES);
    cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  }
  const int blob_table = cls != NULL &&
    class_is(cls, MIZU_IX_CLASS_TABLE, NULL);
  ix_labels lb;
  int rc = 0;
  if (mizu_py_labels_of(obj, nd, shape, &lb) < 0) {
    PyErr_Clear();
    goto out;
  }
  if (nm != NULL ? (lb.names == NULL || !labels_eq_names(lb.names, nm))
                 : lb.names != NULL)
    goto out;
  if (dn != NULL ? !labels_eq_dnval(lb.dimnames, lb.axis_names, dn->val.obj)
                 : (lb.dimnames != NULL || lb.axis_names != NULL))
    goto out;
  if (blob_table != lb.table) goto out;
  rc = 1;
out:
  mizu_py_labels_clear(&lb);
  if (ents != NULL) attr_vals_free(ents, nent);
  return rc;
}

/* The u64 marker's verbatim verdict (the MIZU_CAP_MIZL_U64V leaf form):
   {u64 = TRUE} — consumed, never applied as an attribute. */
static int u64_marked(const attr_ent *ents, int nent) {
  const attr_ent *u = attr_find(ents, nent, MIZU_IX_ATTR_U64);
  return u != NULL && u->kind == AV_INT && u->val.v == 1;
}

/* The no-home error, naming the attribute names and the class. */
static void ixr_no_home(const attr_ent *ents, int n, const char *what) {
  char msg[512];
  size_t off = 0;
  off += snprintf(msg + off, sizeof(msg) - off,
                  "pymizu: no portable home for %s (attributes: ", what);
  for (int i = 0; i < n && off < sizeof(msg) - 48; i++)
    off += snprintf(msg + off, sizeof(msg) - off, "%s\"%.*s\"",
                    i != 0 ? ", " : "", (int) ents[i].key_len, ents[i].key);
  const attr_ent *cls = attr_find(ents, n, MIZU_IX_ATTR_CLASS);
  if (cls != NULL && off < sizeof(msg) - 48) {
    off += snprintf(msg + off, sizeof(msg) - off, "; class: ");
    if (cls->kind == AV_STR) {
      off += snprintf(msg + off, sizeof(msg) - off, "\"%.*s\"",
                      (int) cls->val.count, cls->val.ptr);
    } else if (cls->kind == AV_STRLIST) {
      PyObject *lst = cls->val.obj;
      for (Py_ssize_t i = 0; i < PyList_GET_SIZE(lst) &&
             off < sizeof(msg) - 48; i++) {
        PyObject *s = PyList_GET_ITEM(lst, i);
        off += snprintf(msg + off, sizeof(msg) - off, "%s\"%s\"",
                        i != 0 ? ", " : "",
                        PyUnicode_Check(s) ? PyUnicode_AsUTF8(s) : "?");
      }
    }
  }
  if (off >= sizeof(msg) - 2) off = sizeof(msg) - 2;
  snprintf(msg + off, 2, ")");
  PyErr_SetString(MizuError, msg);
}

/* A factor's levels (AV_STR the length-1 form) into C storage. */
static int levels_read(const attr_ent *e, int32_t **off_out,
                       uint8_t **bytes_out, int64_t *nlev_out,
                       int64_t *blen_out) {
  Py_ssize_t nlev = e->kind == AV_STR ? 1 : PyList_GET_SIZE(e->val.obj);
  int32_t *offs = malloc(((size_t) nlev + 1) * 4);
  uint8_t *bytes = NULL;
  size_t blen = 0, bcap = 0;
  if (offs == NULL) goto nomem;
  offs[0] = 0;
  for (Py_ssize_t i = 0; i < nlev; i++) {
    const char *s;
    Py_ssize_t len;
    if (e->kind == AV_STR) {
      s = (const char *) e->val.ptr;
      len = (Py_ssize_t) e->val.count;
    } else {
      PyObject *o = PyList_GET_ITEM(e->val.obj, i);
      if (!PyUnicode_Check(o)) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "an NA factor level");
        goto fail;
      }
      s = PyUnicode_AsUTF8AndSize(o, &len);
      if (s == NULL) goto fail;
    }
    if (blen + (size_t) len > INT32_MAX) {
      PyErr_SetString(MizuError, "pymizu: factor levels past the int32 "
                    "offset range");
      goto fail;
    }
    if (bytes_reserve(&bytes, &bcap, blen + (size_t) len + 1) != 0)
      goto nomem;
    memcpy(bytes + blen, s, (size_t) len);
    blen += (size_t) len;
    offs[i + 1] = (int32_t) blen;
  }
  bytes_shrink(&bytes, blen);
  *off_out = offs;
  *bytes_out = bytes;
  *nlev_out = nlev;
  *blen_out = (int64_t) blen;
  return 0;
nomem:
  PyErr_NoMemory();
fail:
  free(offs);
  free(bytes);
  return -1;
}

/* A factor's codes (1-based in the span) into 0-based i32 codes,
   MIZU_NA_INT32 for NA. Validated against nlev. */
static int codes_read(const int32_t *src, uint64_t n, int64_t nlev,
                      int32_t **out) {
  int32_t *codes = mizu_py_ix_alloc((uint64_t) n, 4);
  if (codes == NULL) return -1;
  for (uint64_t i = 0; i < n; i++) {
    int32_t v = src[i];
    if (v == MIZU_NA_INT32) {
      codes[i] = MIZU_NA_INT32;
    } else if (v < 1 || (int64_t) v > nlev) {
      free(codes);
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "a factor code outside the levels");
      return -1;
    } else {
      codes[i] = v - 1;
    }
  }
  *out = codes;
  return 0;
}

/* The standalone factor: a list of str | None. */
static PyObject *factor_to_list(const int32_t *codes, uint64_t n,
                                const int32_t *lev_off,
                                const uint8_t *lev_bytes) {
  PyObject *out = PyList_New((Py_ssize_t) n);
  if (out == NULL) return NULL;
  for (uint64_t i = 0; i < n; i++) {
    PyObject *s;
    if (codes[i] == MIZU_NA_INT32) {
      Py_INCREF(Py_None);
      s = Py_None;
    } else {
      s = PyUnicode_DecodeUTF8(
        (const char *) lev_bytes + lev_off[codes[i]],
        (Py_ssize_t) (lev_off[codes[i] + 1] - lev_off[codes[i]]), NULL);
      if (s == NULL) {
        Py_DECREF(out);
        return NULL;
      }
    }
    PyList_SET_ITEM(out, (Py_ssize_t) i, s);
  }
  return out;
}

static PyObject *ixr_attr(mizu_ix *cur, const ixr_mode *mode);

/* The err tag's Python home: a TaskError *value* (remote_type,
   remote_traceback, index when the flags carry one) — a received error is
   data until user code decides to raise (pymizu.is_remote_error). The
   cursor has validated the three bare strings as UTF-8; never NA. */
static PyObject *ixr_err(const mizu_ix_item *it) {
  PyObject *tn = PyUnicode_FromStringAndSize(
    (const char *) it->err_str[0].ptr, (Py_ssize_t) it->err_str[0].len);
  PyObject *ms = PyUnicode_FromStringAndSize(
    (const char *) it->err_str[1].ptr, (Py_ssize_t) it->err_str[1].len);
  PyObject *tbs = PyUnicode_FromStringAndSize(
    (const char *) it->err_str[2].ptr, (Py_ssize_t) it->err_str[2].len);
  PyObject *eidx = NULL;
  if (tn == NULL || ms == NULL || tbs == NULL) goto fail;
  if ((it->err_flags & 1u) != 0) {
    eidx = PyLong_FromUnsignedLongLong(it->err_index);
    if (eidx == NULL) goto fail;
  }
  {
    PyObject *exc = mizu_py_task_error_build(tn, ms, tbs, eidx);
    Py_DECREF(tn);
    Py_DECREF(ms);
    Py_DECREF(tbs);
    Py_XDECREF(eidx);
    return exc;
  }
fail:
  Py_XDECREF(tn);
  Py_XDECREF(ms);
  Py_XDECREF(tbs);
  return NULL;
}

static PyObject *ixr_value(mizu_ix *cur, const ixr_mode *mode) {
  mizu_ix_item it;
  if (mizu_ix_next(cur, &it) != MIZU_OK) {
    ixr_cursor_err();
    return NULL;
  }
  switch (it.kind) {
  case MIZU_IX_NIL: Py_RETURN_NONE;
  case MIZU_IX_LGL:
    if (it.u64[0] == 2) Py_RETURN_NONE;
    return PyBool_FromLong((long) it.u64[0]);
  case MIZU_IX_INT: {
    int64_t v;
    memcpy(&v, &it.u64[0], 8);
    if (v == INT64_MIN) Py_RETURN_NONE;
    return PyLong_FromLongLong(v);
  }
  case MIZU_IX_REAL: {
    double d;
    memcpy(&d, &it.u64[0], 8);
    return PyFloat_FromDouble(d);
  }
  case MIZU_IX_CPLX: {
    double re, im;
    memcpy(&re, &it.u64[0], 8);
    memcpy(&im, &it.u64[1], 8);
    return PyComplex_FromDoubles(re, im);
  }
  case MIZU_IX_STR1:
    return ixr_str(&it);
  case MIZU_IX_BYTES:
    return PyBytes_FromStringAndSize((const char *) it.ptr,
                                     (Py_ssize_t) it.count);
  case MIZU_IX_VEC:
    return ixr_vec((int) it.type, it.ptr, it.count);
  case MIZU_IX_STRV:
    return ixr_strv(cur, it.count, mode);
  case MIZU_IX_LIST:
    return ixr_list(cur, it.count, mode);
  case MIZU_IX_DICT:
    return ixr_dict(cur, it.count, mode);
  case MIZU_IX_ATTR:
    return ixr_attr(cur, mode);
  case MIZU_IX_ERR:
    return ixr_err(&it);
  case MIZU_IX_REF: {
    if (mode == NULL || mode->refs == 0) {
      PyErr_SetString(MizuError, "pymizu: an interop ref cannot cross as "
                      "a channel value");
      return NULL;
    }
    if (mode->refs == 2)
      return ixr_ref_marker(it.ptr, it.len);
    return mizu_py_view_resolve((const char *) it.ptr, (size_t) it.len,
                                mode->rctx);
  }
  case MIZU_IX_TASK:
    PyErr_SetString(MizuError, "pymizu: an interop task is not a value");
    return NULL;
  }
  Py_UNREACHABLE();   /* the cursor's closed kind set */
}

// The attr shape builders -----------------------------------------------------------

/* A realv span to datetime64: days ('D') or seconds to us. NA payloads ->
   NaT; a fractional Date is the informative decline. */
static PyObject *realv_to_datetime(const uint8_t *ptr, uint64_t n, int days) {
  PyObject *np = mizu_py_numpy_module();
  if (np == NULL) {
    PyErr_SetString(MizuError, "pymizu: a temporal interop read needs "
                    "numpy (install it)");
    return NULL;
  }
  Py_ssize_t dims[1] = { (Py_ssize_t) n };
  PyObject *arr = ixr_np_alloc(dims, 1, days ? "datetime64[D]" :
                               "datetime64[us]", 0);
  if (arr == NULL) return NULL;
  Py_buffer v;
  if (PyObject_GetBuffer(arr, &v, PyBUF_ND | PyBUF_WRITABLE) < 0) {
    Py_DECREF(arr);
    return NULL;
  }
  const double *src = (const double *) ptr;
  int64_t *dst = (int64_t *) v.buf;
  int bad = 0;
  for (uint64_t i = 0; i < n; i++) {
    uint64_t bits;
    memcpy(&bits, src + i, 8);
    if (is_na_r(bits)) {
      dst[i] = INT64_MIN;
      continue;
    }
    double x = src[i];
    if (!isfinite(x) || (days && (trunc(x) != x || fabs(x) > 4.0e18)) ||
        (!days && fabs(x) > 9.0e12)) {
      bad = 1;
      break;
    }
    dst[i] = days ? (int64_t) x : (int64_t) llround(x * 1e6);
  }
  PyBuffer_Release(&v);
  if (bad) {
    Py_DECREF(arr);
    PyErr_SetString(MizuError, days ?
                    "pymizu: no portable home for a fractional Date" :
                    "pymizu: no portable home for this POSIXct value");
    return NULL;
  }
  return arr;
}

/* A realv span to timedelta64[us]: the values are in the dict's declared
   units, unit_secs the unit in seconds. NA payloads -> NaT. */
static PyObject *realv_to_timedelta(const uint8_t *ptr, uint64_t n,
                                    double unit_secs) {
  PyObject *np = mizu_py_numpy_module();
  if (np == NULL) {
    PyErr_SetString(MizuError, "pymizu: a temporal interop read needs "
                    "numpy (install it)");
    return NULL;
  }
  Py_ssize_t dims[1] = { (Py_ssize_t) n };
  PyObject *arr = ixr_np_alloc(dims, 1, "timedelta64[us]", 0);
  if (arr == NULL) return NULL;
  Py_buffer v;
  if (PyObject_GetBuffer(arr, &v, PyBUF_ND | PyBUF_WRITABLE) < 0) {
    Py_DECREF(arr);
    return NULL;
  }
  const double *src = (const double *) ptr;
  int64_t *dst = (int64_t *) v.buf;
  int bad = 0;
  for (uint64_t i = 0; i < n; i++) {
    uint64_t bits;
    memcpy(&bits, src + i, 8);
    if (is_na_r(bits)) {
      dst[i] = INT64_MIN;
      continue;
    }
    double x = src[i];
    if (!isfinite(x) || fabs(x) * unit_secs > 9.0e12) {
      bad = 1;
      break;
    }
    dst[i] = (int64_t) llround(x * unit_secs * 1e6);
  }
  PyBuffer_Release(&v);
  if (bad) {
    Py_DECREF(arr);
    PyErr_SetString(MizuError, "pymizu: no portable home for this "
                    "difftime value");
    return NULL;
  }
  return arr;
}

/* The declared difftime units to seconds (R's five units), 0 when the
   string names none of them. */
static double difftime_unit_secs(const char *u, size_t n) {
  if (n == 4 && memcmp(u, MIZU_IX_UNIT_SECS, 4) == 0) return 1.0;
  if (n == 4 && memcmp(u, MIZU_IX_UNIT_MINS, 4) == 0) return 60.0;
  if (n == 5 && memcmp(u, MIZU_IX_UNIT_HOURS, 5) == 0) return 3600.0;
  if (n == 4 && memcmp(u, MIZU_IX_UNIT_DAYS, 4) == 0) return 86400.0;
  if (n == 5 && memcmp(u, MIZU_IX_UNIT_WEEKS, 5) == 0) return 604800.0;
  return 0.0;
}

/* The dim shape: the vector read per its tag's rule, reshaped F-order. */
static PyObject *dim_to_ndarray(int wire_type, const uint8_t *ptr,
                                uint64_t count, const int32_t *dims,
                                uint64_t nd) {
  if (nd == 1)   /* the documented shift: a length-1 dim is a plain vector */
    return ixr_vec(wire_type, ptr, count);
  if (nd == 0 || nd > 32) {
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "a dim past 32 axes");
    return NULL;
  }
  PyObject *np = mizu_py_numpy_module();
  if (np == NULL) {
    PyErr_SetString(MizuError, "pymizu: a dim-array interop read needs "
                    "numpy (install it)");
    return NULL;
  }
  uint64_t prod = 1;
  Py_ssize_t shape[32];
  for (uint64_t i = 0; i < nd; i++) {
    if (dims[i] < 0) {
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "a negative dim");
      return NULL;
    }
    shape[i] = (Py_ssize_t) dims[i];
    if (dims[i] != 0 && prod > UINT64_MAX / (uint64_t) dims[i]) {
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "a dim product overflow");
      return NULL;
    }
    prod *= (uint64_t) dims[i];
  }
  if (prod != count) {
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "the dim product is not the value's element count");
    return NULL;
  }
  const char *dt;
  void (*conv)(uint8_t *, const void *) = NULL;
  i32_span s = { (const int32_t *) ptr, count };
  size_t elt;
  switch (wire_type) {
  case MIZU_TYPE_LGL:
    if (span_has_na32((const int32_t *) ptr, count)) {
      dt = "int32";
      elt = 4;
    } else {
      dt = "bool";
      elt = 1;
      conv = conv_lgl_bool;
    }
    break;
  case MIZU_TYPE_INT:
    if (span_has_na32((const int32_t *) ptr, count)) {
      dt = "float64";
      elt = 8;
      conv = conv_int_f64;
    } else {
      dt = "int32";
      elt = 4;
    }
    break;
  case MIZU_TYPE_REAL: dt = "float64"; elt = 8; break;
  case MIZU_TYPE_CPLX: dt = "complex128"; elt = 16; break;
  case MIZU_TYPE_RAW: dt = "uint8"; elt = 1; break;
  case MIZU_TYPE_INT64: dt = "int64"; elt = 8; break;
  default:
    /* the attr gate admits only the vector items (a character matrix is
       the strv branch above) */
    Py_UNREACHABLE();
  }
  PyObject *arr = ixr_np_alloc(shape, (int) nd, dt, 1);
  if (arr == NULL) return NULL;
  if (ixr_np_fill(arr, conv, &s, conv == NULL ? ptr : NULL,
                  (size_t) count * elt) < 0) {
    Py_DECREF(arr);
    return NULL;
  }
  return arr;
}

/* The attr dispatcher: the value item leads, then the dict, then the
   shape decision (factor / data.frame / dim / Date / POSIXct, else the
   no-home error). */
static PyObject *ixr_attr(mizu_ix *cur, const ixr_mode *mode) {
  mizu_ix_item vit;
  if (mizu_ix_next(cur, &vit) != MIZU_OK) {
    ixr_cursor_err();
    return NULL;
  }
  if (vit.kind == MIZU_IX_LIST)
    return ixr_frame(cur, vit.count, mode);
  PyObject *out = NULL;
  if (vit.kind == MIZU_IX_STRV) {
    /* consume the strings (a character matrix has no home) */
    PyObject *skip = ixr_strv(cur, vit.count, mode);
    if (skip == NULL) return NULL;
    Py_DECREF(skip);
    int nent = 0;
    attr_ent *ents = ixr_attr_dict(cur, &nent, mode);
    if (ents == NULL) return NULL;
    ixr_no_home(ents, nent, "an attributed character vector");
    attr_vals_free(ents, nent);
    return NULL;
  }
  if (vit.kind != MIZU_IX_VEC && vit.kind != MIZU_IX_REAL &&
      vit.kind != MIZU_IX_INT) {
    PyErr_SetString(MizuError, "pymizu: no portable home for an "
                    "attributed value of this form");
    return NULL;
  }
  /* normalize the value to (vptr, vn, vtype) */
  double one_r;
  int64_t one_i;
  const uint8_t *vptr = vit.ptr;
  uint64_t vn = vit.count;
  int vtype = (int) vit.type;
  if (vit.kind == MIZU_IX_REAL) {
    memcpy(&one_r, &vit.u64[0], 8);
    vptr = (const uint8_t *) &one_r;
    vn = 1;
    vtype = MIZU_TYPE_REAL;
  } else if (vit.kind == MIZU_IX_INT) {
    memcpy(&one_i, &vit.u64[0], 8);
    vptr = (const uint8_t *) &one_i;
    vn = 1;
    vtype = MIZU_TYPE_INT;
  }
  int nent = 0;
  attr_ent *ents = ixr_attr_dict(cur, &nent, NULL);
  if (ents == NULL) return NULL;
  const attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  const attr_ent *lv = attr_find(ents, nent, MIZU_IX_ATTR_LEVELS);
  const attr_ent *dm = attr_find(ents, nent, MIZU_IX_ATTR_DIM);
  const attr_ent *tz = attr_find(ents, nent, MIZU_IX_ATTR_TZONE);
  const attr_ent *un = attr_find(ents, nent, MIZU_IX_ATTR_UNITS);
  const attr_ent *dnm = NULL, *dn = NULL;
  if (cls != NULL && lv != NULL && nent == 2 &&
      class_is(cls, MIZU_IX_CLASS_FACTOR, NULL) && vtype == MIZU_TYPE_INT &&
      (lv->kind == AV_STR || lv->kind == AV_STRLIST)) {
    int32_t one32 = one_i == INT64_MIN ? MIZU_NA_INT32 :
      (one_i > -2147483648LL && one_i <= 2147483647LL ? (int32_t) one_i : 0);
    const int32_t *src = vit.kind == MIZU_IX_INT ? &one32 :
      (const int32_t *) vptr;
    int32_t *lev_off = NULL, *codes = NULL;
    uint8_t *lev_bytes = NULL;
    int64_t nlev = 0, blen = 0;
    if (levels_read(lv, &lev_off, &lev_bytes, &nlev, &blen) == 0 &&
        codes_read(src, vn, nlev, &codes) == 0)
      out = factor_to_list(codes, vn, lev_off, lev_bytes);
    free(lev_off);
    free(lev_bytes);
    free(codes);
  } else if (dm != NULL && nent == 1 && vit.kind == MIZU_IX_VEC &&
             (dm->kind == AV_INTSPAN || dm->kind == AV_INT)) {
    int32_t one = (int32_t) dm->val.v;
    const int32_t *dims = dm->kind == AV_INT ? &one :
      (const int32_t *) dm->val.ptr;
    uint64_t nd = dm->kind == AV_INT ? 1 : dm->val.count;
    if (nd == 0) {
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "an empty dim");
    } else {
      out = dim_to_ndarray(vtype, vptr, vn, dims, nd);
    }
  } else if (dm == NULL && lv == NULL &&
             (dnm = attr_find(ents, nent, MIZU_IX_ATTR_NAMES)) != NULL &&
             nent == 1 && vit.kind == MIZU_IX_VEC) {
    /* a named vector (MIZU_CAP_LABELED): the vec home, wrapped with the
       labels — names the strv length, NA elements None */
    PyObject *t = str_tuple_of(dnm);
    if (t == NULL) goto attr_fail;
    if (PyTuple_GET_SIZE(t) != (Py_ssize_t) vn) {
      Py_DECREF(t);
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "the names strv is not the value's length");
      goto attr_fail;
    }
    PyObject *arr = ixr_vec(vtype, vptr, vn);
    if (arr == NULL) {
      Py_DECREF(t);
      goto attr_fail;
    }
    out = dimarray_attach(arr, t, NULL, NULL, 0);
    Py_DECREF(t);
    Py_DECREF(arr);
  } else if (dm != NULL &&
             (dn = attr_find(ents, nent, MIZU_IX_ATTR_DIMNAMES)) != NULL &&
             (nent == 2 || nent == 3) && vit.kind == MIZU_IX_VEC &&
             dn->kind == AV_OBJ &&
             (dm->kind == AV_INTSPAN || dm->kind == AV_INT)) {
    /* a dim array with dimnames (MIZU_CAP_LABELED): the F-order home,
       wrapped with the labels — class exactly "table" when present */
    if (nent == 3 && !class_is(cls, MIZU_IX_CLASS_TABLE, NULL)) {
      ixr_no_home(ents, nent, "an attributed value");
      goto attr_fail;
    }
    int32_t one = (int32_t) dm->val.v;
    const int32_t *dims = dm->kind == AV_INT ? &one :
      (const int32_t *) dm->val.ptr;
    uint64_t nd = dm->kind == AV_INT ? 1 : dm->val.count;
    if (nd == 0) {
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "an empty dim");
      goto attr_fail;
    }
    PyObject *entries = NULL, *axis = NULL;
    if (dimnames_tuples(dn->val.obj, &entries, &axis) < 0) {
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "a dimnames value of an unexpected form");
      goto attr_fail;
    }
    PyObject *arr = dim_to_ndarray(vtype, vptr, vn, dims, nd);
    if (arr == NULL) {
      Py_XDECREF(entries);
      Py_XDECREF(axis);
      goto attr_fail;
    }
    /* dim_to_ndarray validated the product; the entries' per-axis
       lengths are the remaining shape rule */
    const Py_ssize_t ndim = dm->kind == AV_INT ? 1 : (Py_ssize_t) dm->val.count;
    int ok = PyTuple_GET_SIZE(entries) == ndim;
    if (ok && axis != NULL)
      ok = PyTuple_GET_SIZE(axis) == ndim;
    for (Py_ssize_t i = 0; ok && i < ndim; i++) {
      PyObject *e = PyTuple_GET_ITEM(entries, i);
      if (e != Py_None &&
          PyTuple_GET_SIZE(e) != (Py_ssize_t) dims[i])
        ok = 0;
    }
    if (!ok) {
      Py_DECREF(arr);
      Py_XDECREF(entries);
      Py_XDECREF(axis);
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "a dimnames entry length is not its axis extent");
      goto attr_fail;
    }
    out = dimarray_attach(arr, NULL, entries, axis, nent == 3);
    Py_XDECREF(entries);
    Py_XDECREF(axis);
    Py_DECREF(arr);
  } else if (cls != NULL && nent == 1 && class_is(cls, MIZU_IX_CLASS_DATE, NULL) &&
             vtype == MIZU_TYPE_REAL) {
    out = realv_to_datetime(vptr, vn, 1);
  } else if (cls != NULL && (nent == 1 || (nent == 2 && tz != NULL)) &&
             class_is(cls, MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT) &&
             vtype == MIZU_TYPE_REAL) {
    out = realv_to_datetime(vptr, vn, 0);   /* tzone: display metadata,
                                               dropped standalone */
  } else if (cls != NULL && nent == 2 && un != NULL &&
             un->kind == AV_STR && class_is(cls, MIZU_IX_CLASS_DIFFTIME, NULL) &&
             vtype == MIZU_TYPE_REAL &&
             difftime_unit_secs((const char *) un->val.ptr,
                                (size_t) un->val.count) != 0.0) {
    out = realv_to_timedelta(vptr, vn,
                             difftime_unit_secs((const char *) un->val.ptr,
                                                (size_t) un->val.count));
  } else {
    ixr_no_home(ents, nent, "an attributed value");
  }
  attr_vals_free(ents, nent);
  return out;
attr_fail:
  attr_vals_free(ents, nent);
  return NULL;
}

// The frame reader ------------------------------------------------------------------

/* A Date column's doubles to owned i32 days (MIZU_NA_INT32 null); a
   fractional or non-finite value is the informative decline. */
static int fcol_date_fill(const double *src, uint64_t vn, fcol *c) {
  int32_t *days = mizu_py_ix_alloc(vn, 4);
  if (days == NULL) return -1;
  for (uint64_t i = 0; i < vn; i++) {
    uint64_t bits;
    memcpy(&bits, src + i, 8);
    if (is_na_r(bits)) {
      days[i] = MIZU_NA_INT32;
    } else if (!isfinite(src[i]) || trunc(src[i]) != src[i] ||
               fabs(src[i]) > 2.0e9) {
      free(days);
      PyErr_SetString(MizuError, "pymizu: no portable home for a "
                      "fractional Date");
      return -1;
    } else {
      days[i] = (int32_t) src[i];
    }
  }
  c->kind = FCOL_DATE;
  c->n = (int64_t) vn;
  c->values = (uint8_t *) days;
  return 0;
}

/* A POSIXct/difftime column's doubles to owned i64 us (INT64_MIN null),
   scale the value's unit in seconds (1.0 for POSIXct). An R-sentinel NA
   is the null; a non-finite or out-of-range value the informative
   decline. */
static int fcol_scaled_fill(const double *src, uint64_t vn, double scale,
                            int kind, const char *what, fcol *c) {
  int64_t *us = mizu_py_ix_alloc(vn, 8);
  if (us == NULL) return -1;
  for (uint64_t i = 0; i < vn; i++) {
    uint64_t bits;
    memcpy(&bits, src + i, 8);
    if (is_na_r(bits)) {
      us[i] = INT64_MIN;
    } else if (!isfinite(src[i]) || fabs(src[i]) * scale > 9.0e12) {
      free(us);
      PyErr_Format(MizuError, "pymizu: no portable home for this %s "
                   "value", what);
      return -1;
    } else {
      us[i] = (int64_t) llround(src[i] * scale * 1e6);
    }
  }
  c->kind = kind;
  c->n = (int64_t) vn;
  c->values = (uint8_t *) us;
  c->unit = 'u';
  return 0;
}

/* A POSIXct column's fill; the tzone name (empty when naive) is export
   metadata. */
static int fcol_ts_fill(const double *src, uint64_t vn, const char *tz,
                        size_t tz_len, fcol *c) {
  if (fcol_scaled_fill(src, vn, 1.0, FCOL_TS, "POSIXct", c) != 0)
    return -1;
  if (tz_len > 0 && tz_len < sizeof(c->tz))
    snprintf(c->tz, sizeof(c->tz), "%.*s", (int) tz_len, tz);
  return 0;
}

/* A difftime column's fill (in the dict's declared units, unit_secs the
   unit in seconds) — Arrow duration[us]. */
static int fcol_difftime_fill(const double *src, uint64_t vn,
                              double unit_secs, fcol *c) {
  return fcol_scaled_fill(src, vn, unit_secs, FCOL_TD, "difftime", c);
}

/* One frame column: the cursor at the column's first item; fills the
   fcol. */
static int ixr_column(mizu_ix *cur, fcol *c) {
  mizu_ix_item it;
  if (mizu_ix_next(cur, &it) != MIZU_OK) {
    ixr_cursor_err();
    return -1;
  }
  memset(c, 0, sizeof(*c));
  switch (it.kind) {
  case MIZU_IX_VEC: {
    int kind;
    switch (it.type) {
    case MIZU_TYPE_REAL: kind = FCOL_F64; break;
    case MIZU_TYPE_INT: kind = FCOL_I32; break;
    case MIZU_TYPE_INT64: kind = FCOL_I64; break;
    case MIZU_TYPE_RAW: kind = FCOL_U8; break;
    case MIZU_TYPE_CPLX: kind = FCOL_C128; break;
    case MIZU_TYPE_LGL: kind = FCOL_LGL; break;
    default:
      Py_UNREACHABLE();   /* the cursor's closed type set */
    }
    size_t sz = (size_t) it.count * (size_t) fcol_fixed_size(kind);
    c->values = malloc(sz != 0 ? sz : 1);
    if (c->values == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    memcpy(c->values, it.ptr, sz);
    c->kind = kind;
    c->n = (int64_t) it.count;
    return 0;
  }
  case MIZU_IX_STRV: {
    uint64_t n = it.count;
    int32_t *offs = malloc(((size_t) n + 1) * 4);
    uint8_t *bytes = NULL, *valid = NULL;
    size_t blen = 0, bcap = 0;
    int64_t nulls = 0;
    if (offs == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    offs[0] = 0;
    for (uint64_t i = 0; i < n; i++) {
      mizu_ix_item sit;
      if (mizu_ix_next(cur, &sit) != MIZU_OK || sit.kind != MIZU_IX_STR) {
        ixr_cursor_err();
        goto strv_fail;
      }
      if (sit.na) {
        nulls++;
        if (valid == NULL) {
          valid = mizu_py_bitmap_calloc(n);
          if (valid == NULL) goto strv_fail;
          for (uint64_t j = 0; j < i; j++) valid[j >> 3] |= 1 << (j & 7);
        }
      }
      if (!sit.na) {
        if (valid != NULL) valid[i >> 3] |= 1 << (i & 7);
        if (blen + sit.len > INT32_MAX) {
          PyErr_SetString(MizuError, "pymizu: a string column past the "
                          "int32 offset range");
          goto strv_fail;
        }
        if (bytes_reserve(&bytes, &bcap, blen + sit.len + 1) != 0) {
          PyErr_NoMemory();
          goto strv_fail;
        }
        memcpy(bytes + blen, sit.ptr, sit.len);
        blen += sit.len;
      }
      offs[i + 1] = (int32_t) blen;
    }
    bytes_shrink(&bytes, blen);
    c->kind = FCOL_STR;
    c->n = (int64_t) n;
    c->values = (uint8_t *) offs;
    c->bytes = bytes;
    c->bytes_len = (int64_t) blen;
    c->valid = valid;
    c->vnulls = nulls;
    return 0;
  strv_fail:
    free(offs);
    free(bytes);
    free(valid);
    return -1;
  }
  case MIZU_IX_ATTR: {
    mizu_ix_item vit;
    if (mizu_ix_next(cur, &vit) != MIZU_OK) {
      ixr_cursor_err();
      return -1;
    }
    if (vit.kind != MIZU_IX_VEC && vit.kind != MIZU_IX_REAL &&
        vit.kind != MIZU_IX_INT) {
      PyErr_SetString(MizuError, "pymizu: no portable home for a frame "
                      "column of this attributed form");
      return -1;
    }
    double one_r;
    int64_t one_i;
    const uint8_t *vptr = vit.ptr;
    uint64_t vn = vit.count;
    int vtype = (int) vit.type;
    if (vit.kind == MIZU_IX_REAL) {
      memcpy(&one_r, &vit.u64[0], 8);
      vptr = (const uint8_t *) &one_r;
      vn = 1;
      vtype = MIZU_TYPE_REAL;
    } else if (vit.kind == MIZU_IX_INT) {
      memcpy(&one_i, &vit.u64[0], 8);
      vptr = (const uint8_t *) &one_i;
      vn = 1;
      vtype = MIZU_TYPE_INT;
    }
    int nent = 0;
    attr_ent *ents = ixr_attr_dict(cur, &nent, NULL);
    if (ents == NULL) return -1;
    int rc = -1;
    const attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
    const attr_ent *lv = attr_find(ents, nent, MIZU_IX_ATTR_LEVELS);
    const attr_ent *tz = attr_find(ents, nent, MIZU_IX_ATTR_TZONE);
    if (cls != NULL && lv != NULL && nent == 2 &&
        class_is(cls, MIZU_IX_CLASS_FACTOR, NULL) && vtype == MIZU_TYPE_INT &&
        (lv->kind == AV_STR || lv->kind == AV_STRLIST)) {
      int32_t one32 = one_i == INT64_MIN ? MIZU_NA_INT32 :
        (one_i > -2147483648LL && one_i <= 2147483647LL ?
         (int32_t) one_i : 0);
      const int32_t *src = vit.kind == MIZU_IX_INT ? &one32 :
        (const int32_t *) vptr;
      int32_t *codes = NULL;
      if (levels_read(lv, &c->lev_off, &c->bytes, &c->nlev,
                      &c->bytes_len) == 0 &&
          codes_read(src, vn, c->nlev, &codes) == 0) {
        c->kind = FCOL_DICT;
        c->n = (int64_t) vn;
        c->values = (uint8_t *) codes;
        rc = 0;
      }
    } else if (cls != NULL && nent == 1 && class_is(cls, MIZU_IX_CLASS_DATE, NULL) &&
               vtype == MIZU_TYPE_REAL) {
      rc = fcol_date_fill((const double *) vptr, vn, c);
    } else if (cls != NULL && (nent == 1 || (nent == 2 && tz != NULL)) &&
               class_is(cls, MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT) &&
               vtype == MIZU_TYPE_REAL) {
      rc = fcol_ts_fill((const double *) vptr, vn,
                        tz != NULL && tz->kind == AV_STR ?
                          (const char *) tz->val.ptr : "",
                        tz != NULL && tz->kind == AV_STR ?
                          (size_t) tz->val.count : 0, c);
    } else if (cls != NULL && nent == 2 &&
               class_is(cls, MIZU_IX_CLASS_DIFFTIME, NULL) &&
               vtype == MIZU_TYPE_REAL) {
      const attr_ent *un = attr_find(ents, nent, MIZU_IX_ATTR_UNITS);
      const double us = un != NULL && un->kind == AV_STR ?
        difftime_unit_secs((const char *) un->val.ptr,
                           (size_t) un->val.count) : 0.0;
      if (us != 0.0)
        rc = fcol_difftime_fill((const double *) vptr, vn, us, c);
      else
        ixr_no_home(ents, nent, "an attributed frame column");
    } else {
      ixr_no_home(ents, nent, "an attributed frame column");
    }
    attr_vals_free(ents, nent);
    return rc;
  }
  case MIZU_IX_LGL: {
    int32_t *v = malloc(4);
    if (v == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    v[0] = it.u64[0] == 2 ? MIZU_NA_INT32 : (int32_t) it.u64[0];
    c->kind = FCOL_LGL;
    c->n = 1;
    c->values = (uint8_t *) v;
    return 0;
  }
  case MIZU_IX_INT: {
    int64_t x;
    memcpy(&x, &it.u64[0], 8);
    if (x == INT64_MIN || (x > -2147483648LL && x <= 2147483647LL)) {
      int32_t *v = malloc(4);
      if (v == NULL) {
        PyErr_NoMemory();
        return -1;
      }
      v[0] = x == INT64_MIN ? MIZU_NA_INT32 : (int32_t) x;
      c->kind = FCOL_I32;
      c->values = (uint8_t *) v;
    } else {
      int64_t *v = malloc(8);
      if (v == NULL) {
        PyErr_NoMemory();
        return -1;
      }
      v[0] = x;
      c->kind = FCOL_I64;
      c->values = (uint8_t *) v;
    }
    c->n = 1;
    return 0;
  }
  case MIZU_IX_REAL: {
    double *v = malloc(8);
    if (v == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    memcpy(v, &it.u64[0], 8);
    c->kind = FCOL_F64;
    c->n = 1;
    c->values = (uint8_t *) v;
    return 0;
  }
  case MIZU_IX_CPLX: {
    double *v = malloc(16);
    if (v == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    memcpy(v, &it.u64[0], 8);
    memcpy(v + 1, &it.u64[1], 8);
    c->kind = FCOL_C128;
    c->n = 1;
    c->values = (uint8_t *) v;
    return 0;
  }
  case MIZU_IX_STR1: {
    int32_t *offs = malloc(8);
    uint8_t *bytes = NULL, *valid = NULL;
    if (offs == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    offs[0] = 0;
    if (it.na) {
      valid = calloc(1, 1);
      offs[1] = 0;
    } else {
      bytes = malloc(it.len + 1);
      if (bytes == NULL) {
        free(offs);
        PyErr_NoMemory();
        return -1;
      }
      memcpy(bytes, it.ptr, it.len);
      offs[1] = (int32_t) it.len;
    }
    c->kind = FCOL_STR;
    c->n = 1;
    c->values = (uint8_t *) offs;
    c->bytes = bytes;
    c->bytes_len = (int64_t) (it.na ? 0 : (int64_t) it.len);
    c->valid = valid;
    c->vnulls = it.na ? 1 : 0;
    return 0;
  }
  default:
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "a frame column of an unexpected form");
    return -1;
  }
}

/* names into the block's packed store: unique, one per column (the
   AV_STR length-1 form for a single column). */
static int frame_names_build(frame_cols *fc, const attr_ent *nm) {
  const int ncols = fc->ncols;
  Py_ssize_t nn = nm->kind == AV_STR ? 1 :
    nm->kind == AV_STRLIST ? PyList_GET_SIZE(nm->val.obj) : -1;
  if (nn != ncols) {
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "the frame's names do not match its columns");
    return -1;
  }
  size_t name_len = 0;
  for (int i = 0; i < ncols; i++) {
    const char *s;
    Py_ssize_t len;
    if (nm->kind == AV_STR) {
      s = (const char *) nm->val.ptr;
      len = (Py_ssize_t) nm->val.count;
    } else {
      PyObject *o = PyList_GET_ITEM(nm->val.obj, i);
      if (!PyUnicode_Check(o)) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "an NA column name");
        return -1;
      }
      s = PyUnicode_AsUTF8AndSize(o, &len);
      if (s == NULL) return -1;
    }
    /* stamp name_off[i] ahead of the scan: the span of name i-1 ends at it */
    fc->name_off[i] = (int32_t) name_len;
    for (int j = 0; j < i; j++)
      if ((size_t) (fc->name_off[j + 1] - fc->name_off[j]) ==
            (size_t) len &&
          memcmp(fc->names + fc->name_off[j], s, (size_t) len) == 0) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "duplicate column names");
        return -1;
      }
    char *nb = realloc(fc->names, name_len + (size_t) len + 1);
    if (nb == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    if ((size_t) len > (size_t) INT32_MAX - name_len) {
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "column names past 2^31 bytes");
      return -1;
    }
    fc->names = nb;
    memcpy(fc->names + name_len, s, (size_t) len);
    name_len += (size_t) len;
  }
  fc->name_off[ncols] = (int32_t) name_len;
  return 0;
}

/* row.names: compact intv c(NA, +-n), a full intv, an int scalar at
   n == 1, or a strv / str. */
static PyObject *frame_rownames_build(const attr_ent *rn, int64_t nrow) {
  if (rn->kind == AV_INTSPAN) {
    const int32_t *v = (const int32_t *) rn->val.ptr;
    uint64_t nr = rn->val.count;
    if (nr == 2 && v[0] == MIZU_NA_INT32 &&
        (v[1] == nrow || v[1] == -nrow))
      Py_RETURN_NONE;
    if (nr == (uint64_t) nrow && !span_has_na32(v, (size_t) nrow))
      return ixr_vec_raw("int32", (const uint8_t *) v, (uint64_t) nrow, 4);
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "the row.names length is not the row count");
    return NULL;
  }
  if (rn->kind == AV_INT) {
    if (nrow == 1 && rn->val.v != INT64_MIN) {
      int32_t v = (int32_t) rn->val.v;
      return ixr_vec_raw("int32", (const uint8_t *) &v, 1, 4);
    }
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "the row.names length is not the row count");
    return NULL;
  }
  if (rn->kind == AV_STRLIST && PyList_GET_SIZE(rn->val.obj) == nrow) {
    PyObject *rownames = rn->val.obj;
    for (int64_t i = 0; i < nrow; i++)
      if (!PyUnicode_Check(PyList_GET_ITEM(rownames, i))) {
        PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                        "an NA row name");
        return NULL;
      }
    Py_INCREF(rownames);
    return rownames;
  }
  if (rn->kind == AV_STR && nrow == 1) {
    PyObject *s = PyUnicode_DecodeUTF8((const char *) rn->val.ptr,
                                       (Py_ssize_t) rn->val.count, NULL);
    if (s == NULL) return NULL;
    PyObject *rownames = PyList_New(1);
    if (rownames != NULL) {
      PyList_SET_ITEM(rownames, 0, s);
      return rownames;
    }
    Py_DECREF(s);
    return NULL;
  }
  PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                  "the row.names are not int or character of the row "
                  "count");
  return NULL;
}

/* The frame class rule (MIZU_CAP_LABELED's extension): class's last
   element is "data.frame" — AV_STR the length-1 form, a strv for the
   prefix classes (a tibble's c("tbl_df", "tbl", "data.frame")). */
static int class_ends_dataframe(const attr_ent *e) {
  if (class_is(e, MIZU_IX_CLASS_DATAFRAME, NULL)) return 1;
  if (e == NULL || e->kind != AV_STRLIST) return 0;
  PyObject *lst = e->val.obj;
  const Py_ssize_t n = PyList_GET_SIZE(lst);
  return n >= 1 &&
    str_obj_eq(PyList_GET_ITEM(lst, n - 1), MIZU_IX_CLASS_DATAFRAME);
}

/* The extended frame's stash: -1 with the no-home error (an extra key
   colliding with the reserved set — it would shadow a canonical pair,
   and tsp R validates at apply); 0 the plain frame (no stash — its
   bytes stay today's); 1 with *out the (class_tuple, extras) tuple,
   extras a list of (key, raw span) in dict order, never decoded. */
static int frame_stash_build(const attr_ent *ents, int nent,
                             const attr_ent *cls, PyObject **out) {
  int next = 0;
  for (int i = 0; i < nent; i++) {
    const char *k = (const char *) ents[i].key;
    const size_t kl = (size_t) ents[i].key_len;
    if ((kl == strlen(MIZU_IX_ATTR_NAMES) &&
         memcmp(k, MIZU_IX_ATTR_NAMES, kl) == 0) ||
        (kl == strlen(MIZU_IX_ATTR_CLASS) &&
         memcmp(k, MIZU_IX_ATTR_CLASS, kl) == 0) ||
        (kl == strlen(MIZU_IX_ATTR_ROWNAMES) &&
         memcmp(k, MIZU_IX_ATTR_ROWNAMES, kl) == 0))
      continue;
    if (mizu_ix_attr_reserved(k, kl) || ents[i].kind != AV_SPAN) {
      ixr_no_home(ents, nent, "an attributed list");
      return -1;
    }
    /* a comment is nil or character (R's apply rule) — the wire tag,
       never the decoded value */
    if (kl == 7 && memcmp(k, "comment", 7) == 0 &&
        ents[i].val.count != 0 &&
        ents[i].val.ptr[0] != MIZU_IX_TAG_NIL &&
        ents[i].val.ptr[0] != MIZU_IX_TAG_STR &&
        ents[i].val.ptr[0] != MIZU_IX_TAG_STRV) {
      ixr_no_home(ents, nent, "an attributed list");
      return -1;
    }
    next++;
  }
  if (next == 0 && class_is(cls, MIZU_IX_CLASS_DATAFRAME, NULL)) return 0;
  PyObject *kt = str_tuple_of(cls);
  if (kt == NULL) return -1;
  PyObject *ex = PyList_New(next);
  if (ex == NULL) {
    Py_DECREF(kt);
    return -1;
  }
  for (int i = 0, j = 0; i < nent; i++) {
    if (ents[i].kind != AV_SPAN) continue;
    PyObject *ks = PyUnicode_DecodeUTF8((const char *) ents[i].key,
                                        (Py_ssize_t) ents[i].key_len, NULL);
    PyObject *bs = PyBytes_FromStringAndSize(
      (const char *) ents[i].val.ptr, (Py_ssize_t) ents[i].val.count);
    PyObject *t = NULL;
    if (ks != NULL && bs != NULL) t = PyTuple_Pack(2, ks, bs);
    Py_XDECREF(ks);
    Py_XDECREF(bs);
    if (t == NULL) {
      Py_DECREF(kt);
      Py_DECREF(ex);
      return -1;
    }
    PyList_SET_ITEM(ex, j++, t);
  }
  *out = PyTuple_Pack(2, kt, ex);
  Py_DECREF(kt);
  Py_DECREF(ex);
  return *out != NULL ? 1 : -1;
}

/* The data.frame shape: read the columns, then the dict, and validate
   before installing the shell. The extended form (MIZU_CAP_LABELED)
   stashes the full class vector (decoded, for introspection) and every
   extra attribute's raw span on the Frame's _r_attrs — never decoded:
   a pristine echo splices them verbatim, a rebuild drops the stash and
   arrives in R as a plain data.frame. */
static PyObject *ixr_frame(mizu_ix *cur, uint64_t ncols64,
                           const ixr_mode *Py_UNUSED(mode)) {
  if (ncols64 == 0 || ncols64 > (1u << 20)) {
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "a frame without columns");
    return NULL;
  }
  int ncols = (int) ncols64;
  fcol *cols = calloc((size_t) ncols, sizeof(fcol));
  if (cols == NULL) return PyErr_NoMemory();
  int64_t nrow = -1;
  for (int i = 0; i < ncols; i++) {
    if (ixr_column(cur, &cols[i]) < 0) goto fail;
    if (nrow < 0) nrow = cols[i].n;
    else if (cols[i].n != nrow) {
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "the frame's columns differ in length");
      goto fail;
    }
  }
  int nent = 0;
  attr_ent *ents = ixr_attr_dict(cur, &nent, NULL);
  if (ents == NULL) goto fail;
  const attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  const attr_ent *nm = attr_find(ents, nent, MIZU_IX_ATTR_NAMES);
  const attr_ent *rn = attr_find(ents, nent, MIZU_IX_ATTR_ROWNAMES);
  if (cls == NULL || nm == NULL || rn == NULL || nent < 3 ||
      !class_ends_dataframe(cls)) {
    if (!PyErr_Occurred())
      ixr_no_home(ents, nent, "an attributed list");
    attr_vals_free(ents, nent);
    goto fail;
  }
  /* the extended frame's stash: the class vector (decoded, for
     introspection) and every extra's raw span — never decoded; a
     pristine echo splices them, a rebuild drops them */
  PyObject *r_attrs = NULL;
  if (frame_stash_build(ents, nent, cls, &r_attrs) < 0) {
    attr_vals_free(ents, nent);
    goto fail;
  }
  frame_cols *fc = frame_cols_new(ncols, nrow);
  if (fc == NULL) {
    PyErr_NoMemory();
    Py_XDECREF(r_attrs);
    attr_vals_free(ents, nent);
    goto fail;
  }
  if (frame_names_build(fc, nm) < 0) goto dict_fail;
  PyObject *rownames = frame_rownames_build(rn, nrow);
  if (rownames == NULL) goto dict_fail;
  attr_vals_free(ents, nent);
  /* adopt the columns into the block */
  free(fc->cols);
  fc->cols = cols;
  MizuFrame *self = (MizuFrame *) MizuFrameType.tp_alloc(&MizuFrameType, 0);
  if (self == NULL) {
    frame_cols_decref(fc);
    Py_DECREF(rownames);
    Py_XDECREF(r_attrs);
    return NULL;
  }
  self->fc = fc;
  self->row_names = rownames;
  self->loan = NULL;
  self->r_attrs = r_attrs;
  return (PyObject *) self;
dict_fail:
  Py_XDECREF(r_attrs);
  attr_vals_free(ents, nent);
  frame_cols_decref(fc);
fail:
  for (int i = 0; i < ncols; i++) fcol_free(&cols[i]);
  free(cols);
  return NULL;
}

// The MIZL tree wrap (§3.5) ----------------------------------------------------------

/* A leaf's validity pair into the column: a section offset borrows the
   region's bitmap, {0, -1} marks known-NA-free, {0, 0} leaves the lazy
   sentinel scan. */
static void tree_valid(fcol *c, const uint8_t *base, const int64_t valid[2]) {
  if (valid[0] > 0) {
    c->valid = (uint8_t *) (base + valid[0]);
    c->vnulls = valid[1];
  } else if (valid[1] == -1) {
    c->known_free = 1;
  }
}

static int tree_column(const uint8_t *base, size_t size, int64_t i,
                       fcol *c, frame_cols *fc, unsigned depth);
static int tree_column_fill(const uint8_t *base, const mizu_mizl_entry *e,
                            fcol *c, frame_cols *fc, unsigned depth);

/* The path half of a remote leaf's resolve: each intermediate a bare VEC
   leaf, the terminal entry handed out with its tree — pymizu_tree_walk_
   path's discipline, describing rather than wrapping. 0 ok, -1 corrupt. */
static int tree_ref_path(const uint8_t *base, size_t size, const char *path,
                         const uint8_t **out_base, size_t *out_size,
                         int64_t *out_idx, mizu_mizl_entry *ent) {
  const char *p = path;
  if (*p++ != '[') return -1;
  const uint8_t *cur = base;
  size_t cursz = size;
  for (;;) {
    if (*p < '1' || *p > '9') return -1;
    uint64_t v = (uint64_t) (*p++ - '0');
    while (*p >= '0' && *p <= '9') {
      const uint64_t d = (uint64_t) (*p - '0');
      if (v > (uint64_t) INT64_MAX / 10 ||
          (v == (uint64_t) INT64_MAX / 10 &&
           d > (uint64_t) INT64_MAX % 10))
        return -1;
      v = v * 10 + d;
      p++;
    }
    const int64_t idx = (int64_t) v - 1;
    if (mizu_mizl_elem(cur, cursz, idx, ent) != 0) return -1;
    if (*p == ']') {
      if (p[1] != '\0') return -1;
      *out_base = cur;
      *out_size = cursz;
      *out_idx = idx;
      return 0;
    }
    if (*p != ',') return -1;
    p++;
    if ((ent->sexptype & ~(int32_t) MIZU_MIZL_S4) != MIZU_TYPE_VEC ||
        ent->attrs_size != 0)
      return -1;
    cur += ent->data_offset;
    cursz = (size_t) ent->data_size;
  }
}

/* One distinct referenced region's hold on the column block: the mapping
   and its counted loan are the block's, released once in
   frame_cols_decref — the K matched columns of a provenance re-send over
   one region take one open and one loan, not K. */
static int frame_cols_hold_add(frame_cols *fc, const char *name,
                               mizu_shm *shm) {
  if (fc->nholds == fc->holds_cap) {
    size_t cap = fc->holds_cap != 0 ? 2 * fc->holds_cap : 4;
    mizu_shm **nh = realloc(fc->holds, cap * sizeof(*nh));
    if (nh == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    fc->holds = nh;
    char (*nn)[MIZU_NAME_MAX] = realloc(fc->hold_names, cap * sizeof(*nn));
    if (nn == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    fc->hold_names = nn;
    fc->holds_cap = cap;
  }
  if (fc->nholds == 0) fc->holds_pid = mizu_self_pid();
  snprintf(fc->hold_names[fc->nholds], MIZU_NAME_MAX, "%s", name);
  fc->holds[fc->nholds] = shm;
  fc->nholds++;
  return 0;
}

/* A remote leaf (MIZL directory tag 33): the column lives in another
   region and crosses by reference. Resolve the identifier span, validate
   the entry's claims against the referenced leaf, then the local leaf
   dispatch on the referenced region — a path recurses, a bare name fills
   off the root's synthesized entry. The column's bytes live in a region
   the frame's loan does not cover: its mapping rides the column block's
   holds, deduped by name (the scan below), the fcol borrowing. Every
   decline is the corrupt-or-newer shape — behind the capability gate,
   meeting one unadvertised is exactly that. */
static int tree_column_remote(const uint8_t *base, const mizu_mizl_entry *e,
                              fcol *c, frame_cols *fc, unsigned depth) {
  char id[256];
  /* the directory read caps a remote leaf's span at 255, but that cap is
     the ext tier's (it may change without deprecation): keep the local
     bound explicit ahead of the stack copy */
  if (e->data_size < 1 || e->data_size >= (int64_t) sizeof id)
    goto corrupt;
  memcpy(id, base + e->data_offset, (size_t) e->data_size);
  id[e->data_size] = '\0';
  const char *brack = strchr(id, '[');
  size_t name_len = brack != NULL ? (size_t) (brack - id) : strlen(id);
  char name[MIZU_NAME_MAX];
  if (name_len == 0 || name_len >= sizeof name) goto corrupt;
  memcpy(name, id, name_len);
  name[name_len] = '\0';
  mizu_shm *shm = NULL;
  for (size_t k = 0; k < fc->nholds; k++)
    if (strcmp(fc->hold_names[k], name) == 0) {
      shm = fc->holds[k];
      break;
    }
  const int fresh = shm == NULL;
  if (fresh && mizu_shm_open_view(&shm, name) != MIZU_OK) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region (a remote "
                    "leaf's referenced region is gone)");
    return -1;
  }
  const uint8_t *rbase = (const uint8_t *) mizu_shm_addr(shm);
  const size_t rsize = mizu_shm_size(shm);
  if (rsize < MIZU_HEADER_SIZE) goto corrupt_release;

  /* the referenced leaf's descriptor: a bare name off the root header (a
     synthesized entry for the fill), a path off the terminal entry */
  mizu_mizl_entry ref;
  const uint8_t *fbase = rbase;
  size_t fsize = rsize;
  int64_t fidx = -1;
  int na_free = 0;
  if (brack == NULL) {
    uint32_t magic;
    memcpy(&magic, rbase, 4);
    if (magic == MIZU_MAGIC_VEC) {
      int type;
      int64_t n, valid[2], attrs;
      if (mizu_mizh_check(rbase, rsize, &type, &n, valid) != 0)
        goto corrupt_release;
      memcpy(&attrs, rbase + 16, 8);
      ref.data_offset = MIZU_HEADER_SIZE;
      ref.data_size = n * (int64_t) mizu_type_elt_size(type) + attrs;
      ref.sexptype = type;
      ref.attrs_size = (int32_t) attrs;
      ref.length = n;
      ref.valid[0] = valid[0];
      ref.valid[1] = valid[1];
      na_free = valid[0] == 0 && valid[1] == -1;
    } else if (magic == MIZU_MAGIC_STR) {
      int64_t n, block, attrs;
      if (mizu_mizs_check(rbase, rsize, &n, &block, &attrs) != 0)
        goto corrupt_release;
      ref.data_offset = MIZU_HEADER_SIZE;
      ref.data_size = block + attrs;
      ref.sexptype = MIZU_TYPE_STR;
      ref.attrs_size = (int32_t) attrs;
      ref.length = n;
      ref.valid[0] = 0;
      ref.valid[1] = 0;
    } else {
      int64_t n, aoff, attrs, valid[2];
      if (mizu_mizl_check(rbase, rsize, &n, &aoff, &attrs, valid) != 0)
        goto corrupt_release;
      int32_t n32;
      memcpy(&n32, rbase + 4, 4);
      ref.data_offset = MIZU_HEADER_SIZE;
      ref.data_size = 0;
      ref.sexptype = MIZU_TYPE_VEC;
      ref.attrs_size = (int32_t) attrs;
      ref.length = n32;
      ref.valid[0] = 0;
      ref.valid[1] = 0;
    }
  } else {
    if (tree_ref_path(rbase, rsize, brack, &fbase, &fsize, &fidx,
                      &ref) != 0)
      goto corrupt_release;
    const int32_t rtag = ref.sexptype & ~(int32_t) MIZU_MIZL_S4;
    na_free = ref.valid[0] == 0 && ref.valid[1] == -1 &&
      (mizu_type_elt_size(rtag) != 0 || rtag == PYMIZU_MIZL_TAG_REF);
  }

  /* the entry's claims, validated against the resolved leaf */
  if (ref.length != e->length || ref.attrs_size != e->attrs_size ||
      (e->valid[0] == 0 && e->valid[1] == -1 && !na_free)) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region (a remote "
                    "leaf's claims do not match the referenced region)");
    goto release;
  }

  int rc = brack == NULL ? tree_column_fill(rbase, &ref, c, fc, depth + 1) :
    tree_column(fbase, fsize, fidx, c, fc, depth + 1);
  if (rc != 0) goto release;   /* the fill's own error stands */
  if (c->hold == NULL) {
    if (fresh && frame_cols_hold_add(fc, name, shm) < 0) goto release;
    c->hold = shm;   /* borrowed: the block's holds release the loan */
  } else if (fresh) {
    /* a chained reference: the column borrows the deeper region alone —
       this one serves nothing further (a cached hold stays: the deeper
       hold landed on the block too) */
    mizu_zc_unref(shm);
    mizu_shm_close(shm, 0);
  }
  return 0;
corrupt:
  PyErr_SetString(MizuError, "pymizu: corrupt or newer region (a remote "
                  "leaf of an unexpected form)");
  return -1;
corrupt_release:
  PyErr_SetString(MizuError, "pymizu: corrupt or newer region (a remote "
                  "leaf's referenced region is corrupt)");
release:
  if (fresh) {
    mizu_zc_unref(shm);
    mizu_shm_close(shm, 0);
  }
  return -1;
}

/* A frame column off an MIZL directory entry: the §1.0 column set only —
   attribute-free atomics (tag 32 the int64 form) borrow the leaf's bytes,
   a factor blob makes a dictionary column (owned levels, borrowed 1-based
   codes), Date/POSIXct blobs convert (owned), a string leaf borrows its
   MIZS block, a remote leaf resolves by reference; anything else declines
   informatively. */
static int tree_column_fill(const uint8_t *base, const mizu_mizl_entry *e,
                            fcol *c, frame_cols *fc, unsigned depth) {
  if (e->sexptype & MIZU_MIZL_S4) {
    PyErr_SetString(MizuError, "pymizu: no portable home for a frame "
                    "column with the S4 bit");
    return -1;
  }
  const int32_t tag = e->sexptype & ~(int32_t) MIZU_MIZL_S4;
  const uint8_t *data = base + e->data_offset;
  const int64_t body = e->data_size - (int64_t) e->attrs_size;
  if (tag == PYMIZU_MIZL_TAG_REF) {
    if (depth >= MIZU_IX_DEPTH_MAX) goto deep;
    return tree_column_remote(base, e, c, fc, depth);
  }
  if (tag == MIZU_TYPE_STR) {
    if (e->attrs_size != 0) goto newer;
    mizu_mizs_geom g = mizu_mizs_geometry(e->length);
    c->kind = FCOL_STR64;
    c->n = e->length;
    c->values = (uint8_t *) data;   /* the block */
    c->valid = (uint8_t *) (data + g.validity);
    c->bytes_len = body - g.data;
    c->borrowed = 1;
    return 0;
  }
  const size_t elt = mizu_type_elt_size(tag);
  if (elt == 0) {
    PyErr_SetString(MizuError, "pymizu: no portable home for a frame "
                    "column of this form (a list or serialized leaf)");
    return -1;
  }
  if (e->attrs_size == 0) {
    int kind;
    switch (tag) {
    case MIZU_TYPE_REAL: kind = FCOL_F64; break;
    case MIZU_TYPE_INT: kind = FCOL_I32; break;
    case MIZU_TYPE_INT64: kind = FCOL_I64; break;
    case MIZU_TYPE_RAW: kind = FCOL_U8; break;
    case MIZU_TYPE_CPLX: kind = FCOL_C128; break;
    default: kind = FCOL_LGL; break;   /* MIZU_TYPE_LGL (tags pre-checked) */
    }
    c->kind = kind;
    c->n = e->length;
    c->values = (uint8_t *) data;
    c->borrowed = 1;
    tree_valid(c, base, e->valid);
    return 0;
  }
  int nent = 0;
  attr_ent *ents = blob_attrs(data + body, (size_t) e->attrs_size, &nent);
  if (ents == NULL) return -1;
  int rc = -1;
  const attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  const attr_ent *lv = attr_find(ents, nent, MIZU_IX_ATTR_LEVELS);
  const attr_ent *tz = attr_find(ents, nent, MIZU_IX_ATTR_TZONE);
  const attr_ent *ts = attr_find(ents, nent, MIZU_IX_ATTR_TSCALE);
  if (cls != NULL && lv != NULL && nent == 2 &&
      class_is(cls, MIZU_IX_CLASS_FACTOR, NULL) && tag == MIZU_TYPE_INT &&
      (lv->kind == AV_STR || lv->kind == AV_STRLIST)) {
    if (levels_read(lv, &c->lev_off, &c->bytes, &c->nlev,
                    &c->bytes_len) == 0) {
      c->kind = FCOL_DICT;
      c->n = e->length;
      c->values = (uint8_t *) data;   /* the 1-based codes */
      c->codes1 = 1;
      c->borrowed = 1;
      tree_valid(c, base, e->valid);
      rc = 0;
    }
  } else if (cls != NULL && nent == 1 && class_is(cls, MIZU_IX_CLASS_DATE, NULL) &&
             tag == MIZU_TYPE_REAL) {
    rc = fcol_date_fill((const double *) data, (uint64_t) e->length, c);
  } else if (cls != NULL && (nent == 1 || (nent == 2 && tz != NULL)) &&
             class_is(cls, MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT) &&
             tag == MIZU_TYPE_REAL) {
    rc = fcol_ts_fill((const double *) data, (uint64_t) e->length,
                      tz != NULL && tz->kind == AV_STR ?
                        (const char *) tz->val.ptr : "",
                      tz != NULL && tz->kind == AV_STR ?
                        (size_t) tz->val.count : 0, c);
  } else if (cls != NULL && nent == 3 && tz != NULL && tscale_ns(ts) &&
             class_is(cls, MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT) &&
             tag == MIZU_TYPE_INT64) {
    /* the verbatim ns leaf: the attr-free fixed-width fill shape plus
       the unit and the tzone */
    c->kind = FCOL_TS;
    c->n = e->length;
    c->values = (uint8_t *) data;
    c->unit = 'n';
    if (tz->kind == AV_STR && tz->val.count > 0 &&
        tz->val.count < sizeof(c->tz))
      snprintf(c->tz, sizeof(c->tz), "%.*s", (int) tz->val.count,
               (const char *) tz->val.ptr);
    c->borrowed = 1;
    tree_valid(c, base, e->valid);
    rc = 0;
  } else if (cls != NULL && nent == 2 && class_is(cls, MIZU_IX_CLASS_DIFFTIME, NULL) &&
             tag == MIZU_TYPE_REAL) {
    const attr_ent *un = attr_find(ents, nent, MIZU_IX_ATTR_UNITS);
    const double us = un != NULL && un->kind == AV_STR ?
      difftime_unit_secs((const char *) un->val.ptr,
                         (size_t) un->val.count) : 0.0;
    if (us != 0.0)
      rc = fcol_difftime_fill((const double *) data, (uint64_t) e->length,
                              us, c);
    else
      ixr_no_home(ents, nent, "an attributed frame column");
  } else if (cls != NULL && nent == 3 && tscale_ns(ts) &&
             class_is(cls, MIZU_IX_CLASS_DIFFTIME, NULL) &&
             tag == MIZU_TYPE_INT64) {
    /* the verbatim ns difftime: no conversion walk, no range decline */
    c->kind = FCOL_TD;
    c->n = e->length;
    c->values = (uint8_t *) data;
    c->unit = 'n';
    c->borrowed = 1;
    tree_valid(c, base, e->valid);
    rc = 0;
  } else if (nent == 1 && tag == MIZU_TYPE_INT64 &&
             u64_marked(ents, nent)) {
    /* the verbatim uint64 leaf (MIZU_CAP_MIZL_U64V): the attr-free
       fixed-width fill shape, the marker consumed by the verdict */
    c->kind = FCOL_U64;
    c->n = e->length;
    c->values = (uint8_t *) data;
    c->borrowed = 1;
    tree_valid(c, base, e->valid);
    rc = 0;
  } else {
    ixr_no_home(ents, nent, "an attributed frame column");
  }
  attr_vals_free(ents, nent);
  return rc;
deep:
  PyErr_SetString(MizuError, "pymizu: corrupt or newer region (a remote "
                  "leaf chain past the depth cap)");
  return -1;
newer:
  PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                  "(a frame column of an unexpected form)");
  return -1;
}

/* The directory-entry read, then the fill. */
static int tree_column(const uint8_t *base, size_t size, int64_t i,
                       fcol *c, frame_cols *fc, unsigned depth) {
  mizu_mizl_entry e;
  if (mizu_mizl_elem(base, size, i, &e) != 0) {
    PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
    return -1;
  }
  memset(c, 0, sizeof(*c));
  return tree_column_fill(base, &e, c, fc, depth);
}

/* The {dim} blob's home: the borrowed 1-D view reshaped order="F" —
   strides over the shared pages, the loan pinned through the array's
   .base chain (a length-1 dim is the documented plain-vector shift). */
static PyObject *tree_dim_view(PyObject *owner, PyObject *loan, int type,
                               uint8_t *data, int64_t n,
                               const uint8_t *valid, int64_t nulls,
                               const attr_ent *dm) {
  int32_t one;
  const int32_t *dims;
  uint64_t nd;
  if (dm->kind == AV_INT) {
    one = (int32_t) dm->val.v;
    dims = &one;
    nd = 1;
  } else if (dm->kind == AV_INTSPAN) {
    dims = (const int32_t *) dm->val.ptr;
    nd = dm->val.count;
  } else {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                    "(a dim of an unexpected form)");
    return NULL;
  }
  if (nd == 0 || nd > 32) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                    "(a dim past 32 axes)");
    return NULL;
  }
  uint64_t prod = 1;
  for (uint64_t i = 0; i < nd; i++) {
    if (dims[i] < 0) {
      PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                      "(a negative dim)");
      return NULL;
    }
    if (dims[i] != 0 && prod > UINT64_MAX / (uint64_t) dims[i]) {
      PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                      "(a dim product overflow)");
      return NULL;
    }
    prod *= (uint64_t) dims[i];
  }
  if (prod != (uint64_t) n) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region (the dim "
                    "product is not the element count)");
    return NULL;
  }
  PyObject *view = mizu_py_view_borrow(
    owner, loan, data,
    (Py_ssize_t) n * (Py_ssize_t) mizu_type_elt_size(type), type,
    valid, nulls);
  if (view == NULL || nd == 1) return view;
  if (mizu_py_numpy_module() == NULL) {
    Py_DECREF(view);
    PyErr_SetString(MizuError, "pymizu: a dim-array view needs numpy "
                    "(install it)");
    return NULL;
  }
  PyObject *shape = PyTuple_New((Py_ssize_t) nd);
  if (shape == NULL) {
    Py_DECREF(view);
    return NULL;
  }
  for (uint64_t i = 0; i < nd; i++) {
    PyObject *d = PyLong_FromSsize_t((Py_ssize_t) dims[i]);
    if (d == NULL) {
      Py_DECREF(shape);
      Py_DECREF(view);
      return NULL;
    }
    PyTuple_SET_ITEM(shape, (Py_ssize_t) i, d);
  }
  PyObject *meth = PyObject_GetAttrString(view, "reshape");
  PyObject *kw = meth != NULL ? PyDict_New() : NULL;
  PyObject *ord = NULL;
  if (kw != NULL) {
    ord = PyUnicode_FromString("F");
    if (ord == NULL || PyDict_SetItemString(kw, "order", ord) < 0)
      Py_CLEAR(kw);
  }
  PyObject *args = kw != NULL ? PyTuple_Pack(1, shape) : NULL;
  PyObject *out = args != NULL ? PyObject_Call(meth, args, kw) : NULL;
  Py_XDECREF(meth);
  Py_XDECREF(kw);
  Py_XDECREF(ord);
  Py_XDECREF(args);
  Py_DECREF(shape);
  Py_DECREF(view);
  return out;
}

/* An attributed atomic's home (the blob decision shared by an MIZH root
   and an MIZL leaf): the factor shape a list[str|None], {dim} the
   reshaped view (an integer64 class consumed as the wire type), Date /
   POSIXct a datetime64 copy, anything else the no-home error. The loan
   anchor rides any view built here; copies hold no region state. */
PyObject *mizu_py_atomic_home(PyObject *owner, PyObject *loan, int type,
                              uint8_t *data, int64_t n,
                              const uint8_t *valid, int64_t nulls,
                              const uint8_t *blob, size_t blob_size) {
  int nent = 0;
  attr_ent *ents = blob_attrs(blob, blob_size, &nent);
  if (ents == NULL) return NULL;
  PyObject *out = NULL;
  const attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  const attr_ent *lv = attr_find(ents, nent, MIZU_IX_ATTR_LEVELS);
  const attr_ent *dm = attr_find(ents, nent, MIZU_IX_ATTR_DIM);
  const attr_ent *nm2 = NULL, *dn = NULL;
  if (cls != NULL && lv != NULL && nent == 2 &&
      class_is(cls, MIZU_IX_CLASS_FACTOR, NULL) && type == MIZU_TYPE_INT &&
      (lv->kind == AV_STR || lv->kind == AV_STRLIST)) {
    int32_t *lev_off = NULL, *codes = NULL;
    uint8_t *lev_bytes = NULL;
    int64_t nlev = 0, blen = 0;
    if (levels_read(lv, &lev_off, &lev_bytes, &nlev, &blen) == 0 &&
        codes_read((const int32_t *) data, (uint64_t) n, nlev,
                   &codes) == 0)
      out = factor_to_list(codes, (uint64_t) n, lev_off, lev_bytes);
    free(lev_off);
    free(lev_bytes);
    free(codes);
  } else if (dm != NULL &&
             (nent == 1 ||
              (nent == 2 && cls != NULL && type == MIZU_TYPE_REAL &&
               class_is(cls, "integer64", NULL)))) {
    out = tree_dim_view(owner, loan,
                        nent == 2 ? MIZU_TYPE_INT64 : type, data, n,
                        valid, nulls, dm);
  } else if ((nm2 = attr_find(ents, nent, MIZU_IX_ATTR_NAMES)) != NULL &&
             nent == 1 && dm == NULL) {
    /* a named vector (MIZU_CAP_LABELED): the borrowed view wrapped —
       the base chain preserved, so a whole re-send still REFs */
    PyObject *t = str_tuple_of(nm2);
    if (t == NULL) goto home_fail;
    if (PyTuple_GET_SIZE(t) != (Py_ssize_t) n) {
      Py_DECREF(t);
      PyErr_SetString(MizuError, "pymizu: corrupt or newer region (the "
                      "names strv is not the element count)");
      goto home_fail;
    }
    PyObject *view = mizu_py_view_borrow(
      owner, loan, data, (Py_ssize_t) n * (Py_ssize_t) mizu_type_elt_size(type),
      type, valid, nulls);
    if (view == NULL) {
      Py_DECREF(t);
      goto home_fail;
    }
    out = dimarray_attach(view, t, NULL, NULL, 0);
    Py_DECREF(t);
    Py_DECREF(view);
  } else if (dm != NULL &&
             (dn = attr_find(ents, nent, MIZU_IX_ATTR_DIMNAMES)) != NULL &&
             (nent == 2 || nent == 3) && dn->kind == AV_OBJ) {
    /* a dim array with dimnames (MIZU_CAP_LABELED): the F-order view
       wrapped — class exactly "table" when present (an integer64
       array's own class rides the wire tag) */
    if (nent == 3 && !class_is(cls, MIZU_IX_CLASS_TABLE, NULL)) {
      ixr_no_home(ents, nent, "an attributed value");
      goto home_fail;
    }
    int32_t one;
    const int32_t *dims;
    uint64_t nd;
    if (dm->kind == AV_INT) {
      one = (int32_t) dm->val.v;
      dims = &one;
      nd = 1;
    } else if (dm->kind == AV_INTSPAN) {
      dims = (const int32_t *) dm->val.ptr;
      nd = dm->val.count;
    } else {
      PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                      "(a dim of an unexpected form)");
      goto home_fail;
    }
    PyObject *entries = NULL, *axis = NULL;
    if (dimnames_tuples(dn->val.obj, &entries, &axis) < 0) {
      PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                      "(a dimnames value of an unexpected form)");
      goto home_fail;
    }
    PyObject *view = tree_dim_view(owner, loan, type, data, n, valid,
                                   nulls, dm);
    if (view == NULL) {
      Py_XDECREF(entries);
      Py_XDECREF(axis);
      goto home_fail;
    }
    int ok = nd <= 32 && PyTuple_GET_SIZE(entries) == (Py_ssize_t) nd;
    if (ok && axis != NULL)
      ok = PyTuple_GET_SIZE(axis) == (Py_ssize_t) nd;
    for (uint64_t i = 0; ok && i < nd; i++) {
      PyObject *e = PyTuple_GET_ITEM(entries, (Py_ssize_t) i);
      if (e != Py_None &&
          PyTuple_GET_SIZE(e) != (Py_ssize_t) dims[i])
        ok = 0;
    }
    if (!ok) {
      Py_DECREF(view);
      Py_XDECREF(entries);
      Py_XDECREF(axis);
      PyErr_SetString(MizuError, "pymizu: corrupt or newer region (a "
                      "dimnames entry length is not its axis extent)");
      goto home_fail;
    }
    out = dimarray_attach(view, NULL, entries, axis, nent == 3);
    Py_XDECREF(entries);
    Py_XDECREF(axis);
    Py_DECREF(view);
  } else if (cls != NULL && nent == 1 && class_is(cls, MIZU_IX_CLASS_DATE, NULL) &&
             type == MIZU_TYPE_REAL) {
    out = realv_to_datetime(data, (uint64_t) n, 1);
  } else if (cls != NULL &&
             (nent == 1 ||
              (nent == 2 && attr_find(ents, nent, MIZU_IX_ATTR_TZONE) != NULL)) &&
             class_is(cls, MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT) &&
             type == MIZU_TYPE_REAL) {
    out = realv_to_datetime(data, (uint64_t) n, 0);   /* tzone: display
                                                         metadata, dropped */
  } else if (cls != NULL && nent == 2 &&
             class_is(cls, MIZU_IX_CLASS_DIFFTIME, NULL) && type == MIZU_TYPE_REAL) {
    const attr_ent *un = attr_find(ents, nent, MIZU_IX_ATTR_UNITS);
    const double us = un != NULL && un->kind == AV_STR ?
      difftime_unit_secs((const char *) un->val.ptr,
                         (size_t) un->val.count) : 0.0;
    if (us != 0.0)
      out = realv_to_timedelta(data, (uint64_t) n, us);
    else
      ixr_no_home(ents, nent, "an attributed value");
  } else if (nent == 1 && type == MIZU_TYPE_INT64 &&
             u64_marked(ents, nent)) {
    /* a verbatim uint64 leaf as a plain list element: the Frame column
       read's rule (exact uint64, masked promoting to float64) — a view
       at the wire tag would read sign-flipped int64 */
    out = ixr_masked_u64(data, valid, (uint64_t) n);
  } else {
    ixr_no_home(ents, nent, "an attributed value");
  }
  attr_vals_free(ents, nent);
  return out;
home_fail:
  attr_vals_free(ents, nent);
  return NULL;
}

static PyObject *tree_walk(PyObject *owner, PyObject *loan,
                           const uint8_t *base, size_t size,
                           unsigned depth);

/* One directory entry's wrap (generic mode): a nested list recurses, a
   string leaf borrows its block, an attribute-free atomic borrows a view,
   an attributed one runs the blob decision; a serialized leaf, a
   VECSXP/STRSXP entry with a trailing blob, or the S4 bit declines
   informatively. */
static PyObject *tree_element(PyObject *owner, PyObject *loan,
                              const uint8_t *base, size_t size, int64_t i,
                              unsigned depth) {
  mizu_mizl_entry e;
  if (mizu_mizl_elem(base, size, i, &e) != 0) {
    PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
    return NULL;
  }
  if (e.sexptype & MIZU_MIZL_S4) {
    PyErr_SetString(MizuError, "pymizu: R S4 list trees do not cross the "
                    "view tier");
    return NULL;
  }
  const int32_t tag = e.sexptype & ~(int32_t) MIZU_MIZL_S4;
  const uint8_t *data = base + e.data_offset;
  const int64_t body = e.data_size - (int64_t) e.attrs_size;
  if (tag == MIZU_TYPE_VEC) {
    if (e.attrs_size != 0) goto newer;
    /* nested MIZL: capped — a crafted directory can point at its own
       region, and a deep tree at the C stack */
    if (depth >= MIZU_IX_DEPTH_MAX) goto deep;
    return tree_walk(owner, loan, data, (size_t) e.data_size, depth + 1);
  }
  if (tag == MIZU_TYPE_STR) {
    if (e.attrs_size != 0) goto newer;
    mizu_mizs_geom g = mizu_mizs_geometry(e.length);
    return mizu_py_strview_borrow(owner, loan, data, e.length,
                                  body - g.data);
  }
  if (tag == PYMIZU_MIZL_TAG_REF) {
    /* a remote leaf: resolve + the claim validation, the wrap a
       standalone view (its own owner — one loan per remote leaf; no
       handle ctx here, so the open rides no cache). The chain counts
       against the same depth cap — regions can reference in a cycle */
    char id[256];
    /* as in tree_column_remote: the 255 cap is the ext tier's — keep the
       local bound explicit ahead of the stack copy */
    if (e.data_size < 1 || e.data_size >= (int64_t) sizeof id)
      goto newer;
    if (depth >= MIZU_IX_DEPTH_MAX) goto deep;
    memcpy(id, data, (size_t) e.data_size);
    id[e.data_size] = '\0';
    return mizu_py_view_resolve_checked_depth(
      id, (size_t) e.data_size, NULL, e.length, (int64_t) e.attrs_size,
      e.valid[0] == 0 && e.valid[1] == -1, depth + 1);
  }
  const size_t elt = mizu_type_elt_size(tag);
  if (elt == 0) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                    "(a serialized leaf)");
    return NULL;
  }
  const uint8_t *valid = e.valid[0] > 0 ? base + e.valid[0] : NULL;
  if (e.attrs_size == 0)
    return mizu_py_view_borrow(owner, loan, (uint8_t *) data,
                               (Py_ssize_t) e.length * (Py_ssize_t) elt,
                               tag, valid, e.valid[1]);
  return mizu_py_atomic_home(owner, loan, tag, (uint8_t *) data, e.length,
                             valid, e.valid[1], data + body,
                             (size_t) e.attrs_size);
deep:
  PyErr_SetString(MizuError, "pymizu: corrupt or newer region (a list "
                  "tree past the depth cap)");
  return NULL;
newer:
  PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                  "(a leaf of an unexpected form)");
  return NULL;
}

static PyObject *tree_list(PyObject *owner, PyObject *loan,
                           const uint8_t *base, size_t size, int64_t n,
                           unsigned depth) {
  PyObject *out = PyList_New((Py_ssize_t) n);
  if (out == NULL) return NULL;
  for (int64_t i = 0; i < n; i++) {
    PyObject *v = tree_element(owner, loan, base, size, i, depth);
    if (v == NULL) {
      Py_DECREF(out);
      return NULL;
    }
    PyList_SET_ITEM(out, (Py_ssize_t) i, v);
  }
  return out;
}

/* names only: a dict of the element wraps; the names validated (one per
   element, non-NA, unique — the sender's gate, re-checked here). */
static PyObject *tree_dict(PyObject *owner, PyObject *loan,
                           const uint8_t *base, size_t size, int64_t n,
                           const attr_ent *nm, unsigned depth) {
  Py_ssize_t nn = nm->kind == AV_STR ? 1 :
    nm->kind == AV_STRLIST ? PyList_GET_SIZE(nm->val.obj) : -1;
  if (nn != n) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region (the "
                    "names do not match the element count)");
    return NULL;
  }
  PyObject *out = PyDict_New();
  if (out == NULL) return NULL;
  for (int64_t i = 0; i < n; i++) {
    PyObject *k;
    if (nm->kind == AV_STR) {
      k = PyUnicode_DecodeUTF8((const char *) nm->val.ptr,
                               (Py_ssize_t) nm->val.count, NULL);
      if (k == NULL) goto fail;
    } else {
      k = PyList_GET_ITEM(nm->val.obj, (Py_ssize_t) i);
      if (!PyUnicode_Check(k)) {
        PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                        "(an NA name)");
        goto fail;
      }
      Py_INCREF(k);
    }
    int dup = PyDict_Contains(out, k);
    if (dup != 0) {
      Py_DECREF(k);
      if (dup > 0)
        PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                        "(duplicate names)");
      goto fail;
    }
    PyObject *v = tree_element(owner, loan, base, size, i, depth);
    if (v == NULL || PyDict_SetItem(out, k, v) < 0) {
      Py_DECREF(k);
      Py_XDECREF(v);
      goto fail;
    }
    Py_DECREF(k);
    Py_DECREF(v);
  }
  return out;
fail:
  Py_DECREF(out);
  return NULL;
}

/* The data.frame shape: the region-backed Frame — columns off the
   directory entries (a remote leaf's region hold deduped on the block),
   names and row.names off the root blob, the tree's loan anchor on the
   shell. r_attrs the extended frame's stash (NULL for the plain form),
   a reference stolen on success. */
static PyObject *tree_frame(PyObject *Py_UNUSED(owner), PyObject *loan,
                            const uint8_t *base, size_t size, int64_t n,
                            const attr_ent *nm, const attr_ent *rn,
                            PyObject *r_attrs, unsigned depth) {
  if (n == 0 || n > (int64_t) (1u << 20)) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                    "(a frame without columns)");
    return NULL;
  }
  int ncols = (int) n;
  frame_cols *fc = frame_cols_new(ncols, 0);
  if (fc == NULL) return PyErr_NoMemory();
  int64_t nrow = -1;
  for (int i = 0; i < ncols; i++) {
    if (tree_column(base, size, i, &fc->cols[i], fc, depth) < 0) goto fail;
    if (nrow < 0) nrow = fc->cols[i].n;
    else if (fc->cols[i].n != nrow) {
      PyErr_SetString(MizuError, "pymizu: corrupt or newer region (the "
                      "frame's columns differ in length)");
      goto fail;
    }
  }
  fc->nrow = nrow;
  if (frame_names_build(fc, nm) < 0) goto fail;
  PyObject *rownames = frame_rownames_build(rn, nrow);
  if (rownames == NULL) goto fail;
  MizuFrame *self = (MizuFrame *) MizuFrameType.tp_alloc(&MizuFrameType, 0);
  if (self == NULL) {
    Py_DECREF(rownames);
    goto fail;
  }
  self->fc = fc;
  self->row_names = rownames;
  Py_INCREF(loan);
  self->loan = loan;
  self->r_attrs = r_attrs;
  return (PyObject *) self;
fail:
  Py_XDECREF(r_attrs);
  frame_cols_decref(fc);
  return NULL;
}

/* The root and leaf dict's shape decision: no attributes a plain list,
   names only a dict, the data.frame shape a region-backed Frame (the
   extended form stashing the class vector and extras), any other
   attribute set the no-home error. */
static PyObject *tree_home(PyObject *owner, PyObject *loan,
                           const uint8_t *base, size_t size, int64_t n,
                           attr_ent *ents, int nent, unsigned depth) {
  const attr_ent *nm = attr_find(ents, nent, MIZU_IX_ATTR_NAMES);
  const attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  const attr_ent *rn = attr_find(ents, nent, MIZU_IX_ATTR_ROWNAMES);
  if (nent == 0)
    return tree_list(owner, loan, base, size, n, depth);
  if (nent == 1 && nm != NULL)
    return tree_dict(owner, loan, base, size, n, nm, depth);
  if (nent >= 3 && cls != NULL && nm != NULL && rn != NULL &&
      class_ends_dataframe(cls)) {
    PyObject *r_attrs = NULL;
    if (frame_stash_build(ents, nent, cls, &r_attrs) < 0) return NULL;
    return tree_frame(owner, loan, base, size, n, nm, rn, r_attrs,
                      depth);
  }
  ixr_no_home(ents, nent, "an attributed list");
  return NULL;
}

/* A nested MIZL: validate through the core's check, read the blob first,
   then the shape decision. depth caps the recursion — crafted directories
   can nest without bound (a leaf can point at its own region) */
static PyObject *tree_walk(PyObject *owner, PyObject *loan,
                           const uint8_t *base, size_t size,
                           unsigned depth) {
  int64_t n = 0, attrs_off = 0, attrs_size = 0, valid[2];
  if (mizu_mizl_check(base, size, &n, &attrs_off, &attrs_size,
                      valid) != 0) {
    PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
    return NULL;
  }
  int nent = 0;
  attr_ent *ents = NULL;
  if (attrs_size > 0) {
    ents = blob_attrs(base + attrs_off, (size_t) attrs_size, &nent);
    if (ents == NULL) return NULL;
  }
  PyObject *out = tree_home(owner, loan, base, size, n, ents, nent, depth);
  attr_vals_free(ents, nent);
  return out;
}

PyObject *pymizu_tree_wrap_depth(PyObject *owner, PyObject *loan,
                                 const uint8_t *base, size_t size, int64_t n,
                                 int64_t attrs_off, int64_t attrs_size,
                                 unsigned depth) {
  int nent = 0;
  attr_ent *ents = NULL;
  if (attrs_size > 0) {
    ents = blob_attrs(base + attrs_off, (size_t) attrs_size, &nent);
    if (ents == NULL) return NULL;
  }
  PyObject *out = tree_home(owner, loan, base, size, n, ents, nent, depth);
  attr_vals_free(ents, nent);
  return out;
}

PyObject *pymizu_tree_wrap(PyObject *owner, PyObject *loan,
                           const uint8_t *base, size_t size, int64_t n,
                           int64_t attrs_off, int64_t attrs_size) {
  return pymizu_tree_wrap_depth(owner, loan, base, size, n, attrs_off,
                                attrs_size, 0);
}

PyObject *pymizu_tree_walk_path_depth(PyObject *owner, PyObject *loan,
                                      const uint8_t *base, size_t size,
                                      const char *path, unsigned depth) {
  const char *p = path;
  if (*p++ != '[') goto corrupt;
  const uint8_t *cur = base;
  size_t cursz = size;
  for (;;) {
    if (*p < '1' || *p > '9') goto corrupt;
    uint64_t v = (uint64_t) (*p++ - '0');
    while (*p >= '0' && *p <= '9') {
      const uint64_t d = (uint64_t) (*p - '0');
      if (v > (uint64_t) INT64_MAX / 10 ||
          (v == (uint64_t) INT64_MAX / 10 &&
           d > (uint64_t) INT64_MAX % 10))
        goto corrupt;
      v = v * 10 + d;
      p++;
    }
    const int64_t idx = (int64_t) v - 1;
    if (*p == ']') {
      if (p[1] != '\0') goto corrupt;
      return tree_element(owner, loan, cur, cursz, idx, depth);
    }
    if (*p != ',') goto corrupt;
    p++;
    mizu_mizl_entry e;
    if (mizu_mizl_elem(cur, cursz, idx, &e) != 0) goto corrupt;
    if ((e.sexptype & ~(int32_t) MIZU_MIZL_S4) != MIZU_TYPE_VEC ||
        e.attrs_size != 0)
      goto corrupt;
    cur += e.data_offset;
    cursz = (size_t) e.data_size;
  }
corrupt:
  PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
  return NULL;
}

PyObject *pymizu_tree_walk_path(PyObject *owner, PyObject *loan,
                                const uint8_t *base, size_t size,
                                const char *path) {
  return pymizu_tree_walk_path_depth(owner, loan, base, size, path, 0);
}

// The Frame's wire form (writer) ----------------------------------------------------

/* A fixed-width vector body behind a validity mask: an adopted column's
   nulls are bitmap-only (garbage in the data slots), so the emit walks
   per element, sentinel or value. The owned form's single put_vec memcpy
   handles every other case (its nulls ride the data as sentinels). */
static void ixe_vec_masked(ixw *w, int wire, const uint8_t *src,
                           const uint8_t *valid, uint64_t n) {
  const size_t elt = wire == MIZU_TYPE_INT || wire == MIZU_TYPE_LGL ? 4 : 8;
  IXW_PUT(w, ixe_vec_begin(IXW_DST(w), mizu_ix_tag_of(wire), n));
  if (w->decline) return;
  if (w->dst != NULL && w->total + (size_t) n * elt <= w->limit) {
    uint8_t *dst = w->dst + w->total;
    for (uint64_t i = 0; i < n; i++) {
      uint8_t *d = dst + (size_t) i * elt;
      if (bitmap_at(valid, i)) {
        memcpy(d, src + (size_t) i * elt, elt);
      } else if (wire == MIZU_TYPE_REAL) {
        store_na_r(d);
      } else if (wire == MIZU_TYPE_INT64) {
        const int64_t na = MIZU_NA_INT64;
        memcpy(d, &na, 8);
      } else {
        const int32_t na = MIZU_NA_INT32;
        memcpy(d, &na, 4);
      }
    }
  } else if (w->dst != NULL) {
    w->overflow = 1;
    w->dst = NULL;
  }
  w->total += (size_t) n * elt;
}

/* The mask gate for the memcpy arms: a borrowed validity bitmap holding
   nulls (the owned invariant's sentinels make the memcpy exact as-is). */
static int fcol_masked(const fcol *c) {
  return c->valid != NULL && masked_nulls(c->valid, (uint64_t) c->n) != 0;
}

/* The temporal column's unit in seconds (the 'I' REAL form's scale). */
static double unit_scale(char unit) {
  return unit == 's' ? 1.0 : unit == 'm' ? 1e-3 : unit == 'n' ? 1e-9 :
    1e-6;
}

/* One fcol as a frame column value. */
static void ixe_fcol(ixw *w, const fcol *c) {
  switch (c->kind) {
  case FCOL_F64:
    if (fcol_masked(c)) {
      ixe_vec_masked(w, MIZU_TYPE_REAL, c->values, c->valid, (uint64_t) c->n);
    } else {
      IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_REAL, c->values,
                                 (uint64_t) c->n));
    }
    break;
  case FCOL_I32:
    if (fcol_masked(c)) {
      ixe_vec_masked(w, MIZU_TYPE_INT, c->values, c->valid, (uint64_t) c->n);
    } else {
      IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_INT, c->values,
                                 (uint64_t) c->n));
    }
    break;
  case FCOL_I64:
    if (fcol_masked(c)) {
      ixe_vec_masked(w, MIZU_TYPE_INT64, c->values, c->valid, (uint64_t) c->n);
    } else {
      IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_INT64, c->values,
                                 (uint64_t) c->n));
    }
    break;
  case FCOL_U64: {
    /* uint64 has no 'I' form: the REAL conversion an older-version
       reader lands (the core's CVT_U64_REAL rule — past-2^53 valid
       lanes NA, counted; the MIZL frame path is verbatim for a peer
       advertising MIZU_CAP_MIZL_U64V) */
    const uint64_t *us = (const uint64_t *) c->values;
    IXW_PUT(w, ixe_vec_begin(IXW_DST(w), MIZU_IX_TAG_REALV, (uint64_t) c->n));
    if (w->decline) return;
    if (w->dst != NULL && w->total + (size_t) c->n * 8 <= w->limit) {
      uint8_t *dst = w->dst + w->total;
      for (int64_t i = 0; i < c->n; i++) {
        const int na = c->valid != NULL && !bitmap_at(c->valid, i);
        if (na || us[i] > 9007199254740992ULL) {
          store_na_r(dst + 8 * i);
          w->warn.n_range += !na;
        } else {
          double v = (double) us[i];
          memcpy(dst + 8 * i, &v, 8);
        }
      }
    } else if (w->dst != NULL) {
      w->overflow = 1;
      w->dst = NULL;
    }
    w->total += (size_t) c->n * 8;
    break;
  }
  case FCOL_U8:
    IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_RAW, c->values,
                               (uint64_t) c->n));
    break;
  case FCOL_C128:
    IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_CPLX, c->values,
                               (uint64_t) c->n));
    break;
  case FCOL_LGL:
    if (fcol_masked(c)) {
      ixe_vec_masked(w, MIZU_TYPE_LGL, c->values, c->valid, (uint64_t) c->n);
    } else {
      IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_LGL, c->values,
                                 (uint64_t) c->n));
    }
    break;
  case FCOL_STR: {
    const int32_t *offs = (const int32_t *) c->values;
    if (c->n == 1) {
      if (c->valid != NULL && !bitmap_at(c->valid, 0)) {
        IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), NULL, -1));
      } else {
        IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), c->bytes,
                                   offs[1] - offs[0]));
      }
      break;
    }
    IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) c->n));
    for (int64_t i = 0; i < c->n; i++) {
      if (c->valid != NULL && !bitmap_at(c->valid, i)) {
        IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), NULL, -1));
      } else {
        IXW_PUT(w, mizu_ix_put_strelt(
                  IXW_DST(w), c->bytes + offs[i], offs[i + 1] - offs[i]));
      }
    }
    break;
  }
  case FCOL_STR64: {
    /* the tree wrap's region string column: the validity bitmap and the
       i64 offsets walk the MIZS block in place (fcol_str64_check's span
       rule; a torn span declines, both passes agreeing element-wise) */
    mizu_mizs_geom g = mizu_mizs_geometry(c->n);
    const uint8_t *block = c->values;
    const uint8_t *validity = block + g.validity;
    const int64_t *offs64 = (const int64_t *) (block + g.offsets);
    const uint8_t *data = block + g.data;
    if (c->n == 1) {
      const int64_t lo = offs64[0], hi = offs64[1];
      if (!bitmap_at(validity, 0)) {
        IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), NULL, -1));
      } else if (lo < 0 || hi < lo || hi > c->bytes_len ||
                 hi - lo > INT32_MAX) {
        ixw_decline(w, "invalid string data in the frame's shared region");
      } else {
        IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), data + lo,
                                   (int32_t) (hi - lo)));
      }
      break;
    }
    IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) c->n));
    for (int64_t i = 0; i < c->n && !w->decline; i++) {
      if (!bitmap_at(validity, i)) {
        IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), NULL, -1));
        continue;
      }
      const int64_t lo = offs64[i], hi = offs64[i + 1];
      if (lo < 0 || hi < lo || hi > c->bytes_len || hi - lo > INT32_MAX) {
        ixw_decline(w, "invalid string data in the frame's shared region");
        break;
      }
      IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), data + lo,
                                    (int32_t) (hi - lo)));
    }
    break;
  }
  case FCOL_DICT: {
    static const char *cls[1] = { MIZU_IX_CLASS_FACTOR };
    const int32_t *codes = (const int32_t *) c->values;
    /* an adopted column's codes are 0-based (the wire is 1-based) with
       bitmap-only nulls; the region's codes1 are already 1-based */
    IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
    if (c->n == 1) {
      int na = (c->valid != NULL && !bitmap_at(c->valid, 0)) ||
        codes[0] == MIZU_NA_INT32;
      IXW_PUT(w, mizu_ix_put_int(
                IXW_DST(w),
                na ? INT64_MIN : c->codes1 ? codes[0] : codes[0] + 1));
    } else {
      IXW_PUT(w, ixe_vec_begin(IXW_DST(w), MIZU_IX_TAG_INTV,
                               (uint64_t) c->n));
      if (w->decline) return;
      if (w->dst != NULL && w->total + (size_t) c->n * 4 <= w->limit) {
        uint8_t *dst = w->dst + w->total;
        for (int64_t i = 0; i < c->n; i++) {
          int32_t v = (c->valid != NULL && !bitmap_at(c->valid, i)) ||
                      codes[i] == MIZU_NA_INT32 ? MIZU_NA_INT32 :
                      c->codes1 ? codes[i] : codes[i] + 1;
          memcpy(dst + 4 * i, &v, 4);
        }
      } else if (w->dst != NULL) {
        w->overflow = 1;
        w->dst = NULL;
      }
      w->total += (size_t) c->n * 4;
    }
    IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 2));
    ixe_key(w, MIZU_IX_ATTR_LEVELS);
    if (c->nlev == 1) {
      IXW_PUT(w, mizu_ix_put_str(
                IXW_DST(w), c->bytes, c->lev_off[1] - c->lev_off[0]));
    } else {
      IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) c->nlev));
      for (int64_t i = 0; i < c->nlev; i++)
        IXW_PUT(w, mizu_ix_put_strelt(
                  IXW_DST(w), c->bytes + c->lev_off[i],
                  c->lev_off[i + 1] - c->lev_off[i]));
    }
    ixe_key(w, MIZU_IX_ATTR_CLASS);
    ixe_class(w, cls, 1);
    break;
  }
  case FCOL_DATE: {
    static const char *cls[1] = { MIZU_IX_CLASS_DATE };
    const int32_t *days = (const int32_t *) c->values;
    IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
    IXW_PUT(w, ixe_vec_begin(IXW_DST(w), MIZU_IX_TAG_REALV, (uint64_t) c->n));
    if (w->decline) return;
    if (w->dst != NULL && w->total + (size_t) c->n * 8 <= w->limit) {
      uint8_t *dst = w->dst + w->total;
      for (int64_t i = 0; i < c->n; i++) {
        if ((c->valid != NULL && !bitmap_at(c->valid, i)) ||
            days[i] == MIZU_NA_INT32) {
          store_na_r(dst + 8 * i);
        } else {
          double v = (double) days[i];
          memcpy(dst + 8 * i, &v, 8);
        }
      }
    } else if (w->dst != NULL) {
      w->overflow = 1;
      w->dst = NULL;
    }
    w->total += (size_t) c->n * 8;
    IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 1));
    ixe_key(w, MIZU_IX_ATTR_CLASS);
    ixe_class(w, cls, 1);
    break;
  }
  case FCOL_TS:
    ixe_posixct(w, (const int64_t *) c->values, c->valid, (uint64_t) c->n,
                c->tz[0] != '\0' ? c->tz : "UTC", unit_scale(c->unit));
    break;
  case FCOL_TD:
    ixe_difftime(w, (const int64_t *) c->values, c->valid, (uint64_t) c->n,
                unit_scale(c->unit));
    break;
  }
}

/* The frame shape: attr(list cols, {names, class: "data.frame",
   row.names}). Names and row.names follow the length-1 scalar rule. */
/* A class vector verbatim (the stashed form): the tuple's strings as
   the strv, the 0x04 scalar at length 1 (ixe_class's shape). */
static void ixe_class_tuple(ixw *w, PyObject *kt) {
  const Py_ssize_t n = PyTuple_GET_SIZE(kt);
  if (n == 1) {
    Py_ssize_t len;
    const char *s =
      PyUnicode_AsUTF8AndSize(PyTuple_GET_ITEM(kt, 0), &len);
    if (s == NULL || len > INT32_MAX) {
      PyErr_Clear();
      ixw_decline(w, "a class name is not writable as UTF-8");
      return;
    }
    IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), s, (int32_t) len));
    return;
  }
  IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) n));
  for (Py_ssize_t i = 0; i < n && !w->decline; i++) {
    Py_ssize_t len;
    const char *s =
      PyUnicode_AsUTF8AndSize(PyTuple_GET_ITEM(kt, i), &len);
    if (s == NULL || len > INT32_MAX) {
      PyErr_Clear();
      ixw_decline(w, "a class name is not writable as UTF-8");
      return;
    }
    IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), s, (int32_t) len));
  }
}

/* The raw byte splice (an extra's stashed span): the put helpers'
   overflow discipline, no sizing walk — the span is the value's
   complete encoding, re-emitted verbatim. */
static void ixe_span(ixw *w, const void *src, size_t n) {
  if (w->decline) return;
  if (w->dst != NULL) {
    if (w->total + n > w->limit) {
      w->overflow = 1;
      w->dst = NULL;
    } else {
      memcpy(w->dst + w->total, src, n);
    }
  }
  w->total += n;
}

static void ixe_frame(ixw *w, const frame_cols *fc, PyObject *row_names,
                      PyObject *r_attrs) {
  /* attr + the column list + a column's own attr shape (3 at worst:
     factor/Date/TS/TD) */
  if (!ixw_attr_depth(w, 5)) return;
  /* the extended frame's stash (MIZU_CAP_LABELED): the class vector
     verbatim and the extras spliced raw in stored order — never
     decoded; the peer's mask must carry the bit (never a strip) */
  if (r_attrs != NULL && !ixw_labeled_gate(w)) return;
  const int nex = r_attrs != NULL ?
    (int) PyList_GET_SIZE(PyTuple_GET_ITEM(r_attrs, 1)) : 0;
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  IXW_PUT(w, mizu_ix_put_list_begin(IXW_DST(w), (uint64_t) fc->ncols));
  for (int i = 0; i < fc->ncols; i++) ixe_fcol(w, &fc->cols[i]);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), (uint64_t) (3 + nex)));
  ixe_key(w, MIZU_IX_ATTR_NAMES);
  if (fc->ncols == 1) {
    IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), fc->names,
                               fc->name_off[1] - fc->name_off[0]));
  } else {
    IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) fc->ncols));
    for (int i = 0; i < fc->ncols; i++)
      IXW_PUT(w, mizu_ix_put_strelt(
                IXW_DST(w), fc->names + fc->name_off[i],
                fc->name_off[i + 1] - fc->name_off[i]));
  }
  ixe_key(w, MIZU_IX_ATTR_CLASS);
  if (r_attrs != NULL) {
    ixe_class_tuple(w, PyTuple_GET_ITEM(r_attrs, 0));
  } else {
    static const char *cls[1] = { MIZU_IX_CLASS_DATAFRAME };
    ixe_class(w, cls, 1);
  }
  ixe_key(w, MIZU_IX_ATTR_ROWNAMES);
  if (row_names == Py_None) {
    int64_t vals[2] = { 0, -fc->nrow };
    int na[2] = { 1, 0 };
    ixe_intv(w, 2, vals, na);
  } else if (PyList_Check(row_names)) {
    Py_ssize_t n = PyList_GET_SIZE(row_names);
    if (n == 1) {
      Py_ssize_t l = 0;
      const char *s = PyUnicode_AsUTF8AndSize(PyList_GET_ITEM(row_names, 0),
                                              &l);
      if (s == NULL || l > INT32_MAX) {
        PyErr_Clear();
        ixw_decline(w, "a row name is not writable as UTF-8");
        return;
      }
      IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), s, (int32_t) l));
    } else {
      IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) n));
      for (Py_ssize_t i = 0; i < n && !w->decline; i++) {
        Py_ssize_t l = 0;
        const char *s = PyUnicode_AsUTF8AndSize(PyList_GET_ITEM(row_names,
                                                                i), &l);
        if (s == NULL || l > INT32_MAX) {
          PyErr_Clear();
          ixw_decline(w, "a row name is not writable as UTF-8");
          return;
        }
        IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), s, (int32_t) l));
      }
    }
  } else {
    /* the numpy int32 array / memoryview form */
    Py_buffer v;
    if (PyObject_GetBuffer(row_names, &v, PyBUF_ND | PyBUF_FORMAT) == 0) {
      ixe_intv_i32(w, (const int32_t *) v.buf, v.len / 4);
      PyBuffer_Release(&v);
    }
  }
  if (w->decline) return;
  if (r_attrs != NULL) {
    /* the extras: (key, raw span) pairs in stored order, spliced
       verbatim — never decoded */
    PyObject *ex = PyTuple_GET_ITEM(r_attrs, 1);
    for (int i = 0; i < nex && !w->decline; i++) {
      PyObject *t = PyList_GET_ITEM(ex, i);
      PyObject *ks = PyTuple_GET_ITEM(t, 0);
      PyObject *bs = PyTuple_GET_ITEM(t, 1);
      Py_ssize_t kn;
      const char *k = PyUnicode_AsUTF8AndSize(ks, &kn);
      if (k == NULL) {
        PyErr_Clear();
        ixw_decline(w, "an extra key is not writable as UTF-8");
        return;
      }
      IXW_PUT(w, mizu_ix_put_key(IXW_DST(w), k, (uint32_t) kn));
      ixe_span(w, PyBytes_AS_STRING(bs), (size_t) PyBytes_GET_SIZE(bs));
    }
  }
}

// The writer's numpy and temporal leaves --------------------------------------------

/* The scalar-tag mapping for 0-d buffers and numpy scalars. */
static void ixe_scalar_dtype(ixw *w, const char *f, Py_ssize_t itemsize,
                             const uint8_t *data) {
  const cvt_row *row = mizu_py_cvt_for_buffer(f, itemsize);
  if (row == NULL || row->cvt == CVT_U64_REAL) {
    if (row != NULL && row->cvt == CVT_U64_REAL && itemsize == 8) {
      uint64_t v;
      memcpy(&v, data, 8);
      if (v <= INT64_MAX) {
        IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), (int64_t) v));
        return;
      }
    }
    ixw_decline(w, "a numpy scalar of this dtype has no portable home");
    return;
  }
  /* One decode site for every conversion row (the table's own converter,
     16 bytes covering the widest output) — the scalar path then chooses
     only the output tag, so a new row can never drift from a
     hand-written decode here. */
  uint8_t buf[16];
  mizu_cvt_convert(buf, data, NULL, 0, 1, row, &w->warn);
  switch (row->wire) {
  case MIZU_TYPE_LGL: {
    int32_t v;
    memcpy(&v, buf, 4);
    IXW_PUT(w, mizu_ix_put_lgl(IXW_DST(w), v != 0));
    break;
  }
  case MIZU_TYPE_INT: {
    int32_t v;
    memcpy(&v, buf, 4);
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), (int64_t) v));
    break;
  }
  case MIZU_TYPE_INT64: {
    int64_t v;
    memcpy(&v, buf, 8);
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), v));
    break;
  }
  case MIZU_TYPE_REAL: {
    double v;
    memcpy(&v, buf, 8);
    IXW_PUT(w, mizu_ix_put_real(IXW_DST(w), v));
    break;
  }
  case MIZU_TYPE_CPLX: {
    double z[2];
    memcpy(z, buf, 16);
    IXW_PUT(w, mizu_ix_put_cplx(IXW_DST(w), z[0], z[1]));
    break;
  }
  default:   /* RAW: no scalar form — a uint8 scalar reads back as int */
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), (int64_t) buf[0]));
    break;
  }
}

/* A vector body through the conversion row: contiguous (stride 0) or
   per-element strided. */
static void ixe_cvt_body(ixw *w, const uint8_t *src, uint64_t n,
                         const cvt_row *row, int64_t stride) {
  IXW_PUT(w, ixe_vec_begin(IXW_DST(w), mizu_ix_tag_of(row->wire), n));
  if (w->decline) return;
  if (w->dst != NULL && w->total + (size_t) n * row->w_out <= w->limit) {
    uint8_t *dst = w->dst + w->total;
    if (stride == 0) {
      mizu_cvt_convert(dst, src, NULL, 0, (size_t) n, row, &w->warn);
    } else {
      for (uint64_t i = 0; i < n; i++)
        mizu_cvt_convert(dst + i * row->w_out,
                            src + (uint64_t) i * (uint64_t) stride, NULL,
                            0, 1, row, &w->warn);
    }
  } else if (w->dst != NULL) {
    w->overflow = 1;
    w->dst = NULL;
  }
  w->total += (size_t) n * row->w_out;
}

/* A 2-D transpose gather in 32x32 element tiles: the inner loop walks a
   source column (stride s0) writing sequentially, the tile keeping both
   the read and write footprints cache-resident, and pointer marching
   replaces the generic walk's div/mod pair per element. Identity rows
   take fixed-width copies (alignment-safe single load/store pairs, the
   cvt_run discipline); conversion rows keep the per-element convert
   call. */
#define IXE_T2_TILE 32
static void ixe_transpose2(uint8_t *dst, const uint8_t *src, uint64_t m,
                           uint64_t n, int64_t s0, int64_t s1,
                           const cvt_row *row, cvt_warn *warn) {
  const uint64_t wo = row->w_out;
  const int copy = row->cvt == CVT_COPY;
  for (uint64_t j0 = 0; j0 < n; j0 += IXE_T2_TILE) {
    const uint64_t j1 = n - j0 < IXE_T2_TILE ? n : j0 + IXE_T2_TILE;
    for (uint64_t i0 = 0; i0 < m; i0 += IXE_T2_TILE) {
      const uint64_t i1 = m - i0 < IXE_T2_TILE ? m : i0 + IXE_T2_TILE;
      for (uint64_t j = j0; j < j1; j++) {
        const uint8_t *sp = src + (int64_t) i0 * s0 + (int64_t) j * s1;
        uint8_t *dp = dst + (i0 + j * m) * wo;
        for (uint64_t i = i0; i < i1; i++) {
          if (copy) {
            switch (wo) {
            case 1: memcpy(dp, sp, 1); break;
            case 4: memcpy(dp, sp, 4); break;
            case 8: memcpy(dp, sp, 8); break;
            case 16: memcpy(dp, sp, 16); break;
            default: Py_UNREACHABLE();   /* the cvt table's closed widths */
            }
          } else {
            mizu_cvt_convert(dp, sp, NULL, 0, 1, row, warn);
          }
          sp += s0;
          dp += wo;
        }
      }
    }
  }
}

/* The F-order gather (pyinterop.h's, shared with layout.c's leaves): the
   contiguous convert when the F-order verdict holds — axes of length
   <= 1 exempt their stride (numpy's own rule; the elements are laid out
   F-contiguously regardless of the exempt stride) — the tiled transpose
   for 2-D, the generic div/mod walk past it. */
void mizu_py_nd_gather(uint8_t *dst, const uint8_t *src, int nd,
                       const int64_t *shape, const int64_t *strides,
                       const cvt_row *row, cvt_warn *warn) {
  uint64_t total = 1;
  for (int i = 0; i < nd; i++) total *= (uint64_t) shape[i];
  int64_t expect = row->w_in;
  int f_contig = 1;
  for (int i = 0; i < nd; i++) {
    if (shape[i] > 1 && strides[i] != expect) {
      f_contig = 0;
      break;
    }
    expect *= shape[i];
  }
  if (f_contig) {
    mizu_cvt_convert(dst, src, NULL, 0, (size_t) total, row, warn);
    return;
  }
  if (nd == 2) {
    ixe_transpose2(dst, src, (uint64_t) shape[0], (uint64_t) shape[1],
                   strides[0], strides[1], row, warn);
    return;
  }
  for (uint64_t t = 0; t < total; t++) {
    uint64_t rem = t;
    int64_t off = 0;
    for (int k = 0; k < nd; k++) {
      uint64_t dim = (uint64_t) shape[k];
      int64_t c = dim != 0 ? (int64_t) (rem % dim) : 0;
      rem = dim != 0 ? rem / dim : 0;
      off += c * strides[k];
    }
    mizu_cvt_convert(dst + t * row->w_out, src + off, NULL, 0, 1, row,
                        warn);
  }
}

/* A multi-dimensional array's body: the vec header, then the F-order
   gather (each inside the one stage copy). */
static void ixe_nd_body(ixw *w, const uint8_t *src, int nd,
                        const int64_t *shape, const int64_t *strides,
                        const cvt_row *row) {
  uint64_t total = 1;
  for (int i = 0; i < nd; i++) total *= (uint64_t) shape[i];
  IXW_PUT(w, ixe_vec_begin(IXW_DST(w), mizu_ix_tag_of(row->wire), total));
  if (w->decline) return;
  if (w->dst != NULL &&
      w->total + (size_t) total * row->w_out <= w->limit) {
    mizu_py_nd_gather(w->dst + w->total, src, nd, shape, strides, row,
                      &w->warn);
  } else if (w->dst != NULL) {
    w->overflow = 1;
    w->dst = NULL;
  }
  w->total += (size_t) total * row->w_out;
}

/* The dim shape: attr(vec, {dim: intv}). */
static void ixe_dim(ixw *w, const uint8_t *src, int nd,
                    const int64_t *shape, const int64_t *strides,
                    const cvt_row *row) {
  if (!ixw_attr_depth(w, 3)) return;   /* attr + vec + dict's dim intv */
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_nd_body(w, src, nd, shape, strides, row);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 1));
  ixe_key(w, MIZU_IX_ATTR_DIM);
  int64_t dims[32];
  for (int i = 0; i < nd && i < 32; i++) dims[i] = shape[i];
  ixe_intv(w, nd, dims, NULL);
}

/* The labeled shapes' send gate (MIZU_CAP_LABELED): every emission site
   declines locally on a caps-less peer — never a strip (metadata is
   data). */
static int ixw_labeled_gate(ixw *w) {
  if (w->caps & MIZU_CAP_LABELED) return 1;
  ixw_decline(w, "the peer predates the labeled attribute capability");
  return 0;
}

/* A labels strv (validated by mizu_py_labels_of): None as the -1 form,
   always the strv tag — never the length-1 scalar collapse (an extent-1
   dimnames entry must arrive a list, an NA one distinct from a nil
   axis). */
static void ixe_labels_strv(ixw *w, PyObject *seq, Py_ssize_t n) {
  IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) n));
  for (Py_ssize_t i = 0; i < n && !w->decline; i++) {
    PyObject *e = PySequence_GetItem(seq, i);
    if (e == NULL) {
      PyErr_Clear();
      ixw_decline(w, "a label sequence changed under the write");
      return;
    }
    if (e == Py_None) {
      IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), NULL, -1));
    } else {
      Py_ssize_t len;
      const char *s = PyUnicode_AsUTF8AndSize(e, &len);
      if (s == NULL || len > INT32_MAX) {
        PyErr_Clear();
        Py_DECREF(e);
        ixw_decline(w, "a label is not writable as UTF-8 (a lone "
                       "surrogate)");
        return;
      }
      IXW_PUT(w, mizu_ix_put_strelt(IXW_DST(w), s, (int32_t) len));
    }
    Py_DECREF(e);
  }
}

static void ixe_dimnames(ixw *w, PyObject *dimnames, PyObject *axis_names,
                         int nd);

/* The dst-forms (pyinterop.h): the 'I' writer's own emitters over a
   local ixw, so the wire forms cannot drift — NULL dst sizes, a real
   dst writes behind the size pass's total (the blob buffer discipline,
   mizu_py_blob_frame's pattern). */
size_t mizu_py_ix_labels_strv(uint8_t *dst, PyObject *seq, Py_ssize_t n) {
  ixw w;
  memset(&w, 0, sizeof(w));
  w.dst = dst;
  w.limit = SIZE_MAX;
  ixe_labels_strv(&w, seq, n);
  return w.total;
}

size_t mizu_py_ix_dimnames(uint8_t *dst, PyObject *dimnames,
                           PyObject *axis_names, int nd) {
  ixw w;
  memset(&w, 0, sizeof(w));
  w.dst = dst;
  w.limit = SIZE_MAX;
  ixe_dimnames(&w, dimnames, axis_names, nd);
  return w.total;
}

/* The dimnames value: the plain list (nil / strv entries), or
   attr(list, {names}) when axis_names is set — the one site a
   names-attributed list is not the 0x0d dict. */
static void ixe_dimnames(ixw *w, PyObject *dimnames, PyObject *axis_names,
                         int nd) {
  const int named = axis_names != NULL;
  if (named) IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  IXW_PUT(w, mizu_ix_put_list_begin(IXW_DST(w), (uint64_t) nd));
  w->depth++;
  for (int i = 0; i < nd && !w->decline; i++) {
    PyObject *e = PySequence_GetItem(dimnames, i);
    if (e == NULL) {
      PyErr_Clear();
      ixw_decline(w, "a dimnames sequence changed under the write");
      return;
    }
    if (e == Py_None) {
      IXW_PUT(w, mizu_ix_put_nil(IXW_DST(w)));
    } else {
      ixe_labels_strv(w, e, PySequence_Size(e));
    }
    Py_DECREF(e);
  }
  if (w->depth > 0) w->depth--;
  if (named) {
    IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 1));
    ixe_key(w, MIZU_IX_ATTR_NAMES);
    ixe_labels_strv(w, axis_names, nd);
  }
}

/* The 1-D body emitter (defined with the numpy leaves below). */
static void ixe_cvt_body(ixw *w, const uint8_t *src, uint64_t n,
                         const cvt_row *row, int64_t stride);

/* The named vector shape (MIZU_CAP_LABELED): attr(vec, {names: strv}). */
static void ixe_named_vec(ixw *w, const uint8_t *src, uint64_t n,
                          const cvt_row *row, int64_t stride,
                          PyObject *names) {
  if (!ixw_labeled_gate(w)) return;
  if (!ixw_attr_depth(w, 3)) return;   /* attr + vec + dict's names strv */
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_cvt_body(w, src, n, row, stride);
  if (w->decline) return;
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 1));
  ixe_key(w, MIZU_IX_ATTR_NAMES);
  ixe_labels_strv(w, names, (Py_ssize_t) n);
}

/* The dim + dimnames shape (MIZU_CAP_LABELED): attr(vec, {dim,
   dimnames[, class = "table"]}) — the labeled half of ixe_dim (a C-order
   gather or a strided one rides the same body). */
static void ixe_dim_labeled(ixw *w, const uint8_t *src, int nd,
                            const int64_t *shape, const int64_t *strides,
                            const cvt_row *row, const ix_labels *lb) {
  if (!ixw_labeled_gate(w)) return;
  /* attr + vec + (dict, its list, the carrier's attr + dict) */
  if (!ixw_attr_depth(w, 5)) return;
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_nd_body(w, src, nd, shape, strides, row);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w),
                                    (uint64_t) (lb->table ? 3 : 2)));
  ixe_key(w, MIZU_IX_ATTR_DIM);
  if (nd == 1) {
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), shape[0]));
  } else {
    int64_t dims[32];
    for (int i = 0; i < nd && i < 32; i++) dims[i] = shape[i];
    ixe_intv(w, nd, dims, NULL);
  }
  ixe_key(w, MIZU_IX_ATTR_DIMNAMES);
  ixe_dimnames(w, lb->dimnames, lb->axis_names, nd);
  if (lb->table) {
    ixe_key(w, MIZU_IX_ATTR_CLASS);
    static const char *cls[1] = { MIZU_IX_CLASS_TABLE };
    ixe_class(w, cls, 1);
  }
}

/* The temporal dtype units are pyinterop.h's (shared with layout.c). */
static int temporal_unit(const char *s, size_t n) {
  if (n == 1) {
    switch (s[0]) {
    case 'D': return TU_DAY;
    case 'W': return TU_WEEK;
    case 'h': return TU_HOUR;
    case 'm': return TU_MIN;
    case 's': return TU_SEC;
    case 'Y': case 'M': return TU_CAL;
    }
    return TU_NONE;
  }
  if (n == 2) {
    if (s[0] == 'm' && s[1] == 's') return TU_MS;
    if (s[0] == 'u' && s[1] == 's') return TU_US;
    if (s[0] == 'n' && s[1] == 's') return TU_NS;
    return TU_SUB;   /* ps, fs, as */
  }
  return TU_NONE;
}

/* The temporal dtype probe off .dtype.str, ahead of any buffer
   acquisition: the unit parsed between the brackets, the kind out.
   mizu_py_np_temporal's 1 datetime64 / 2 timedelta64, 0 not temporal. */
int mizu_py_np_temporal_unit(PyObject *obj, int *unit, int *delta) {
  if (mizu_py_np_kind(obj) == 0) return 0;
  int out = 0;
  PyObject *dt = PyObject_GetAttrString(obj, "dtype");
  PyObject *ds = dt != NULL ? PyObject_GetAttrString(dt, "str") : NULL;
  if (ds != NULL && PyUnicode_Check(ds)) {
    const char *s = PyUnicode_AsUTF8(ds);
    if (s != NULL) {
      const char *p = s;
      if (*p == '=' || *p == '<' || *p == '>' || *p == '|') p++;
      if ((p[0] == 'M' || p[0] == 'm') && p[1] == '8' && p[2] == '[') {
        const char *close = strchr(p + 3, ']');
        if (close != NULL) {
          if (unit != NULL)
            *unit = temporal_unit(p + 3, (size_t) (close - (p + 3)));
          if (delta != NULL) *delta = p[0] == 'm';
          out = p[0] == 'm' ? 2 : 1;
        }
      }
    }
  }
  Py_XDECREF(ds);
  Py_XDECREF(dt);
  if (PyErr_Occurred()) PyErr_Clear();
  return out;
}

/* A numpy datetime64/timedelta64 array or scalar (both refuse the buffer
   protocol; the unit was parsed off .dtype.str by the caller). 1-D or
   0-d only. */
static void ixw_numpy_temporal(ixw *w, PyObject *obj, int unit, int delta) {
  if (delta) {
    if (unit == TU_CAL) {
      ixw_decline(w, "a calendar-dependent timedelta64 unit "
                  "('Y'/'M') has no portable home");
      return;
    }
    if (unit == TU_SUB || unit == TU_NONE) {
      ixw_decline(w, "a timedelta64 unit below nanosecond resolution "
                  "has no portable home");
      return;
    }
    /* int64 counts to seconds: a day/week unit first, then the sub-day
       scales; a double second carries ns resolution to ~104 days */
    const double scale = unit == TU_DAY ? 86400.0 :
      unit == TU_WEEK ? 604800.0 : mizu_py_temporal_scale(unit);
    /* .view("int64") and the ND|FORMAT export cannot fail for a
       classified object (an exact ndarray or a numpy scalar views
       same-itemsize, and numpy scalars export the buffer protocol) — a
       failure would be a numpy change, not user input; decline as
       today */
    PyObject *view = PyObject_CallMethod(obj, "view", "s", "int64");
    Py_buffer bv;
    if (view == NULL ||
        PyObject_GetBuffer(view, &bv, PyBUF_ND | PyBUF_FORMAT) != 0) {
      PyErr_Clear();
      Py_XDECREF(view);
      ixw_decline_type(w, obj, "a timedelta64 value that cannot cross");
      return;
    }
    if (bv.ndim > 1) {
      PyBuffer_Release(&bv);
      Py_DECREF(view);
      ixw_decline(w, "a timedelta64 array past 1-D has no portable home");
      return;
    }
    ixe_difftime(w, (const int64_t *) bv.buf, NULL,
                 (uint64_t) (bv.len / 8), scale);
    PyBuffer_Release(&bv);
    Py_DECREF(view);
    return;
  }
  if (unit == TU_CAL) {
    ixw_decline(w, "a calendar-dependent datetime64 unit "
                "('Y'/'M') has no portable home");
    return;
  }
  if (unit == TU_SUB || unit == TU_NONE) {
    ixw_decline(w, "a datetime64 unit below microsecond resolution "
                "has no portable home");
    return;
  }
  /* the int64 counts: the timedelta arm's view-and-export */
  PyObject *view = PyObject_CallMethod(obj, "view", "s", "int64");
  Py_buffer bv;
  if (view == NULL ||
      PyObject_GetBuffer(view, &bv, PyBUF_ND | PyBUF_FORMAT) != 0) {
    PyErr_Clear();
    Py_XDECREF(view);
    ixw_decline_type(w, obj, "a datetime64 value that cannot cross");
    return;
  }
  if (bv.ndim > 1) {
    PyBuffer_Release(&bv);
    Py_DECREF(view);
    ixw_decline(w, "a datetime64 array past 1-D has no portable home");
    return;
  }
  const int64_t *counts = (const int64_t *) bv.buf;
  const uint64_t n = (uint64_t) (bv.len / 8);
  if (unit == TU_DAY || unit == TU_WEEK) {
    ixe_date(w, counts, n, unit == TU_WEEK ? 7.0 : 1.0);
  } else {
    ixe_posixct(w, counts, NULL, n, "UTC", mizu_py_temporal_scale(unit));
  }
  PyBuffer_Release(&bv);
  Py_DECREF(view);
}

/* The stage gate's temporal probe: 1 datetime64, 2 timedelta64, 0 not a
   temporal numpy object. A temporal numpy scalar exports its bytes as
   uint8 (the buffer protocol has no datetime), which the raw tier would
   otherwise claim as a byte vector — the gate skips the raw tier for
   these so the 'I' writer (or pickle) sees the value. */
int mizu_py_np_temporal(PyObject *obj) {
  return mizu_py_np_temporal_unit(obj, NULL, NULL);
}

/* The buffer-protocol leaf (and numpy scalar) walk. */
static void ixw_buffer(ixw *w, PyObject *obj) {
  if (mizu_py_buffer_subclass_reject(obj)) {
    ixw_decline_type(w, obj, "a buffer subclass (e.g. MaskedArray, "
                     "memmap) has no portable home");
    return;
  }
  /* the labeled-array flag ahead of the temporal probe: a labeled
     temporal has no labeled shape (a named Date / POSIXct stays
     declined) and must not drop its labels silently */
  const int labeled = mizu_py_dimarray_labeled(obj);
  /* temporal dtypes refuse the buffer protocol: probe .dtype.str first */
  int unit = TU_NONE, delta = 0;
  if (mizu_py_np_temporal_unit(obj, &unit, &delta) != 0) {
    if (labeled) {
      ixw_decline(w, "a labeled temporal array has no portable home");
      return;
    }
    ixw_numpy_temporal(w, obj, unit, delta);
    return;
  }
  Py_buffer v;
  int strided = 0;
  if (PyObject_GetBuffer(obj, &v, PyBUF_ND | PyBUF_FORMAT) < 0) {
    PyErr_Clear();
    if (PyObject_GetBuffer(obj, &v, PyBUF_FULL_RO) < 0) {
      PyErr_Clear();
      ixw_decline_type(w, obj, "a buffer of this dtype has no "
                       "portable home");
      return;
    }
    strided = 1;
  }
  if (v.suboffsets != NULL) {
    PyBuffer_Release(&v);
    ixw_decline(w, "a buffer with suboffsets has no portable home");
    return;
  }
  const cvt_row *row = mizu_py_cvt_for_buffer(v.format, v.itemsize);
  if (row == NULL) {
    PyBuffer_Release(&v);
    ixw_decline_type(w, obj, "a buffer of this dtype has no "
                     "portable home");
    return;
  }
  /* a strided buffer emits through the same stride-aware forms at any
     depth: a container leaf is a plain vec, a cvt body, or
     attr(vec, {dim}) — the gathers are F-order throughout */
  if (labeled) {
    /* the labeled shapes (MIZU_CAP_LABELED): the validated stash drives
       the emission; an invalid one declines with its message */
    int64_t shape64[32];
    for (int i = 0; i < v.ndim && i < 32; i++)
      shape64[i] = (int64_t) v.shape[i];
    ix_labels lb;
    if (mizu_py_labels_of(obj, v.ndim, shape64, &lb) < 0) {
      PyObject *et, *ev, *tb;
      PyErr_Fetch(&et, &ev, &tb);
      PyObject *es = ev != NULL ? PyObject_Str(ev) : NULL;
      ixw_decline(w, es != NULL ? PyUnicode_AsUTF8(es) :
                    "an invalid label stash");
      Py_XDECREF(es);
      Py_XDECREF(et);
      Py_XDECREF(ev);
      Py_XDECREF(tb);
      PyBuffer_Release(&v);
      return;
    }
    if (lb.names != NULL) {
      const int64_t stride = strided ? (int64_t) v.strides[0] : 0;
      ixe_named_vec(w, (const uint8_t *) v.buf, (uint64_t) v.shape[0],
                    row, stride, lb.names);
    } else {
      int64_t strides64[32];
      if (strided) {
        for (int i = 0; i < v.ndim; i++)
          strides64[i] = (int64_t) v.strides[i];
      } else {
        /* C-order strides (the plain path's computation) */
        int64_t acc = row->w_in;
        for (int i = v.ndim - 1; i >= 0; i--) {
          strides64[i] = acc;
          acc *= shape64[i];
        }
      }
      ixe_dim_labeled(w, (const uint8_t *) v.buf, v.ndim, shape64,
                      strides64, row, &lb);
    }
    mizu_py_labels_clear(&lb);
    PyBuffer_Release(&v);
    return;
  }
  if (v.ndim == 0) {
    ixe_scalar_dtype(w, v.format, v.itemsize, (const uint8_t *) v.buf);
  } else if (v.ndim == 1) {
    uint64_t n = (uint64_t) v.shape[0];
    if (row->cvt == CVT_COPY && !strided) {
      IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), row->wire, v.buf, n));
    } else {
      int64_t stride = strided ? (int64_t) v.strides[0] : 0;
      ixe_cvt_body(w, (const uint8_t *) v.buf, n, row, stride);
    }
  } else if (v.ndim <= 32) {
    int64_t shape[32], strides[32];
    for (int i = 0; i < v.ndim; i++) shape[i] = (int64_t) v.shape[i];
    if (strided) {
      for (int i = 0; i < v.ndim; i++) strides[i] = (int64_t) v.strides[i];
    } else {
      /* C-order strides */
      int64_t acc = row->w_in;
      for (int i = v.ndim - 1; i >= 0; i--) {
        strides[i] = acc;
        acc *= (int64_t) v.shape[i];
      }
    }
    ixe_dim(w, (const uint8_t *) v.buf, v.ndim, shape, strides, row);
  } else {
    ixw_decline(w, "an array past 32 dimensions has no portable home");
  }
  PyBuffer_Release(&v);
}

/* A stdlib datetime.datetime scalar: the POSIXct(1) shape — an aware
   datetime's zoneinfo key as tzone, a naive one "UTC". */
static void ixw_stdlib_datetime(ixw *w, PyObject *obj) {
  int64_t y = attr_as_long(obj, "year", NULL);
  int64_t mo = attr_as_long(obj, "month", NULL);
  int64_t d = attr_as_long(obj, "day", NULL);
  int64_t hh = attr_as_long(obj, "hour", NULL);
  int64_t mi = attr_as_long(obj, "minute", NULL);
  int64_t ss = attr_as_long(obj, "second", NULL);
  int64_t us = attr_as_long(obj, "microsecond", NULL);
  int64_t days = days_from_civil(y, mo, d);
  int64_t instant = ((days * 86400 + hh * 3600 + mi * 60 + ss) * 1000000) +
    us;
  char tz[64] = "UTC";
  PyObject *tzi = PyObject_GetAttrString(obj, "tzinfo");
  if (tzi != Py_None) {
    PyObject *off = PyObject_CallMethod(obj, "utcoffset", NULL);
    if (off != NULL && off != Py_None) {
      int ok2 = 1;
      int64_t od = attr_as_long(off, "days", &ok2);
      int64_t os = attr_as_long(off, "seconds", &ok2);
      int64_t ous = attr_as_long(off, "microseconds", &ok2);
      if (ok2)
        instant -= ((od * 86400 + os) * 1000000) + ous;
      else
        PyErr_Clear();
    } else if (off == NULL) {
      PyErr_Clear();
    }
    Py_XDECREF(off);
    PyObject *key = PyObject_GetAttrString(tzi, "key");
    if (key == NULL) PyErr_Clear();   /* no zoneinfo key: the str() probe */
    if (key != NULL && PyUnicode_Check(key)) {
      snprintf(tz, sizeof(tz), "%s", PyUnicode_AsUTF8(key));
    } else {
      Py_CLEAR(key);
      PyObject *r = PyObject_Str(tzi);
      if (r != NULL && PyUnicode_Check(r) &&
          PyUnicode_CompareWithASCIIString(r, "UTC") == 0) {
        /* datetime.timezone.utc: "UTC" */
      } else {
        ixw_decline(w, "an aware datetime whose tzinfo has no zoneinfo "
                    "key has no portable home");
        Py_XDECREF(r);
        Py_DECREF(tzi);
        return;
      }
      Py_XDECREF(r);
    }
    Py_XDECREF(key);
  }
  Py_XDECREF(tzi);
  ixe_posixct(w, &instant, NULL, 1, tz, 1e-6);
}

static void ixw_stdlib_date(ixw *w, PyObject *obj) {
  int64_t y = attr_as_long(obj, "year", NULL);
  int64_t mo = attr_as_long(obj, "month", NULL);
  int64_t d = attr_as_long(obj, "day", NULL);
  int64_t days = days_from_civil(y, mo, d);
  ixe_date(w, &days, 1, 1.0);
}

/* A datetime.timedelta scalar: the normalized (days, seconds,
   microseconds) triple to int64 us, emitted as one difftime second. */
static void ixw_stdlib_timedelta(ixw *w, PyObject *obj) {
  int64_t d = attr_as_long(obj, "days", NULL);
  int64_t s = attr_as_long(obj, "seconds", NULL);
  int64_t us = attr_as_long(obj, "microseconds", NULL);
  if (d > 106000 || d < -106000) {   /* |us| would overflow int64 */
    ixw_decline(w, "a timedelta past the int64 microsecond range has no "
                "portable home");
    return;
  }
  int64_t total = (d * 86400 + s) * 1000000 + us;
  ixe_difftime(w, &total, NULL, 1, 1e-6);
}

/* The plan's SHM_VEC candidate: the frame_buf_write body (one MIZH
   layout write into the stream's single spill checkout, the
   producer-loan retain), then the ref leaf with the fresh region's name
   (F1's D1: the one tag serves both by-reference cases). The size pass
   counts the conservative reservation — the fresh region's name length
   is known only at the checkout. A checkout failure (a churn race)
   abandons: nothing is retained yet, the caller re-runs with no_zc = 1. */
static void ixw_zc_leaf(ixw *w, PyObject *obj) {
  /* write-pass only (the plan's zc_node is never set on a count walk; the
     conservative reservation it mirrors, 2 + (MIZU_NAME_MAX - 1), lives in
     stage_task's selection arithmetic) */
  Py_buffer v;
  if (PyObject_GetBuffer(obj, &v, PyBUF_ND | PyBUF_FORMAT) < 0) {
    PyErr_Clear();
    /* the plan and the write cannot disagree (eligibility is a pure
       function and no verbs run between the passes) — a caller bug */
    ixw_decline(w, "the recorded zero-copy node is no longer eligible");
    return;
  }
  int type = mizu_py_wire_type_of(&v);
  size_t n = (size_t) v.len;
  mizu_shm *shm = NULL;
  if (v.strides != NULL || type == 0) {
    PyBuffer_Release(&v);
    ixw_decline(w, "the recorded zero-copy node is no longer eligible");
    return;
  }
  if (mizu_stage_spill_get(w->h, MIZU_HEADER_SIZE + n, &shm) != MIZU_OK) {
    PyBuffer_Release(&v);
    w->abandon = 1;
    w->decline = 1;
    return;
  }
  uint8_t *base = (uint8_t *) mizu_shm_addr(shm);
  mizu_mizh_write(base, type, (int64_t) (n / mizu_type_elt_size(type)));
  /* a Python buffer carries no NAs: known-NA-free (stage_raw's stamp) */
  mizu_mizh_validity_set(base, 0, -1);
  memcpy(base + MIZU_HEADER_SIZE, v.buf, n);
  PyBuffer_Release(&v);
  mizu_stage_retain_zc(w->h, shm);
  IXW_PUT(w, mizu_ix_put_ref(IXW_DST(w), shm->name,
                             (uint32_t) shm->name_len));
}

static void ixw_value(ixw *w, PyObject *obj);

/* The F1 ref gate (inline in ixw_node ahead of the dispatch): a
   re-sendable view emits the 0x13 leaf (the region identifier, zero
   value bytes) — REFHELD OR'd into the region's flags (the holder set
   widens beyond the direct peer), the emission recorded for the caller's
   spec pin (D4: the loan rides the claim-side release). A
   caps-insufficient view falls through to the value write, the §4.2
   filter behavior. */
static void ixw_node(ixw *w, PyObject *obj) {
  if (w->decline) return;
  if (w->depth > MIZU_IX_DEPTH_MAX) {
    ixw_decline(w, "the value nests past the depth cap (64)");
    return;
  }
  if (w->refs && obj != Py_None) {
    if (obj == w->zc_node && !w->zc_spent && w->h != NULL && !w->no_zc) {
      w->zc_spent = 1;
      ixw_zc_leaf(w, obj);
      return;
    }
    if (w->h == NULL && Py_TYPE(obj) == &MizuIxRefType) {
      /* the corpus's marker (the hook writer, never a live stage) */
      MizuIxRef *m = (MizuIxRef *) obj;
      Py_ssize_t mn;
      const char *ms = PyUnicode_AsUTF8AndSize(m->id, &mn);
      if (ms == NULL) {
        PyErr_Clear();
        ixw_decline(w, "a ref identifier is not writable as UTF-8");
        return;
      }
      if (mn < 1 || mn > 255) {
        ixw_decline(w, "a ref identifier length outside 1..255");
        return;
      }
      IXW_PUT(w, mizu_ix_put_ref(IXW_DST(w), ms, (uint32_t) mn));
      return;
    }
    PyObject *save = NULL;
    mizu_shm *shm = NULL;
    uint32_t need = 0;
    if (mizu_py_view_ref_probe(obj, &shm, &save, &need) &&
        (w->h == NULL || (w->caps & need) == need)) {
      if (w->dst != NULL)
        atomic_fetch_or_explicit(mizu_zc_flags_(mizu_shm_addr(shm)),
                                 MIZU_ZC_FLAG_REFHELD,
                                 memory_order_acq_rel);
      IXW_PUT(w, mizu_ix_put_ref(IXW_DST(w), shm->name,
                                 (uint32_t) shm->name_len));
      w->ref_emitted = 1;
      Py_DECREF(save);         /* the spec's pin covers the loan (D4) */
      return;
    }
    Py_XDECREF(save);
    /* the W2 fold: the plan's candidate record lives in the walk itself
       (the pre-scan's predicates are the gate's own, a re-sendable view
       never a candidate — the branch above returned). A layout-eligible
       buffer records and walks by value for its subtree size, nested
       detection suppressed; the optimistic write's tail past the first
       candidate is abandoned (selection follows the walk). */
    /* exact bytes stay the 0x05 bytes form at every size: a by-reference
       candidate would read back as a uint8 array — a size-dependent type */
    if (w->plan != NULL && !w->plan_suppress && !w->churn &&
        w->plan->ncand < 16 && !PyBytes_CheckExact(obj) &&
        PyObject_CheckBuffer(obj)) {
      Py_buffer v;
      if (PyObject_GetBuffer(obj, &v, PyBUF_ND | PyBUF_FORMAT) == 0) {
        const size_t zc_gate = mizu_py_zc_gate(w->inline_max);
        const int ok = v.strides == NULL && mizu_py_wire_type_of(&v) != 0 &&
          (size_t) v.len >= zc_gate;
        PyBuffer_Release(&v);
        if (ok) {
          w->dst = NULL;
          const size_t start = w->total;
          w->plan_suppress = 1;
          ixw_value(w, obj);
          w->plan_suppress = 0;
          if (!w->decline) {
            w->plan->cand[w->plan->ncand] = obj;
            w->plan->size[w->plan->ncand] = w->total - start;
            w->plan->ncand++;
          }
          return;
        }
      } else {
        PyErr_Clear();
      }
    }
  }
  ixw_value(w, obj);
}

/* The value dispatch (the ref gate's fall-through): every position the
   gate covers reaches here — the gate itself, or a recorded candidate's
   by-value subtree walk. */
static void ixw_value(ixw *w, PyObject *obj) {
  if (obj == Py_None) {
    IXW_PUT(w, mizu_ix_put_nil(IXW_DST(w)));
    return;
  }
  if (PyBool_Check(obj)) {
    IXW_PUT(w, mizu_ix_put_lgl(IXW_DST(w), obj == Py_True));
    return;
  }
  if (PyLong_CheckExact(obj)) {
    long long v = PyLong_AsLongLong(obj);
    if (v == -1 && PyErr_Occurred()) {
      PyErr_Clear();
      ixw_decline(w, "an int past int64 has no portable home");
      return;
    }
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), (int64_t) v));
    return;
  }
  if (PyFloat_CheckExact(obj)) {
    IXW_PUT(w, mizu_ix_put_real(IXW_DST(w), PyFloat_AS_DOUBLE(obj)));
    return;
  }
  if (PyComplex_CheckExact(obj)) {
    double re = PyComplex_RealAsDouble(obj);
    double im = PyComplex_ImagAsDouble(obj);
    IXW_PUT(w, mizu_ix_put_cplx(IXW_DST(w), re, im));
    return;
  }
  if (PyUnicode_CheckExact(obj)) {
    Py_ssize_t n;
    const char *s = PyUnicode_AsUTF8AndSize(obj, &n);
    if (s == NULL) {
      PyErr_Clear();
      ixw_decline(w, "a string is not writable as UTF-8 "
                  "(a lone surrogate)");
      return;
    }
    if (n > INT32_MAX) {
      ixw_decline(w, "a string past 2^31 bytes has no portable home");
      return;
    }
    IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), s, (int32_t) n));
    return;
  }
  if (PyBytes_CheckExact(obj)) {
    IXW_PUT(w, mizu_ix_put_bytes(IXW_DST(w), PyBytes_AS_STRING(obj),
                                 (uint64_t) PyBytes_GET_SIZE(obj)));
    return;
  }
  if (PyList_CheckExact(obj) || PyTuple_CheckExact(obj)) {
    int is_list = PyList_CheckExact(obj) != 0;
    Py_ssize_t n = is_list ? PyList_GET_SIZE(obj) : PyTuple_GET_SIZE(obj);
    if (is_list) {
      /* a list[str | None] crosses as the 0x0b string vector — the home
         the MIZS tier picks past the zc floor, at every size (tuples keep
         the list tag: a tuple is a list on the wire, never a strv) */
      int64_t str_bytes = 0;
      if (mizu_py_strlist_probe(obj, &str_bytes)) {
        ixe_strlist(w, obj);
        return;
      }
    }
    IXW_PUT(w, mizu_ix_put_list_begin(IXW_DST(w), (uint64_t) n));
    w->depth++;
    for (Py_ssize_t i = 0; i < n && !w->decline; i++) {
      size_t save = w->path_len;
      ixw_path_index(w, i);
      ixw_node(w, is_list ? PyList_GET_ITEM(obj, i) :
                PyTuple_GET_ITEM(obj, i));
      if (!w->decline) ixw_path_pop(w, save);
    }
    w->depth--;
    return;
  }
  if (PyDict_CheckExact(obj)) {
    Py_ssize_t n = PyDict_Size(obj);
    IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), (uint64_t) n));
    w->depth++;
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (!w->decline && PyDict_Next(obj, &pos, &k, &v)) {
      if (!PyUnicode_CheckExact(k)) {
        ixw_decline_type(w, k, "a non-str dict key has no portable home");
        break;
      }
      Py_ssize_t kn;
      const char *ks = PyUnicode_AsUTF8AndSize(k, &kn);
      if (ks == NULL) {
        PyErr_Clear();
        ixw_decline(w, "a dict key is not writable as UTF-8 "
                    "(a lone surrogate)");
        break;
      }
      if (kn > UINT32_MAX) {
        ixw_decline(w, "a dict key past 2^32 bytes has no portable home");
        break;
      }
      size_t save = w->path_len;
      ixw_path_key(w, k);
      IXW_PUT(w, mizu_ix_put_key(IXW_DST(w), ks, (uint32_t) kn));
      ixw_node(w, v);
      if (!w->decline) ixw_path_pop(w, save);
    }
    w->depth--;
    return;
  }
  if (Py_TYPE(obj) == &MizuFrameType) {
    MizuFrame *f = (MizuFrame *) obj;
    ixe_frame(w, f->fc, f->row_names, f->r_attrs);
    return;
  }
  datetime_probe();
  if (mizu_dt_datetime != NULL && Py_TYPE(obj) == mizu_dt_datetime) {
    ixw_stdlib_datetime(w, obj);
    return;
  }
  if (mizu_dt_date != NULL && Py_TYPE(obj) == mizu_dt_date) {
    ixw_stdlib_date(w, obj);
    return;
  }
  if (mizu_dt_timedelta != NULL && Py_TYPE(obj) == mizu_dt_timedelta) {
    ixw_stdlib_timedelta(w, obj);
    return;
  }
  if (PyObject_CheckBuffer(obj) || mizu_py_np_kind(obj) != 0) {
    ixw_buffer(w, obj);
    return;
  }
  ixw_decline_type(w, obj, "a value of this type has no portable home");
}

// The Arrow stream front-end ----------------------------------------------------------

/* The pulled-batch state (ixs_hold / ixs_pcol / ixs) and the per-batch
   Arrow helpers (mizu_arrow_valid, mizu_arrow_str_at) are the ext tier's
   dual-form inlines — shared
   with shmframe.c's MIZL writer. */

static void ixs_hold_free(ixs_hold *h) {
  for (size_t i = 0; i < h->nb; i++)
    if (h->arrs[i].release != NULL) h->arrs[i].release(&h->arrs[i]);
  free(h->arrs);
  memset(h, 0, sizeof(*h));
}

static void ixs_free(ixs *x) {
  if (!x->borrowed_hold) {
    ixs_hold_free(&x->hold);
    if (x->schema_owned && x->schema.release != NULL)
      x->schema.release(&x->schema);
    if (x->st != NULL && x->st->release != NULL) x->st->release(x->st);
  }
  free(x->cols);
  Py_XDECREF(x->cap);
  memset(x, 0, sizeof(*x));
}

/* A decline from the front-end: DeclinedError with the cause named. */
static void ixs_decline(const char *what) {
  PyErr_Format(MizuDeclinedError,
               "pymizu: value is not portable to the peer (%s)", what);
}

// The front-end driver -------------------------------------------------------------

/* Pull every batch (the size pass holds them; the write pass reads from
   the hold — the capsule is single-consumption). The batch vetting is the
   core's mizu_arrow_open. */
static int ixs_pull(ixs *x) {
  for (;;) {
    if (x->hold.nb == x->hold.cap) {
      size_t cap = x->hold.cap != 0 ? 2 * x->hold.cap : 4;
      ArrowArray *nb = realloc(x->hold.arrs, cap * sizeof(ArrowArray));
      if (nb == NULL) {
        PyErr_NoMemory();
        return -1;
      }
      x->hold.arrs = nb;
      x->hold.cap = cap;
    }
    ArrowArray *a = &x->hold.arrs[x->hold.nb];
    memset(a, 0, sizeof(*a));
    if (x->st->get_next(x->st, a) != 0) {
      const char *e = x->st->get_last_error != NULL ?
        x->st->get_last_error(x->st) : NULL;
      char msg[256];
      snprintf(msg, sizeof(msg), "the Arrow stream failed: %s",
               e != NULL ? e : "unknown error");
      ixs_decline(msg);
      return -1;
    }
    if (a->release == NULL) break;   /* the end of the stream */
    x->hold.nb++;
    x->hold.rows += a->length;
  }
  return 0;
}

/* The core's classify + vet + validate over the pulled batches (arrow.c;
   its thread-local decline re-raised here by the caller). The batches
   pointer array (and the single path's wrapper arrays) is caller-owned —
   it must outlive the handle: keep1 / keep2 take the allocations, freed
   by the caller after mizu_arrow_close. */
static mizu_arrow *ixs_arrow_open(ixs *x, uint32_t peer_caps, void **keep1,
                                  void **keep2) {
  *keep1 = NULL;
  *keep2 = NULL;
  if (x->single) {
    /* the single path wrapped as a one-column struct: the same writer
       serves both (a non-struct child keeps its own schema; the name
       check wants a non-NULL name, unused on this path). One carved
       allocation: the wrapper structs, their child pointers, and the
       batches pointer array (all caller-owned past the open). */
    ArrowSchema kid = x->schema;
    if (kid.name == NULL) kid.name = "";
    ArrowSchema *kids[1] = { &kid };
    ArrowSchema root;
    memset(&root, 0, sizeof(root));
    root.format = "+s";
    root.n_children = 1;
    root.children = kids;
    const size_t nb = x->hold.nb != 0 ? x->hold.nb : 1;
    uint8_t *blk = malloc(nb * (sizeof(ArrowArray) +
                                2 * sizeof(ArrowArray *)));
    if (blk == NULL) {
      PyErr_NoMemory();
      return NULL;
    }
    ArrowArray *wraps = (ArrowArray *) blk;
    ArrowArray **wkids = (ArrowArray **) (blk + nb * sizeof(ArrowArray));
    const ArrowArray **batches = (const ArrowArray **) (wkids + nb);
    for (size_t b = 0; b < x->hold.nb; b++) {
      wkids[b] = &x->hold.arrs[b];
      wraps[b] = x->hold.arrs[b];
      wraps[b].children = &wkids[b];
      wraps[b].n_children = 1;
      /* the synthetic struct carries no validity of its own: the child's
         nulls ride the child's bitmap, and the core's batch gate reads
         the struct's offset / null_count as the batch shape */
      wraps[b].offset = 0;
      wraps[b].null_count = 0;
      batches[b] = &wraps[b];
    }
    mizu_arrow *ar = NULL;
    mizu_status st = mizu_arrow_open(&ar, &root, batches, x->hold.nb,
                                     peer_caps);
    if (st != MIZU_OK) {
      free(blk);
      return NULL;
    }
    *keep1 = blk;
    return ar;
  }
  const ArrowArray **batches = malloc((x->hold.nb != 0 ? x->hold.nb : 1) *
                                      sizeof(*batches));
  if (batches == NULL) {
    PyErr_NoMemory();
    return NULL;
  }
  for (size_t b = 0; b < x->hold.nb; b++) batches[b] = &x->hold.arrs[b];
  mizu_arrow *ar = NULL;
  mizu_status st = mizu_arrow_open(&ar, &x->schema, batches, x->hold.nb,
                                   peer_caps);
  if (st != MIZU_OK) {
    free(batches);
    return NULL;
  }
  *keep1 = (void *) batches;
  return ar;
}

/* The core's per-column plans into the emitters' form: the kind orders
   coincide (both registries are append-never); the bodies and dicts are
   the core's per-column writers over the open handle (x->ar). */
static void ixs_plans_fill(ixs *x, mizu_arrow *ar) {
  _Static_assert(MIZU_ARROW_CVT == PC_CVT && MIZU_ARROW_STR == PC_STR &&
                 MIZU_ARROW_DICT == PC_DICT && MIZU_ARROW_DATE == PC_DATE &&
                 MIZU_ARROW_TS == PC_TS && MIZU_ARROW_TD == PC_TD,
                 "the column-kind orders coincide");
  for (int i = 0; i < x->ncols; i++) {
    mizu_arrow_col ci;
    mizu_arrow_col_info(ar, i, &ci);
    ixs_pcol *pc = &x->cols[i];
    pc->kind = ci.kind;
    pc->row = ci.row;
    pc->str_form = ci.str_form;
  }
}

/* The names array: the core's open already vetted present-and-unique. */
static char **ixs_names_collect(ixs *x) {
  char **names = calloc((size_t) x->ncols, sizeof(char *));
  if (names == NULL) {
    PyErr_NoMemory();
    return NULL;
  }
  for (int i = 0; i < x->ncols; i++)
    names[i] = (char *) x->schema.children[i]->name;
  return names;
}

static int ixs_run_frame(ixs *x, mizu_slot_hdr *hdr, uint8_t *payload,
                         uint32_t inline_max, mizu_handle *h) {
  x->ncols = (int) x->schema.n_children;
  if (x->ncols < 1 || x->schema.children == NULL) {
    ixs_decline("a zero-column Arrow frame has no portable home");
    return 1;
  }
  x->cols = calloc((size_t) x->ncols, sizeof(ixs_pcol));
  if (x->cols == NULL) {
    PyErr_NoMemory();
    return 1;
  }
  if (ixs_pull(x) < 0) return 1;
  /* the classification, batch vetting and dictionary validation are the
     core's (arrow.c), the TLS decline re-raised as this front-end's */
  void *keep1, *keep2;
  mizu_arrow *ar = ixs_arrow_open(x, x->caps, &keep1, &keep2);
  if (ar == NULL) {
    if (PyErr_Occurred() == NULL) ixs_decline(mizu_last_error_message());
    return 1;
  }
  x->ar = ar;
  int rc = 1;
  char **names = NULL;
  ixs_plans_fill(x, ar);
  {
    /* the MIZL tier (3.6/3.8) decides on the memoized frame span — the
       'I' sizing's per-element string walk runs only on the by-value
       branches below (mizu's own gate; the span >= the 'I' size, so the
       region becomes marginally more likely for the same data): past the
       zero-copy gate, no churn, and the peer passing the frame
       conjunction (the binding's tier-attempt predicate; the core
       writer's stage enforces the same conjunction internally now — one
       home for the rule) — the provenance REF of an unmodified round
       trip first, else the core writer's layout stage; anything less
       falls to the copy tiers */
    const size_t zc_gate = mizu_py_zc_gate(inline_max);
    if (mizu_arrow_size(ar) > zc_gate && !mizu_handle_churn(h)) {
      int any_str = 0;
      for (int i = 0; i < x->ncols; i++)
        any_str |= x->cols[i].kind == PC_STR;
      const uint32_t need = MIZU_CAP_ATTRS | MIZU_CAP_MIZL |
        (any_str ? MIZU_CAP_MIZS : 0u);
      if ((x->caps & need) == need) {
        /* the names array serves the provenance matches alone: collect it
           only on the MIZL attempt, off the by-value paths */
        if ((names = ixs_names_collect(x)) == NULL)
          goto done;
        rc = pymizu_shmframe_pv_match(x, names, hdr, payload,
                                      inline_max);
        if (rc == 0) goto done;
        /* the per-column REF (F2): the whole-frame path behind, any
           matched column registers as a remote leaf — the conditional
           conjunction: a peer short of the bit gets full layout leaves
           for every column, never a partial-remote tree */
        struct pv_entry const **hits =
          calloc((size_t) x->ncols, sizeof(*hits));
        if (hits == NULL) {
          PyErr_NoMemory();
          goto done;
        }
        int nremote = pymizu_shmframe_pv_match_cols(x, names, hits);
        if (nremote != 0 &&
            (x->caps & (need | MIZU_CAP_MIZL_REF)) ==
              (need | MIZU_CAP_MIZL_REF)) {
          for (int i = 0; i < x->ncols; i++) {
            if (hits[i] == NULL) continue;
            mizu_shm *acq = pymizu_shmframe_pv_acq(hits[i]);
            /* a torn referenced tree: the column stays local */
            (void) mizu_arrow_remote(ar, i, acq->name,
                                     (size_t) acq->name_len,
                                     mizu_shm_addr(acq),
                                     mizu_shm_size(acq), i);
          }
        }
        free(hits);
        mizu_arrow_warn warn;
        if (mizu_arrow_stage(h, ar, hdr, payload, &warn) == MIZU_OK) {
          /* staged: the conversion warnings raise after the write half
             completed (a raise-as-error rolls the unpublished
             reservation back) */
          rc = mizu_py_cvt_warn(&warn, x->lang) != 0 ? 1 : 0;
          goto done;
        }
      }
    }
    /* the by-value 'I' form: one core write straight off the Arrow
       buffers (dst NULL sizes), inline within the budget, else into the
       reservation — no scratch copy */
    const size_t n = mizu_arrow_write_ix(ar, NULL, &x->warn);
    if (n <= (size_t) inline_max) {
      mizu_arrow_write_ix(ar, payload, &x->warn);
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
      rc = 0;
    } else {
      uint8_t *dst = mizu_py_stage_reserve(n, hdr, payload, h);
      if (dst == NULL) goto done;
      mizu_arrow_write_ix(ar, dst, &x->warn);
      rc = 0;
    }
  }
done:
  free(names);
  mizu_arrow_close(ar);
  free(keep1);
  free(keep2);
  /* the write half completed: raise the accumulated conversion warnings
     (the MIZL paths raised their own and carry none here) */
  return rc == 0 && mizu_py_cvt_warn(&x->warn, x->lang) != 0 ? 1 : rc;
}

/* The single (Series / ChunkedArray) paths: the conversion table onto the
   bare-bytes tiers, the strv / factor / temporal value forms the core's
   per-column 'I' writer (mizu_arrow_write_ix_col). */
static int ixs_run_single(ixs *x, mizu_slot_hdr *hdr, uint8_t *payload,
                          uint32_t inline_max, mizu_handle *h) {
  x->ncols = 1;
  x->single = 1;
  x->cols = calloc(1, sizeof(ixs_pcol));
  if (x->cols == NULL) {
    PyErr_NoMemory();
    return 1;
  }
  if (!x->borrowed_hold && ixs_pull(x) < 0) return 1;
  /* the classification and validation are the core's (the single column
     wrapped as a one-column struct), the TLS decline re-raised here; the
     handle stays open through the emit — the column bodies and dicts are
     its per-column writers */
  void *keep1, *keep2;
  mizu_arrow *ar = ixs_arrow_open(x, 0, &keep1, &keep2);
  if (ar == NULL) {
    if (PyErr_Occurred() == NULL) ixs_decline(mizu_last_error_message());
    return 1;
  }
  x->ar = ar;
  ixs_plans_fill(x, ar);
  int rc = 1;
  ixs_pcol *pc = &x->cols[0];
  if (pc->kind == PC_CVT) {
    /* the bare-bytes tiers first: one reservation, converted in place */
    uint64_t total = (uint64_t) x->hold.rows;
    uint8_t *dst = mizu_stage_raw(h, total * pc->row->w_out, pc->row->wire,
                                  hdr, payload, inline_max);
    if (dst != NULL) {
      cvt_warn warn = { 0, 0, 0 };
      mizu_arrow_col_body(ar, 0, dst, &warn);
      rc = mizu_py_cvt_warn(&warn, x->lang) != 0 ? 1 : 0;
      goto done;
    }
    /* a declined reservation degrades to the 'I' stream */
  }
  {
    /* the 'I' value form: the header, then the column's value form off
       the core writer (dst NULL sizes) — inline within the budget, else
       into the reservation */
    const size_t n = mizu_ix_put_header(NULL) +
      mizu_arrow_write_ix_col(ar, 0, NULL, &x->warn);
    if (n <= (size_t) inline_max) {
      size_t off = mizu_ix_put_header(payload);
      off += mizu_arrow_write_ix_col(ar, 0, payload + off, &x->warn);
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
      rc = 0;
    } else {
      uint8_t *dst = mizu_py_stage_reserve(n, hdr, payload, h);
      if (dst == NULL) goto done;
      size_t off = mizu_ix_put_header(dst);
      off += mizu_arrow_write_ix_col(ar, 0, dst + off, &x->warn);
      rc = 0;
    }
    rc = rc == 0 && mizu_py_cvt_warn(&x->warn, x->lang) != 0 ? 1 : rc;
  }
done:
  mizu_arrow_close(ar);
  free(keep1);
  free(keep2);
  return rc;
}

static int ixs_run(ixs *x, mizu_slot_hdr *hdr, uint8_t *payload,
                   uint32_t inline_max, mizu_handle *h) {
  memset(&x->schema, 0, sizeof(x->schema));
  if (x->st->get_schema(x->st, &x->schema) != 0) {
    const char *e = x->st->get_last_error != NULL ?
      x->st->get_last_error(x->st) : NULL;
    PyErr_Format(MizuError, "pymizu: the Arrow export failed: %s",
                 e != NULL ? e : "unknown error");
    return 1;
  }
  x->schema_owned = 1;
  if (x->schema.format == NULL || x->schema.release == NULL) {
    ixs_decline("an invalid Arrow C Data Interface export");
    return 1;
  }
  if (strcmp(x->schema.format, "+s") == 0)
    return ixs_run_frame(x, hdr, payload, inline_max, h);
  return ixs_run_single(x, hdr, payload, inline_max, h);
}

/* An __arrow_c_array__ producer whose format the conversion table
   declines (a StringArray, DictionaryArray, DateArray, ...): the single
   path over the capsule pair as one borrowed batch. fn is the front-end's
   probed bound method, stolen. */
static int ixs_stage_arrow_pair(PyObject *fn, mizu_slot_hdr *hdr,
                                uint8_t *payload, uint32_t inline_max,
                                mizu_handle *h) {
  PyObject *pair = PyObject_CallNoArgs(fn);
  Py_DECREF(fn);
  if (pair == NULL) return 1;   /* the object claimed the interface */
  if (!PyTuple_Check(pair) || PyTuple_GET_SIZE(pair) != 2) {
    Py_DECREF(pair);
    PyErr_SetString(PyExc_TypeError, "pymizu: __arrow_c_array__ must return "
                    "a (schema, array) capsule pair");
    return 1;
  }
  ArrowSchema *schema = (ArrowSchema *) PyCapsule_GetPointer(
    PyTuple_GET_ITEM(pair, 0), "arrow_schema");
  ArrowArray *array = (ArrowArray *) PyCapsule_GetPointer(
    PyTuple_GET_ITEM(pair, 1), "arrow_array");
  if (schema == NULL || array == NULL) {
    Py_DECREF(pair);
    return 1;
  }
  ixs x;
  memset(&x, 0, sizeof(x));
  x.cap = pair;
  memcpy(&x.schema, schema, sizeof(x.schema));
  x.schema.release = NULL;
  ArrowArray one;
  memcpy(&one, array, sizeof(one));
  one.release = NULL;
  x.borrowed_hold = 1;
  x.hold.arrs = &one;
  x.hold.nb = 1;
  x.hold.cap = 1;
  x.hold.rows = one.length;
  int rc = ixs_run_single(&x, hdr, payload, inline_max, h);
  x.hold.arrs = NULL;
  /* ixs_free drops the pair, whose capsules release and free the Arrow
     structs — reading schema->/array->release here would touch freed
     memory, and the producer's capsule destructors own the release */
  ixs_free(&x);
  return rc;
}

/* The __arrow_c_stream__ front-end. */
int pymizu_ix_stage_arrow_stream(PyObject *obj, mizu_slot_hdr *hdr,
                                 uint8_t *payload, uint32_t inline_max,
                                 mizu_handle *h, uint32_t peer_lang,
                                 uint32_t peer_caps) {
  if (Py_TYPE(obj) == &MizuFrameType) return -1;   /* the writer's case */
  PyObject *fn = NULL;
  int prc = mizu_py_get_optional_attr(obj, mizu_py_str_arrow_c_stream, &fn);
  if (prc < 0) return 1;
  if (fn == NULL) {
    /* not a stream producer: an array-only one takes the pair path */
    prc = mizu_py_get_optional_attr(obj, mizu_py_str_arrow_c_array, &fn);
    if (prc < 0) return 1;
    if (fn == NULL) return -1;
    return ixs_stage_arrow_pair(fn, hdr, payload, inline_max, h);
  }
  PyObject *cap = PyObject_CallNoArgs(fn);
  Py_DECREF(fn);
  if (cap == NULL) {
    /* the producer claimed the interface and failed: pandas' missing
       pyarrow names the install */
    if (PyErr_ExceptionMatches(PyExc_ImportError)) {
      PyObject *et, *ev, *tb;
      PyErr_Fetch(&et, &ev, &tb);
      PyObject *es = ev != NULL ? PyObject_Str(ev) : NULL;
      PyErr_Format(MizuDeclinedError,
                   "pymizu: value is not portable to the peer (the Arrow "
                   "export needs pyarrow: %s)",
                   es != NULL ? PyUnicode_AsUTF8(es) : "not installed");
      Py_XDECREF(es);
      Py_XDECREF(et);
      Py_XDECREF(ev);
      Py_XDECREF(tb);
    } else {
      PyObject *et, *ev, *tb;
      PyErr_Fetch(&et, &ev, &tb);
      PyObject *es = ev != NULL ? PyObject_Str(ev) : NULL;
      PyErr_Format(MizuDeclinedError,
                   "pymizu: value is not portable to the peer (the Arrow "
                   "export failed: %s)",
                   es != NULL ? PyUnicode_AsUTF8(es) : "unknown error");
      Py_XDECREF(es);
      Py_XDECREF(et);
      Py_XDECREF(ev);
      Py_XDECREF(tb);
    }
    return 1;
  }
  ArrowArrayStream *st = (ArrowArrayStream *) PyCapsule_GetPointer(
    cap, "arrow_array_stream");
  if (st == NULL) {
    Py_DECREF(cap);
    return 1;
  }
  ixs x;
  memset(&x, 0, sizeof(x));
  x.st = st;
  x.cap = cap;
  x.caps = peer_caps;
  x.lang = peer_lang;
  int rc = ixs_run(&x, hdr, payload, inline_max, h);
  ixs_free(&x);
  return rc;
}

/* The same-language MIZL branch (Phase 3.6): a Frame past the zero-copy
   floor stages as one MIZL region on same-language handles — the true
   mirror of R's MIZL, container-exact (it reads back as a region-backed
   Frame), while pandas, polars and pyarrow frames keep pickle. Below the
   floor or on any decline (a complex column; the churn check is the core
   stage's now) it falls back to pickle: -1 with the error cleared. The
   same-language rules ride the overrides: the tzone verbatim off the
   format, the caller's row_names as the root blob. */
static int ixs_run_frame_mizl(ixs *x, mizu_slot_hdr *hdr, uint8_t *payload,
                              uint32_t inline_max, mizu_handle *h,
                              uint32_t peer_caps) {
  char **names = NULL;
  uint8_t *blob = NULL;
  mizu_arrow *ar = NULL;
  void *keep1 = NULL, *keep2 = NULL;
  int rc = -1;
  memset(&x->schema, 0, sizeof(x->schema));
  if (x->st->get_schema(x->st, &x->schema) != 0) goto out;
  x->schema_owned = 1;
  if (x->schema.format == NULL || x->schema.release == NULL ||
      strcmp(x->schema.format, "+s") != 0)
    goto out;
  x->ncols = (int) x->schema.n_children;
  if (x->ncols < 1 || x->schema.children == NULL) goto out;
  x->cols = calloc((size_t) x->ncols, sizeof(ixs_pcol));
  if (x->cols == NULL) goto out;
  if (ixs_pull(x) < 0) goto out;
  /* the real peer caps word: the core stage's conjunction needs it (the
     same-language paths used to carry 0 — the drift dies here) */
  ar = ixs_arrow_open(x, peer_caps, &keep1, &keep2);
  if (ar == NULL) goto out;
  ixs_plans_fill(x, ar);
  /* the overrides feed the memoized span (the size pass re-runs after
     each), so they precede the gate: the tzone verbatim (the
     naivety-preserving form), and a custom row_names as the root blob */
  for (int i = 0; i < x->ncols; i++)
    if (x->cols[i].kind == PC_TS)
      mizu_arrow_tz(ar, i, x->schema.children[i]->format + 4);
  if (x->r_attrs != NULL ||
      (x->row_names != NULL && x->row_names != Py_None)) {
    /* the names array serves the blob alone */
    if ((names = ixs_names_collect(x)) == NULL)
      goto out;
    size_t bn = mizu_py_blob_frame(NULL, names, x->ncols, x->hold.rows,
                                   x->row_names, x->r_attrs, peer_caps);
    if (bn == 0) goto out;
    blob = malloc(bn);
    if (blob == NULL) goto out;
    mizu_py_blob_frame(blob, names, x->ncols, x->hold.rows, x->row_names,
                       x->r_attrs, peer_caps);
    mizu_arrow_root_blob(ar, blob, bn);
  }
  /* the zero-copy gate on the memoized frame span (was: the 'I' sizing's
     per-element walk, discarded whenever the region won) */
  const size_t zc_gate = mizu_py_zc_gate(inline_max);
  if (mizu_arrow_size(ar) <= zc_gate) goto out;
  {
    mizu_arrow_warn warn;
    if (mizu_arrow_stage(h, ar, hdr, payload, &warn) != MIZU_OK) goto out;
    /* same-language only: the R-semantics sentinel counts are noise here
       (the columns cross identically), n_range still reports */
    rc = mizu_py_cvt_warn(&warn, MIZU_LANG_PYTHON) != 0 ? 1 : 0;
  }
out:
  free(blob);
  free(names);
  if (ar != NULL) mizu_arrow_close(ar);
  free(keep1);
  free(keep2);
  if (rc != 0) {
    PyErr_Clear();
    rc = -1;
  }
  return rc;
}

/* stage_impl's exact-Frame branch (pyinterop.h). 0 staged, -1 fall back
   to pickle. */
int pymizu_frame_stage_mizl(PyObject *obj, mizu_slot_hdr *hdr,
                            uint8_t *payload, uint32_t inline_max,
                            mizu_handle *h, uint32_t peer_caps) {
  if (Py_TYPE(obj) != &MizuFrameType) return -1;
  MizuFrame *f = (MizuFrame *) obj;
  PyObject *args = PyTuple_New(0);
  if (args == NULL) {
    PyErr_Clear();
    return -1;
  }
  PyObject *cap = Frame_arrow_c_stream(f, args, NULL);
  Py_DECREF(args);
  if (cap == NULL) {
    PyErr_Clear();
    return -1;
  }
  ArrowArrayStream *st = (ArrowArrayStream *) PyCapsule_GetPointer(
    cap, "arrow_array_stream");   /* the Frame's own export's capsule */
  ixs x;
  memset(&x, 0, sizeof(x));
  x.st = st;
  x.cap = cap;
  x.row_names = f->row_names;
  x.r_attrs = f->r_attrs;
  int rc = ixs_run_frame_mizl(&x, hdr, payload, inline_max, h, peer_caps);
  ixs_free(&x);
  return rc;
}

// The Frame.from_arrow builder (pyshmframe.h) --------------------------------------

/* Re-raise a front-end decline as the constructor's MizuError (an
   unsupported type is the foreign decline's shape, surfaced informatively);
   MizuError and out-of-contract exceptions pass through. name names the
   column when known. */
static void fc_err_reraise(const char *name) {
  PyObject *et, *ev, *tb;
  PyErr_Fetch(&et, &ev, &tb);   /* the caller's failure always carries one */
  if (!PyErr_GivenExceptionMatches(et, MizuDeclinedError)) {
    PyErr_Restore(et, ev, tb);
    return;
  }
  PyObject *es = ev != NULL ? PyObject_Str(ev) : NULL;
  const char *msg = es != NULL ? PyUnicode_AsUTF8(es) : NULL;
  if (msg != NULL && strncmp(msg, "pymizu: ", 8) == 0) msg += 8;
  if (name != NULL)
    PyErr_Format(MizuError, "pymizu: Frame.from_arrow: column '%s': %s",
                 name, msg != NULL ? msg : "a failure");
  else
    PyErr_Format(MizuError, "pymizu: Frame.from_arrow: %s",
                 msg != NULL ? msg : "a failure");
  Py_XDECREF(es);
  Py_DECREF(et);
  Py_XDECREF(ev);
  Py_XDECREF(tb);
}

/* The adoption verdict for one column of a single offset-0 batch: the fcol
   layout IS the Arrow layout for these types, nulls included (the validity
   bitmap adopts with the values — every fcol reader consults it before the
   values). 1 adopt, 0 copy (producer geometry is trusted: the core's open
   vetted it). */
/* The uint64 format's verbatim classify: a 'L' column builds/adopts
   FCOL_U64 off the format — the Frame exposes uint64 exactly, so the
   caps-0 open's conversion verdict (its eager float64 build, with the
   under-2^53 silent dtype loss) never applies. */
static int u64_arrow_format(const char *f) {
  return f != NULL && f[0] == 'L' && f[1] == '\0';
}

/* The dictionary index form off the format char (the core classify's
   mapping; the from_arrow path derives its own — the core's per-column
   writers own the staging uses now). */
static void fcol_dict_idx_form(const char *f, int *w, int *sig) {
  *sig = 0;
  switch (f[0]) {
  case 'c': *w = 1; *sig = 1; break;
  case 'C': *w = 1; break;
  case 's': *w = 2; *sig = 1; break;
  case 'S': *w = 2; break;
  case 'i': *w = 4; *sig = 1; break;
  case 'I': *w = 4; break;
  case 'l': *w = 8; *sig = 1; break;
  default:  *w = 8; break;   /* 'L' */
  }
}

static int fc_adopt_gate(ixs *x, int col) {
  const ixs_pcol *pc = &x->cols[col];
  const ArrowArray *a = x->hold.arrs[0].children[col];
  const ArrowSchema *sc = x->schema.children[col];
  if (a->offset != 0) return 0;   /* a sliced column: the copy arm rebases */
  switch (pc->kind) {
  case PC_CVT:
    if (u64_arrow_format(sc->format))
      return 1;   /* the buffers are already the u64 fcol layout */
    return pc->row->cvt == CVT_COPY &&
      (pc->row->wire == MIZU_TYPE_REAL || pc->row->wire == MIZU_TYPE_INT ||
       pc->row->wire == MIZU_TYPE_INT64 || pc->row->wire == MIZU_TYPE_RAW);
  case PC_STR: {
    if (pc->str_form != 1) return 0;
    /* monotonic offsets: the core's open vetted them (arrow.c) */
    const int32_t *offs = (const int32_t *) a->buffers[1];
    return offs != NULL && offs[0] == 0;
  }
  case PC_DICT: {
    /* i32 codes, utf8 levels on offset-0 buffers (the level contents —
       monotonic offsets, no null levels, code bounds — are the validation
       pass's verdicts, already run) */
    const ArrowArray *d = a->dictionary;
    int w, sig;
    fcol_dict_idx_form(sc->format, &w, &sig);
    if (w != 4 || !sig || pc->str_form != 1 ||
        d == NULL || d->offset != 0)
      return 0;
    if (d->length == 0) return 1;
    const int32_t *loff = (const int32_t *) d->buffers[1];
    return loff != NULL && d->buffers[2] != NULL && loff[0] == 0;
  }
  case PC_DATE: return 1;
  case PC_TS: case PC_TD:
    /* i64 counts at any unit: the fcol layout IS the Arrow layout */
    return 1;
  }
  Py_UNREACHABLE();   /* the core classify's closed kinds */
}

/* The copy arm for the cvt-table kinds (bool, narrow ints, uint32/64,
   half/float — and the adoptable types whose gates failed): one fused
   masked convert per batch into the owned buffer. */
static int fcol_build_cvt(ixs *x, int col, fcol *c, const cvt_row *row,
                          int kind, cvt_warn *warn) {
  int64_t n = x->hold.rows;
  uint8_t *buf = mizu_py_ix_alloc((uint64_t) n, row->w_out);
  if (buf == NULL) return -1;
  size_t off = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    ArrowArray *a = x->hold.arrs[b].children[col];
    const uint8_t *data = (const uint8_t *) a->buffers[1];
    const uint8_t *valid = (const uint8_t *) a->buffers[0];
    uint64_t aoff = (uint64_t) a->offset;
    size_t alen = (size_t) a->length;
    if (row->cvt == CVT_BIT_LGL) {
      mizu_cvt_convert(buf + off, data, valid, aoff, alen, row, warn);
    } else {
      mizu_cvt_convert(buf + off, data + aoff * row->w_in, valid, aoff,
                          alen, row, warn);
    }
    off += alen * row->w_out;
  }
  c->kind = kind;
  c->n = n;
  c->values = buf;
  return 0;
}

/* The uint64 copy arm: the identity lanes verbatim, nulls an owned
   bitmap — no mizu_cvt row applies (pymizu's identity static is never
   the core's CVT_ROW_I64_V twin, so the masked convert would count the
   genuine INT64_MIN lanes verbatim uint64 must treat as values), and no
   sentinel form exists (the fill collides with 2^63). */
static int fcol_build_u64(ixs *x, int col, fcol *c) {
  int64_t n = x->hold.rows;
  uint64_t *buf = mizu_py_ix_alloc((uint64_t) n, 8);
  if (buf == NULL) return -1;
  uint8_t *valid = NULL;
  int64_t nulls = 0, done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    ArrowArray *a = x->hold.arrs[b].children[col];
    const uint8_t *data = (const uint8_t *) a->buffers[1];
    memcpy(buf + done, data + (size_t) a->offset * 8,
           (size_t) a->length * 8);
    for (int64_t k = 0; k < a->length; k++) {
      const int64_t i = done + k;
      if (mizu_arrow_valid(a, k)) {
        if (valid != NULL) valid[i >> 3] |= 1 << (i & 7);
      } else {
        nulls++;
        if (valid == NULL) {
          valid = mizu_py_bitmap_calloc((uint64_t) n);
          if (valid == NULL) {
            free(buf);
            return -1;
          }
          for (int64_t q = 0; q < i; q++) valid[q >> 3] |= 1 << (q & 7);
        }
      }
    }
    done += a->length;
  }
  c->kind = FCOL_U64;
  c->n = n;
  c->values = (uint8_t *) buf;
  c->valid = valid;
  c->valid_owned = valid != NULL;
  c->vnulls = nulls;
  if (valid == NULL) c->known_free = 1;
  return 0;
}

/* The string copy: per-batch gather with rebased i32 offsets, a null slot
   collapsed to a zero-byte span with its bitmap bit clear (the owned
   invariant); a malformed record degrades to NA, the wire's rule. */
static int fcol_build_str(ixs *x, int col, fcol *c, char **names) {
  const ixs_pcol *pc = &x->cols[col];
  int64_t n = x->hold.rows;
  int32_t *offs = malloc(((size_t) n + 1) * 4);
  uint8_t *bytes = NULL, *valid = NULL;
  size_t blen = 0, bcap = 0;
  if (offs == NULL) goto nomem;
  offs[0] = 0;
  {
    int64_t done = 0, nulls = 0;
    for (size_t b = 0; b < x->hold.nb; b++) {
      ArrowArray *a = x->hold.arrs[b].children[col];
      for (int64_t k = 0; k < a->length; k++) {
        const uint8_t *s;
        int32_t len;
        int64_t i = done + k;
        if (mizu_arrow_str_at(a, k, pc->str_form, &s, &len) == 1) {
          if (valid != NULL) valid[i >> 3] |= 1 << (i & 7);
          if (blen + (size_t) len > INT32_MAX) {
            PyErr_Format(MizuError, "pymizu: Frame.from_arrow: column "
                         "'%s' is past the int32 offset range",
                         names[col]);
            goto fail;
          }
          if (bytes_reserve(&bytes, &bcap, blen + (size_t) len + 1) != 0)
            goto nomem;
          memcpy(bytes + blen, s, (size_t) len);
          blen += (size_t) len;
        } else {
          nulls++;
          if (valid == NULL) {
            valid = mizu_py_bitmap_calloc((uint64_t) n);
            if (valid == NULL) goto nomem;
            for (int64_t q = 0; q < i; q++) valid[q >> 3] |= 1 << (q & 7);
          }
        }
        offs[i + 1] = (int32_t) blen;
      }
      done += a->length;
    }
    c->vnulls = nulls;
  }
  bytes_shrink(&bytes, blen);
  c->kind = FCOL_STR;
  c->n = n;
  c->values = (uint8_t *) offs;
  c->bytes = bytes;
  c->bytes_len = (int64_t) blen;
  c->valid = valid;
  return 0;
nomem:
  PyErr_NoMemory();
fail:
  free(offs);
  free(bytes);
  free(valid);
  return -1;
}

/* The dictionary copy: the codes to owned i32 (NA32 at nulls — the owned
   invariant), the canonical levels packed off batch 0's dictionary (the
   core's validation already ran: every record resolves). */
static int fcol_build_dict(ixs *x, int col, fcol *c, char **names) {
  const ixs_pcol *pc = &x->cols[col];
  int64_t n = x->hold.rows;
  int32_t *codes = mizu_py_ix_alloc((uint64_t) n, 4);
  if (codes == NULL) return -1;
  int idx_w, idx_signed;
  fcol_dict_idx_form(x->schema.children[col]->format, &idx_w, &idx_signed);
  int64_t done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    ArrowArray *a = x->hold.arrs[b].children[col];
    const uint8_t *idx = (const uint8_t *) a->buffers[1];
    for (int64_t k = 0; k < a->length; k++) {
      int32_t v;
      if (!mizu_arrow_valid(a, k)) {
        v = MIZU_NA_INT32;
      } else {
        int64_t i = a->offset + k, cv;
        switch (idx_w) {
        case 1:
          cv = idx_signed ? (int64_t) ((const int8_t *) idx)[i] :
            (int64_t) ((const uint8_t *) idx)[i];
          break;
        case 2:
          cv = idx_signed ? (int64_t) ((const int16_t *) idx)[i] :
            (int64_t) ((const uint16_t *) idx)[i];
          break;
        case 4:
          cv = idx_signed ? (int64_t) ((const int32_t *) idx)[i] :
            (int64_t) ((const uint32_t *) idx)[i];
          break;
        default:
          cv = ((const int64_t *) idx)[i];
          break;
        }
        v = (int32_t) cv;   /* the bounds are the validation pass's */
      }
      codes[done + k] = v;
    }
    done += a->length;
  }
  const ArrowArray *d = x->hold.nb != 0 ?
    x->hold.arrs[0].children[col]->dictionary : NULL;
  const int64_t nlev = d != NULL ? d->length : 0;
  int32_t *offs = malloc(((size_t) nlev + 1) * 4);
  uint8_t *bytes = NULL;
  if (offs == NULL) {
    PyErr_NoMemory();
    goto fail;
  }
  offs[0] = 0;
  size_t blen = 0, bcap = 0;
  for (int64_t i = 0; i < nlev; i++) {
    const uint8_t *s;
    int32_t len;
    (void) mizu_arrow_str_at(d, i, pc->str_form, &s, &len);
    if (blen + (size_t) len > INT32_MAX) {
      PyErr_Format(MizuError, "pymizu: Frame.from_arrow: column "
                   "'%s' is past the int32 offset range",
                   names[col]);
      goto fail;
    }
    if (bytes_reserve(&bytes, &bcap, blen + (size_t) len + 1) != 0) {
      PyErr_NoMemory();
      goto fail;
    }
    memcpy(bytes + blen, s, (size_t) len);
    blen += (size_t) len;
    offs[i + 1] = (int32_t) blen;
  }
  bytes_shrink(&bytes, blen);
  c->kind = FCOL_DICT;
  c->n = n;
  c->values = (uint8_t *) codes;
  c->lev_off = offs;
  c->bytes = bytes;
  c->nlev = nlev;
  c->bytes_len = (int64_t) blen;
  return 0;
fail:
  free(offs);
  free(bytes);
  free(codes);
  return -1;
}

/* The date copy: i32 days verbatim, NA32 at masked lanes (a hand loop,
   not the "i" cvt row — the row counts genuine INT32_MIN lanes for R's
   NA_integer_ warning, which is not a date's semantics). */
static int fcol_build_date(ixs *x, int col, fcol *c) {
  int64_t n = x->hold.rows;
  int32_t *days = mizu_py_ix_alloc((uint64_t) n, 4);
  if (days == NULL) return -1;
  int64_t done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    ArrowArray *a = x->hold.arrs[b].children[col];
    const int32_t *src = (const int32_t *) a->buffers[1];
    for (int64_t k = 0; k < a->length; k++)
      days[done + k] = mizu_arrow_valid(a, k) ? src[a->offset + k] :
        MIZU_NA_INT32;
    done += a->length;
  }
  c->kind = FCOL_DATE;
  c->n = n;
  c->values = (uint8_t *) days;
  return 0;
}

/* The temporal copy: i64 counts verbatim at the column's unit, masked
   (NaT the INT64 sentinel; the unit rides the column, never a rescale). */
static int fcol_build_counts(ixs *x, int col, fcol *c) {
  const ixs_pcol *pc = &x->cols[col];
  int64_t n = x->hold.rows;
  int64_t *counts = mizu_py_ix_alloc((uint64_t) n, 8);
  if (counts == NULL) return -1;
  int64_t done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    ArrowArray *a = x->hold.arrs[b].children[col];
    const int64_t *src = (const int64_t *) a->buffers[1];
    for (int64_t k = 0; k < a->length; k++)
      counts[done + k] = mizu_arrow_valid(a, k) ? src[a->offset + k] :
        MIZU_NA_INT64;
    done += a->length;
  }
  c->kind = pc->kind == PC_TS ? FCOL_TS : FCOL_TD;
  c->n = n;
  c->values = (uint8_t *) counts;
  c->unit = x->schema.children[col]->format[2];
  if (pc->kind == PC_TS) {
    const char *tz = x->schema.children[col]->format + 4;
    snprintf(c->tz, sizeof(c->tz), "%s", tz);
  }
  return 0;
}

/* One adopted column: borrow the producer's buffers and stamp the
   null-free / advisory-nulls state. The batch still sits in the pull slot
   (the hold move happens only after every column built). */
static void fcol_adopt(ixs *x, int col, fcol *c, mizu_py_arrow_hold *hold) {
  const ixs_pcol *pc = &x->cols[col];
  const ArrowArray *a = x->hold.arrs[0].children[col];
  switch (pc->kind) {
  case PC_CVT:
    c->kind = u64_arrow_format(x->schema.children[col]->format) ?
      FCOL_U64 :
      pc->row->wire == MIZU_TYPE_REAL ? FCOL_F64 :
      pc->row->wire == MIZU_TYPE_INT ? FCOL_I32 :
      pc->row->wire == MIZU_TYPE_INT64 ? FCOL_I64 : FCOL_U8;
    c->values = (uint8_t *) a->buffers[1];
    break;
  case PC_STR: {
    c->kind = FCOL_STR;
    c->values = (uint8_t *) a->buffers[1];
    c->bytes = (uint8_t *) a->buffers[2];
    c->bytes_len = ((const int32_t *) a->buffers[1])[a->length];
    break;
  }
  case PC_DICT: {
    const ArrowArray *d = a->dictionary;
    c->kind = FCOL_DICT;
    c->values = (uint8_t *) a->buffers[1];
    c->bytes = (uint8_t *) d->buffers[2];
    c->lev_off = (int32_t *) d->buffers[1];
    c->nlev = d->length;
    c->bytes_len = d->length > 0 ? c->lev_off[d->length] : 0;
    break;
  }
  case PC_DATE:
    c->kind = FCOL_DATE;
    c->values = (uint8_t *) a->buffers[1];
    break;
  case PC_TS: case PC_TD:
    c->kind = pc->kind == PC_TS ? FCOL_TS : FCOL_TD;
    c->values = (uint8_t *) a->buffers[1];
    c->unit = x->schema.children[col]->format[2];
    if (pc->kind == PC_TS) {
      const char *tz = x->schema.children[col]->format + 4;
      snprintf(c->tz, sizeof(c->tz), "%s", tz);
    }
    break;
  }
  c->n = a->length;
  if (a->null_count == 0 || a->buffers[0] == NULL) {
    c->known_free = 1;
  } else {
    c->valid = (uint8_t *) a->buffers[0];
    c->vnulls = a->null_count;   /* the producer's count, served at export;
                                    value readers decide per bit */
    if (c->vnulls < 0) {
      /* "not yet computed" is spec-legal: count the bitmap now so the
         export never serves a negative count (read as OOM) */
      int64_t nn = 0;
      for (int64_t i = 0; i < a->length; i++)
        nn += !bitmap_at(c->valid, i);
      c->vnulls = nn;
    }
  }
  c->ahold = hold;
  atomic_fetch_add_explicit(&hold->refs, 1, memory_order_relaxed);
}

frame_cols *ixs_frame_cols_build(PyObject *obj) {
  /* the producer gate is the __arrow_c_stream__ attribute alone (pyarrow,
     polars, duckdb producers all qualify) */
  PyObject *fn = NULL;
  int prc = mizu_py_get_optional_attr(obj, mizu_py_str_arrow_c_stream, &fn);
  if (prc < 0) return NULL;
  if (fn == NULL) {
    PyErr_SetString(MizuError, "pymizu: Frame.from_arrow: object has no "
                    "__arrow_c_stream__ (a pyarrow Table or "
                    "RecordBatchReader, a polars DataFrame, a duckdb "
                    "relation)");
    return NULL;
  }
  PyObject *cap = PyObject_CallNoArgs(fn);
  Py_DECREF(fn);
  if (cap == NULL) {
    /* the producer claimed the interface and failed: the foreign
       front-end's split, as MizuError */
    if (PyErr_ExceptionMatches(PyExc_ImportError)) {
      PyObject *et, *ev, *tb;
      PyErr_Fetch(&et, &ev, &tb);
      PyObject *es = ev != NULL ? PyObject_Str(ev) : NULL;
      PyErr_Format(MizuError, "pymizu: Frame.from_arrow: the Arrow export "
                   "needs pyarrow: %s",
                   es != NULL ? PyUnicode_AsUTF8(es) : "not installed");
      Py_XDECREF(es);
      Py_XDECREF(et);
      Py_XDECREF(ev);
      Py_XDECREF(tb);
    } else {
      PyObject *et, *ev, *tb;
      PyErr_Fetch(&et, &ev, &tb);
      PyObject *es = ev != NULL ? PyObject_Str(ev) : NULL;
      PyErr_Format(MizuError, "pymizu: Frame.from_arrow: the Arrow export "
                   "failed: %s",
                   es != NULL ? PyUnicode_AsUTF8(es) : "unknown error");
      Py_XDECREF(es);
      Py_XDECREF(et);
      Py_XDECREF(ev);
      Py_XDECREF(tb);
    }
    return NULL;
  }
  ArrowArrayStream *st = (ArrowArrayStream *) PyCapsule_GetPointer(
    cap, "arrow_array_stream");
  if (st == NULL) {
    Py_DECREF(cap);
    return NULL;
  }
  ixs x;
  memset(&x, 0, sizeof(x));
  x.st = st;
  x.cap = cap;
  frame_cols *fc = NULL;
  char **names = NULL;
  mizu_py_arrow_hold *hold = NULL;
  int *adopt = NULL;
  memset(&x.schema, 0, sizeof(x.schema));
  if (x.st->get_schema(x.st, &x.schema) != 0) {
    const char *e = x.st->get_last_error != NULL ?
      x.st->get_last_error(x.st) : NULL;
    PyErr_Format(MizuError, "pymizu: Frame.from_arrow: the Arrow export "
                 "failed: %s", e != NULL ? e : "unknown error");
    goto done;
  }
  x.schema_owned = 1;
  if (x.schema.format == NULL || x.schema.release == NULL ||
      strcmp(x.schema.format, "+s") != 0 ||
      x.schema.n_children < 1 || x.schema.children == NULL) {
    PyErr_SetString(MizuError, "pymizu: Frame.from_arrow: the stream's "
                    "root is not a struct with at least one column (a "
                    "table, not an array)");
    goto done;
  }
  x.ncols = (int) x.schema.n_children;
  x.cols = calloc((size_t) x.ncols, sizeof(ixs_pcol));
  if (x.cols == NULL) {
    PyErr_NoMemory();
    goto done;
  }
  if (ixs_pull(&x) < 0) {
    fc_err_reraise(NULL);
    goto done;
  }
  /* the classification, batch vetting and dictionary validation are the
     core's (arrow.c); its TLS decline already names the column */
  {
    void *keep1, *keep2;
    mizu_arrow *ar = ixs_arrow_open(&x, 0, &keep1, &keep2);
    if (ar == NULL) {
      if (PyErr_Occurred() == NULL)
        PyErr_Format(MizuError, "pymizu: Frame.from_arrow: %s",
                     mizu_last_error_message());
      goto done;
    }
    ixs_plans_fill(&x, ar);
    mizu_arrow_close(ar);
    free(keep1);
    free(keep2);
  }
  names = ixs_names_collect(&x);
  if (names == NULL) goto done;
  /* the dispositions: one struct batch on offset-0 boundaries adopts per
     column (with or without nulls); anything else copies per column */
  {
    int single = x.hold.nb == 1 && x.hold.arrs[0].offset == 0 &&
      x.hold.rows > 0;
    int nadopt = 0;
    if (single) {
      adopt = calloc((size_t) x.ncols, sizeof(int));
      if (adopt == NULL) {
        PyErr_NoMemory();
        goto done;
      }
      for (int i = 0; i < x.ncols; i++) {
        adopt[i] = fc_adopt_gate(&x, i);
        nadopt += adopt[i];
      }
      if (nadopt > 0) {
        hold = malloc(sizeof(*hold));
        if (hold == NULL) {
          PyErr_NoMemory();
          goto done;
        }
        /* the batch moves into the hold only after every column built
           (the copy arms read the pull slot): until then the hold is an
           empty shell — release NULL, so a mid-build failure decrefs it
           away and ixs_free releases the batch */
        memset(&hold->arr, 0, sizeof(hold->arr));
        atomic_init(&hold->refs, 0);
      }
    }
    fc = frame_cols_new(x.ncols, x.hold.rows);
    if (fc == NULL) {
      PyErr_NoMemory();
      goto done_hold;
    }
    {
      cvt_warn warn = { 0, 0, 0 };
      for (int i = 0; i < x.ncols; i++) {
        ixs_pcol *pc = &x.cols[i];
        fcol *c = &fc->cols[i];
        if (adopt != NULL && adopt[i]) {
          fcol_adopt(&x, i, c, hold);
          continue;
        }
        int rc;
        switch (pc->kind) {
        case PC_CVT:
          rc = u64_arrow_format(x.schema.children[i]->format) ?
            fcol_build_u64(&x, i, c) :
            fcol_build_cvt(&x, i, c, pc->row,
                           pc->row->wire == MIZU_TYPE_REAL ? FCOL_F64 :
                           pc->row->wire == MIZU_TYPE_INT ? FCOL_I32 :
                           pc->row->wire == MIZU_TYPE_INT64 ? FCOL_I64 :
                           pc->row->wire == MIZU_TYPE_RAW ? FCOL_U8 :
                           FCOL_LGL, &warn);
          break;
        case PC_STR: rc = fcol_build_str(&x, i, c, names); break;
        case PC_DICT: rc = fcol_build_dict(&x, i, c, names); break;
        case PC_DATE: rc = fcol_build_date(&x, i, c); break;
        default: rc = fcol_build_counts(&x, i, c); break;
        }
        if (rc < 0) {
          frame_cols_decref(fc);
          fc = NULL;
          goto done;
        }
      }
      /* the adoption has no peer (the Frame's destination is unknown at
         construction): the counts report unconditionally */
      if (mizu_py_cvt_warn(&warn, MIZU_LANG_R) != 0) {
        frame_cols_decref(fc);
        fc = NULL;
        goto done;
      }
    }
    /* names into the block's packed store (the core's open already proved them
       present and unique) */
    {
      size_t name_len = 0;
      for (int i = 0; i < x.ncols; i++) {
        size_t len = strlen(names[i]);
        char *nb = realloc(fc->names, name_len + len + 1);
        if (nb == NULL) {
          PyErr_NoMemory();
          frame_cols_decref(fc);
          fc = NULL;
          goto done;
        }
        if (len > (size_t) INT32_MAX - name_len) {
          PyErr_SetString(MizuError, "pymizu: column names past 2^31 bytes");
          frame_cols_decref(fc);
          fc = NULL;
          goto done;
        }
        fc->names = nb;
        memcpy(fc->names + name_len, names[i], len);
        fc->name_off[i] = (int32_t) name_len;
        name_len += len;
      }
      fc->name_off[x.ncols] = (int32_t) name_len;
    }
    if (hold != NULL) {
      /* the batch struct moves into the hold (the pull slot zeroed, so
         ixs_free skips it); the parent struct's release covers the
         children and their dictionaries */
      hold->arr = x.hold.arrs[0];
      memset(&x.hold.arrs[0], 0, sizeof(x.hold.arrs[0]));
    }
  }
  goto done;
done_hold:
  if (hold != NULL) free(hold);   /* the batch never moved: ixs_free's */
done:
  free(adopt);
  free(names);
  ixs_free(&x);
  return fc;
}

// Entry points ---------------------------------------------------------------------

/* The err tag (0x11) framer: bounded by truncation — type past 128 bytes,
   message past half the inline budget (the task flatten's share), detail
   past what remains, each cut at a UTF-8 boundary — so the frame fits the
   slot by construction and the caller stamps INLINE with the keeperless
   claim: the writer cannot fail. The budget algorithm is the core's
   mizu_ix_write_err (the byte-shape helper registry); the span
   resolution stays with _pymizu.c's callers. Serves the peer shim's
   _send_error and Phase 4's ERR publish. */

/* Run the two-pass walk over obj: header + one value. */
static void ixw_stream(ixw *w, PyObject *obj) {
  IXW_PUT(w, mizu_ix_put_header(IXW_DST(w)));
  ixw_node(w, obj);
}

// Task streams (Phase 4) -------------------------------------------------------

static void ixw_raise(ixw *w);

/* The two-pass task walk off the spec's components: the header fields,
   then the code string, the positional list and the named dict —
   elements through the generic writer, never an intermediate object. */
/* The task fields' emission, shared by the task writer and the map
   descriptor writer: the code string, the positional list and the named
   dict, elements through the generic writer. A decline (a non-portable
   argument, bad names) latches on w. */
static void ixw_task_fields(ixw *w, const char *code, Py_ssize_t code_n,
                            PyObject *args, PyObject *kwargs) {
  if (code_n > INT32_MAX) {
    ixw_decline(w, "a task code string past 2^31 bytes has no portable home");
    return;
  }
  IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), code, (int32_t) code_n));
  w->depth++;
  Py_ssize_t na = PyTuple_GET_SIZE(args);
  IXW_PUT(w, mizu_ix_put_list_begin(IXW_DST(w), (uint64_t) na));
  for (Py_ssize_t i = 0; i < na && !w->decline; i++) {
    size_t save = w->path_len;
    ixw_path_index(w, i);
    ixw_node(w, PyTuple_GET_ITEM(args, i));
    if (!w->decline) ixw_path_pop(w, save);
  }
  Py_ssize_t nk = PyDict_Size(kwargs);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), (uint64_t) nk));
  PyObject *k, *v;
  Py_ssize_t pos = 0;
  while (!w->decline && PyDict_Next(kwargs, &pos, &k, &v)) {
    if (!PyUnicode_CheckExact(k)) {
      ixw_decline_type(w, k, "a non-str dict key has no portable home");
      break;
    }
    Py_ssize_t kn;
    const char *ks = PyUnicode_AsUTF8AndSize(k, &kn);
    if (ks == NULL) {
      PyErr_Clear();
      ixw_decline(w, "a dict key is not writable as UTF-8 "
                    "(a lone surrogate)");
      break;
    }
    if (kn > UINT32_MAX) {
      ixw_decline(w, "a dict key past 2^32 bytes has no portable home");
      break;
    }
    size_t save = w->path_len;
    ixw_path_key(w, k);
    IXW_PUT(w, mizu_ix_put_key(IXW_DST(w), ks, (uint32_t) kn));
    ixw_node(w, v);
    if (!w->decline) ixw_path_pop(w, save);
  }
  w->depth--;
}

static void ixw_task(ixw *w, const char *code, Py_ssize_t code_n, int kind,
                     PyObject *args, PyObject *kwargs, uint32_t target,
                     uint64_t ident) {
  IXW_PUT(w, mizu_ix_put_header(IXW_DST(w)));
  IXW_PUT(w, mizu_ix_put_task(IXW_DST(w), (int) target, kind, ident));
  ixw_task_fields(w, code, code_n, args, kwargs);
}

int pymizu_ix_stage_task(PyObject *spec, mizu_slot_hdr *hdr,
                         uint8_t *payload, uint32_t inline_max,
                         mizu_handle *h, uint64_t ident) {
  if (mizu_handle_kind(h) != MIZU_HTYPE_POOL) {
    PyErr_SetString(MizuError, "pymizu: a call spec submits through "
                    "Pool.submit");
    return 1;
  }
  PyObject *code = NULL, *kind_o = NULL, *args = NULL, *kwargs = NULL;
  int rc = 1;
  code = PyObject_GetAttrString(spec, "code");
  kind_o = PyObject_GetAttrString(spec, "kind");
  args = PyObject_GetAttrString(spec, "args");
  kwargs = PyObject_GetAttrString(spec, "kwargs");
  if (code == NULL || kind_o == NULL || args == NULL || kwargs == NULL)
    goto out;
  const char *code_s = NULL;
  Py_ssize_t code_n = 0;
  if (PyUnicode_CheckExact(code))
    code_s = PyUnicode_AsUTF8AndSize(code, &code_n);
  long kind = PyLong_CheckExact(kind_o) ? PyLong_AsLong(kind_o) : -1;
  if (code_s == NULL || kind < 0 || kind > 255 ||
      !PyTuple_CheckExact(args) || !PyDict_CheckExact(kwargs)) {
    PyErr_SetString(MizuError, "pymizu: a malformed call spec");
    goto out;
  }
  /* the target is the pool word's language byte (re-read: a submit can
     race ahead of the first join; the word never resets once set) */
  const uint64_t word = mizu_pool_worker_ident((mizu_pool *) h);
  if (word == 0) {
    PyErr_SetString(MizuError, "pymizu: no worker has joined this pool");
    goto out;
  }
  {
    /* the W2 fold: one record walk — the optimistic write into payload
       flips to count-only at the first SHM_VEC candidate, so a
       candidate-free inline spec completes in a single walk. Selection
       is arithmetic over the recorded subtree sizes (F1's D3, the first
       candidate whose remainder fits), never a size pass per candidate */
    ixp_plan plan;
    plan.ncand = 0;
    ixw w;
    memset(&w, 0, sizeof(w));
    memcpy(w.path, "args", 5);
    w.path_len = 4;
    w.h = h;
    w.caps = MIZU_PY_IDENT_CAPS(word);
    w.inline_max = inline_max;
    w.churn = mizu_handle_churn(h);
    w.refs = 1;
    w.dst = payload;
    w.limit = inline_max;
    w.plan = &plan;
    ixw_task(&w, code_s, code_n, (int) kind, args, kwargs,
             MIZU_PY_IDENT_LANG(word), ident);
    if (w.decline) {
      ixw_raise(&w);
      goto out;
    }
    const size_t total = w.total;
    const int has_ref = w.ref_emitted;
    PyObject *zc_node = NULL;
    for (int i = 0; i < plan.ncand; i++) {
      if (total - plan.size[i] + (2 + (MIZU_NAME_MAX - 1)) <=
          (size_t) inline_max) {
        zc_node = plan.cand[i];
        break;
      }
    }
    if ((has_ref || zc_node != NULL) && !(w.caps & MIZU_CAP_TASKREF)) {
      /* D2: fail locally rather than remotely — a remote failure loses
         the work to a task error stream */
      ixw dw;
      memcpy(&dw, &w, sizeof(dw));
      ixw_decline(&dw, "the pool's workers cannot read by-reference task "
                       "arguments (upgrade the workers' binding)");
      ixw_raise(&dw);
      goto out;
    }
    if (zc_node != NULL) {
      /* the selected candidate: one write walk, the single checkout
         inline-fitting by construction */
      ixw w2;
      memcpy(&w2, &w, sizeof(w2));
      w2.total = 0;
      w2.decline = 0;
      w2.zc_spent = 0;
      w2.ref_emitted = 0;
      memset(&w2.warn, 0, sizeof(w2.warn));
      w2.plan = NULL;
      w2.dst = payload;   /* the record walk flipped it to count-only */
      w2.zc_node = zc_node;
      ixw_task(&w2, code_s, code_n, (int) kind, args, kwargs,
               MIZU_PY_IDENT_LANG(word), ident);
      if (!w2.abandon) {
        if (w2.decline) {
          ixw_raise(&w2);
          goto out;
        }
        /* the sentinel counts key on the worker language (the pool
           word's language byte), not the submitter's */
        if (mizu_py_cvt_warn(&w2.warn, MIZU_PY_IDENT_LANG(word)) != 0)
          goto out;
        hdr->kind = MIZU_KIND_INLINE;
        hdr->len = (uint32_t) w2.total;
        hdr->aux = 0;      /* no keeperless claim: inert on task entries */
        if (w2.ref_emitted) {
          /* D4: the submit-side handoff pins the spec (every view the
             argument trees carry) until the claim-side release */
          Py_INCREF(spec);
          mizu_stage_pin(h, (void *) spec);
        }
        rc = 0;
        goto out;
      }
      /* a mid-write checkout failure (a churn race): by value, never a
         partial stream */
      zc_node = NULL;
    }
    if (plan.ncand == 0 && total <= (size_t) inline_max) {
      /* one walk: the record walk's write is the stream, and its warn
         record is complete — nothing flipped it to count-only */
      if (mizu_py_cvt_warn(&w.warn, MIZU_PY_IDENT_LANG(word)) != 0)
        goto out;
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) total;
      hdr->aux = 0;        /* no keeperless claim: inert on task entries */
      if (has_ref) {
        Py_INCREF(spec);
        mizu_stage_pin(h, (void *) spec);
      }
      rc = 0;
      goto out;
    }
    /* candidates recorded, none fitting (or the zc write abandoned): the
       record walk counted the by-value stream — always past inline here
       (a recorded candidate is itself at or past the zc gate), so the
       rewrite rides the reservation, which stamps hdr */
    uint8_t *dst = mizu_py_stage_reserve(total, hdr, payload, h);
    if (dst == NULL) goto out;
    ixw w2;
    memcpy(&w2, &w, sizeof(w2));
    w2.total = 0;
    w2.decline = 0;
    w2.ref_emitted = 0;
    memset(&w2.warn, 0, sizeof(w2.warn));
    w2.plan = NULL;
    w2.dst = dst;
    w2.limit = total;   /* exact: the deterministic write re-counts to it */
    w2.no_zc = 1;
    ixw_task(&w2, code_s, code_n, (int) kind, args, kwargs,
             MIZU_PY_IDENT_LANG(word), ident);
    if (w2.decline) {
      ixw_raise(&w2);
      goto out;
    }
    /* the complete warn record is the rewrite walk's — the record walk's
       was cut short at the flip to count-only, so it is never raised */
    if (mizu_py_cvt_warn(&w2.warn, MIZU_PY_IDENT_LANG(word)) != 0)
      goto out;
    if (w2.ref_emitted) {
      Py_INCREF(spec);
      mizu_stage_pin(h, (void *) spec);
    }
    rc = 0;
  }
out:
  Py_XDECREF(code);
  Py_XDECREF(kind_o);
  Py_XDECREF(args);
  Py_XDECREF(kwargs);
  return rc;
}

/* The _write_task test hook's half: the task writer as bytes. */
PyObject *pymizu_ix_write_task_stream(PyObject *code, long kind,
                                      PyObject *args, PyObject *kwargs,
                                      uint32_t target, uint64_t ident) {
  if (!PyUnicode_CheckExact(code) || !PyTuple_CheckExact(args) ||
      !PyDict_CheckExact(kwargs)) {
    PyErr_SetString(PyExc_TypeError,
                    "pymizu: _write_task expects (str, int, tuple, dict)");
    return NULL;
  }
  Py_ssize_t code_n;
  const char *code_s = PyUnicode_AsUTF8AndSize(code, &code_n);
  if (code_s == NULL) return NULL;
  ixw w;
  memset(&w, 0, sizeof(w));
  memcpy(w.path, "args", 5);
  w.path_len = 4;
  w.refs = 1;              /* the hook mode: markers and REF leaves emit */
  w.caps = MIZU_PY_CAPS;   /* this build's word, as the corpus hook's */
  ixw_task(&w, code_s, code_n, (int) kind, args, kwargs, target, ident);
  if (w.decline) {
    ixw_raise(&w);
    return NULL;
  }
  size_t n = w.total;
  uint8_t *buf = malloc(n != 0 ? n : 1);
  if (buf == NULL) return PyErr_NoMemory();
  ixw w2;
  memcpy(&w2, &w, sizeof(w2));
  w2.dst = buf;
  w2.total = 0;
  w2.decline = 0;
  w2.limit = n;
  memset(&w2.warn, 0, sizeof(w2.warn));
  ixw_task(&w2, code_s, code_n, (int) kind, args, kwargs, target, ident);
  if (w2.decline || w2.total != n) {
    free(buf);
    PyErr_SetString(MizuError, "pymizu: interop write mismatch");
    return NULL;
  }
  if (mizu_py_cvt_warn(&w2.warn, target) != 0) {
    free(buf);
    return NULL;
  }
  PyObject *out = PyBytes_FromStringAndSize((const char *) buf,
                                            (Py_ssize_t) n);
  free(buf);
  return out;
}

// The task stream decode (Phase 4) ---------------------------------------------

static PyObject *ixt_stop_tls(void) {
  ixr_cursor_err();
  return NULL;
}

static int ixt_next(mizu_ix *cur, mizu_ix_item *it) {
  if (mizu_ix_next(cur, it) != MIZU_OK) {
    ixt_stop_tls();
    return -1;
  }
  return 0;
}

/* The per-field shape checks are the core's mizu_ixt_* decode shim (the
   byte-shape helper registry): the field tags are the builder's check,
   not the cursor's, and the texts record through the TLS slot, surfacing
   here through ixt_stop_tls like any cursor failure. */

/* One named-argument key: a bare string under the duplicate-key rule (the
   builder's check — the cursor holds no key set). */
static PyObject *ixt_key(mizu_ix *cur, PyObject *keyset) {
  mizu_ix_item key;
  if (ixt_next(cur, &key) < 0) return NULL;
  /* the cursor serves a bare string at every dict-key position (its
     2*count parity), kind and key flag guaranteed */
  PyObject *ko = PyUnicode_FromStringAndSize((const char *) key.ptr,
                                             (Py_ssize_t) key.len);
  if (ko == NULL) return NULL;
  int dup = PySet_Contains(keyset, ko);
  if (dup < 0) {
    Py_DECREF(ko);
    return NULL;
  }
  if (dup) {
    Py_DECREF(ko);
    PyErr_SetString(MizuError, "pymizu: malformed task stream: a "
                    "duplicate dict key");
    return NULL;
  }
  if (PySet_Add(keyset, ko) < 0) {
    Py_DECREF(ko);
    return NULL;
  }
  return ko;
}

/* Resolve the qualified name through the worker's own module machinery
   (there is no name registry): split at the last dot, import the module,
   take the attribute. */
static PyObject *ixt_resolve_name(const unsigned char *code, uint64_t len) {
  uint64_t dot = len;
  while (dot > 0 && code[dot - 1] != '.') dot--;
  if (dot <= 1 || dot == len) {
    PyErr_SetString(MizuError, "pymizu: malformed task stream: the task "
                    "name is not qualified");
    return NULL;
  }
  PyObject *modname = PyUnicode_FromStringAndSize((const char *) code,
                                                  (Py_ssize_t) (dot - 1));
  PyObject *attr = PyUnicode_FromStringAndSize((const char *) code + dot,
                                               (Py_ssize_t) (len - dot));
  if (modname == NULL || attr == NULL) {
    Py_XDECREF(modname);
    Py_XDECREF(attr);
    return NULL;
  }
  PyObject *importlib = PyImport_ImportModule("importlib");
  PyObject *mod = importlib == NULL ? NULL :
    PyObject_CallMethod(importlib, "import_module", "O", modname);
  Py_XDECREF(importlib);
  Py_DECREF(modname);
  if (mod == NULL) {
    Py_DECREF(attr);
    return NULL;              /* ImportError: the task's error */
  }
  PyObject *fn = PyObject_GetAttr(mod, attr);
  Py_DECREF(mod);
  Py_DECREF(attr);
  return fn;                  /* AttributeError likewise: the task's */
}

/* pymizu._exec_source, resolved lazily (the package is fully imported by
   the time a task runs). */
static PyObject *exec_source_fn(void) {
  static PyObject *fn = NULL;
  if (fn == NULL) {
    PyObject *mod = PyImport_ImportModule("pymizu");
    if (mod == NULL) return NULL;
    fn = PyObject_GetAttrString(mod, "_exec_source");
    Py_DECREF(mod);
  }
  return fn;
}

/* The exec-hook decode and run: the call is built straight off the cursor
   — name kind: an argument array plus a kwnames tuple for
   PyObject_Vectorcall; source kind: the arguments bound in a fresh
   namespace over __builtins__ (positional as "_1", "_2", ...), then the
   ast split (exec the prefix, eval the trailing expression). The stream
   is fully validated before anything executes. */
PyObject *pymizu_ix_task_run(const uint8_t *src, size_t n,
                             uint64_t *ident_out, mizu_read_ctx *ctx) {
  const ixr_mode mode = { 1, ctx };
  mizu_ix cur;
  mizu_ix_item it, code, pos, named;
  if (mizu_ixt_open(&cur, src, n, &it, 1) != MIZU_OK) return ixt_stop_tls();
  *ident_out = it.u64[0];
  const int kind = (int) it.task_kind;
  if (mizu_ixt_want_code(&cur, &code) != MIZU_OK) return ixt_stop_tls();
  PyObject *fn = NULL, *ns = NULL;
  if (kind == 0) {
    fn = ixt_resolve_name(code.ptr, code.len);
    if (fn == NULL) return NULL;
  } else {
    ns = PyDict_New();
    if (ns == NULL) return NULL;
    if (PyDict_SetItemString(ns, "__builtins__", PyEval_GetBuiltins()) < 0) {
      Py_DECREF(ns);
      return NULL;
    }
  }
  PyObject *result = NULL, *kwnames = NULL, *keyset = NULL;
  PyObject **args = NULL;
  size_t nargs = 0, nkw = 0, filled = 0;
  int ok = 0;
  if (mizu_ixt_want_list(&cur, &pos) != MIZU_OK) {
    ixt_stop_tls();
    goto out;
  }
  nargs = (size_t) pos.count;
  args = PyMem_Malloc((nargs != 0 ? nargs : 1) * sizeof(PyObject *));
  if (args == NULL) {
    PyErr_NoMemory();
    goto out;
  }
  for (size_t i = 0; i < nargs; i++) {
    PyObject *v = ixr_value(&cur, &mode);
    if (v == NULL) goto out;
    if (kind == 0) {
      args[filled++] = v;
    } else {
      char key[16];
      snprintf(key, sizeof key, "_%zu", i + 1);
      int rc = PyDict_SetItemString(ns, key, v);
      Py_DECREF(v);
      if (rc < 0) goto out;
    }
  }
  if (mizu_ixt_want_dict(&cur, &named) != MIZU_OK) {
    ixt_stop_tls();
    goto out;
  }
  nkw = (size_t) named.count;
  if (nkw != 0) {
    keyset = PySet_New(NULL);
    if (keyset == NULL) goto out;
    if (kind == 0) {
      kwnames = PyTuple_New((Py_ssize_t) nkw);
      if (kwnames == NULL) goto out;
      PyObject **grown =
        PyMem_Realloc(args, (nargs + nkw) * sizeof(PyObject *));
      if (grown == NULL) {
        PyErr_NoMemory();
        goto out;
      }
      args = grown;
    }
    for (size_t j = 0; j < nkw; j++) {
      PyObject *ko = ixt_key(&cur, keyset);
      if (ko == NULL) goto out;
      PyObject *v = ixr_value(&cur, &mode);
      if (v == NULL) {
        Py_DECREF(ko);
        goto out;
      }
      if (kind == 0) {
        PyTuple_SET_ITEM(kwnames, (Py_ssize_t) j, ko);
        args[filled++] = v;
      } else {
        int rc = PyDict_SetItem(ns, ko, v);
        Py_DECREF(ko);
        Py_DECREF(v);
        if (rc < 0) goto out;
      }
    }
  }
  if (mizu_ix_end(&cur) != MIZU_OK) {
    ixt_stop_tls();
    goto out;
  }
  ok = 1;
out:
  if (ok) {
    if (kind == 0) {
      result = PyObject_Vectorcall(fn, args, (Py_ssize_t) nargs,
                                   nkw != 0 ? kwnames : NULL);
    } else {
      PyObject *cs = PyUnicode_FromStringAndSize((const char *) code.ptr,
                                                 (Py_ssize_t) code.len);
      PyObject *efn = exec_source_fn();
      if (cs != NULL && efn != NULL)
        result = PyObject_CallFunctionObjArgs(efn, cs, ns, NULL);
      Py_XDECREF(cs);
    }
  }
  for (size_t i = 0; i < filled; i++) Py_DECREF(args[i]);
  PyMem_Free(args);
  Py_XDECREF(kwnames);
  Py_XDECREF(keyset);
  Py_XDECREF(fn);
  Py_XDECREF(ns);
  return result;
}

/* The _read_task test hook's half: the same cursor walk and shape checks,
   building the components for inspection (no resolution, no eval). */
PyObject *pymizu_ix_read_task_components(const uint8_t *src, size_t n) {
  const ixr_mode mode = { 2, NULL };
  mizu_ix cur;
  mizu_ix_item it, code, pos, named;
  if (mizu_ixt_open(&cur, src, n, &it, 1) != MIZU_OK) return ixt_stop_tls();
  if (mizu_ixt_want_code(&cur, &code) != MIZU_OK) return ixt_stop_tls();
  if (mizu_ixt_want_list(&cur, &pos) != MIZU_OK) return ixt_stop_tls();
  PyObject *positional = NULL, *kwargs = NULL, *keyset = NULL, *out = NULL;
  positional = PyList_New((Py_ssize_t) pos.count);
  if (positional == NULL) return NULL;
  for (uint64_t i = 0; i < pos.count; i++) {
    PyObject *v = ixr_value(&cur, &mode);
    if (v == NULL) goto fail;
    PyList_SET_ITEM(positional, (Py_ssize_t) i, v);
  }
  if (mizu_ixt_want_dict(&cur, &named) != MIZU_OK) {
    ixt_stop_tls();
    goto fail;
  }
  kwargs = PyDict_New();
  if (kwargs == NULL) goto fail;
  keyset = PySet_New(NULL);
  if (keyset == NULL) goto fail;
  for (uint64_t j = 0; j < named.count; j++) {
    PyObject *ko = ixt_key(&cur, keyset);
    if (ko == NULL) goto fail;
    PyObject *v = ixr_value(&cur, &mode);
    if (v == NULL) {
      Py_DECREF(ko);
      goto fail;
    }
    int rc = PyDict_SetItem(kwargs, ko, v);
    Py_DECREF(ko);
    Py_DECREF(v);
    if (rc < 0) goto fail;
  }
  if (mizu_ix_end(&cur) != MIZU_OK) {
    ixt_stop_tls();
    goto fail;
  }
  {
    PyObject *cs = PyUnicode_FromStringAndSize((const char *) code.ptr,
                                               (Py_ssize_t) code.len);
    if (cs == NULL) goto fail;
    PyObject *idn = PyLong_FromUnsignedLongLong(it.u64[0]);
    if (idn == NULL) {
      Py_DECREF(cs);
      goto fail;
    }
    out = Py_BuildValue("(i i N N N N)", (int) it.target,
                        (int) it.task_kind, idn, cs, positional, kwargs);
  }
  Py_XDECREF(keyset);
  return out;
fail:
  Py_XDECREF(positional);
  Py_XDECREF(kwargs);
  Py_XDECREF(keyset);
  return NULL;
}

// Map descriptors and runner tasks (Phase 5) --------------------------------------

/* The map descriptor's 'I' form, two-pass: one complete stream,
   list[task, x | nil] — the f spec nested as a kind 0/1 task tag (target
   byte and submitter identity included, as the exec tasks'), then the
   list-x values as a bare 0x0c list, or nil when the raw section carries
   them. A decline (a non-portable constant or element) latches on w. */
static void ixw_map_desc(ixw *w, const char *code, Py_ssize_t code_n,
                         int kind, PyObject *args, PyObject *kwargs,
                         PyObject *x, uint32_t target, uint64_t ident) {
  /* refs on (F1's D6): a view constant or a view x crosses as a ref —
     the worker's map context owns the resolved view between morsels */
  w->refs = 1;
  IXW_PUT(w, mizu_ix_put_header(IXW_DST(w)));
  IXW_PUT(w, mizu_ix_put_list_begin(IXW_DST(w), 2));
  IXW_PUT(w, mizu_ix_put_task(IXW_DST(w), (int) target, kind, ident));
  ixw_task_fields(w, code, code_n, args, kwargs);
  if (w->decline) return;
  if (x == Py_None) {
    IXW_PUT(w, mizu_ix_put_nil(IXW_DST(w)));
    return;
  }
  memcpy(w->path, "x", 2);
  w->path_len = 1;
  if (!PyList_CheckExact(x)) {
    /* a view x: one ref leaf — the worker's batch loop reads the
       resolved view off the shared pages */
    ixw_node(w, x);
    return;
  }
  Py_ssize_t n = PyList_GET_SIZE(x);
  IXW_PUT(w, mizu_ix_put_list_begin(IXW_DST(w), (uint64_t) n));
  w->depth++;
  for (Py_ssize_t i = 0; i < n && !w->decline; i++) {
    size_t save = w->path_len;
    ixw_path_index(w, i);
    ixw_node(w, PyList_GET_ITEM(x, i));
    if (!w->decline) ixw_path_pop(w, save);
  }
  w->depth--;
}

/* The descriptor write as bytes: code/kind/args/kwargs the spec's
   components, x the list-x or None (the raw section's case), target the
   pool word's language byte. */
PyObject *pymizu_ix_write_map_desc(PyObject *code, long kind,
                                   PyObject *args, PyObject *kwargs,
                                   PyObject *x, uint32_t target) {
  /* x is the element list, a re-sendable view (one ref leaf, F1's D6),
     or None */
  mizu_shm *x_shm = NULL;
  PyObject *x_view = NULL;
  uint32_t x_caps = 0;
  const int x_ref = x != Py_None &&
    mizu_py_view_ref_probe(x, &x_shm, &x_view, &x_caps);
  Py_XDECREF(x_view);
  if (!PyUnicode_CheckExact(code) || !PyTuple_CheckExact(args) ||
      !PyDict_CheckExact(kwargs) ||
      (x != Py_None && !PyList_CheckExact(x) && !x_ref)) {
    PyErr_SetString(PyExc_TypeError,
                    "pymizu: _map_desc_write expects (str, int, tuple, "
                    "dict, list|view|None, int)");
    return NULL;
  }
  Py_ssize_t code_n;
  const char *code_s = PyUnicode_AsUTF8AndSize(code, &code_n);
  if (code_s == NULL) return NULL;
  ixw w;
  memset(&w, 0, sizeof(w));
  memcpy(w.path, "args", 5);
  w.path_len = 4;
  w.caps = MIZU_PY_CAPS;   /* this build's word, as the corpus hook's */
  ixw_map_desc(&w, code_s, code_n, (int) kind, args, kwargs, x, target,
               MIZU_PY_IDENT);
  if (w.decline) {
    ixw_raise(&w);
    return NULL;
  }
  size_t n = w.total;
  uint8_t *buf = malloc(n != 0 ? n : 1);
  if (buf == NULL) return PyErr_NoMemory();
  ixw w2;
  memcpy(&w2, &w, sizeof(w2));
  w2.dst = buf;
  w2.total = 0;
  w2.decline = 0;
  w2.limit = n;
  memset(&w2.warn, 0, sizeof(w2.warn));
  ixw_map_desc(&w2, code_s, code_n, (int) kind, args, kwargs, x, target,
               MIZU_PY_IDENT);
  if (mizu_py_cvt_warn(&w2.warn, target) != 0) {
    free(buf);
    return NULL;
  }
  PyObject *out = PyBytes_FromStringAndSize((const char *) buf,
                                            (Py_ssize_t) n);
  free(buf);
  return out;
}

/* The kind-2 (runner) task stream off the _RunnerFrame's fields: the
   header, the region name, the packed ordinal+generation i64 (ordinal the
   high 32 bits, the morsel generation the low 32 — DESIGN.md's task kind
   registry), and the seed as nil or the (seed, offset) i64v[2]. Bounded
   by the region name's MIZU_NAME_MAX — always inline. 0 staged, 1
   error. */
static size_t ixw_runner(uint8_t *dst, const char *name,
                         Py_ssize_t name_n, int64_t gen_field,
                         PyObject *seed, uint32_t target, uint64_t ident) {
  size_t total = mizu_ix_put_header(dst);
  total += mizu_ix_put_task(dst != NULL ? dst + total : NULL,
                            (int) target, 2, ident);
  total += mizu_ix_put_str(dst != NULL ? dst + total : NULL, name,
                           (int32_t) name_n);
  total += mizu_ix_put_int(dst != NULL ? dst + total : NULL, gen_field);
  if (seed == Py_None) {
    total += mizu_ix_put_nil(dst != NULL ? dst + total : NULL);
  } else {
    int64_t pair[2];
    pair[0] = PyLong_AsLongLong(PyTuple_GET_ITEM(seed, 0));
    pair[1] = PyLong_AsLongLong(PyTuple_GET_ITEM(seed, 1));
    total += mizu_ix_put_vec(dst != NULL ? dst + total : NULL,
                             MIZU_TYPE_INT64, pair, 2);
  }
  return total;
}

int pymizu_ix_stage_runner(PyObject *frame, mizu_slot_hdr *hdr,
                           uint8_t *payload, uint32_t inline_max,
                           mizu_handle *h, uint64_t ident) {
  PyObject *name = PyTuple_GET_ITEM(frame, 0);
  PyObject *gen = PyTuple_GET_ITEM(frame, 1);
  PyObject *seed = PyTuple_GET_ITEM(frame, 2);
  Py_ssize_t name_n;
  const char *name_s = PyUnicode_AsUTF8AndSize(name, &name_n);
  if (name_s == NULL) return 1;
  int overflow = 0;
  long long gen_field = PyLong_AsLongLongAndOverflow(gen, &overflow);
  if (gen_field == -1 && (PyErr_Occurred() || overflow != 0)) {
    if (!PyErr_Occurred())
      PyErr_SetString(PyExc_OverflowError,
                      "pymizu: runner generation out of int64 range");
    return 1;
  }
  if (seed != Py_None &&
      (!PyTuple_CheckExact(seed) || PyTuple_GET_SIZE(seed) != 2 ||
       !PyLong_CheckExact(PyTuple_GET_ITEM(seed, 0)) ||
       !PyLong_CheckExact(PyTuple_GET_ITEM(seed, 1)))) {
    PyErr_SetString(MizuError, "pymizu: a malformed map runner frame");
    return 1;
  }
  const uint64_t word = mizu_pool_worker_ident((mizu_pool *) h);
  if (word == 0) {
    PyErr_SetString(MizuError, "pymizu: no worker has joined this pool");
    return 1;
  }
  size_t n = ixw_runner(NULL, name_s, name_n, (int64_t) gen_field, seed,
                        MIZU_PY_IDENT_LANG(word), ident);
  if (n <= (size_t) inline_max) {
    ixw_runner(payload, name_s, name_n, (int64_t) gen_field, seed,
               MIZU_PY_IDENT_LANG(word), ident);
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = 0;          /* no keeperless claim: inert on task entries */
    return 0;
  }
  /* unreachable in practice (bounded by MIZU_NAME_MAX): the spill path
     keeps the discipline honest */
  uint8_t *buf = malloc(n);
  if (buf == NULL) {
    PyErr_NoMemory();
    return 1;
  }
  ixw_runner(buf, name_s, name_n, (int64_t) gen_field, seed,
             MIZU_PY_IDENT_LANG(word), ident);
  int rc = mizu_py_stage_bytes(buf, n, hdr, payload, inline_max, h);
  free(buf);
  return rc;
}

/* The runner writer as bytes (the test hook's half). The seed shape check
   mirrors pymizu_ix_stage_runner's: ixw_runner reads the pair without
   checks, so every caller validates. */
PyObject *pymizu_ix_write_runner_stream(PyObject *name, int64_t gen_field,
                                        PyObject *seed, uint32_t target,
                                        uint64_t ident) {
  Py_ssize_t name_n;
  const char *name_s = PyUnicode_AsUTF8AndSize(name, &name_n);
  if (name_s == NULL) return NULL;
  if (seed != Py_None &&
      (!PyTuple_CheckExact(seed) || PyTuple_GET_SIZE(seed) != 2 ||
       !PyLong_CheckExact(PyTuple_GET_ITEM(seed, 0)) ||
       !PyLong_CheckExact(PyTuple_GET_ITEM(seed, 1)))) {
    PyErr_SetString(MizuError, "pymizu: a malformed map runner frame");
    return NULL;
  }
  size_t n = ixw_runner(NULL, name_s, name_n, gen_field, seed, target,
                        ident);
  uint8_t *buf = malloc(n);
  if (buf == NULL) return PyErr_NoMemory();
  ixw_runner(buf, name_s, name_n, gen_field, seed, target, ident);
  PyObject *out = PyBytes_FromStringAndSize((const char *) buf,
                                            (Py_ssize_t) n);
  free(buf);
  return out;
}

/* The map descriptor read: list[task, x | nil] — the descriptor reader is
   the one other builder with a task case (DESIGN.md). Returns (kind,
   code, positional, kwargs, x|None): the spec's components, for the
   worker's runner context to bind per element. */
PyObject *pymizu_ix_read_map_desc(const uint8_t *src, size_t n) {
  /* refs resolve (F1's D6): a view x arrives as a ref — the worker's map
     context owns the resolved view between morsels; one fresh open per
     resolve, no handle cache rides a descriptor */
  const ixr_mode mode = { 1, NULL };
  mizu_ix cur;
  mizu_ix_item it, code, pos, named;
  if (mizu_ix_open(&cur, src, n) != MIZU_OK) return ixt_stop_tls();
  if (ixt_next(&cur, &it) < 0) return NULL;
  if (it.kind != MIZU_IX_LIST || it.count != 2) {
    PyErr_SetString(MizuError, "pymizu: malformed map descriptor: not a "
                    "two-element list");
    return NULL;
  }
  if (ixt_next(&cur, &it) < 0) return NULL;
  if (it.kind != MIZU_IX_TASK) {
    PyErr_SetString(MizuError, "pymizu: malformed map descriptor: no task "
                    "tag");
    return NULL;
  }
  if (it.task_kind > 1) {
    PyErr_Format(MizuError, "pymizu: malformed map descriptor: kind 0x%02X "
                 "is not a call spec", it.task_kind);
    return NULL;
  }
  if (mizu_ixt_want_code(&cur, &code) != MIZU_OK) return ixt_stop_tls();
  if (mizu_ixt_want_list(&cur, &pos) != MIZU_OK) return ixt_stop_tls();
  PyObject *positional = NULL, *kwargs = NULL, *keyset = NULL, *x = NULL;
  PyObject *cs = NULL, *out = NULL;
  positional = PyList_New((Py_ssize_t) pos.count);
  if (positional == NULL) goto fail;
  for (uint64_t i = 0; i < pos.count; i++) {
    PyObject *v = ixr_value(&cur, &mode);
    if (v == NULL) goto fail;
    PyList_SET_ITEM(positional, (Py_ssize_t) i, v);
  }
  if (mizu_ixt_want_dict(&cur, &named) != MIZU_OK) {
    ixt_stop_tls();
    goto fail;
  }
  kwargs = PyDict_New();
  if (kwargs == NULL) goto fail;
  keyset = PySet_New(NULL);
  if (keyset == NULL) goto fail;
  for (uint64_t j = 0; j < named.count; j++) {
    PyObject *ko = ixt_key(&cur, keyset);
    if (ko == NULL) goto fail;
    PyObject *v = ixr_value(&cur, &mode);
    if (v == NULL) {
      Py_DECREF(ko);
      goto fail;
    }
    int rc = PyDict_SetItem(kwargs, ko, v);
    Py_DECREF(ko);
    Py_DECREF(v);
    if (rc < 0) goto fail;
  }
  x = ixr_value(&cur, &mode);
  if (x == NULL) goto fail;
  if (mizu_ix_end(&cur) != MIZU_OK) {
    ixt_stop_tls();
    goto fail;
  }
  cs = PyUnicode_FromStringAndSize((const char *) code.ptr,
                                   (Py_ssize_t) code.len);
  if (cs == NULL) goto fail;
  out = Py_BuildValue("(i N N N N)", (int) it.task_kind, cs, positional,
                      kwargs, x);
  Py_XDECREF(keyset);
  return out;
fail:
  Py_XDECREF(positional);
  Py_XDECREF(kwargs);
  Py_XDECREF(keyset);
  Py_XDECREF(x);
  Py_XDECREF(cs);
  return NULL;
}

/* pymizu._map._runner_ix, resolved lazily (the subpackage is imported by
   the time a runner task executes). The static owns the reference — the
   exec_source_fn discipline. */
static PyObject *runner_ix_fn(void) {
  static PyObject *fn = NULL;
  if (fn == NULL) {
    PyObject *mod = PyImport_ImportModule("pymizu._map");
    if (mod == NULL) return NULL;
    fn = PyObject_GetAttrString(mod, "_runner_ix");
    Py_DECREF(mod);
  }
  return fn;
}

/* The exec-hook kind-2 (runner) decode and run: the region reference off
   the cursor — the submitter identity stashed ahead of every field read,
   as the call kinds' — then the binding's own runner loop against the
   named region (_runner_ix unpacks the ordinal and generation, rebuilds
   this language's seed spec from the neutral pair, and runs _runner).
   Returns the runner's (histories, values) result, or NULL with an
   exception set. */
PyObject *pymizu_ix_runner_run(const uint8_t *src, size_t n,
                               uint64_t *ident_out) {
  mizu_ix cur;
  mizu_ix_item it, name, gen, seed;
  if (mizu_ixt_open(&cur, src, n, &it, 2) != MIZU_OK) return ixt_stop_tls();
  *ident_out = it.u64[0];
  /* kind 2 by construction: the exec dispatch keys the same kind byte */
  if (ixt_next(&cur, &name) < 0) return NULL;
  if (name.kind != MIZU_IX_STR1 || name.na) {
    PyErr_SetString(MizuError, "pymizu: malformed runner stream: the "
                    "region name is not a string");
    return NULL;
  }
  if (ixt_next(&cur, &gen) < 0) return NULL;
  if (gen.kind != MIZU_IX_INT) {
    PyErr_SetString(MizuError, "pymizu: malformed runner stream: the "
                    "generation is not an integer");
    return NULL;
  }
  if (ixt_next(&cur, &seed) < 0) return NULL;
  int64_t gv;
  memcpy(&gv, &gen.u64[0], 8);
  PyObject *no = NULL, *go = NULL, *so = Py_None, *fn = NULL, *out = NULL;
  if (seed.kind == MIZU_IX_VEC && seed.type == MIZU_TYPE_INT64 &&
      seed.count == 2) {
    int64_t pair[2];
    memcpy(pair, seed.ptr, 16);
    so = Py_BuildValue("(L L)", (long long) pair[0], (long long) pair[1]);
    if (so == NULL) goto out;
  } else if (seed.kind != MIZU_IX_NIL) {
    PyErr_SetString(MizuError, "pymizu: malformed runner stream: the seed "
                    "is not nil or an i64 pair");
    goto out;
  }
  if (mizu_ix_end(&cur) != MIZU_OK) {
    ixt_stop_tls();
    goto out;
  }
  no = PyUnicode_FromStringAndSize((const char *) name.ptr,
                                   (Py_ssize_t) name.len);
  go = PyLong_FromLongLong(gv);
  fn = runner_ix_fn();
  if (no != NULL && go != NULL && fn != NULL)
    out = PyObject_CallFunctionObjArgs(fn, no, go, so, NULL);
out:
  Py_XDECREF(no);
  Py_XDECREF(go);
  if (so != Py_None) Py_XDECREF(so);
  return out;
}

static void ixw_raise(ixw *w) {
  /* the path's repr truncation can cut a multi-byte UTF-8 sequence:
     decode with replacement so the DeclinedError itself always raises */
  PyObject *p = PyUnicode_DecodeUTF8(w->path, (Py_ssize_t) w->path_len,
                                     "replace");
  PyObject *r = PyUnicode_FromString(w->reason);
  if (p == NULL || r == NULL) {
    Py_XDECREF(p);
    Py_XDECREF(r);
    return;
  }
  PyObject *msg = PyUnicode_FromFormat(
    "pymizu: value is not portable to the peer (%U at %U)", r, p);
  if (msg == NULL) {
    Py_DECREF(p);
    Py_DECREF(r);
    return;
  }
  PyObject *exc = PyObject_CallFunctionObjArgs(MizuDeclinedError, msg,
                                               NULL);
  Py_DECREF(msg);
  if (exc == NULL) {
    Py_DECREF(p);
    Py_DECREF(r);
    return;
  }
  if (PyObject_SetAttrString(exc, "path", p) < 0 ||
      PyObject_SetAttrString(exc, "reason", r) < 0) {
    Py_DECREF(p);
    Py_DECREF(r);
    Py_DECREF(exc);
    return;
  }
  Py_DECREF(p);
  Py_DECREF(r);
  PyErr_SetObject(MizuDeclinedError, exc);
  Py_DECREF(exc);
}

/* The 'I' writer onto the tiers: the slot payload is scratch until the
   hook returns (transactional staging), so the one walk writes into it
   while sizing — an inline-fitting value stages in one walk. Past the
   budget the writes stop at the limit (the count runs on), and the
   ARENA/SHM_RAW reservation takes the second walk directly — no temp
   buffer, no second memcpy. The warn raise stays behind the final write
   (the stage_convert discipline: a raise as error leaves no claimed
   reservation half-written — the core rolls it back). */
int pymizu_ix_stage(PyObject *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                    uint32_t inline_max, mizu_handle *h, uint32_t caps) {
  ixw w;
  memset(&w, 0, sizeof(w));
  memcpy(w.path, "x", 2);
  w.path_len = 1;
  w.dst = payload;
  w.limit = inline_max;
  w.caps = caps;
  ixw_stream(&w, obj);
  if (w.decline) {
    ixw_raise(&w);
    return 1;
  }
  const size_t n = w.total;
  if (!w.overflow) {   /* n <= inline_max: the one-walk INLINE frame */
    /* the 'I' codec's reader is an R peer by construction (stage_impl's
       foreign block), so the sentinel counts always report here */
    if (mizu_py_cvt_warn(&w.warn, MIZU_LANG_R) != 0) return 1;
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = MIZU_AUX_F_KEEPERLESS;
    return 0;
  }
  uint8_t *dst = mizu_py_stage_reserve(n, hdr, payload, h);
  if (dst == NULL) return 1;
  ixw w2;
  memcpy(&w2, &w, sizeof(w2));
  w2.total = 0;
  w2.overflow = 0;
  w2.decline = 0;
  w2.zc_spent = 0;
  w2.ref_emitted = 0;
  w2.dst = dst;
  w2.limit = n;   /* exact: the deterministic write re-counts to n */
  memset(&w2.warn, 0, sizeof(w2.warn));
  ixw_stream(&w2, obj);
  /* a write-pass decline (or any count disagreement) leaves the
     reservation's tail unwritten — stale bytes under the n-byte header */
  if (w2.decline || w2.total != n) {
    PyErr_SetString(MizuError, "pymizu: interop write mismatch");
    return 1;
  }
  /* the corpus hook: the 'I' stream's reference reader is R */
  return mizu_py_cvt_warn(&w2.warn, MIZU_LANG_R);
}

/* The 'I' builder. The pool collect-side result reader resolves a 0x13
   leaf through the handle's view cache; the channel value reader
   declines it (py_chan_read consumes the failure). */
PyObject *pymizu_ix_read(const uint8_t *src, size_t n, mizu_read_ctx *ctx) {
  const int pool = ctx != NULL && ctx->handle != NULL &&
    mizu_handle_kind(ctx->handle) == MIZU_HTYPE_POOL;
  const ixr_mode mode = { pool ? 1 : 0, pool ? ctx : NULL };
  mizu_ix cur;
  if (mizu_ix_open(&cur, src, n) != MIZU_OK) {
    ixr_cursor_err();
    return NULL;
  }
  PyObject *v = ixr_value(&cur, &mode);
  if (v == NULL) return NULL;
  if (mizu_ix_end(&cur) != MIZU_OK) {
    Py_DECREF(v);
    ixr_cursor_err();
    return NULL;
  }
  return v;
}

/* The _write_stream test hook: the writer, as bytes. */
PyObject *pymizu_ix_write_stream(PyObject *obj) {
  ixw w;
  memset(&w, 0, sizeof(w));
  memcpy(w.path, "x", 2);
  w.path_len = 1;
  w.caps = MIZU_PY_CAPS;   /* the corpus hook: this build's full word */
  ixw_stream(&w, obj);
  if (w.decline) {
    ixw_raise(&w);
    return NULL;
  }
  size_t n = w.total;
  uint8_t *buf = malloc(n != 0 ? n : 1);
  if (buf == NULL) return PyErr_NoMemory();
  ixw w2;
  memcpy(&w2, &w, sizeof(w2));
  w2.dst = buf;
  w2.total = 0;
  w2.decline = 0;
  w2.limit = n;
  memset(&w2.warn, 0, sizeof(w2.warn));
  ixw_stream(&w2, obj);
  if (w2.decline || w2.total != n) {
    free(buf);
    PyErr_SetString(MizuError, "pymizu: interop write mismatch");
    return NULL;
  }
  if (mizu_py_cvt_warn(&w2.warn, MIZU_LANG_R) != 0) {
    free(buf);
    return NULL;
  }
  PyObject *out = PyBytes_FromStringAndSize((const char *) buf,
                                            (Py_ssize_t) n);
  free(buf);
  return out;
}

/* Module init: the shared exceptions, the Frame half (interop_frame.c's
   register), _IxRef. */
int mizu_py_interop_register(PyObject *m, PyObject *mizu_error,
                             PyObject *declined_error) {
  MizuError = mizu_error;
  MizuDeclinedError = declined_error;
  mizu_py_shmframe_register(mizu_error, declined_error);
  if (mizu_py_frame_register(m, mizu_error) < 0) return -1;
  if (PyType_Ready(&MizuIxRefType) < 0) return -1;
  Py_INCREF(&MizuIxRefType);
  return PyModule_AddObject(m, "_IxRef", (PyObject *) &MizuIxRefType);
}

