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

static int64_t attr_as_long(PyObject *obj, const char *name, int *ok) {
  PyObject *v = PyObject_GetAttrString(obj, name);
  if (v == NULL) {
    *ok = 0;
    return 0;
  }
  long long x = PyLong_AsLongLong(v);
  Py_DECREF(v);
  if (x == -1 && PyErr_Occurred()) {
    *ok = 0;
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

static int ixref_init(PyObject *self, PyObject *args, PyObject *kwargs) {
  PyObject *id = NULL;
  if (!PyArg_ParseTuple(args, "U", &id)) return -1;
  Py_INCREF(id);
  Py_XSETREF(((MizuIxRef *) self)->id, id);
  return 0;
}

static PyMemberDef ixref_members[] = {
  { "id", T_OBJECT_EX, offsetof(MizuIxRef, id), READONLY,
    "the region identifier" },
  { NULL }
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

/* An intv value, or the 0x02 scalar at length 1. Vector elements must fit
   int32 (the callers' contract); the scalar form keeps the full i64. */
static void ixe_intv(ixw *w, Py_ssize_t n, const int64_t *vals,
                     const int *na) {
  if (n == 1) {
    int64_t v = na != NULL && na[0] ? INT64_MIN : vals[0];
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), v));
    return;
  }
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
static void ixe_realv_counts(ixw *w, const int64_t *counts, uint64_t n,
                             double scale) {
  IXW_PUT(w, ixe_vec_begin(IXW_DST(w), MIZU_IX_TAG_REALV, n));
  if (w->decline) return;
  if (w->dst != NULL && w->total + (size_t) n * 8 <= w->limit) {
    uint8_t *dst = w->dst + w->total;
    for (uint64_t i = 0; i < n; i++) {
      if (counts[i] == INT64_MIN) {
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

/* The attr shapes (the corpus-pinned forms):
   factor:  attr(intv codes, {levels: strv, class: "factor"})
   Date:    attr(realv days, {class: "Date"})
   POSIXct: attr(realv seconds, {class: ["POSIXct", "POSIXt"], tzone: tz})
*/
static void ixe_date(ixw *w, const int64_t *days, uint64_t n, double scale) {
  static const char *cls[1] = { MIZU_IX_CLASS_DATE };
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_realv_counts(w, days, n, scale);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 1));
  ixe_key(w, MIZU_IX_ATTR_CLASS);
  ixe_class(w, cls, 1);
}

static void ixe_posixct(ixw *w, const int64_t *counts, uint64_t n,
                        const char *tz, double scale) {
  static const char *cls[2] = { MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT };
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_realv_counts(w, counts, n, scale);
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
static void ixe_difftime(ixw *w, const int64_t *counts, uint64_t n,
                         double scale) {
  static const char *cls[1] = { MIZU_IX_CLASS_DIFFTIME };
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_realv_counts(w, counts, n, scale);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 2));
  ixe_key(w, MIZU_IX_ATTR_CLASS);
  ixe_class(w, cls, 1);
  ixe_key(w, MIZU_IX_ATTR_UNITS);
  const char *one[1] = { MIZU_IX_UNIT_SECS };
  int64_t len[1] = { 4 };
  ixe_strv(w, 1, one, len);
}

// The layout attribute blobs (pyshmframe.h's; complete 'I' streams) ----------------

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

/* The factor dict {levels, class = "factor"} — the inline attr shape's
   form exactly, as a complete stream. */
size_t mizu_py_blob_factor(uint8_t *dst, const uint8_t *bytes,
                           const int32_t *offs, int64_t nlev) {
  ixw w;
  memset(&w, 0, sizeof(w));
  w.dst = dst;
  w.limit = SIZE_MAX;   /* the caller's buffer: unbounded (size pass NULL) */
  IXW_PUT(&w, mizu_ix_put_header(IXW_DST(&w)));
  IXW_PUT(&w, mizu_ix_put_dict_begin(IXW_DST(&w), 2));
  ixe_key(&w, MIZU_IX_ATTR_LEVELS);
  if (nlev == 1) {
    IXW_PUT(&w, mizu_ix_put_str(IXW_DST(&w), bytes, offs[1] - offs[0]));
  } else {
    IXW_PUT(&w, mizu_ix_put_strv_begin(IXW_DST(&w), (uint64_t) nlev));
    for (int64_t i = 0; i < nlev; i++)
      IXW_PUT(&w, mizu_ix_put_strelt(IXW_DST(&w), bytes + offs[i],
                                     offs[i + 1] - offs[i]));
  }
  ixe_key(&w, MIZU_IX_ATTR_CLASS);
  {
    static const char *cls[1] = { MIZU_IX_CLASS_FACTOR };
    ixe_class(&w, cls, 1);
  }
  return w.total;
}

size_t mizu_py_blob_date(uint8_t *dst) {
  static const char *cls[1] = { MIZU_IX_CLASS_DATE };
  ixw w;
  memset(&w, 0, sizeof(w));
  w.dst = dst;
  w.limit = SIZE_MAX;   /* the caller's buffer: unbounded (size pass NULL) */
  IXW_PUT(&w, mizu_ix_put_header(IXW_DST(&w)));
  IXW_PUT(&w, mizu_ix_put_dict_begin(IXW_DST(&w), 1));
  ixe_key(&w, MIZU_IX_ATTR_CLASS);
  ixe_class(&w, cls, 1);
  return w.total;
}

size_t mizu_py_blob_ts(uint8_t *dst, const char *tz) {
  static const char *cls[2] = { MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT };
  ixw w;
  memset(&w, 0, sizeof(w));
  w.dst = dst;
  w.limit = SIZE_MAX;   /* the caller's buffer: unbounded (size pass NULL) */
  IXW_PUT(&w, mizu_ix_put_header(IXW_DST(&w)));
  IXW_PUT(&w, mizu_ix_put_dict_begin(IXW_DST(&w), 2));
  ixe_key(&w, MIZU_IX_ATTR_CLASS);
  ixe_class(&w, cls, 2);
  ixe_key(&w, MIZU_IX_ATTR_TZONE);
  IXW_PUT(&w, mizu_ix_put_str(IXW_DST(&w), tz, (int32_t) strlen(tz)));
  return w.total;
}

/* The difftime dict {class = "difftime", units = "secs"} — the layout
   column's doubles are seconds, so the blob is unit-fixed. */
size_t mizu_py_blob_difftime(uint8_t *dst) {
  static const char *cls[1] = { MIZU_IX_CLASS_DIFFTIME };
  ixw w;
  memset(&w, 0, sizeof(w));
  w.dst = dst;
  w.limit = SIZE_MAX;   /* the caller's buffer: unbounded (size pass NULL) */
  IXW_PUT(&w, mizu_ix_put_header(IXW_DST(&w)));
  IXW_PUT(&w, mizu_ix_put_dict_begin(IXW_DST(&w), 2));
  ixe_key(&w, MIZU_IX_ATTR_CLASS);
  ixe_class(&w, cls, 1);
  ixe_key(&w, MIZU_IX_ATTR_UNITS);
  IXW_PUT(&w, mizu_ix_put_str(IXW_DST(&w), MIZU_IX_UNIT_SECS,
                              (int32_t) (sizeof MIZU_IX_UNIT_SECS - 1)));
  return w.total;
}

/* The frame dict {names, class = "data.frame", row.names}: row.names the
   automatic c(NA, -n), or a same-language Frame's int32 / character form.
   0 with an exception set on a malformed row_names (never on the size
   pass's own account). */
size_t mizu_py_blob_frame(uint8_t *dst, char **names, int ncols, int64_t rows,
                          PyObject *row_names) {
  ixw w;
  memset(&w, 0, sizeof(w));
  w.dst = dst;
  w.limit = SIZE_MAX;   /* the caller's buffer: unbounded (size pass NULL) */
  IXW_PUT(&w, mizu_ix_put_header(IXW_DST(&w)));
  IXW_PUT(&w, mizu_ix_put_dict_begin(IXW_DST(&w), 3));
  ixe_key(&w, MIZU_IX_ATTR_NAMES);
  ixe_names(&w, names, ncols);
  ixe_key(&w, MIZU_IX_ATTR_CLASS);
  {
    static const char *cls[1] = { MIZU_IX_CLASS_DATAFRAME };
    ixe_class(&w, cls, 1);
  }
  ixe_key(&w, MIZU_IX_ATTR_ROWNAMES);
  if (row_names == NULL || row_names == Py_None) {
    int64_t vals[2] = { 0, -rows };
    int na[2] = { 1, 0 };
    ixe_intv(&w, 2, vals, na);
    return w.total;
  }
  if (PyList_Check(row_names)) {
    Py_ssize_t nr = PyList_GET_SIZE(row_names);
    if (nr != (Py_ssize_t) rows) goto malformed;
    if (nr == 1) {
      Py_ssize_t len;
      const char *u =
        PyUnicode_AsUTF8AndSize(PyList_GET_ITEM(row_names, 0), &len);
      if (u == NULL) return 0;
      IXW_PUT(&w, mizu_ix_put_str(IXW_DST(&w), u, (int32_t) len));
    } else {
      IXW_PUT(&w, mizu_ix_put_strv_begin(IXW_DST(&w), (uint64_t) nr));
      for (Py_ssize_t i = 0; i < nr; i++) {
        Py_ssize_t len;
        const char *u =
          PyUnicode_AsUTF8AndSize(PyList_GET_ITEM(row_names, i), &len);
        if (u == NULL) return 0;
        IXW_PUT(&w, mizu_ix_put_strelt(IXW_DST(&w), u, (int32_t) len));
      }
    }
    return w.total;
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
    return w.total;
  }
malformed:
  PyErr_SetString(MizuError, "pymizu: a Frame's row_names are not int32 "
                  "or character of the row count");
  return 0;
}

// The builder -----------------------------------------------------------------------

/* np.empty(dims, dtype=dt[, order="F"]); NULL with an exception set. */
static PyObject *ixr_np_alloc(const Py_ssize_t *dims, int nd, const char *dt,
                              int fortran) {
  PyObject *np = mizu_py_numpy_module();
  if (np == NULL) {
    PyErr_SetString(MizuError,
                    "pymizu: this interop read needs numpy (install it)");
    return NULL;
  }
  PyObject *empty = PyObject_GetAttrString(np, "empty");
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
   the buffer size must match n. An F-order array's buffer refuses the
   C-contiguous request — it IS the wire's layout, so either contiguity
   fills by one memcpy. */
static int ixr_np_fill(PyObject *arr, void (*conv)(uint8_t *, const void *),
                       const void *ctx, const void *src, size_t n) {
  Py_buffer v;
  if (PyObject_GetBuffer(arr, &v, PyBUF_ND | PyBUF_WRITABLE) < 0) {
    PyErr_Clear();
    if (PyObject_GetBuffer(arr, &v, PyBUF_F_CONTIGUOUS | PyBUF_WRITABLE) < 0)
      return -1;
  }
  if ((size_t) v.len != n) {
    PyBuffer_Release(&v);
    PyErr_SetString(MizuError, "pymizu: numpy buffer size mismatch");
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
  PyErr_SetString(MizuError, "pymizu: unknown interop vector wire type");
  return NULL;
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

static PyObject *ixr_value(mizu_ix *cur, const ixr_mode *mode);
static PyObject *ixr_frame(mizu_ix *cur, uint64_t ncols,
                           const ixr_mode *mode);

static PyObject *ixr_str(const mizu_ix_item *it) {
  if (it->na) Py_RETURN_NONE;
  return PyUnicode_DecodeUTF8((const char *) it->ptr, (Py_ssize_t) it->len,
                              NULL);
}

static PyObject *ixr_strv(mizu_ix *cur, uint64_t count, const ixr_mode *mode) {
  PyObject *out = PyList_New((Py_ssize_t) count);
  if (out == NULL) return NULL;
  for (uint64_t i = 0; i < count; i++) {
    mizu_ix_item it;
    if (mizu_ix_next(cur, &it) != MIZU_OK || it.kind != MIZU_IX_STR) {
      Py_DECREF(out);
      PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
      PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
   (0x02), or a built object. The shape readers consume the spans without
   an intermediate conversion (a compact row.names intv carries an NA the
   float64 home would hide). */
enum { AV_OBJ, AV_STR, AV_STRLIST, AV_INTSPAN, AV_INT };

typedef struct {
  PyObject *obj;             /* AV_OBJ / AV_STRLIST */
  const uint8_t *ptr;        /* AV_STR / AV_INTSPAN */
  uint64_t count;            /* AV_STR (bytes) / AV_INTSPAN (elements) */
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
  if (n > 16) {
    PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                    "an attribute dict past 16 entries");
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
      PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
    mizu_ix_item vit;
    if (mizu_ix_next(cur, &vit) != MIZU_OK) {
      PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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

static attr_ent *attr_find(attr_ent *ents, int n, const char *key) {
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
    PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
    return NULL;
  }
  attr_ent *ents = ixr_attr_dict(&cur, n_out, NULL);
  if (ents == NULL) return NULL;
  if (mizu_ix_end(&cur) != MIZU_OK) {
    PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
    attr_vals_free(ents, *n_out);
    return NULL;
  }
  return ents;
}

static int str_obj_eq(PyObject *s, const char *c) {
  return PyUnicode_Check(s) && PyUnicode_CompareWithASCIIString(s, c) == 0;
}

/* class == [a] (AV_STR is the length-1 form) or [a, b]. */
static int class_is(attr_ent *e, const char *a, const char *b) {
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

/* The no-home error, naming the attribute names and the class. */
static void ixr_no_home(attr_ent *ents, int n, const char *what) {
  char msg[512];
  size_t off = 0;
  off += snprintf(msg + off, sizeof(msg) - off,
                  "pymizu: no portable home for %s (attributes: ", what);
  for (int i = 0; i < n && off < sizeof(msg) - 48; i++)
    off += snprintf(msg + off, sizeof(msg) - off, "%s\"%.*s\"",
                    i != 0 ? ", " : "", (int) ents[i].key_len, ents[i].key);
  attr_ent *cls = attr_find(ents, n, MIZU_IX_ATTR_CLASS);
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
static int levels_read(attr_ent *e, int32_t **off_out, uint8_t **bytes_out,
                       int64_t *nlev_out, int64_t *blen_out) {
  Py_ssize_t nlev = e->kind == AV_STR ? 1 : PyList_GET_SIZE(e->val.obj);
  int32_t *offs = malloc(((size_t) nlev + 1) * 4);
  uint8_t *bytes = NULL;
  size_t blen = 0;
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
    uint8_t *nb = realloc(bytes, blen + (size_t) len + 1);
    if (nb == NULL) goto nomem;
    bytes = nb;
    memcpy(bytes + blen, s, (size_t) len);
    blen += (size_t) len;
    offs[i + 1] = (int32_t) blen;
  }
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
  int32_t *codes = malloc((size_t) (n != 0 ? n : 1) * 4);
  if (codes == NULL) {
    PyErr_NoMemory();
    return -1;
  }
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
    PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
  PyErr_SetString(MizuError, "pymizu: corrupt interop stream");
  return NULL;
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
    PyErr_SetString(MizuError, "pymizu: no portable home for a "
                    "character matrix");
    return NULL;
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
    PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
  attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  attr_ent *lv = attr_find(ents, nent, MIZU_IX_ATTR_LEVELS);
  attr_ent *dm = attr_find(ents, nent, MIZU_IX_ATTR_DIM);
  attr_ent *tz = attr_find(ents, nent, MIZU_IX_ATTR_TZONE);
  attr_ent *un = attr_find(ents, nent, MIZU_IX_ATTR_UNITS);
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
}

// The frame reader ------------------------------------------------------------------

/* A Date column's doubles to owned i32 days (MIZU_NA_INT32 null); a
   fractional or non-finite value is the informative decline. */
static int fcol_date_fill(const double *src, uint64_t vn, fcol *c) {
  int32_t *days = malloc((size_t) (vn != 0 ? vn : 1) * 4);
  if (days == NULL) {
    PyErr_NoMemory();
    return -1;
  }
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

/* A POSIXct column's doubles to owned i64 microseconds (INT64_MIN null);
   the tzone name (empty when naive) is export metadata. */
static int fcol_ts_fill(const double *src, uint64_t vn, const char *tz,
                        size_t tz_len, fcol *c) {
  int64_t *us = malloc((size_t) (vn != 0 ? vn : 1) * 8);
  if (us == NULL) {
    PyErr_NoMemory();
    return -1;
  }
  for (uint64_t i = 0; i < vn; i++) {
    uint64_t bits;
    memcpy(&bits, src + i, 8);
    if (is_na_r(bits)) {
      us[i] = INT64_MIN;
    } else if (!isfinite(src[i]) || fabs(src[i]) > 9.0e12) {
      free(us);
      PyErr_SetString(MizuError, "pymizu: no portable home for this "
                      "POSIXct value");
      return -1;
    } else {
      us[i] = (int64_t) llround(src[i] * 1e6);
    }
  }
  c->kind = FCOL_TS;
  c->n = (int64_t) vn;
  c->values = (uint8_t *) us;
  if (tz_len > 0 && tz_len < sizeof(c->tz))
    snprintf(c->tz, sizeof(c->tz), "%.*s", (int) tz_len, tz);
  return 0;
}

/* A difftime column's doubles (in the dict's declared units, unit_secs
   the unit in seconds) to owned i64 us (MIZU_NA_INT64 null) — Arrow
   duration[us]. */
static int fcol_difftime_fill(const double *src, uint64_t vn,
                              double unit_secs, fcol *c) {
  int64_t *us = malloc((size_t) (vn != 0 ? vn : 1) * 8);
  if (us == NULL) {
    PyErr_NoMemory();
    return -1;
  }
  for (uint64_t i = 0; i < vn; i++) {
    uint64_t bits;
    memcpy(&bits, src + i, 8);
    if (is_na_r(bits)) {
      us[i] = INT64_MIN;
    } else if (!isfinite(src[i]) || fabs(src[i]) * unit_secs > 9.0e12) {
      free(us);
      PyErr_SetString(MizuError, "pymizu: no portable home for this "
                      "difftime value");
      return -1;
    } else {
      us[i] = (int64_t) llround(src[i] * unit_secs * 1e6);
    }
  }
  c->kind = FCOL_TD;
  c->n = (int64_t) vn;
  c->values = (uint8_t *) us;
  return 0;
}

/* One frame column: the cursor at the column's first item; fills the
   fcol. */
static int ixr_column(mizu_ix *cur, fcol *c) {
  mizu_ix_item it;
  if (mizu_ix_next(cur, &it) != MIZU_OK) {
    PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
      PyErr_SetString(MizuError, "pymizu: malformed interop stream: "
                      "a frame column of an unexpected wire type");
      return -1;
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
    size_t blen = 0;
    if (offs == NULL) {
      PyErr_NoMemory();
      return -1;
    }
    offs[0] = 0;
    for (uint64_t i = 0; i < n; i++) {
      mizu_ix_item sit;
      if (mizu_ix_next(cur, &sit) != MIZU_OK || sit.kind != MIZU_IX_STR) {
        PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
        goto strv_fail;
      }
      if (sit.na && valid == NULL) {
        valid = calloc(((size_t) n + 7) / 8, 1);
        if (valid == NULL) {
          PyErr_NoMemory();
          goto strv_fail;
        }
        for (uint64_t j = 0; j < i; j++) valid[j >> 3] |= 1 << (j & 7);
      }
      if (!sit.na) {
        if (valid != NULL) valid[i >> 3] |= 1 << (i & 7);
        if (blen + sit.len > INT32_MAX) {
          PyErr_SetString(MizuError, "pymizu: a string column past the "
                          "int32 offset range");
          goto strv_fail;
        }
        uint8_t *nb = realloc(bytes, blen + sit.len + 1);
        if (nb == NULL) {
          PyErr_NoMemory();
          goto strv_fail;
        }
        bytes = nb;
        memcpy(bytes + blen, sit.ptr, sit.len);
        blen += sit.len;
      }
      offs[i + 1] = (int32_t) blen;
    }
    c->kind = FCOL_STR;
    c->n = (int64_t) n;
    c->values = (uint8_t *) offs;
    c->bytes = bytes;
    c->bytes_len = (int64_t) blen;
    c->valid = valid;
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
      PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
    attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
    attr_ent *lv = attr_find(ents, nent, MIZU_IX_ATTR_LEVELS);
    attr_ent *tz = attr_find(ents, nent, MIZU_IX_ATTR_TZONE);
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
      attr_ent *un = attr_find(ents, nent, MIZU_IX_ATTR_UNITS);
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
    fc->names = nb;
    memcpy(fc->names + name_len, s, (size_t) len);
    fc->name_off[i] = (int32_t) name_len;
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

/* The data.frame shape: read the columns, then the dict, and validate
   before installing the shell. */
static PyObject *ixr_frame(mizu_ix *cur, uint64_t ncols64,
                           const ixr_mode *mode) {
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
  attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  attr_ent *nm = attr_find(ents, nent, MIZU_IX_ATTR_NAMES);
  attr_ent *rn = attr_find(ents, nent, MIZU_IX_ATTR_ROWNAMES);
  if (cls == NULL || nm == NULL || rn == NULL || nent != 3 ||
      !class_is(cls, MIZU_IX_CLASS_DATAFRAME, NULL)) {
    if (!PyErr_Occurred())
      ixr_no_home(ents, nent, "an attributed list");
    attr_vals_free(ents, nent);
    goto fail;
  }
  frame_cols *fc = frame_cols_new(ncols, nrow);
  if (fc == NULL) {
    PyErr_NoMemory();
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
    return NULL;
  }
  self->fc = fc;
  self->row_names = rownames;
  self->loan = NULL;
  return (PyObject *) self;
dict_fail:
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
                       fcol *c, unsigned depth);
static int tree_column_fill(const uint8_t *base, const mizu_mizl_entry *e,
                            fcol *c, unsigned depth);

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

/* A remote leaf (MIZL directory tag 33): the column lives in another
   region and crosses by reference. Resolve the identifier span, validate
   the entry's claims against the referenced leaf, then the local leaf
   dispatch on the referenced region — a path recurses, a bare name fills
   off the root's synthesized entry. The column's bytes live in a region
   the frame's loan does not cover: a fresh mapping and its counted loan
   hang off the fcol's hold. Every decline is the corrupt-or-newer shape —
   behind the capability gate, meeting one unadvertised is exactly that. */
static int tree_column_remote(const uint8_t *base, const mizu_mizl_entry *e,
                              fcol *c, unsigned depth) {
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
  mizu_shm *shm;
  if (mizu_shm_open_view(&shm, name) != MIZU_OK) {
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

  int rc = brack == NULL ? tree_column_fill(rbase, &ref, c, depth + 1) :
    tree_column(fbase, fsize, fidx, c, depth + 1);
  if (rc != 0) goto release;   /* the fill's own error stands */
  if (c->hold == NULL) {
    c->hold = shm;
    c->hold_pid = mizu_self_pid();
  } else {
    /* a chained reference: the column borrows the deeper region alone —
       this one serves nothing further */
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
  mizu_zc_unref(shm);
  mizu_shm_close(shm, 0);
  return -1;
}

/* A frame column off an MIZL directory entry: the §1.0 column set only —
   attribute-free atomics (tag 32 the int64 form) borrow the leaf's bytes,
   a factor blob makes a dictionary column (owned levels, borrowed 1-based
   codes), Date/POSIXct blobs convert (owned), a string leaf borrows its
   MIZS block, a remote leaf resolves by reference; anything else declines
   informatively. */
static int tree_column_fill(const uint8_t *base, const mizu_mizl_entry *e,
                            fcol *c, unsigned depth) {
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
    return tree_column_remote(base, e, c, depth);
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
  attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  attr_ent *lv = attr_find(ents, nent, MIZU_IX_ATTR_LEVELS);
  attr_ent *tz = attr_find(ents, nent, MIZU_IX_ATTR_TZONE);
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
  } else if (cls != NULL && nent == 2 && class_is(cls, MIZU_IX_CLASS_DIFFTIME, NULL) &&
             tag == MIZU_TYPE_REAL) {
    attr_ent *un = attr_find(ents, nent, MIZU_IX_ATTR_UNITS);
    const double us = un != NULL && un->kind == AV_STR ?
      difftime_unit_secs((const char *) un->val.ptr,
                         (size_t) un->val.count) : 0.0;
    if (us != 0.0)
      rc = fcol_difftime_fill((const double *) data, (uint64_t) e->length,
                              us, c);
    else
      ixr_no_home(ents, nent, "an attributed frame column");
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
                       fcol *c, unsigned depth) {
  mizu_mizl_entry e;
  if (mizu_mizl_elem(base, size, i, &e) != 0) {
    PyErr_SetString(MizuError, "pymizu: corrupt payload slot");
    return -1;
  }
  memset(c, 0, sizeof(*c));
  return tree_column_fill(base, &e, c, depth);
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
  attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  attr_ent *lv = attr_find(ents, nent, MIZU_IX_ATTR_LEVELS);
  attr_ent *dm = attr_find(ents, nent, MIZU_IX_ATTR_DIM);
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
    attr_ent *un = attr_find(ents, nent, MIZU_IX_ATTR_UNITS);
    const double us = un != NULL && un->kind == AV_STR ?
      difftime_unit_secs((const char *) un->val.ptr,
                         (size_t) un->val.count) : 0.0;
    if (us != 0.0)
      out = realv_to_timedelta(data, (uint64_t) n, us);
    else
      ixr_no_home(ents, nent, "an attributed value");
  } else {
    ixr_no_home(ents, nent, "an attributed value");
  }
  attr_vals_free(ents, nent);
  return out;
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
   directory entries, names and row.names off the root blob, the tree's
   loan anchor on the shell. */
static PyObject *tree_frame(PyObject *owner, PyObject *loan,
                            const uint8_t *base, size_t size, int64_t n,
                            const attr_ent *nm, const attr_ent *rn,
                            unsigned depth) {
  if (n == 0 || n > (int64_t) (1u << 20)) {
    PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                    "(a frame without columns)");
    return NULL;
  }
  int ncols = (int) n;
  fcol *cols = calloc((size_t) ncols, sizeof(fcol));
  if (cols == NULL) return PyErr_NoMemory();
  int64_t nrow = -1;
  for (int i = 0; i < ncols; i++) {
    if (tree_column(base, size, i, &cols[i], depth) < 0) goto fail;
    if (nrow < 0) nrow = cols[i].n;
    else if (cols[i].n != nrow) {
      PyErr_SetString(MizuError, "pymizu: corrupt or newer region (the "
                      "frame's columns differ in length)");
      goto fail;
    }
  }
  frame_cols *fc = frame_cols_new(ncols, nrow);
  if (fc == NULL) {
    PyErr_NoMemory();
    goto fail;
  }
  if (frame_names_build(fc, nm) < 0) goto names_fail;
  PyObject *rownames = frame_rownames_build(rn, nrow);
  if (rownames == NULL) goto names_fail;
  free(fc->cols);
  fc->cols = cols;
  MizuFrame *self = (MizuFrame *) MizuFrameType.tp_alloc(&MizuFrameType, 0);
  if (self == NULL) {
    frame_cols_decref(fc);
    Py_DECREF(rownames);
    return NULL;
  }
  self->fc = fc;
  self->row_names = rownames;
  Py_INCREF(loan);
  self->loan = loan;
  return (PyObject *) self;
names_fail:
  frame_cols_decref(fc);
fail:
  for (int i = 0; i < ncols; i++) fcol_free(&cols[i]);
  free(cols);
  return NULL;
}

/* The root and leaf dict's shape decision: no attributes a plain list,
   names only a dict, the data.frame shape a region-backed Frame, any
   other attribute set the no-home error. */
static PyObject *tree_home(PyObject *owner, PyObject *loan,
                           const uint8_t *base, size_t size, int64_t n,
                           attr_ent *ents, int nent, unsigned depth) {
  attr_ent *nm = attr_find(ents, nent, MIZU_IX_ATTR_NAMES);
  attr_ent *cls = attr_find(ents, nent, MIZU_IX_ATTR_CLASS);
  attr_ent *rn = attr_find(ents, nent, MIZU_IX_ATTR_ROWNAMES);
  if (nent == 0)
    return tree_list(owner, loan, base, size, n, depth);
  if (nent == 1 && nm != NULL)
    return tree_dict(owner, loan, base, size, n, nm, depth);
  if (nent == 3 && cls != NULL && nm != NULL && rn != NULL &&
      class_is(cls, MIZU_IX_CLASS_DATAFRAME, NULL))
    return tree_frame(owner, loan, base, size, n, nm, rn, depth);
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

/* One fcol as a frame column value. */
static void ixe_fcol(ixw *w, const fcol *c) {
  switch (c->kind) {
  case FCOL_F64:
    IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_REAL, c->values,
                               (uint64_t) c->n));
    break;
  case FCOL_I32:
    IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_INT, c->values,
                               (uint64_t) c->n));
    break;
  case FCOL_I64:
    IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_INT64, c->values,
                               (uint64_t) c->n));
    break;
  case FCOL_U8:
    IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_RAW, c->values,
                               (uint64_t) c->n));
    break;
  case FCOL_C128:
    IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_CPLX, c->values,
                               (uint64_t) c->n));
    break;
  case FCOL_LGL:
    IXW_PUT(w, mizu_ix_put_vec(IXW_DST(w), MIZU_TYPE_LGL, c->values,
                               (uint64_t) c->n));
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
  case FCOL_DICT: {
    static const char *cls[1] = { MIZU_IX_CLASS_FACTOR };
    const int32_t *codes = (const int32_t *) c->values;
    IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
    if (c->n == 1) {
      IXW_PUT(w, mizu_ix_put_int(
                IXW_DST(w),
                codes[0] == MIZU_NA_INT32 ? INT64_MIN : codes[0] + 1));
    } else {
      IXW_PUT(w, ixe_vec_begin(IXW_DST(w), MIZU_IX_TAG_INTV,
                               (uint64_t) c->n));
      if (w->decline) return;
      if (w->dst != NULL && w->total + (size_t) c->n * 4 <= w->limit) {
        uint8_t *dst = w->dst + w->total;
        for (int64_t i = 0; i < c->n; i++) {
          int32_t v = codes[i] == MIZU_NA_INT32 ? MIZU_NA_INT32 :
            codes[i] + 1;
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
        if (days[i] == MIZU_NA_INT32) {
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
    ixe_posixct(w, (const int64_t *) c->values, (uint64_t) c->n,
                c->tz[0] != '\0' ? c->tz : "UTC", 1e-6);
    break;
  case FCOL_TD:
    ixe_difftime(w, (const int64_t *) c->values, (uint64_t) c->n, 1e-6);
    break;
  }
}

/* The frame shape: attr(list cols, {names, class: "data.frame",
   row.names}). Names and row.names follow the length-1 scalar rule. */
static void ixe_frame(ixw *w, const frame_cols *fc, PyObject *row_names) {
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  IXW_PUT(w, mizu_ix_put_list_begin(IXW_DST(w), (uint64_t) fc->ncols));
  for (int i = 0; i < fc->ncols; i++) ixe_fcol(w, &fc->cols[i]);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 3));
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
  {
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
      IXW_PUT(w, mizu_ix_put_str(IXW_DST(w), s, (int32_t) l));
    } else {
      IXW_PUT(w, mizu_ix_put_strv_begin(IXW_DST(w), (uint64_t) n));
      for (Py_ssize_t i = 0; i < n; i++) {
        Py_ssize_t l = 0;
        const char *s = PyUnicode_AsUTF8AndSize(PyList_GET_ITEM(row_names,
                                                                i), &l);
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
  switch (row->wire) {
  case MIZU_TYPE_LGL:
    IXW_PUT(w, mizu_ix_put_lgl(IXW_DST(w), data[0] != 0));
    break;
  case MIZU_TYPE_INT: {
    int64_t v;
    switch (row->w_in) {
    case 1: v = (int64_t) *(const int8_t *) data; break;
    case 2:
      if (row->cvt == CVT_U16_INT) {
        uint16_t x;
        memcpy(&x, data, 2);
        v = x;
      } else {
        int16_t x;
        memcpy(&x, data, 2);
        v = x;
      }
      break;
    default: {
      int32_t x;
      memcpy(&x, data, 4);
      v = x;
      break;
    }
    }
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), v));
    break;
  }
  case MIZU_TYPE_INT64: {
    int64_t v;
    memcpy(&v, data, 8);
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), v));
    break;
  }
  case MIZU_TYPE_REAL: {
    double v;
    if (row->w_in == 4) {
      float x;
      memcpy(&x, data, 4);
      v = x;
    } else {
      memcpy(&v, data, 8);
    }
    IXW_PUT(w, mizu_ix_put_real(IXW_DST(w), v));
    break;
  }
  case MIZU_TYPE_CPLX: {
    double re, im;
    if (row->w_in == 8) {
      float x[2];
      memcpy(x, data, 8);
      re = x[0];
      im = x[1];
    } else {
      memcpy(&re, data, 8);
      memcpy(&im, data + 8, 8);
    }
    IXW_PUT(w, mizu_ix_put_cplx(IXW_DST(w), re, im));
    break;
  }
  default:   /* RAW: no scalar form — a uint8 scalar reads back as int */
    IXW_PUT(w, mizu_ix_put_int(IXW_DST(w), (int64_t) data[0]));
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
      mizu_py_cvt_convert(dst, src, NULL, 0, (size_t) n, row, &w->warn);
    } else {
      for (uint64_t i = 0; i < n; i++)
        mizu_py_cvt_convert(dst + i * row->w_out,
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
            default: memcpy(dp, sp, wo); break;
            }
          } else {
            mizu_py_cvt_convert(dp, sp, NULL, 0, 1, row, warn);
          }
          sp += s0;
          dp += wo;
        }
      }
    }
  }
}

/* A multi-dimensional array's body: the F-order memcpy when contiguous,
   the transpose gather otherwise — tiled for 2-D, the generic div/mod
   walk past it (each inside the one stage copy). */
static void ixe_nd_body(ixw *w, const uint8_t *src, int nd,
                        const int64_t *shape, const int64_t *strides,
                        const cvt_row *row) {
  uint64_t total = 1;
  for (int i = 0; i < nd; i++) total *= (uint64_t) shape[i];
  IXW_PUT(w, ixe_vec_begin(IXW_DST(w), mizu_ix_tag_of(row->wire), total));
  if (w->decline) return;
  if (w->dst != NULL &&
      w->total + (size_t) total * row->w_out <= w->limit) {
    uint8_t *dst = w->dst + w->total;
    int64_t expect = row->w_in;
    int f_contig = 1;
    for (int i = 0; i < nd; i++) {
      if (strides[i] != expect) {
        f_contig = 0;
        break;
      }
      expect *= shape[i];
    }
    if (f_contig) {
      mizu_py_cvt_convert(dst, src, NULL, 0, (size_t) total, row, &w->warn);
    } else if (nd == 2) {
      ixe_transpose2(dst, src, (uint64_t) shape[0], (uint64_t) shape[1],
                     strides[0], strides[1], row, &w->warn);
    } else {
      for (uint64_t t = 0; t < total; t++) {
        uint64_t rem = t;
        int64_t off = 0;
        for (int k = 0; k < nd; k++) {
          uint64_t dim = (uint64_t) shape[k];
          int64_t c = dim != 0 ? (int64_t) (rem % dim) : 0;
          rem = dim != 0 ? rem / dim : 0;
          off += c * strides[k];
        }
        mizu_py_cvt_convert(dst + t * row->w_out, src + off, NULL, 0, 1,
                            row, &w->warn);
      }
    }
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
  IXW_PUT(w, mizu_ix_put_attr(IXW_DST(w)));
  ixe_nd_body(w, src, nd, shape, strides, row);
  IXW_PUT(w, mizu_ix_put_dict_begin(IXW_DST(w), 1));
  ixe_key(w, MIZU_IX_ATTR_DIM);
  int64_t dims[32];
  for (int i = 0; i < nd && i < 32; i++) dims[i] = shape[i];
  ixe_intv(w, nd, dims, NULL);
}

/* The temporal dtype units. */
enum { TU_DAY, TU_WEEK, TU_HOUR, TU_MIN, TU_SEC, TU_MS, TU_US, TU_NS,
       TU_CAL, TU_SUB, TU_NONE };

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

static double temporal_scale(int unit) {
  switch (unit) {
  case TU_WEEK: return 7.0;
  case TU_HOUR: return 3600.0;
  case TU_MIN: return 60.0;
  case TU_SEC: return 1.0;
  case TU_MS: return 1e-3;
  case TU_US: return 1e-6;
  case TU_NS: return 1e-9;
  }
  return 1.0;
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
      unit == TU_WEEK ? 604800.0 : temporal_scale(unit);
    PyObject *view = PyObject_CallMethod(obj, "view", "s", "int64");
    if (view == NULL) {
      PyErr_Clear();
      view = PyObject_CallMethod(obj, "astype", "s", "int64");
      if (view == NULL) {
        PyErr_Clear();
        ixw_decline_type(w, obj, "a timedelta64 value that cannot cross");
        return;
      }
    }
    int64_t one = 0;
    const int64_t *counts = &one;
    uint64_t n = 1;
    Py_buffer bv;
    int got = PyObject_GetBuffer(view, &bv, PyBUF_ND | PyBUF_FORMAT) == 0;
    if (got && bv.ndim > 1) {
      PyBuffer_Release(&bv);
      Py_DECREF(view);
      ixw_decline(w, "a timedelta64 array past 1-D has no portable home");
      return;
    }
    if (got) {
      counts = (const int64_t *) bv.buf;
      n = (uint64_t) (bv.len / 8);
    } else {
      PyErr_Clear();
      PyObject *i = PyNumber_Index(view);
      if (i == NULL) {
        PyErr_Clear();
        Py_DECREF(view);
        ixw_decline_type(w, obj, "a timedelta64 value that cannot cross");
        return;
      }
      one = PyLong_AsLongLong(i);
      Py_DECREF(i);
    }
    ixe_difftime(w, counts, n, scale);
    if (got) PyBuffer_Release(&bv);
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
  /* the int64 counts: an ndarray views int64 and exports a buffer; a
     scalar converts through astype */
  PyObject *view = PyObject_CallMethod(obj, "view", "s", "int64");
  if (view == NULL) {
    PyErr_Clear();
    view = PyObject_CallMethod(obj, "astype", "s", "int64");
    if (view == NULL) {
      PyErr_Clear();
      ixw_decline_type(w, obj, "a datetime64 value that cannot cross");
      return;
    }
  }
  int64_t one = 0;
  const int64_t *counts = &one;
  uint64_t n = 1;
  Py_buffer bv;
  int got = PyObject_GetBuffer(view, &bv, PyBUF_ND | PyBUF_FORMAT) == 0;
  if (got && bv.ndim > 1) {
    PyBuffer_Release(&bv);
    Py_DECREF(view);
    ixw_decline(w, "a datetime64 array past 1-D has no portable home");
    return;
  }
  if (got) {
    counts = (const int64_t *) bv.buf;
    n = (uint64_t) (bv.len / 8);
  } else {
    PyErr_Clear();
    PyObject *i = PyNumber_Index(view);
    if (i == NULL) {
      PyErr_Clear();
      Py_DECREF(view);
      ixw_decline_type(w, obj, "a datetime64 value that cannot cross");
      return;
    }
    one = PyLong_AsLongLong(i);
    Py_DECREF(i);
  }
  if (unit == TU_DAY || unit == TU_WEEK) {
    ixe_date(w, counts, n, unit == TU_WEEK ? 7.0 : 1.0);
  } else {
    ixe_posixct(w, counts, n, "UTC", temporal_scale(unit));
  }
  if (got) PyBuffer_Release(&bv);
  Py_DECREF(view);
}

/* The stage gate's temporal probe: 1 datetime64, 2 timedelta64, 0 not a
   temporal numpy object. A temporal numpy scalar exports its bytes as
   uint8 (the buffer protocol has no datetime), which the raw tier would
   otherwise claim as a byte vector — the gate skips the raw tier for
   these so the 'I' writer (or pickle) sees the value. */
int mizu_py_np_temporal(PyObject *obj) {
  const int nk = mizu_py_np_kind(obj);
  if (nk == 0) return 0;
  PyObject *dt = PyObject_GetAttrString(obj, "dtype");
  PyObject *ds = dt != NULL ? PyObject_GetAttrString(dt, "str") : NULL;
  int out = 0;
  if (ds != NULL && PyUnicode_Check(ds)) {
    const char *s = PyUnicode_AsUTF8(ds);
    if (s != NULL) {
      const char *p = s;
      if (*p == '=' || *p == '<' || *p == '>' || *p == '|') p++;
      if (p[0] == 'M' && p[1] == '8' && p[2] == '[') out = 1;
      else if (p[0] == 'm' && p[1] == '8' && p[2] == '[') out = 2;
    }
  }
  Py_XDECREF(ds);
  Py_XDECREF(dt);
  if (PyErr_Occurred()) PyErr_Clear();
  return out;
}

/* The buffer-protocol leaf (and numpy scalar) walk. */
static void ixw_buffer(ixw *w, PyObject *obj) {
  if (mizu_py_buffer_subclass_reject(obj)) {
    ixw_decline_type(w, obj, "a buffer subclass (e.g. MaskedArray, "
                     "memmap) has no portable home");
    return;
  }
  /* temporal dtypes refuse the buffer protocol: probe .dtype.str first */
  int npk = mizu_py_np_kind(obj);
  if (npk != 0) {
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
            int unit = temporal_unit(p + 3, (size_t) (close - (p + 3)));
            /* p points into ds's UTF-8 buffer — read the kind out before
               the decrefs free it */
            int delta = p[0] == 'm';
            Py_DECREF(ds);
            Py_DECREF(dt);
            ixw_numpy_temporal(w, obj, unit, delta);
            return;
          }
        }
      }
    }
    Py_XDECREF(ds);
    Py_XDECREF(dt);
    if (PyErr_Occurred()) PyErr_Clear();
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
  if (row->cvt == CVT_U64_REAL && w->depth > 0) {
    PyBuffer_Release(&v);
    ixw_decline(w, "a uint64 array nested in a container has no "
                "portable home (its conversion past 2^53 is lossy)");
    return;
  }
  if (strided && w->depth > 0) {
    PyBuffer_Release(&v);
    ixw_decline(w, "a strided array nested in a container has no "
                "portable home");
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
  int ok = 1;
  int64_t y = attr_as_long(obj, "year", &ok);
  int64_t mo = attr_as_long(obj, "month", &ok);
  int64_t d = attr_as_long(obj, "day", &ok);
  int64_t hh = attr_as_long(obj, "hour", &ok);
  int64_t mi = attr_as_long(obj, "minute", &ok);
  int64_t ss = attr_as_long(obj, "second", &ok);
  int64_t us = attr_as_long(obj, "microsecond", &ok);
  if (!ok) {
    PyErr_Clear();
    ixw_decline_type(w, obj, "a datetime value that cannot cross");
    return;
  }
  int64_t days = days_from_civil(y, mo, d);
  int64_t instant = ((days * 86400 + hh * 3600 + mi * 60 + ss) * 1000000) +
    us;
  char tz[64] = "UTC";
  PyObject *tzi = PyObject_GetAttrString(obj, "tzinfo");
  if (tzi == NULL) {
    PyErr_Clear();
  } else if (tzi != Py_None) {
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
  ixe_posixct(w, &instant, 1, tz, 1e-6);
}

static void ixw_stdlib_date(ixw *w, PyObject *obj) {
  int ok = 1;
  int64_t y = attr_as_long(obj, "year", &ok);
  int64_t mo = attr_as_long(obj, "month", &ok);
  int64_t d = attr_as_long(obj, "day", &ok);
  if (!ok) {
    PyErr_Clear();
    ixw_decline_type(w, obj, "a date value that cannot cross");
    return;
  }
  int64_t days = days_from_civil(y, mo, d);
  ixe_date(w, &days, 1, 1.0);
}

/* A datetime.timedelta scalar: the normalized (days, seconds,
   microseconds) triple to int64 us, emitted as one difftime second. */
static void ixw_stdlib_timedelta(ixw *w, PyObject *obj) {
  int ok = 1;
  int64_t d = attr_as_long(obj, "days", &ok);
  int64_t s = attr_as_long(obj, "seconds", &ok);
  int64_t us = attr_as_long(obj, "microseconds", &ok);
  if (!ok) {
    PyErr_Clear();
    ixw_decline_type(w, obj, "a timedelta value that cannot cross");
    return;
  }
  if (d > 106000 || d < -106000) {   /* |us| would overflow int64 */
    ixw_decline(w, "a timedelta past the int64 microsecond range has no "
                "portable home");
    return;
  }
  int64_t total = (d * 86400 + s) * 1000000 + us;
  ixe_difftime(w, &total, 1, 1e-6);
}

/* The plan's SHM_VEC candidate: the frame_buf_write body (one MIZH
   layout write into the stream's single spill checkout, the
   producer-loan retain), then the ref leaf with the fresh region's name
   (F1's D1: the one tag serves both by-reference cases). The size pass
   counts the conservative reservation — the fresh region's name length
   is known only at the checkout. A checkout failure (a churn race)
   abandons: nothing is retained yet, the caller re-runs with no_zc = 1. */
static void ixw_zc_leaf(ixw *w, PyObject *obj) {
  if (w->dst == NULL) {
    w->total += 2 + (MIZU_NAME_MAX - 1);
    return;
  }
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
    if (w->plan != NULL && !w->plan_suppress && !w->churn &&
        w->plan->ncand < 16 && PyObject_CheckBuffer(obj)) {
      Py_buffer v;
      if (PyObject_GetBuffer(obj, &v, PyBUF_ND | PyBUF_FORMAT) == 0) {
        const size_t zc_gate = (size_t) w->inline_max > MIZU_ZC_FLOOR ?
          (size_t) w->inline_max : MIZU_ZC_FLOOR;
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
    ixe_frame(w, f->fc, f->row_names);
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
   Arrow helpers (arrow_valid, arrow_str_at) are pyshmframe.h's — shared
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
  if (x->lev_offs != NULL) {
    for (int i = 0; i < x->ncols; i++) {
      free(x->lev_offs[i]);
      free(x->lev_bytes[i]);
    }
  }
  free(x->lev_offs);
  free(x->lev_bytes);
  free(x->nlevs);
  free(x->lev_blens);
  free(x->cols);
  Py_XDECREF(x->cap);
  memset(x, 0, sizeof(*x));
}

/* A decline from the front-end: DeclinedError with the cause named. */
static void ixs_decline(const char *what) {
  PyErr_Format(MizuDeclinedError,
               "pymizu: value is not portable to the peer (%s)", what);
}

/* The dictionary levels of batch 0 (the canonical set later batches must
   match): read into offsets + bytes. */
static int ixs_levels_read(ixs *x, int col, int32_t **off_out,
                           uint8_t **bytes_out, int64_t *nlev_out,
                           int64_t *blen_out) {
  const ArrowArray *first = x->single ? &x->hold.arrs[0] :
    x->hold.arrs[0].children[col];
  const ArrowArray *dict = first->dictionary;
  if (dict == NULL || dict->n_buffers < 3) {
    PyErr_SetString(MizuError, "pymizu: invalid Arrow dictionary export");
    return -1;
  }
  int form = x->cols[col].str_form;
  int64_t nlev = dict->length;
  int32_t *offs = malloc(((size_t) nlev + 1) * 4);
  uint8_t *bytes = NULL;
  size_t blen = 0;
  if (offs == NULL) goto nomem;
  offs[0] = 0;
  for (int64_t i = 0; i < nlev; i++) {
    const uint8_t *s;
    int32_t len;
    if (!arrow_valid(dict, i)) {
      /* a null level: decline (R factor levels are non-NA) */
      free(offs);
      free(bytes);
      PyErr_SetString(MizuError, "pymizu: a dictionary column with a "
                      "null level has no portable home");
      return -1;
    }
    if (arrow_str_at(dict, i, form, &s, &len) != 1) {
      free(offs);
      free(bytes);
      PyErr_SetString(MizuError, "pymizu: invalid Arrow dictionary export");
      return -1;
    }
    if (blen + (size_t) len > INT32_MAX) {
      free(offs);
      free(bytes);
      PyErr_SetString(MizuError, "pymizu: dictionary levels past the "
                      "int32 offset range");
      return -1;
    }
    uint8_t *nb = realloc(bytes, blen + (size_t) len + 1);
    if (nb == NULL) goto nomem;
    bytes = nb;
    memcpy(bytes + blen, s, (size_t) len);
    blen += (size_t) len;
    offs[i + 1] = (int32_t) blen;
  }
  *off_out = offs;
  *bytes_out = bytes;
  *nlev_out = nlev;
  *blen_out = (int64_t) blen;
  return 0;
nomem:
  free(offs);
  free(bytes);
  PyErr_NoMemory();
  return -1;
}

/* A later batch's dictionary must equal the canonical set. */
static int ixs_levels_match(ixs *x, int col, const ArrowArray *dict,
                            const int32_t *offs, const uint8_t *bytes,
                            int64_t nlev) {
  if (dict == NULL || dict->length != nlev) return 0;
  int form = x->cols[col].str_form;
  for (int64_t i = 0; i < nlev; i++) {
    const uint8_t *s;
    int32_t len;
    if (arrow_str_at(dict, i, form, &s, &len) != 1) return 0;
    if (offs[i + 1] - offs[i] != len ||
        memcmp(bytes + offs[i], s, (size_t) len) != 0)
      return 0;
  }
  return 1;
}

/* The column emitters: two passes over the held batches (dst NULL sizes).
   All return the byte count; the write pass cannot fail (validation ran
   in the size pass). */

typedef struct {
  uint8_t *dst;
  size_t total;
} ixe;

#define IXE_PUT(x, call) do { (x)->total += call; } while (0)
#define IXE_DST(x) ((x)->dst != NULL ? (x)->dst + (x)->total : NULL)

static void ixs_emit_cvt_col(ixe *e, ixs *x, int col) {
  ixs_pcol *pc = &x->cols[col];
  IXE_PUT(e, ixe_vec_begin(IXE_DST(e), mizu_ix_tag_of(pc->row->wire),
                           (uint64_t) x->hold.rows));
  if (e->dst != NULL) {
    uint8_t *dst = e->dst + e->total;
    size_t out_off = 0;
    cvt_warn warn = { 0, 0 };
    for (size_t b = 0; b < x->hold.nb; b++) {
      ArrowArray *a = x->single ? &x->hold.arrs[b] :
        x->hold.arrs[b].children[col];
      const uint8_t *data = (const uint8_t *) a->buffers[1];
      const uint8_t *valid = (const uint8_t *) a->buffers[0];
      uint64_t off = (uint64_t) a->offset;
      size_t n = (size_t) a->length;
      if (pc->row->cvt == CVT_BIT_LGL) {
        mizu_py_cvt_convert(dst + out_off, data, valid, off, n, pc->row,
                            &warn);
      } else {
        mizu_py_cvt_convert(dst + out_off, data + off * pc->row->w_in,
                            valid, off, n, pc->row, &warn);
      }
      out_off += n * pc->row->w_out;
    }
  }
  e->total += (size_t) x->hold.rows * pc->row->w_out;
}

static void ixs_emit_str_col(ixe *e, ixs *x, int col) {
  ixs_pcol *pc = &x->cols[col];
  IXE_PUT(e, mizu_ix_put_strv_begin(IXE_DST(e), (uint64_t) x->hold.rows));
  for (size_t b = 0; b < x->hold.nb; b++) {
    ArrowArray *a = x->single ? &x->hold.arrs[b] :
      x->hold.arrs[b].children[col];
    for (int64_t k = 0; k < a->length; k++) {
      const uint8_t *s;
      int32_t len;
      /* a malformed record (a broken producer) degrades to NA like a
         null — both passes see the same verdict, so the stream stays
         consistent; it must never reach the put with a garbage span */
      if (arrow_str_at(a, k, pc->str_form, &s, &len) != 1) {
        IXE_PUT(e, mizu_ix_put_strelt(IXE_DST(e), NULL, -1));
      } else {
        IXE_PUT(e, mizu_ix_put_strelt(IXE_DST(e), s, len));
      }
    }
  }
}

static void ixs_emit_date_col(ixe *e, ixs *x, int col) {
  static const char *cls[1] = { MIZU_IX_CLASS_DATE };
  IXE_PUT(e, mizu_ix_put_attr(IXE_DST(e)));
  IXE_PUT(e, ixe_vec_begin(IXE_DST(e), MIZU_IX_TAG_REALV,
                           (uint64_t) x->hold.rows));
  if (e->dst != NULL) {
    uint8_t *dst = e->dst + e->total;
    size_t off = 0;
    for (size_t b = 0; b < x->hold.nb; b++) {
      ArrowArray *a = x->single ? &x->hold.arrs[b] :
        x->hold.arrs[b].children[col];
      const int32_t *days = (const int32_t *) a->buffers[1];
      for (int64_t k = 0; k < a->length; k++) {
        if (!arrow_valid(a, k)) {
          store_na_r(dst + 8 * (off + (size_t) k));
        } else {
          double v = (double) days[a->offset + k];
          memcpy(dst + 8 * (off + (size_t) k), &v, 8);
        }
      }
      off += (size_t) a->length;
    }
  }
  e->total += (size_t) x->hold.rows * 8;
  IXE_PUT(e, mizu_ix_put_dict_begin(IXE_DST(e), 1));
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_CLASS, IX_KEYLEN(MIZU_IX_ATTR_CLASS)));
  {
    ixw w2;
    memset(&w2, 0, sizeof(w2));
    w2.dst = IXE_DST(e);
    w2.limit = SIZE_MAX;   /* the frame emitter's buffer: unbounded */
    ixe_class(&w2, cls, 1);
    e->total += w2.total;
  }
}

static void ixs_emit_ts_col(ixe *e, ixs *x, int col) {
  ixs_pcol *pc = &x->cols[col];
  IXE_PUT(e, mizu_ix_put_attr(IXE_DST(e)));
  IXE_PUT(e, ixe_vec_begin(IXE_DST(e), MIZU_IX_TAG_REALV,
                           (uint64_t) x->hold.rows));
  if (e->dst != NULL) {
    uint8_t *dst = e->dst + e->total;
    size_t off = 0;
    for (size_t b = 0; b < x->hold.nb; b++) {
      ArrowArray *a = x->single ? &x->hold.arrs[b] :
        x->hold.arrs[b].children[col];
      const int64_t *counts = (const int64_t *) a->buffers[1];
      for (int64_t k = 0; k < a->length; k++) {
        if (!arrow_valid(a, k)) {
          store_na_r(dst + 8 * (off + (size_t) k));
        } else {
          double v = (double) counts[a->offset + k] * pc->ts_scale;
          memcpy(dst + 8 * (off + (size_t) k), &v, 8);
        }
      }
      off += (size_t) a->length;
    }
  }
  e->total += (size_t) x->hold.rows * 8;
  IXE_PUT(e, mizu_ix_put_dict_begin(IXE_DST(e), 2));
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_CLASS, IX_KEYLEN(MIZU_IX_ATTR_CLASS)));
  {
    static const char *cls[2] = { MIZU_IX_CLASS_POSIXCT, MIZU_IX_CLASS_POSIXT };
    ixw w2;
    memset(&w2, 0, sizeof(w2));
    w2.dst = IXE_DST(e);
    w2.limit = SIZE_MAX;   /* the frame emitter's buffer: unbounded */
    ixe_class(&w2, cls, 2);
    e->total += w2.total;
  }
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_TZONE, IX_KEYLEN(MIZU_IX_ATTR_TZONE)));
  {
    const char *tz = pc->tz;
    ixw w2;
    memset(&w2, 0, sizeof(w2));
    w2.dst = IXE_DST(e);
    w2.limit = SIZE_MAX;   /* the frame emitter's buffer: unbounded */
    const char *strs[1] = { tz };
    int64_t lens[1] = { (int64_t) strlen(tz) };
    ixe_strv(&w2, 1, strs, lens);
    e->total += w2.total;
  }
}

/* A duration column (Arrow duration[unit], the scale in seconds) to the
   difftime shape: realv seconds + the {class, units} dict. */
static void ixs_emit_td_col(ixe *e, ixs *x, int col) {
  ixs_pcol *pc = &x->cols[col];
  IXE_PUT(e, mizu_ix_put_attr(IXE_DST(e)));
  IXE_PUT(e, ixe_vec_begin(IXE_DST(e), MIZU_IX_TAG_REALV,
                           (uint64_t) x->hold.rows));
  if (e->dst != NULL) {
    uint8_t *dst = e->dst + e->total;
    size_t off = 0;
    for (size_t b = 0; b < x->hold.nb; b++) {
      ArrowArray *a = x->single ? &x->hold.arrs[b] :
        x->hold.arrs[b].children[col];
      const int64_t *counts = (const int64_t *) a->buffers[1];
      for (int64_t k = 0; k < a->length; k++) {
        if (!arrow_valid(a, k)) {
          store_na_r(dst + 8 * (off + (size_t) k));
        } else {
          double v = (double) counts[a->offset + k] * pc->ts_scale;
          memcpy(dst + 8 * (off + (size_t) k), &v, 8);
        }
      }
      off += (size_t) a->length;
    }
  }
  e->total += (size_t) x->hold.rows * 8;
  IXE_PUT(e, mizu_ix_put_dict_begin(IXE_DST(e), 2));
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_CLASS, IX_KEYLEN(MIZU_IX_ATTR_CLASS)));
  {
    static const char *cls[1] = { MIZU_IX_CLASS_DIFFTIME };
    ixw w2;
    memset(&w2, 0, sizeof(w2));
    w2.dst = IXE_DST(e);
    w2.limit = SIZE_MAX;   /* the frame emitter's buffer: unbounded */
    ixe_class(&w2, cls, 1);
    e->total += w2.total;
  }
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_UNITS, IX_KEYLEN(MIZU_IX_ATTR_UNITS)));
  {
    ixw w2;
    memset(&w2, 0, sizeof(w2));
    w2.dst = IXE_DST(e);
    w2.limit = SIZE_MAX;   /* the frame emitter's buffer: unbounded */
    const char *strs[1] = { MIZU_IX_UNIT_SECS };
    int64_t lens[1] = { 4 };
    ixe_strv(&w2, 1, strs, lens);
    e->total += w2.total;
  }
}

static void ixs_emit_dict_col(ixe *e, ixs *x, int col, const int32_t *offs,
                              const uint8_t *bytes, int64_t nlev) {
  ixs_pcol *pc = &x->cols[col];
  IXE_PUT(e, mizu_ix_put_attr(IXE_DST(e)));
  IXE_PUT(e, ixe_vec_begin(IXE_DST(e), MIZU_IX_TAG_INTV,
                           (uint64_t) x->hold.rows));
  if (e->dst != NULL) {
    uint8_t *dst = e->dst + e->total;
    size_t off = 0;
    for (size_t b = 0; b < x->hold.nb; b++) {
      ArrowArray *a = x->single ? &x->hold.arrs[b] :
        x->hold.arrs[b].children[col];
      const uint8_t *idx = (const uint8_t *) a->buffers[1];
      for (int64_t k = 0; k < a->length; k++) {
        int32_t v;
        if (!arrow_valid(a, k)) {
          v = MIZU_NA_INT32;
        } else {
          int64_t i = a->offset + k;
          int64_t c;
          switch (pc->idx_w) {
          case 1:
            c = pc->idx_signed ? (int64_t) ((const int8_t *) idx)[i] :
              (int64_t) ((const uint8_t *) idx)[i];
            break;
          case 2:
            c = pc->idx_signed ? (int64_t) ((const int16_t *) idx)[i] :
              (int64_t) ((const uint16_t *) idx)[i];
            break;
          case 4:
            c = pc->idx_signed ? (int64_t) ((const int32_t *) idx)[i] :
              (int64_t) ((const uint32_t *) idx)[i];
            break;
          default:
            c = ((const int64_t *) idx)[i];
            break;
          }
          v = (int32_t) (c + 1);
        }
        memcpy(dst + 4 * (off + (size_t) k), &v, 4);
      }
      off += (size_t) a->length;
    }
  }
  e->total += (size_t) x->hold.rows * 4;
  IXE_PUT(e, mizu_ix_put_dict_begin(IXE_DST(e), 2));
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_LEVELS, IX_KEYLEN(MIZU_IX_ATTR_LEVELS)));
  if (nlev == 1) {
    IXE_PUT(e, mizu_ix_put_str(IXE_DST(e), bytes, offs[1] - offs[0]));
  } else {
    IXE_PUT(e, mizu_ix_put_strv_begin(IXE_DST(e), (uint64_t) nlev));
    for (int64_t i = 0; i < nlev; i++)
      IXE_PUT(e, mizu_ix_put_strelt(IXE_DST(e), bytes + offs[i],
                                    offs[i + 1] - offs[i]));
  }
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_CLASS, IX_KEYLEN(MIZU_IX_ATTR_CLASS)));
  {
    static const char *cls[1] = { MIZU_IX_CLASS_FACTOR };
    ixw w2;
    memset(&w2, 0, sizeof(w2));
    w2.dst = IXE_DST(e);
    w2.limit = SIZE_MAX;   /* the frame emitter's buffer: unbounded */
    ixe_class(&w2, cls, 1);
    e->total += w2.total;
  }
}

/* The frame shape over the held batches. */
static void ixs_emit_frame(ixe *e, ixs *x, char **names) {
  IXE_PUT(e, mizu_ix_put_attr(IXE_DST(e)));
  IXE_PUT(e, mizu_ix_put_list_begin(IXE_DST(e), (uint64_t) x->ncols));
  for (int i = 0; i < x->ncols; i++) {
    ixs_pcol *pc = &x->cols[i];
    switch (pc->kind) {
    case PC_CVT: ixs_emit_cvt_col(e, x, i); break;
    case PC_STR: ixs_emit_str_col(e, x, i); break;
    case PC_DICT:
      ixs_emit_dict_col(e, x, i, x->lev_offs[i], x->lev_bytes[i],
                        x->nlevs[i]);
      break;
    case PC_DATE: ixs_emit_date_col(e, x, i); break;
    case PC_TS: ixs_emit_ts_col(e, x, i); break;
    case PC_TD: ixs_emit_td_col(e, x, i); break;
    }
  }
  IXE_PUT(e, mizu_ix_put_dict_begin(IXE_DST(e), 3));
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_NAMES, IX_KEYLEN(MIZU_IX_ATTR_NAMES)));
  if (x->ncols == 1) {
    IXE_PUT(e, mizu_ix_put_str(IXE_DST(e), names[0],
                               (int32_t) strlen(names[0])));
  } else {
    IXE_PUT(e, mizu_ix_put_strv_begin(IXE_DST(e), (uint64_t) x->ncols));
    for (int i = 0; i < x->ncols; i++)
      IXE_PUT(e, mizu_ix_put_strelt(IXE_DST(e), names[i],
                                    (int32_t) strlen(names[i])));
  }
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_CLASS, IX_KEYLEN(MIZU_IX_ATTR_CLASS)));
  {
    static const char *cls[1] = { MIZU_IX_CLASS_DATAFRAME };
    ixw w2;
    memset(&w2, 0, sizeof(w2));
    w2.dst = IXE_DST(e);
    w2.limit = SIZE_MAX;   /* the frame emitter's buffer: unbounded */
    ixe_class(&w2, cls, 1);
    e->total += w2.total;
  }
  IXE_PUT(e, mizu_ix_put_key(IXE_DST(e), MIZU_IX_ATTR_ROWNAMES, IX_KEYLEN(MIZU_IX_ATTR_ROWNAMES)));
  {
    ixw w2;
    memset(&w2, 0, sizeof(w2));
    w2.dst = IXE_DST(e);
    w2.limit = SIZE_MAX;   /* the frame emitter's buffer: unbounded */
    int64_t vals[2] = { 0, -x->hold.rows };
    int na[2] = { 1, 0 };
    ixe_intv(&w2, 2, vals, na);
    e->total += w2.total;
  }
}


// The front-end driver -------------------------------------------------------------

/* Classify one schema (a frame's child, or the single non-struct column)
   into a plan column. top is set for the single column (uint64's lossy
   row is top-level-only). */
static int ixs_classify(ixs *x, int col, const ArrowSchema *sc, int top) {
  ixs_pcol *pc = &x->cols[col];
  memset(pc, 0, sizeof(*pc));
  const char *f = sc->format;
  if (f == NULL) {
    ixs_decline("an Arrow column with no format");
    return -1;
  }
  if (sc->dictionary != NULL) {
    if (sc->flags & PYMIZU_ARROW_FLAG_DICTIONARY_ORDERED) {
      ixs_decline("an ordered dictionary column has no portable home "
                  "(polars Enum: cast to pl.Categorical)");
      return -1;
    }
    const char *vf = sc->dictionary->format;
    if (vf == NULL ||
        (strcmp(vf, "u") != 0 && strcmp(vf, "U") != 0 &&
         strcmp(vf, "vu") != 0)) {
      ixs_decline("a dictionary column of non-string values has no "
                  "portable home");
      return -1;
    }
    pc->str_form = vf[0] == 'v' ? 3 : vf[0] == 'U' ? 2 : 1;
    if (f[1] != '\0') goto bad_index;
    switch (f[0]) {
    case 'c': pc->idx_w = 1; pc->idx_signed = 1; break;
    case 'C': pc->idx_w = 1; break;
    case 's': pc->idx_w = 2; pc->idx_signed = 1; break;
    case 'S': pc->idx_w = 2; break;
    case 'i': pc->idx_w = 4; pc->idx_signed = 1; break;
    case 'I': pc->idx_w = 4; break;
    case 'l': pc->idx_w = 8; pc->idx_signed = 1; break;
    case 'L': pc->idx_w = 8; break;
    default:
    bad_index:
      ixs_decline("a dictionary column with a non-integer index has no "
                  "portable home");
      return -1;
    }
    pc->kind = PC_DICT;
    return 0;
  }
  if (strcmp(f, "u") == 0) {
    pc->kind = PC_STR;
    pc->str_form = 1;
    return 0;
  }
  if (strcmp(f, "U") == 0) {
    pc->kind = PC_STR;
    pc->str_form = 2;
    return 0;
  }
  if (strcmp(f, "vu") == 0) {
    pc->kind = PC_STR;
    pc->str_form = 3;
    return 0;
  }
  if (strcmp(f, "tdD") == 0) {
    pc->kind = PC_DATE;
    return 0;
  }
  if (f[0] == 't' && f[1] == 's' && f[3] == ':' &&
      (f[2] == 's' || f[2] == 'm' || f[2] == 'u' || f[2] == 'n')) {
    pc->ts_scale = f[2] == 's' ? 1.0 : f[2] == 'm' ? 1e-3 :
      f[2] == 'u' ? 1e-6 : 1e-9;
    const char *tz = f + 4;
    snprintf(pc->tz, sizeof(pc->tz), "%s", tz[0] != '\0' ? tz : "UTC");
    pc->kind = PC_TS;
    return 0;
  }
  if (f[0] == 't' && f[1] == 'D' && f[3] == '\0' &&
      (f[2] == 's' || f[2] == 'm' || f[2] == 'u' || f[2] == 'n')) {
    pc->ts_scale = f[2] == 's' ? 1.0 : f[2] == 'm' ? 1e-3 :
      f[2] == 'u' ? 1e-6 : 1e-9;
    pc->kind = PC_TD;
    return 0;
  }
  const cvt_row *row = mizu_py_cvt_for_arrow(f);
  if (row != NULL) {
    if (row->cvt == CVT_U64_REAL && !top) {
      ixs_decline("a uint64 column has no portable home "
                  "(its conversion past 2^53 is lossy)");
      return -1;
    }
    pc->kind = PC_CVT;
    pc->row = row;
    return 0;
  }
  {
    char msg[160];
    snprintf(msg, sizeof(msg), "an Arrow type with no portable home "
             "(format '%s')", f);
    ixs_decline(msg);
  }
  return -1;
}

/* One batch's shape checks. */
static int ixs_check_batch(ixs *x, const ArrowArray *a) {
  for (int i = 0; i < x->ncols; i++) {
    const ArrowArray *c = x->single ? a : a->children[i];
    ixs_pcol *pc = &x->cols[i];
    if (!x->single && c->length != a->length) {
      ixs_decline("an Arrow batch whose columns differ in length");
      return -1;
    }
    int64_t need = pc->kind == PC_STR ? (pc->str_form == 3 ? 2 : 3) : 2;
    if (c->n_buffers < need || c->buffers == NULL || c->length < 0 ||
        c->offset < 0 || c->null_count < -1 ||
        c->null_count > c->length ||
        (c->buffers[0] == NULL && c->null_count > 0) ||
        (c->buffers[1] == NULL && c->length != 0)) {
      ixs_decline("an invalid Arrow C Data Interface batch");
      return -1;
    }
    if (pc->kind == PC_STR && pc->str_form != 3 &&
        c->buffers[2] == NULL && c->length != 0) {
      ixs_decline("an invalid Arrow C Data Interface batch");
      return -1;
    }
    if (pc->kind == PC_DICT && c->dictionary == NULL) {
      ixs_decline("an invalid Arrow C Data Interface batch");
      return -1;
    }
    /* R raw vectors have no NA: the masked convert would write a 4-byte
       sentinel into a 1-byte column (the array front-end declines there
       too) */
    if (pc->kind == PC_CVT && pc->row->wire == MIZU_TYPE_RAW &&
        c->buffers[0] != NULL && c->null_count != 0) {
      ixs_decline("Arrow nulls cannot cross in a uint8 column "
                  "(R raw vectors have no NA)");
      return -1;
    }
  }
  return 0;
}

/* Pull every batch (the size pass holds them; the write pass reads from
   the hold — the capsule is single-consumption). */
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
    if (!x->single &&
        (a->n_children != x->ncols || a->children == NULL)) {
      ixs_decline("an Arrow batch whose column count differs from the "
                  "schema");
      return -1;
    }
    if (ixs_check_batch(x, a) < 0) return -1;
    x->hold.nb++;
    x->hold.rows += a->length;
  }
  return 0;
}

/* The dictionary columns' canonical levels (batch 0) and index bounds,
   validated before any emit. */
static int ixs_validate_dicts(ixs *x) {
  x->lev_offs = calloc((size_t) x->ncols, sizeof(int32_t *));
  x->lev_bytes = calloc((size_t) x->ncols, sizeof(uint8_t *));
  x->nlevs = calloc((size_t) x->ncols, sizeof(int64_t));
  x->lev_blens = calloc((size_t) x->ncols, sizeof(int64_t));
  if (x->lev_offs == NULL || x->lev_bytes == NULL || x->nlevs == NULL ||
      x->lev_blens == NULL) {
    PyErr_NoMemory();
    return -1;
  }
  for (int i = 0; i < x->ncols; i++) {
    ixs_pcol *pc = &x->cols[i];
    if (pc->kind != PC_DICT) continue;
    if (ixs_levels_read(x, i, &x->lev_offs[i], &x->lev_bytes[i],
                        &x->nlevs[i], &x->lev_blens[i]) < 0)
      return -1;
    for (size_t b = 0; b < x->hold.nb; b++) {
      ArrowArray *a = x->single ? &x->hold.arrs[b] :
        x->hold.arrs[b].children[i];
      if (b != 0 && !ixs_levels_match(x, i, a->dictionary,
                                      x->lev_offs[i], x->lev_bytes[i],
                                      x->nlevs[i])) {
        ixs_decline("a dictionary column whose levels differ across "
                    "batches");
        return -1;
      }
      const uint8_t *idx = (const uint8_t *) a->buffers[1];
      for (int64_t k = 0; k < a->length; k++) {
        if (!arrow_valid(a, k)) continue;
        int64_t j = a->offset + k, c;
        switch (pc->idx_w) {
        case 1:
          c = pc->idx_signed ? (int64_t) ((const int8_t *) idx)[j] :
            (int64_t) ((const uint8_t *) idx)[j];
          break;
        case 2:
          c = pc->idx_signed ? (int64_t) ((const int16_t *) idx)[j] :
            (int64_t) ((const uint16_t *) idx)[j];
          break;
        case 4:
          c = pc->idx_signed ? (int64_t) ((const int32_t *) idx)[j] :
            (int64_t) ((const uint32_t *) idx)[j];
          break;
        default:
          c = ((const int64_t *) idx)[j];
          break;
        }
        if (c < 0 || c >= x->nlevs[i]) {
          ixs_decline("a dictionary index outside the levels");
          return -1;
        }
      }
    }
  }
  return 0;
}

/* Stage a finished stream: inline within the budget, else the bytes
   tiers. emit runs twice (size, then write). */
static int ixs_stage_emit(ixs *x, mizu_slot_hdr *hdr, uint8_t *payload,
                          uint32_t inline_max, mizu_handle *h,
                          void (*emit)(ixe *, ixs *)) {
  ixe e = { NULL, 0 };
  e.total += mizu_ix_put_header(NULL);
  emit(&e, x);
  size_t n = e.total;
  ixe w = { NULL, 0 };
  if (n <= (size_t) inline_max) {
    w.dst = payload;
    w.total += mizu_ix_put_header(payload);
    emit(&w, x);
    hdr->kind = MIZU_KIND_INLINE;
    hdr->len = (uint32_t) n;
    hdr->aux = MIZU_AUX_F_KEEPERLESS;
    return 0;
  }
  uint8_t *buf = malloc(n);
  if (buf == NULL) {
    PyErr_NoMemory();
    return 1;
  }
  w.dst = buf;
  w.total += mizu_ix_put_header(buf);
  emit(&w, x);
  int rc = mizu_py_stage_bytes(buf, n, hdr, payload, inline_max, h);
  free(buf);
  return rc;
}

static char **ixs_names(ixs *x) {
  char **names = calloc((size_t) x->ncols, sizeof(char *));
  if (names == NULL) {
    PyErr_NoMemory();
    return NULL;
  }
  for (int i = 0; i < x->ncols; i++) {
    const char *nm = x->schema.children[i]->name;
    if (nm == NULL) {
      ixs_decline("an unnamed Arrow column has no portable home "
                  "(an R data.frame needs names)");
      goto fail;
    }
    for (int j = 0; j < i; j++)
      if (strcmp(names[j], nm) == 0) {
        ixs_decline("an Arrow frame with duplicate column names");
        goto fail;
      }
    names[i] = (char *) nm;
  }
  return names;
fail:
  free(names);
  return NULL;
}

static void ixs_emit_frame(ixe *e, ixs *x, char **names);

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
  char **names = ixs_names(x);
  if (names == NULL) return 1;
  int rc = 1;
  for (int i = 0; i < x->ncols; i++)
    if (ixs_classify(x, i, x->schema.children[i], 0) < 0) goto done;
  if (ixs_pull(x) < 0 || ixs_validate_dicts(x) < 0) goto done;
  {
    ixe e = { NULL, 0 };
    e.total += mizu_ix_put_header(NULL);
    ixs_emit_frame(&e, x, names);
    size_t n = e.total;
    ixe w = { NULL, 0 };
    if (n <= (size_t) inline_max) {
      w.dst = payload;
      w.total += mizu_ix_put_header(payload);
      ixs_emit_frame(&w, x, names);
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) n;
      hdr->aux = MIZU_AUX_F_KEEPERLESS;
      rc = 0;
    } else {
      /* the MIZL tier (3.6/3.8): past the zero-copy gate, no churn, and
         the peer passing the frame conjunction — the provenance REF of an
         unmodified round trip first, else the layout write; anything less
         falls to the copy tiers */
      const size_t zc_gate = (size_t) inline_max > MIZU_ZC_FLOOR ?
        (size_t) inline_max : (size_t) MIZU_ZC_FLOOR;
      if (n > zc_gate && !mizu_handle_churn(h)) {
        int any_str = 0;
        for (int i = 0; i < x->ncols; i++)
          any_str |= x->cols[i].kind == PC_STR;
        const uint32_t need = MIZU_CAP_ATTRS | MIZU_CAP_MIZL |
          (any_str ? MIZU_CAP_MIZS : 0u);
        if ((x->caps & need) == need) {
          rc = pymizu_shmframe_pv_match(x, names, hdr, payload,
                                        inline_max);
          if (rc == 0) goto done;
          /* the per-column REF (F2): the whole-frame path behind, any
             matched column goes out as a remote leaf — the conditional
             conjunction: a peer short of the bit gets full layout leaves
             for every column, never a partial-remote tree */
          struct pv_entry const **hits =
            calloc((size_t) x->ncols, sizeof(*hits));
          if (hits == NULL) {
            PyErr_NoMemory();
            goto done;
          }
          int nremote = pymizu_shmframe_pv_match_cols(x, names, hits);
          if (nremote == 0 ||
              (x->caps & (need | MIZU_CAP_MIZL_REF)) !=
                (need | MIZU_CAP_MIZL_REF)) {
            free(hits);
            hits = NULL;
          }
          rc = pymizu_shmframe_write(x, names, hdr, payload, inline_max,
                                     h, 0, hits);
          free(hits);
          if (rc >= 0) goto done;
        }
      }
      uint8_t *buf = malloc(n);
      if (buf == NULL) {
        PyErr_NoMemory();
        goto done;
      }
      w.dst = buf;
      w.total += mizu_ix_put_header(buf);
      ixs_emit_frame(&w, x, names);
      rc = mizu_py_stage_bytes(buf, n, hdr, payload, inline_max, h);
      free(buf);
    }
  }
done:
  free(names);
  return rc;
}

/* The single (Series / ChunkedArray) paths: the conversion table onto the
   bare-bytes tiers, 0x0b for strings, the factor and temporal shapes. */
static void ixs_single_str_emit(ixe *e, ixs *x) {
  ixs_emit_str_col(e, x, 0);
}

static void ixs_single_dict_emit(ixe *e, ixs *x) {
  ixs_emit_dict_col(e, x, 0, x->lev_offs[0], x->lev_bytes[0], x->nlevs[0]);
}

static void ixs_single_date_emit(ixe *e, ixs *x) {
  ixs_emit_date_col(e, x, 0);
}

static void ixs_single_ts_emit(ixe *e, ixs *x) {
  ixs_emit_ts_col(e, x, 0);
}

static void ixs_single_td_emit(ixe *e, ixs *x) {
  ixs_emit_td_col(e, x, 0);
}

static void ixs_single_cvt_emit(ixe *e, ixs *x) {
  ixs_emit_cvt_col(e, x, 0);
}

static int ixs_run_single(ixs *x, mizu_slot_hdr *hdr, uint8_t *payload,
                          uint32_t inline_max, mizu_handle *h) {
  x->ncols = 1;
  x->single = 1;
  x->cols = calloc(1, sizeof(ixs_pcol));
  if (x->cols == NULL) {
    PyErr_NoMemory();
    return 1;
  }
  if (ixs_classify(x, 0, &x->schema, 1) < 0) return 1;
  if (!x->borrowed_hold && ixs_pull(x) < 0) return 1;
  if (ixs_validate_dicts(x) < 0) return 1;
  ixs_pcol *pc = &x->cols[0];
  if (pc->kind == PC_CVT) {
    /* the bare-bytes tiers first: one reservation, converted per batch */
    uint64_t total = (uint64_t) x->hold.rows;
    uint8_t *dst = mizu_stage_raw(h, total * pc->row->w_out, pc->row->wire,
                                  hdr, payload, inline_max);
    if (dst != NULL) {
      size_t off = 0;
      cvt_warn warn = { 0, 0 };
      for (size_t b = 0; b < x->hold.nb; b++) {
        ArrowArray *a = &x->hold.arrs[b];
        const uint8_t *data = (const uint8_t *) a->buffers[1];
        const uint8_t *valid = (const uint8_t *) a->buffers[0];
        uint64_t off2 = (uint64_t) a->offset;
        size_t n = (size_t) a->length;
        if (pc->row->cvt == CVT_BIT_LGL) {
          mizu_py_cvt_convert(dst + off, data, valid, off2, n, pc->row,
                              &warn);
        } else {
          mizu_py_cvt_convert(dst + off, data + off2 * pc->row->w_in,
                              valid, off2, n, pc->row, &warn);
        }
        off += n * pc->row->w_out;
      }
      return mizu_py_cvt_warn(&warn) != 0 ? 1 : 0;
    }
    /* a declined reservation degrades to the 'I' stream */
  }
  void (*emit)(ixe *, ixs *) =
    pc->kind == PC_STR ? ixs_single_str_emit :
    pc->kind == PC_DICT ? ixs_single_dict_emit :
    pc->kind == PC_DATE ? ixs_single_date_emit :
    pc->kind == PC_TD ? ixs_single_td_emit :
    pc->kind == PC_TS ? ixs_single_ts_emit : ixs_single_cvt_emit;
  return ixs_stage_emit(x, hdr, payload, inline_max, h, emit);
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
   path over the capsule pair as one borrowed batch. */
static int ixs_stage_arrow_pair(PyObject *obj, mizu_slot_hdr *hdr,
                                uint8_t *payload, uint32_t inline_max,
                                mizu_handle *h) {
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
                                 mizu_handle *h, uint32_t peer_caps) {
  if (Py_TYPE(obj) == &MizuFrameType) return -1;   /* the writer's case */
  if (!PyObject_HasAttrString(obj, "__arrow_c_stream__"))
    return PyObject_HasAttrString(obj, "__arrow_c_array__") ?
      ixs_stage_arrow_pair(obj, hdr, payload, inline_max, h) : -1;
  PyObject *fn = PyObject_GetAttrString(obj, "__arrow_c_stream__");
  if (fn == NULL) return 1;
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
  int rc = ixs_run(&x, hdr, payload, inline_max, h);
  ixs_free(&x);
  return rc;
}

/* The same-language MIZL branch (Phase 3.6): a Frame past the zero-copy
   floor stages as one MIZL region on same-language handles — the true
   mirror of R's MIZL, container-exact (it reads back as a region-backed
   Frame), while pandas, polars and pyarrow frames keep pickle. Below the
   floor, under churn, or on any decline (a complex column) it falls back
   to pickle: -1 with the error cleared. */
static int ixs_run_frame_mizl(ixs *x, mizu_slot_hdr *hdr, uint8_t *payload,
                              uint32_t inline_max, mizu_handle *h) {
  char **names = NULL;
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
  names = ixs_names(x);
  if (names == NULL) goto out;
  for (int i = 0; i < x->ncols; i++)
    if (ixs_classify(x, i, x->schema.children[i], 0) < 0) goto out;
  if (ixs_pull(x) < 0 || ixs_validate_dicts(x) < 0) goto out;
  {
    ixe e = { NULL, 0 };
    e.total += mizu_ix_put_header(NULL);
    ixs_emit_frame(&e, x, names);
    const size_t zc_gate = (size_t) inline_max > MIZU_ZC_FLOOR ?
      (size_t) inline_max : (size_t) MIZU_ZC_FLOOR;
    if (e.total <= zc_gate || mizu_handle_churn(h)) goto out;
  }
  rc = pymizu_shmframe_write(x, names, hdr, payload, inline_max, h, 1,
                             NULL);
out:
  free(names);
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
                            mizu_handle *h) {
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
    cap, "arrow_array_stream");
  if (st == NULL) {
    Py_DECREF(cap);
    PyErr_Clear();
    return -1;
  }
  ixs x;
  memset(&x, 0, sizeof(x));
  x.st = st;
  x.cap = cap;
  x.row_names = f->row_names;
  int rc = ixs_run_frame_mizl(&x, hdr, payload, inline_max, h);
  ixs_free(&x);
  return rc;
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
    w.caps = (uint32_t) (word >> 32);
    w.inline_max = inline_max;
    w.churn = mizu_handle_churn(h);
    w.refs = 1;
    w.dst = payload;
    w.limit = inline_max;
    w.plan = &plan;
    ixw_task(&w, code_s, code_n, (int) kind, args, kwargs,
             (uint32_t) (word & 0xff), ident);
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
               (uint32_t) (word & 0xff), ident);
      if (!w2.abandon) {
        if (w2.decline) {
          ixw_raise(&w2);
          goto out;
        }
        if (mizu_py_cvt_warn(&w2.warn) != 0) goto out;
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
    /* the by-value paths: the record walk's warn record is the write's
       own (the deterministic walk warns identically) */
    if (mizu_py_cvt_warn(&w.warn) != 0) goto out;
    if (plan.ncand == 0 && total <= (size_t) inline_max) {
      /* one walk: the record walk's write is the stream */
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
       record walk counted the by-value stream — rewrite it inline, or
       stage it into the reservation at the recorded size */
    uint8_t *dst = payload;
    if (total > (size_t) inline_max) {
      dst = mizu_py_stage_reserve(total, hdr, payload, h);
      if (dst == NULL) goto out;
    }
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
             (uint32_t) (word & 0xff), ident);
    if (w2.decline) {
      ixw_raise(&w2);
      goto out;
    }
    if (total <= (size_t) inline_max) {
      hdr->kind = MIZU_KIND_INLINE;
      hdr->len = (uint32_t) w2.total;
      hdr->aux = 0;        /* no keeperless claim: inert on task entries */
    }
    /* past inline the reservation stamped hdr already */
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
  if (mizu_py_cvt_warn(&w2.warn) != 0) {
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
  PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
  if (key.kind != MIZU_IX_STR || !key.key) {
    PyErr_SetString(MizuError, "pymizu: malformed task stream: a dict "
                    "key was expected");
    return NULL;
  }
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
  if (mizu_py_cvt_warn(&w2.warn) != 0) {
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
                        (uint32_t) (word & 0xff), ident);
  if (n <= (size_t) inline_max) {
    ixw_runner(payload, name_s, name_n, (int64_t) gen_field, seed,
               (uint32_t) (word & 0xff), ident);
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
             (uint32_t) (word & 0xff), ident);
  int rc = mizu_py_stage_bytes(buf, n, hdr, payload, inline_max, h);
  free(buf);
  return rc;
}

/* The runner writer as bytes (the test hook's half). */
PyObject *pymizu_ix_write_runner_stream(PyObject *name, int64_t gen_field,
                                        PyObject *seed, uint32_t target,
                                        uint64_t ident) {
  Py_ssize_t name_n;
  const char *name_s = PyUnicode_AsUTF8AndSize(name, &name_n);
  if (name_s == NULL) return NULL;
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
  if (it.task_kind != 2) {
    PyErr_SetString(MizuError, "pymizu: malformed runner stream: not a "
                    "runner task");
    return NULL;
  }
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
  PyObject *msg = PyUnicode_FromFormat(
    "pymizu: value is not portable to the peer (%s at %s)", w->reason,
    w->path);
  if (msg == NULL) return;
  PyObject *exc = PyObject_CallFunctionObjArgs(MizuDeclinedError, msg,
                                               NULL);
  Py_DECREF(msg);
  if (exc == NULL) return;
  PyObject *p = PyUnicode_FromString(w->path);
  PyObject *r = PyUnicode_FromString(w->reason);
  if (p == NULL || r == NULL ||
      PyObject_SetAttrString(exc, "path", p) < 0 ||
      PyObject_SetAttrString(exc, "reason", r) < 0) {
    Py_XDECREF(p);
    Py_XDECREF(r);
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
                    uint32_t inline_max, mizu_handle *h) {
  ixw w;
  memset(&w, 0, sizeof(w));
  memcpy(w.path, "x", 2);
  w.path_len = 1;
  w.dst = payload;
  w.limit = inline_max;
  ixw_stream(&w, obj);
  if (w.decline) {
    ixw_raise(&w);
    return 1;
  }
  const size_t n = w.total;
  if (!w.overflow) {   /* n <= inline_max: the one-walk INLINE frame */
    if (mizu_py_cvt_warn(&w.warn) != 0) return 1;
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
  if (w2.overflow) {
    PyErr_SetString(MizuError, "pymizu: interop write mismatch");
    return 1;
  }
  return mizu_py_cvt_warn(&w2.warn);
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
    PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
    return NULL;
  }
  PyObject *v = ixr_value(&cur, &mode);
  if (v == NULL) return NULL;
  if (mizu_ix_end(&cur) != MIZU_OK) {
    Py_DECREF(v);
    PyErr_Format(MizuError, "pymizu: %s", mizu_last_error_message());
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
  if (mizu_py_cvt_warn(&w2.warn) != 0) {
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

