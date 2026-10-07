/* interop_frame.c — the pymizu.Frame object: the Python-facing half of
 * the 'I' codec's columnar home (the type object, methods, the
 * __arrow_c_stream__ export, the pickle form) plus the frame_cols
 * lifecycle shared with interop.c's wire side (pyframe.h carries the
 * structs). The Arrow export's release callbacks are pure C, callable
 * from any thread — the export holds its own mapping and zc loan, never
 * a Python reference. */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdatomic.h>
#include <string.h>

#include "pyframe.h"
#include "pyshmframe.h"

static PyObject *MizuError;
static PyObject *frame_rebuild_fn;

// The column block's lifecycle (pyframe.h's) -------------------------------------

void fcol_free(fcol *c) {
  if (c->ahold != NULL) {
    /* an adopted column is wholly borrowed — values, validity, bytes and
       lev_off ride the hold's batch release, and the lazy owned buffers
       (bits / codes0 / valid_owned) belong to LGL and region kinds an
       adopted column never takes: nothing to free, just the decref */
    ahold_decref(c->ahold);
    return;
  }
  if (!c->borrowed) {
    free(c->values);
    free(c->valid);
  } else if (c->valid_owned) {
    free(c->valid);   /* a borrowed column's lazily built bitmap */
  }
  free(c->bytes);
  free(c->lev_off);
  free(c->bits);
  free(c->codes0);
  if (c->hold != NULL) {
    if (c->hold_pid == mizu_self_pid()) mizu_zc_unref(c->hold);
    mizu_shm_close(c->hold, 0);
  }
}

void frame_cols_decref(frame_cols *fc) {
  if (atomic_fetch_sub_explicit(&fc->refs, 1, memory_order_acq_rel) != 1)
    return;
  for (int i = 0; i < fc->ncols; i++) fcol_free(&fc->cols[i]);
  free(fc->cols);
  free(fc->names);
  free(fc->name_off);
  free(fc);
}

frame_cols *frame_cols_new(int ncols, int64_t nrow) {
  frame_cols *fc = calloc(1, sizeof(*fc));
  if (fc == NULL) return NULL;
  atomic_init(&fc->refs, 1);
  fc->ncols = ncols;
  fc->nrow = nrow;
  fc->cols = calloc((size_t) ncols, sizeof(fcol));
  fc->name_off = calloc((size_t) ncols + 1, sizeof(int32_t));
  if (fc->cols == NULL || fc->name_off == NULL) {
    free(fc->cols);
    free(fc->name_off);
    free(fc);
    return NULL;
  }
  return fc;
}

int fcol_fixed_size(int kind) {
  switch (kind) {
  case FCOL_F64: case FCOL_I64: case FCOL_TS: case FCOL_TD: return 8;
  case FCOL_I32: case FCOL_LGL: case FCOL_DATE: case FCOL_DICT: return 4;
  case FCOL_C128: return 16;
  case FCOL_U8: return 1;
  }
  return 0;
}

/* The column-body byte total: the FRAMEREF gate's cheap data-size lower
   bound (directory, blobs and validity tails excluded — the MIZL write
   prices those itself). */
uint64_t frame_data_size(const frame_cols *fc) {
  uint64_t total = 0;
  for (int i = 0; i < fc->ncols; i++) {
    const fcol *c = &fc->cols[i];
    switch (c->kind) {
    case FCOL_STR:
      total += 4 * (uint64_t) (c->n + 1) + (uint64_t) c->bytes_len;
      break;
    case FCOL_DICT:
      total += 4 * (uint64_t) c->n + 4 * (uint64_t) (c->nlev + 1) +
        (uint64_t) c->bytes_len;
      break;
    case FCOL_STR64:
      total += (uint64_t) c->bytes_len;
      break;
    default:
      total += (uint64_t) c->n * (uint64_t) fcol_fixed_size(c->kind);
      break;
    }
  }
  return total;
}

// Frame methods ---------------------------------------------------------------------

static void Frame_dealloc(MizuFrame *self) {
  Py_CLEAR(self->row_names);
  Py_CLEAR(self->loan);
  if (self->fc != NULL) frame_cols_decref(self->fc);
  MizuFrameType.tp_free((PyObject *) self);
}

static PyObject *Frame_repr(MizuFrame *self) {
  return PyUnicode_FromFormat("pymizu.Frame(%lld rows x %d cols)",
                              (long long) self->fc->nrow, self->fc->ncols);
}

static Py_ssize_t Frame_len(MizuFrame *self) {
  return (Py_ssize_t) self->fc->nrow;
}

static PyObject *Frame_names(MizuFrame *self, void *Py_UNUSED(closure)) {
  frame_cols *fc = self->fc;
  PyObject *out = PyTuple_New(fc->ncols);
  if (out == NULL) return NULL;
  for (int i = 0; i < fc->ncols; i++) {
    PyObject *s = PyUnicode_DecodeUTF8(fc->names + fc->name_off[i],
                                       fc->name_off[i + 1] - fc->name_off[i],
                                       NULL);
    if (s == NULL) {
      Py_DECREF(out);
      return NULL;
    }
    PyTuple_SET_ITEM(out, i, s);
  }
  return out;
}

static PyObject *Frame_row_names_get(MizuFrame *self,
                                     void *Py_UNUSED(closure)) {
  Py_INCREF(self->row_names);
  return self->row_names;
}

/* A column's list form for to_dict: str/dict columns and the no-numpy
   fallbacks live here. */
static PyObject *fcol_to_list(const fcol *c) {
  PyObject *out = PyList_New((Py_ssize_t) c->n);
  if (out == NULL) return NULL;
  if (c->kind == FCOL_STR64) {
    mizu_mizs_geom g = mizu_mizs_geometry(c->n);
    const uint8_t *block = c->values;
    const uint8_t *validity = block + g.validity;
    const int64_t *offs = (const int64_t *) (block + g.offsets);
    const uint8_t *data = block + g.data;
    for (int64_t i = 0; i < c->n; i++) {
      PyObject *s;
      if (!(validity[i / 8] & (1u << (i % 8)))) {
        Py_INCREF(Py_None);
        s = Py_None;
      } else {
        int64_t lo = offs[i], hi = offs[i + 1];
        if (lo < 0 || hi < lo || hi > c->bytes_len) {
          Py_DECREF(out);
          PyErr_SetString(MizuError, "pymizu: invalid string data in "
                          "shared region");
          return NULL;
        }
        s = PyUnicode_DecodeUTF8((const char *) data + lo,
                                 (Py_ssize_t) (hi - lo), NULL);
        if (s == NULL) {
          Py_DECREF(out);
          return NULL;
        }
      }
      PyList_SET_ITEM(out, (Py_ssize_t) i, s);
    }
    return out;
  }
  if (c->kind == FCOL_STR) {
    const int32_t *offs = (const int32_t *) c->values;
    for (int64_t i = 0; i < c->n; i++) {
      PyObject *s;
      if (c->valid != NULL && !bitmap_at(c->valid, i)) {
        Py_INCREF(Py_None);
        s = Py_None;
      } else {
        s = PyUnicode_DecodeUTF8((const char *) c->bytes + offs[i],
                                 offs[i + 1] - offs[i], NULL);
        if (s == NULL) {
          Py_DECREF(out);
          return NULL;
        }
      }
      PyList_SET_ITEM(out, (Py_ssize_t) i, s);
    }
    return out;
  }
  /* FCOL_DICT */
  const int32_t *codes = (const int32_t *) c->values;
  for (int64_t i = 0; i < c->n; i++) {
    PyObject *s;
    /* the bitmap is authoritative (an adopted column's codes at null
       slots are unspecified); the sentinel is the owned invariant */
    if ((c->valid != NULL && !bitmap_at(c->valid, i)) ||
        codes[i] == MIZU_NA_INT32) {
      Py_INCREF(Py_None);
      s = Py_None;
    } else {
      int32_t k = c->codes1 ? codes[i] - 1 : codes[i];
      if (k < 0 || k >= c->nlev) {
        Py_DECREF(out);
        PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                        "(a factor code outside the levels)");
        return NULL;
      }
      s = PyUnicode_DecodeUTF8((const char *) c->bytes + c->lev_off[k],
                               c->lev_off[k + 1] - c->lev_off[k], NULL);
      if (s == NULL) {
        Py_DECREF(out);
        return NULL;
      }
    }
    PyList_SET_ITEM(out, (Py_ssize_t) i, s);
  }
  return out;
}

/* The column's region validity state for a borrowed view: the section
   bitmap and count, known-NA-free, or the lazy-scan default. An owned
   (lazily built) bitmap is not the region's — the view re-scans. */
static void fcol_view_valid(const fcol *c, const uint8_t **valid,
                            int64_t *nulls) {
  if (c->valid != NULL && !c->valid_owned) {
    *valid = c->valid;
    *nulls = c->vnulls;
  } else if (c->known_free) {
    *valid = NULL;
    *nulls = -1;
  } else {
    *valid = NULL;
    *nulls = 0;
  }
}

/* to_dict's column object: a borrowed fixed-width column wraps as a view
   over the region (owner/loan the frame's anchor); the rest copy. A
   remote column borrows a region the frame's anchor does not cover: the
   wrap rides a fresh open of the referenced region (the copy tiers the
   fallback on a gone one). */
static PyObject *fcol_to_obj(const fcol *c, PyObject *owner,
                             PyObject *loan) {
  uint64_t n = (uint64_t) c->n;
  if (c->borrowed && c->hold != NULL) {
    int type;
    switch (c->kind) {
    case FCOL_F64: type = MIZU_TYPE_REAL; break;
    case FCOL_I32: type = MIZU_TYPE_INT; break;
    case FCOL_I64: type = MIZU_TYPE_INT64; break;
    case FCOL_U8: type = MIZU_TYPE_RAW; break;
    case FCOL_C128: type = MIZU_TYPE_CPLX; break;
    default: type = 0; break;
    }
    if (type != 0) {
      const uint8_t *valid;
      int64_t nulls;
      fcol_view_valid(c, &valid, &nulls);
      PyObject *v = mizu_py_view_borrow_remote(
        c->hold, c->values, valid,
        (Py_ssize_t) c->n * (Py_ssize_t) mizu_type_elt_size(type),
        type, nulls);
      if (v != NULL) return v;
      PyErr_Clear();   /* a gone referenced region: the copy forms below */
    }
  }
  if (c->borrowed && c->hold == NULL) {
    int type;
    switch (c->kind) {
    case FCOL_F64: type = MIZU_TYPE_REAL; break;
    case FCOL_I32: type = MIZU_TYPE_INT; break;
    case FCOL_I64: type = MIZU_TYPE_INT64; break;
    case FCOL_U8: type = MIZU_TYPE_RAW; break;
    case FCOL_C128: type = MIZU_TYPE_CPLX; break;
    default: type = 0; break;
    }
    if (type != 0) {
      const uint8_t *valid;
      int64_t nulls;
      fcol_view_valid(c, &valid, &nulls);
      return mizu_py_view_borrow(owner, loan, c->values,
                                 (Py_ssize_t) c->n *
                                 (Py_ssize_t) mizu_type_elt_size(type),
                                 type, valid, nulls);
    }
  }
  switch (c->kind) {
  case FCOL_F64:
    return ixr_masked_f64(c->values, c->valid, n);
  case FCOL_I32:
    return ixr_masked_i32(c->values, c->valid, n);
  case FCOL_I64:
    return ixr_masked_i64(c->values, c->valid, n);
  case FCOL_U8:
    return ixr_vec_raw("uint8", c->values, n, 1);
  case FCOL_C128:
    return ixr_vec_raw("complex128", c->values, n, 16);
  case FCOL_LGL: {
    PyObject *np = mizu_py_numpy_module();
    if (np == NULL) return ixr_memoryview(c->values, (size_t) c->n * 4);
    if (span_has_na32((const int32_t *) c->values, (size_t) c->n))
      return ixr_vec_conv("int32", c->values, n, 4, NULL, NULL);
    i32_span s = { (const int32_t *) c->values, n };
    return ixr_vec_conv("bool", c->values, n, 1, conv_lgl_bool, &s);
  }
  case FCOL_STR:
  case FCOL_STR64:
  case FCOL_DICT:
    return fcol_to_list(c);
  case FCOL_DATE: {
    PyObject *np = mizu_py_numpy_module();
    if (np == NULL) {
      PyErr_SetString(MizuError, "pymizu: a Date column needs numpy for "
                      "to_dict() (install it)");
      return NULL;
    }
    return ixr_masked_days(c->values, c->valid, n);
  }
  case FCOL_TS: {
    PyObject *np = mizu_py_numpy_module();
    if (np == NULL) {
      PyErr_SetString(MizuError, "pymizu: a POSIXct column needs numpy "
                      "for to_dict() (install it)");
      return NULL;
    }
    return ixr_masked_us("datetime64[us]", c->values, c->valid, n);
  }
  case FCOL_TD: {
    PyObject *np = mizu_py_numpy_module();
    if (np == NULL) {
      PyErr_SetString(MizuError, "pymizu: a difftime column needs numpy "
                      "for to_dict() (install it)");
      return NULL;
    }
    return ixr_masked_us("timedelta64[us]", c->values, c->valid, n);
  }
  }
  PyErr_SetString(MizuError, "pymizu: unknown frame column kind");
  return NULL;
}

PyDoc_STRVAR(frame_to_dict_doc,
"to_dict() -> dict\n\n\
The frame as a dict of columns: numpy arrays for numeric columns (or\n\
memoryviews without numpy), datetime64 for Date / POSIXct, and\n\
list[str | None] for string and factor columns. Complex columns carry\n\
complex128 here (the Arrow export has no complex type).");

static PyObject *Frame_to_dict(MizuFrame *self, PyObject *Py_UNUSED(a)) {
  frame_cols *fc = self->fc;
  PyObject *owner = self->loan != NULL ? mizu_py_loan_owner(self->loan) :
    NULL;
  PyObject *out = PyDict_New();
  if (out == NULL) return NULL;
  for (int i = 0; i < fc->ncols; i++) {
    PyObject *v = fcol_to_obj(&fc->cols[i], owner, self->loan);
    if (v == NULL) {
      Py_DECREF(out);
      return NULL;
    }
    PyObject *k = PyUnicode_DecodeUTF8(fc->names + fc->name_off[i],
                                       fc->name_off[i + 1] - fc->name_off[i],
                                       NULL);
    if (k == NULL || PyDict_SetItem(out, k, v) < 0) {
      Py_XDECREF(k);
      Py_DECREF(v);
      Py_DECREF(out);
      return NULL;
    }
    Py_DECREF(k);
    Py_DECREF(v);
  }
  return out;
}

/* The STR64 column's one pre-export pass: span bounds and the encoding
   bytes (a latin1/bytes element declines, naming its index; CE_NATIVE
   spans UTF-8-validate — the sender's MIZS filter keeps one off this
   path, a defense), fused with the null count. */
static int64_t fcol_str64_check(fcol *c) {
  mizu_mizs_geom g = mizu_mizs_geometry(c->n);
  const uint8_t *block = c->values;
  const uint8_t *validity = block + g.validity;
  const int64_t *offs = (const int64_t *) (block + g.offsets);
  const uint8_t *enc = block + g.encoding;
  const uint8_t *data = block + g.data;
  const int64_t str_bytes = c->bytes_len;
  int64_t nulls = 0;
  for (int64_t i = 0; i < c->n; i++) {
    if (!(validity[i / 8] & (1u << (i % 8)))) {
      nulls++;
      continue;
    }
    int64_t lo = offs[i], hi = offs[i + 1];
    if (lo < 0 || hi < lo || hi > str_bytes) {
      PyErr_SetString(MizuError,
                      "pymizu: invalid string data in shared region");
      return -1;
    }
    if (enc[i] == MIZU_CE_LATIN1 || enc[i] == MIZU_CE_BYTES) {
      PyErr_Format(MizuError,
                   "pymizu: string %lld has an encoding that does not "
                   "cross to Arrow (latin1/bytes)", (long long) i);
      return -1;
    }
    if (enc[i] == MIZU_CE_NATIVE &&
        !mizu_ix_utf8_valid(data + lo, (size_t) (hi - lo))) {
      PyErr_Format(MizuError,
                   "pymizu: string %lld is not valid UTF-8",
                   (long long) i);
      return -1;
    }
  }
  c->enc_ok = 1;
  return nulls;
}

/* The validity bitmap + (for LGL) the bit-packed values + (for a
   region-borrowed DICT) the 0-based codes, built lazily at the export.
   Returns the null count, -1 on OOM. */
static int64_t fcol_ensure_export(fcol *c) {
  int64_t n = c->n, nulls = 0;
  if (c->kind == FCOL_U8) return 0;
  if (c->kind == FCOL_STR64) {
    if (!c->enc_ok) {
      int64_t nn = fcol_str64_check(c);
      if (nn < 0) return -1;
      nulls = nn;
    } else if (c->valid != NULL) {
      for (int64_t i = 0; i < n; i++) nulls += !bitmap_at(c->valid, i);
    }
    return nulls;
  }
  if (c->valid == NULL && !c->known_free) {
    uint8_t *valid = calloc(((size_t) n + 7) / 8, 1);
    if (valid == NULL) return -1;
    int any = 0;
    for (int64_t i = 0; i < n; i++) {
      int na;
      switch (c->kind) {
      case FCOL_F64: {
        uint64_t b;
        memcpy(&b, c->values + 8 * i, 8);
        na = is_na_r(b);
        break;
      }
      case FCOL_C128: {
        uint64_t b[2];
        memcpy(b, c->values + 16 * i, 16);
        na = is_na_r(b[0]) && is_na_r(b[1]);
        break;
      }
      case FCOL_I64: case FCOL_TS: case FCOL_TD: {
        int64_t v;
        memcpy(&v, c->values + 8 * i, 8);
        na = v == MIZU_NA_INT64;
        break;
      }
      default: {
        int32_t v;
        memcpy(&v, c->values + 4 * i, 4);
        na = v == MIZU_NA_INT32;
        break;
      }
      }
      if (na) {
        nulls++;
        any = 1;
      } else {
        valid[i >> 3] |= 1 << (i & 7);
      }
    }
    if (!any) {
      free(valid);
      valid = NULL;
    } else {
      c->valid = valid;
      c->valid_owned = 1;
    }
  } else if (c->valid != NULL) {
    /* a pre-existing bitmap (a region's): counted once here — a bitmap
       the scan just built already contributed its count above */
    for (int64_t i = 0; i < n; i++) nulls += !bitmap_at(c->valid, i);
  }
  /* the bit-packed values build at export, nulls or not */
  if (c->kind == FCOL_LGL && c->bits == NULL) {
    c->bits = calloc(((size_t) n + 7) / 8, 1);
    if (c->bits == NULL) return -1;
    const int32_t *v = (const int32_t *) c->values;
    for (int64_t i = 0; i < n; i++)
      if ((c->valid == NULL || bitmap_at(c->valid, i)) && v[i] != 0)
        c->bits[i >> 3] |= 1 << (i & 7);
  }
  /* the region's 1-based factor codes shift to 0-based at export */
  if (c->kind == FCOL_DICT && c->codes1 && c->codes0 == NULL) {
    c->codes0 = malloc((size_t) (n != 0 ? n : 1) * 4);
    if (c->codes0 == NULL) return -1;
    const int32_t *v = (const int32_t *) c->values;
    for (int64_t i = 0; i < n; i++) {
      if (v[i] == MIZU_NA_INT32) {
        c->codes0[i] = MIZU_NA_INT32;
      } else if (v[i] < 1 || (int64_t) v[i] > c->nlev) {
        PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                        "(a factor code outside the levels)");
        return -1;
      } else {
        c->codes0[i] = v[i] - 1;
      }
    }
  }
  return nulls;
}

// The Frame's Arrow export ------------------------------------------------------------

/* The exported stream: one struct batch over the column buffers. The
   holder (frame_export) is refcounted between the stream and every
   exported schema/array struct (their release callbacks decref it — pure
   C, callable from any thread); the frame_stream shell is the capsule's,
   freed by the capsule destructor. */
typedef struct frame_export {
  _Atomic size_t refs;
  frame_cols *fc;
  int schema_taken;
  int served;
  int released;             /* the stream's own release obligation */
  char err[160];            /* get_last_error's text */
  mizu_shm *acq;            /* a region-backed frame's own acquisition
                               (one fresh mapping + counted loan per
                               export, as __arrow_c_array__'s) */
  long acq_pid;
  ptrdiff_t acq_delta;      /* its mapping minus the frame's (the rebase) */
  ArrowSchema root_s;
  ArrowSchema *col_s;         /* [ncols] */
  ArrowSchema **col_s_ptr;    /* [ncols] */
  ArrowSchema *dict_s;        /* [ncols], DICT columns only */
  ArrowArray root_a;
  ArrowArray *col_a;          /* [ncols] */
  ArrowArray **col_a_ptr;     /* [ncols] */
  ArrowArray *dict_a;         /* [ncols], DICT columns only */
  const void **bufs;          /* [3 * ncols] */
  const void **dict_bufs;     /* [3 * ncols] */
  const void *root_buf[1];
  char (*fmts)[64];           /* [ncols] */
  char *cnames;               /* the NUL-terminated column names */
  char **cname_ptrs;          /* [ncols] */
} frame_export;

/* The exported capsule holds a bare ArrowArrayStream whose private_data
   is the frame_export: every callback — the stream's own release
   included — works through the holder, never through a shell. The holder
   pins one reference to the capsule itself (dropped by fx_capsule_free),
   so it outlives every callback whatever the consumer's move/drop order;
   the stream struct is the capsule's and freed by the destructor alone. */


static void frame_export_decref(frame_export *ex) {
  if (atomic_fetch_sub_explicit(&ex->refs, 1, memory_order_acq_rel) != 1)
    return;
  pymizu_shmframe_pv_unregister(ex);   /* before the mapping closes */
  if (ex->acq != NULL) {
    mizu_py_debug_span_remove(mizu_shm_addr(ex->acq));
    if (ex->acq_pid == mizu_self_pid()) mizu_zc_unref(ex->acq);
    mizu_shm_close(ex->acq, 0);
  }
  frame_cols_decref(ex->fc);
  free(ex->col_s);
  free(ex->col_s_ptr);
  free(ex->dict_s);
  free(ex->col_a);
  free(ex->col_a_ptr);
  free(ex->dict_a);
  free(ex->bufs);
  free(ex->dict_bufs);
  free(ex->fmts);
  free(ex->cnames);
  free(ex->cname_ptrs);
  free(ex);
}

/* The C Data Interface's release discipline: the consumer releases the
   struct it received (the root); children and dictionaries ride the
   parent's release. Each struct's callback is still idempotent (a
   consumer that releases children itself finds release NULL). */
static void fx_schema_release(ArrowSchema *s) {
  frame_export *ex = (frame_export *) s->private_data;
  s->release = NULL;
  for (int64_t i = 0; i < s->n_children; i++)
    if (s->children[i]->release != NULL)
      s->children[i]->release(s->children[i]);
  if (s->dictionary != NULL && s->dictionary->release != NULL)
    s->dictionary->release(s->dictionary);
  frame_export_decref(ex);
}

static void fx_array_release(ArrowArray *a) {
  frame_export *ex = (frame_export *) a->private_data;
  a->release = NULL;
  for (int64_t i = 0; i < a->n_children; i++)
    if (a->children[i]->release != NULL)
      a->children[i]->release(a->children[i]);
  if (a->dictionary != NULL && a->dictionary->release != NULL)
    a->dictionary->release(a->dictionary);
  frame_export_decref(ex);
}

static int fx_get_schema(ArrowArrayStream *st, ArrowSchema *out) {
  frame_export *ex = (frame_export *) st->private_data;
  if (ex->schema_taken) {
    snprintf(ex->err, sizeof(ex->err), "the schema was already served");
    return -1;
  }
  ex->schema_taken = 1;
  int ncols = ex->fc->ncols;
  *out = ex->root_s;
  out->release = fx_schema_release;
  out->private_data = ex;
  atomic_fetch_add_explicit(&ex->refs, 1, memory_order_relaxed);
  for (int i = 0; i < ncols; i++) {
    out->children[i]->release = fx_schema_release;
    out->children[i]->private_data = ex;
    atomic_fetch_add_explicit(&ex->refs, 1, memory_order_relaxed);
    if (out->children[i]->dictionary != NULL) {
      out->children[i]->dictionary->release = fx_schema_release;
      out->children[i]->dictionary->private_data = ex;
      atomic_fetch_add_explicit(&ex->refs, 1, memory_order_relaxed);
    }
  }
  return 0;
}

static int fx_get_next(ArrowArrayStream *st, ArrowArray *out) {
  frame_export *ex = (frame_export *) st->private_data;
  if (ex->served) {
    memset(out, 0, sizeof(*out));
    return 0;
  }
  ex->served = 1;
  int ncols = ex->fc->ncols;
  *out = ex->root_a;
  out->release = fx_array_release;
  out->private_data = ex;
  atomic_fetch_add_explicit(&ex->refs, 1, memory_order_relaxed);
  for (int i = 0; i < ncols; i++) {
    out->children[i]->release = fx_array_release;
    out->children[i]->private_data = ex;
    atomic_fetch_add_explicit(&ex->refs, 1, memory_order_relaxed);
    if (out->children[i]->dictionary != NULL) {
      out->children[i]->dictionary->release = fx_array_release;
      out->children[i]->dictionary->private_data = ex;
      atomic_fetch_add_explicit(&ex->refs, 1, memory_order_relaxed);
    }
  }
  return 0;
}

static const char *fx_get_last_error(ArrowArrayStream *st) {
  return ((frame_export *) st->private_data)->err;
}

/* The stream's release: idempotent; nulls the release field (the
   consumer-side cleanup check) and drops both of the holder's initial
   references (the stream's and the capsule's) — after a proper move the
   capsule's destructor can no longer reach the holder through a
   zeroed private_data, so the release call is the one place that can. */
static void fx_stream_release(ArrowArrayStream *st) {
  frame_export *ex = (frame_export *) st->private_data;
  st->release = NULL;
  if (ex->released) return;
  ex->released = 1;
  frame_export_decref(ex);   /* the stream's own reference */
  frame_export_decref(ex);   /* the capsule's */
}

/* The backstop for an unconsumed stream: a moved stream has its release
   field zeroed by the consumer (and its private_data may be zeroed too),
   so a NULL release means the consumer owns the release obligation and
   the destructor frees only the struct. */
static void fx_capsule_free(PyObject *cap) {
  ArrowArrayStream *st = (ArrowArrayStream *) PyCapsule_GetPointer(
    cap, "arrow_array_stream");
  if (st == NULL) {
    PyErr_Clear();
    return;
  }
  if (st->release != NULL && st->private_data != NULL) {
    frame_export *ex = (frame_export *) st->private_data;
    if (!ex->released) fx_stream_release(st);
  }
  free(st);
}

PyDoc_STRVAR(frame_arrow_stream_doc,
"__arrow_c_stream__(requested_schema=None) -> capsule\n\n\
The Arrow C Stream Interface export: the frame as one struct batch.\n\
Consumers take it in one line: pl.from_arrow(f), pa.table(f), or\n\
pd.DataFrame.from_arrow(f). A complex column raises (no Arrow type).");

PyObject *Frame_arrow_c_stream(MizuFrame *self, PyObject *args,
                                      PyObject *kw) {
  static char *kwlist[] = { "requested_schema", NULL };
  PyObject *req = Py_None;
  if (!PyArg_ParseTupleAndKeywords(args, kw, "|O:__arrow_c_stream__",
                                   kwlist, &req))
    return NULL;
  (void) req;   /* schema negotiation: the one schema is offered */
  frame_cols *fc = self->fc;
  int ncols = fc->ncols;
  /* validity bitmaps + bool bit-packing, lazily at this first export */
  int64_t *nulls = calloc((size_t) ncols, sizeof(int64_t));
  if (nulls == NULL) return PyErr_NoMemory();
  for (int i = 0; i < ncols; i++) {
    if (fc->cols[i].kind == FCOL_C128) {
      free(nulls);
      PyErr_Format(MizuError, "pymizu: a complex column has no Arrow "
                   "type (column '%.*s')",
                   (int) (fc->name_off[i + 1] - fc->name_off[i]),
                   fc->names + fc->name_off[i]);
      return NULL;
    }
    int64_t nn = fcol_ensure_export(&fc->cols[i]);
    if (nn < 0) {
      free(nulls);
      if (!PyErr_Occurred()) PyErr_NoMemory();
      return NULL;
    }
    nulls[i] = nn;
  }
  frame_export *ex = calloc(1, sizeof(*ex));
  ArrowArrayStream *st = calloc(1, sizeof(*st));
  if (ex == NULL || st == NULL) {
    free(ex);
    free(st);
    free(nulls);
    return PyErr_NoMemory();
  }
  atomic_init(&ex->refs, 2);   /* the capsule's + the stream's release */
  ex->fc = fc;
  atomic_fetch_add_explicit(&fc->refs, 1, memory_order_relaxed);
  if (self->loan != NULL) {
    /* the region-backed export's own mapping and counted loan, so the
       exported arrays stay valid past the frame's death (the release
       callbacks subs and unmap, pure C with no GIL) */
    mizu_shm *oshm = mizu_py_loan_shm(self->loan);
    mizu_shm *acq = NULL;
    if (oshm == NULL ||
        mizu_shm_open_view(&acq, mizu_shm_name(oshm)) != MIZU_OK) {
      PyErr_SetString(MizuError, oshm == NULL ?
                      "pymizu: the frame has no region" :
                      mizu_last_error_message());
      free(nulls);
      frame_export_decref(ex);
      frame_export_decref(ex);
      free(st);
      return NULL;
    }
    ex->acq = acq;
    ex->acq_pid = mizu_self_pid();
    ex->acq_delta = (const uint8_t *) mizu_shm_addr(acq) -
      (const uint8_t *) mizu_shm_addr(oshm);
    mizu_py_debug_span_add(mizu_shm_addr(acq), mizu_shm_size(acq));
  }
  ex->col_s = calloc((size_t) ncols, sizeof(ArrowSchema));
  ex->col_s_ptr = calloc((size_t) ncols, sizeof(ArrowSchema *));
  ex->dict_s = calloc((size_t) ncols, sizeof(ArrowSchema));
  ex->col_a = calloc((size_t) ncols, sizeof(ArrowArray));
  ex->col_a_ptr = calloc((size_t) ncols, sizeof(ArrowArray *));
  ex->dict_a = calloc((size_t) ncols, sizeof(ArrowArray));
  ex->bufs = calloc((size_t) ncols * 3, sizeof(void *));
  ex->dict_bufs = calloc((size_t) ncols * 3, sizeof(void *));
  ex->fmts = calloc((size_t) ncols, 64);
  ex->cname_ptrs = calloc((size_t) ncols, sizeof(char *));
  if (ex->col_s == NULL || ex->col_s_ptr == NULL || ex->dict_s == NULL ||
      ex->col_a == NULL || ex->col_a_ptr == NULL || ex->dict_a == NULL ||
      ex->bufs == NULL || ex->dict_bufs == NULL || ex->fmts == NULL ||
      ex->cname_ptrs == NULL) {
    free(nulls);
    frame_export_decref(ex);
    frame_export_decref(ex);
    free(st);
    return PyErr_NoMemory();
  }
  /* ArrowSchema.name is a C string: the packed store is not NUL-
     terminated, so the export copies the names out */
  {
    size_t total = (size_t) fc->name_off[ncols] + (size_t) ncols;
    ex->cnames = malloc(total);
    if (ex->cnames == NULL) {
      free(nulls);
      frame_export_decref(ex);
      frame_export_decref(ex);
      free(st);
      return PyErr_NoMemory();
    }
    char *p = ex->cnames;
    for (int i = 0; i < ncols; i++) {
      int32_t len = fc->name_off[i + 1] - fc->name_off[i];
      memcpy(p, fc->names + fc->name_off[i], (size_t) len);
      p[len] = '\0';
      ex->cname_ptrs[i] = p;
      p += len + 1;
    }
  }
  /* the root: a struct of ncols children */
  ex->root_s.format = "+s";
  ex->root_s.name = "";
  ex->root_s.flags = 0;
  ex->root_s.n_children = ncols;
  ex->root_s.children = ex->col_s_ptr;
  ex->root_s.release = NULL;
  ex->root_buf[0] = NULL;
  ex->root_a.length = fc->nrow;
  ex->root_a.null_count = 0;
  ex->root_a.offset = 0;
  ex->root_a.n_buffers = 1;
  ex->root_a.buffers = ex->root_buf;
  ex->root_a.n_children = ncols;
  ex->root_a.children = ex->col_a_ptr;
  ex->root_a.release = NULL;
  for (int i = 0; i < ncols; i++) {
    fcol *c = &fc->cols[i];
    ArrowSchema *s = &ex->col_s[i];
    ArrowArray *a = &ex->col_a[i];
    ex->col_s_ptr[i] = s;
    ex->col_a_ptr[i] = a;
    const void **bufs = &ex->bufs[3 * i];
    const char *fmt;
    switch (c->kind) {
    case FCOL_F64: fmt = "g"; break;
    case FCOL_I32: fmt = "i"; break;
    case FCOL_I64: fmt = "l"; break;
    case FCOL_U8: fmt = "C"; break;
    case FCOL_LGL: fmt = "b"; break;
    case FCOL_STR: fmt = "u"; break;
    case FCOL_STR64: fmt = "U"; break;
    case FCOL_DICT: fmt = "i"; break;
    case FCOL_DATE: fmt = "tdD"; break;
    case FCOL_TD: fmt = "tDu"; break;
    case FCOL_TS:
      fmt = ex->fmts[i];
      snprintf(ex->fmts[i], 64, "tsu:%s", c->tz);
      break;
    default: fmt = "g"; break;
    }
    s->format = fmt;
    s->name = ex->cname_ptrs[i];
    s->flags = PYMIZU_ARROW_FLAG_NULLABLE;
    s->n_children = 0;
    s->release = NULL;
    a->length = c->n;
    a->null_count = nulls[i];   /* 0 pairs with a NULL bitmap */
    a->offset = 0;
    a->buffers = bufs;
    a->n_children = 0;
    a->release = NULL;
    /* a borrowed column's region pointers rebase onto this export's own
       mapping (an owned product — the lazy bitmap, the codes shift, the
       bit-pack — does not). A remote column borrows its hold's region,
       not the frame's: no rebase — the export's fc reference pins the
       hold through the same release chain. */
    const uint8_t *vals = c->values;
    const uint8_t *vld = c->valid;
    if (c->borrowed && c->hold == NULL) {
      vals += ex->acq_delta;
      if (vld != NULL && !c->valid_owned) vld += ex->acq_delta;
    }
    bufs[0] = vld;
    switch (c->kind) {
    case FCOL_STR:
      a->n_buffers = 3;
      bufs[1] = c->values;
      bufs[2] = c->bytes;
      break;
    case FCOL_STR64: {
      mizu_mizs_geom g = mizu_mizs_geometry(c->n);
      a->n_buffers = 3;
      bufs[0] = vals + g.validity;
      bufs[1] = vals + g.offsets;
      bufs[2] = vals + g.data;
      break;
    }
    case FCOL_LGL:
      a->n_buffers = 2;
      bufs[1] = c->bits;
      break;
    case FCOL_DICT: {
      a->n_buffers = 2;
      bufs[1] = c->codes1 ? (const void *) c->codes0 : (const void *) vals;
      ArrowSchema *ds = &ex->dict_s[i];
      ArrowArray *da = &ex->dict_a[i];
      ds->format = "u";
      ds->name = "";
      ds->flags = 0;
      ds->n_children = 0;
      ds->release = NULL;
      s->dictionary = ds;
      const void **dbufs = &ex->dict_bufs[3 * i];
      dbufs[0] = NULL;
      dbufs[1] = c->lev_off;
      dbufs[2] = c->bytes;
      da->length = c->nlev;
      da->null_count = 0;
      da->offset = 0;
      da->n_buffers = 3;
      da->buffers = dbufs;
      da->n_children = 0;
      da->release = NULL;
      a->dictionary = da;
      break;
    }
    default:
      a->n_buffers = 2;
      bufs[1] = vals;
      break;
    }
  }
  free(nulls);
  st->get_schema = fx_get_schema;
  st->get_next = fx_get_next;
  st->get_last_error = fx_get_last_error;
  st->release = fx_stream_release;
  st->private_data = ex;
  /* the export-provenance record (3.8): what this acquisition hands out,
     so an unmodified round trip of the frame stages as REF */
  if (ex->acq != NULL)
    pymizu_shmframe_pv_register(ex->acq, ncols, ex->cname_ptrs, ex->col_s,
                                ex->col_a, ex);
  PyObject *cap = PyCapsule_New(st, "arrow_array_stream", fx_capsule_free);
  if (cap == NULL) {
    fx_stream_release(st);
    free(st);
  }
  return cap;
}

// The Frame's pickle form -------------------------------------------------------------

/* The plain-Python state for __reduce__: (names, columns, row_names) with
   per-column (kind, payload[, aux]) tuples. Payloads are plain lists, so
   the state is numpy-free. */
static PyObject *Frame_reduce(MizuFrame *self, PyObject *Py_UNUSED(a)) {
  frame_cols *fc = self->fc;
  PyObject *names = PyList_New(fc->ncols);
  PyObject *columns = PyList_New(fc->ncols);
  if (names == NULL || columns == NULL) {
    Py_XDECREF(names);
    Py_XDECREF(columns);
    return NULL;
  }
  for (int i = 0; i < fc->ncols; i++) {
    PyObject *s = PyUnicode_DecodeUTF8(
      fc->names + fc->name_off[i], fc->name_off[i + 1] - fc->name_off[i],
      NULL);
    if (s == NULL) goto fail;
    PyList_SET_ITEM(names, i, s);
  }
  for (int i = 0; i < fc->ncols; i++) {
    fcol *c = &fc->cols[i];
    PyObject *payload = PyList_New((Py_ssize_t) c->n);
    PyObject *tuple = NULL;
    const char *kind = c->kind == FCOL_F64 ? "g" :
      c->kind == FCOL_I32 ? "i" :
      c->kind == FCOL_I64 ? "l" :
      c->kind == FCOL_U8 ? "C" :
      c->kind == FCOL_C128 ? "Z" :
      c->kind == FCOL_LGL ? "b" :
      c->kind == FCOL_STR || c->kind == FCOL_STR64 ? "s" :
      c->kind == FCOL_DICT ? "d" :
      c->kind == FCOL_DATE ? "D" :
      c->kind == FCOL_TD ? "T" : "t";
    if (payload == NULL) goto fail;
    for (int64_t j = 0; j < c->n; j++) {
      PyObject *v = NULL;
      /* the bitmap is authoritative wherever one exists (an adopted
         column's values at null slots are unspecified); the switch's
         sentinel checks are the owned invariant */
      if (c->valid != NULL && !bitmap_at(c->valid, j)) {
        Py_INCREF(Py_None);
        v = Py_None;
      } else switch (c->kind) {
      case FCOL_F64: {
        double x;
        memcpy(&x, c->values + 8 * j, 8);
        v = PyFloat_FromDouble(x);
        break;
      }
      case FCOL_I32: case FCOL_LGL: case FCOL_DATE: {
        int32_t x;
        memcpy(&x, c->values + 4 * j, 4);
        if (c->kind == FCOL_LGL) {
          v = x == MIZU_NA_INT32 ? (Py_INCREF(Py_None), Py_None) :
            PyBool_FromLong(x != 0);
        } else {
          v = x == MIZU_NA_INT32 ? (Py_INCREF(Py_None), Py_None) :
            PyLong_FromLong(x);
        }
        break;
      }
      case FCOL_I64: case FCOL_TS: case FCOL_TD: {
        int64_t x;
        memcpy(&x, c->values + 8 * j, 8);
        v = x == MIZU_NA_INT64 ? (Py_INCREF(Py_None), Py_None) :
          PyLong_FromLongLong(x);
        break;
      }
      case FCOL_U8:
        v = PyLong_FromLong(c->values[j]);
        break;
      case FCOL_C128: {
        double re, im;
        memcpy(&re, c->values + 16 * j, 8);
        memcpy(&im, c->values + 16 * j + 8, 8);
        v = PyComplex_FromDoubles(re, im);
        break;
      }
      case FCOL_STR: {
        const int32_t *offs = (const int32_t *) c->values;
        if (c->valid != NULL && !bitmap_at(c->valid, j)) {
          Py_INCREF(Py_None);
          v = Py_None;
        } else {
          v = PyUnicode_DecodeUTF8((const char *) c->bytes + offs[j],
                                   offs[j + 1] - offs[j], NULL);
        }
        break;
      }
      case FCOL_STR64: {
        mizu_mizs_geom g = mizu_mizs_geometry(c->n);
        const uint8_t *block = c->values;
        if (!(block[g.validity + j / 8] & (1u << (j % 8)))) {
          Py_INCREF(Py_None);
          v = Py_None;
        } else {
          const int64_t *offs = (const int64_t *) (block + g.offsets);
          int64_t lo = offs[j], hi = offs[j + 1];
          if (lo < 0 || hi < lo || hi > c->bytes_len) {
            PyErr_SetString(MizuError, "pymizu: invalid string data in "
                            "shared region");
          } else {
            v = PyUnicode_DecodeUTF8((const char *) block + g.data + lo,
                                     (Py_ssize_t) (hi - lo), NULL);
          }
        }
        break;
      }
      case FCOL_DICT: {
        const int32_t *codes = (const int32_t *) c->values;
        if (codes[j] == MIZU_NA_INT32) {
          Py_INCREF(Py_None);
          v = Py_None;
        } else {
          int32_t k = c->codes1 ? codes[j] - 1 : codes[j];
          if (k < 0 || k >= c->nlev) {
            PyErr_SetString(MizuError, "pymizu: corrupt or newer region "
                            "(a factor code outside the levels)");
          } else {
            v = PyLong_FromLong(k);
          }
        }
        break;
      }
      }
      if (v == NULL) {
        Py_DECREF(payload);
        goto fail;
      }
      PyList_SET_ITEM(payload, (Py_ssize_t) j, v);
    }
    /* PyTuple_Pack borrows its arguments: every fresh reference built for
       a pack must leave with the tuple */
    PyObject *kobj = PyUnicode_FromString(kind);
    if (kobj == NULL) {
      Py_DECREF(payload);
      goto fail;
    }
    if (c->kind == FCOL_DICT) {
      PyObject *levels = PyList_New((Py_ssize_t) c->nlev);
      if (levels == NULL) {
        Py_DECREF(kobj);
        Py_DECREF(payload);
        goto fail;
      }
      for (int64_t j = 0; j < c->nlev; j++) {
        PyObject *s = PyUnicode_DecodeUTF8(
          (const char *) c->bytes + c->lev_off[j],
          c->lev_off[j + 1] - c->lev_off[j], NULL);
        if (s == NULL) {
          Py_DECREF(levels);
          Py_DECREF(kobj);
          Py_DECREF(payload);
          goto fail;
        }
        PyList_SET_ITEM(levels, (Py_ssize_t) j, s);
      }
      tuple = PyTuple_Pack(3, kobj, payload, levels);
      Py_DECREF(levels);
    } else if (c->kind == FCOL_TS) {
      PyObject *tzobj = PyUnicode_FromString(c->tz);
      tuple = tzobj != NULL ? PyTuple_Pack(3, kobj, payload, tzobj) : NULL;
      Py_XDECREF(tzobj);
    } else {
      tuple = PyTuple_Pack(2, kobj, payload);
    }
    Py_DECREF(kobj);
    Py_DECREF(payload);
    if (tuple == NULL) goto fail;
    PyList_SET_ITEM(columns, i, tuple);
  }
  PyObject *row_names = self->row_names;
  PyObject *rn = NULL;
  if (row_names == Py_None || PyList_Check(row_names)) {
    rn = row_names;
    Py_INCREF(rn);
  } else {
    /* the numpy int32 / memoryview form -> a list of ints */
    Py_buffer v;
    if (PyObject_GetBuffer(row_names, &v, PyBUF_ND | PyBUF_FORMAT) == 0) {
      Py_ssize_t n = v.len / 4;
      rn = PyList_New(n);
      if (rn != NULL)
        for (Py_ssize_t j = 0; j < n; j++) {
          PyObject *x = PyLong_FromLong(((const int32_t *) v.buf)[j]);
          if (x == NULL) {
            Py_CLEAR(rn);
            break;
          }
          PyList_SET_ITEM(rn, j, x);
        }
      PyBuffer_Release(&v);
    }
    if (rn == NULL) goto fail;
  }
  PyObject *state = PyTuple_Pack(3, names, columns, rn);
  Py_DECREF(names);
  Py_DECREF(columns);
  Py_DECREF(rn);
  if (state == NULL) return NULL;
  Py_INCREF(frame_rebuild_fn);
  PyObject *out = PyTuple_Pack(2, frame_rebuild_fn, state);
  Py_DECREF(frame_rebuild_fn);
  Py_DECREF(state);
  return out;
fail:
  Py_DECREF(names);
  Py_DECREF(columns);
  return NULL;
}

/* Rebuild a Frame from the pickle state. */
static PyObject *frame_rebuild(PyObject *Py_UNUSED(m), PyObject *args) {
  PyObject *names, *columns, *rn;
  if (!PyArg_ParseTuple(args, "OOO:_frame_rebuild", &names, &columns, &rn))
    return NULL;
  if (!PyList_Check(names) || !PyList_Check(columns) ||
      PyList_GET_SIZE(names) != PyList_GET_SIZE(columns) ||
      PyList_GET_SIZE(names) == 0) {
    PyErr_SetString(PyExc_ValueError,
                    "pymizu: malformed frame rebuild state");
    return NULL;
  }
  int ncols = (int) PyList_GET_SIZE(names);
  /* first pass: the row count from column 0's payload */
  int64_t nrow = -1;
  frame_cols *fc = NULL;
  fcol *cols = calloc((size_t) ncols, sizeof(fcol));
  if (cols == NULL) return PyErr_NoMemory();
  for (int i = 0; i < ncols; i++) {
    PyObject *t = PyList_GET_ITEM(columns, i);
    if (!PyTuple_Check(t) || PyTuple_GET_SIZE(t) < 2) goto malformed;
    const char *kind = PyUnicode_AsUTF8(PyTuple_GET_ITEM(t, 0));
    PyObject *payload = PyTuple_GET_ITEM(t, 1);
    if (kind == NULL || !PyList_Check(payload)) goto malformed;
    Py_ssize_t n = PyList_GET_SIZE(payload);
    if (nrow < 0) nrow = n;
    if (n != nrow) goto malformed;
    fcol *c = &cols[i];
    c->n = n;
    switch (kind[0]) {
    case 'g': case 'Z': case 'C': case 'i': case 'l': case 'b':
    case 'D': case 't': case 'T': case 's': case 'd':
      break;
    default:
      goto malformed;
    }
    c->kind = kind[0] == 'g' ? FCOL_F64 :
      kind[0] == 'i' ? FCOL_I32 :
      kind[0] == 'l' ? FCOL_I64 :
      kind[0] == 'C' ? FCOL_U8 :
      kind[0] == 'Z' ? FCOL_C128 :
      kind[0] == 'b' ? FCOL_LGL :
      kind[0] == 'D' ? FCOL_DATE :
      kind[0] == 'T' ? FCOL_TD :
      kind[0] == 't' ? FCOL_TS :
      kind[0] == 's' ? FCOL_STR : FCOL_DICT;
    int elt = fcol_fixed_size(c->kind);
    if (c->kind == FCOL_STR) {
      int32_t *offs = malloc(((size_t) n + 1) * 4);
      uint8_t *bytes = NULL, *valid = NULL;
      size_t blen = 0;
      if (offs == NULL) goto nomem;
      offs[0] = 0;
      for (Py_ssize_t j = 0; j < n; j++) {
        PyObject *s = PyList_GET_ITEM(payload, j);
        if (s == Py_None) {
          if (valid == NULL) {
            valid = calloc(((size_t) n + 7) / 8, 1);
            if (valid == NULL) goto str_nomem;
            for (Py_ssize_t q = 0; q < j; q++) valid[q >> 3] |= 1 << (q & 7);
          }
        } else {
          if (!PyUnicode_Check(s)) goto str_malformed;
          if (valid != NULL) valid[j >> 3] |= 1 << (j & 7);
          Py_ssize_t l;
          const char *u = PyUnicode_AsUTF8AndSize(s, &l);
          if (u == NULL) goto str_malformed;
          uint8_t *nb = realloc(bytes, blen + (size_t) l + 1);
          if (nb == NULL) goto str_nomem;
          bytes = nb;
          memcpy(bytes + blen, u, (size_t) l);
          blen += (size_t) l;
        }
        offs[j + 1] = (int32_t) blen;
      }
      c->values = (uint8_t *) offs;
      c->bytes = bytes;
      c->bytes_len = (int64_t) blen;
      c->valid = valid;
      goto next_col;
    str_nomem:
      free(offs);
      free(bytes);
      free(valid);
      goto nomem;
    str_malformed:
      free(offs);
      free(bytes);
      free(valid);
      goto malformed;
    }
    if (c->kind == FCOL_DICT) {
      PyObject *levels = PyTuple_GET_SIZE(t) >= 3 ? PyTuple_GET_ITEM(t, 2) :
        NULL;
      if (levels == NULL || !PyList_Check(levels)) goto malformed;
      Py_ssize_t nlev = PyList_GET_SIZE(levels);
      int32_t *codes = malloc((size_t) (n != 0 ? n : 1) * 4);
      int32_t *loffs = malloc(((size_t) nlev + 1) * 4);
      uint8_t *bytes = NULL;
      size_t blen = 0;
      if (codes == NULL || loffs == NULL) goto dict_nomem;
      loffs[0] = 0;
      for (Py_ssize_t j = 0; j < nlev; j++) {
        PyObject *s = PyList_GET_ITEM(levels, j);
        if (!PyUnicode_Check(s)) goto dict_malformed;
        Py_ssize_t l;
        const char *u = PyUnicode_AsUTF8AndSize(s, &l);
        if (u == NULL) goto dict_malformed;
        uint8_t *nb = realloc(bytes, blen + (size_t) l + 1);
        if (nb == NULL) goto dict_nomem;
        bytes = nb;
        memcpy(bytes + blen, u, (size_t) l);
        blen += (size_t) l;
        loffs[j + 1] = (int32_t) blen;
      }
      for (Py_ssize_t j = 0; j < n; j++) {
        PyObject *v = PyList_GET_ITEM(payload, j);
        if (v == Py_None) {
          codes[j] = MIZU_NA_INT32;
        } else {
          long x = PyLong_AsLong(v);
          if (x == -1 && PyErr_Occurred()) goto dict_malformed;
          if (x < 0 || x >= nlev) goto dict_malformed;
          codes[j] = (int32_t) x;
        }
      }
      c->values = (uint8_t *) codes;
      c->lev_off = loffs;
      c->bytes = bytes;
      c->nlev = nlev;
      c->bytes_len = (int64_t) blen;
      goto next_col;
    dict_nomem:
      free(codes);
      free(loffs);
      free(bytes);
      goto nomem;
    dict_malformed:
      free(codes);
      free(loffs);
      free(bytes);
      goto malformed;
    }
    c->values = malloc((size_t) (n != 0 ? n : 1) * (size_t) elt);
    if (c->values == NULL) goto nomem;
    for (Py_ssize_t j = 0; j < n; j++) {
      PyObject *v = PyList_GET_ITEM(payload, j);
      switch (c->kind) {
      case FCOL_F64: {
        double x;
        if (v == Py_None) {
          /* an adopted column's pickle form carries None at nulls (the
             owned form's NaN float also reads here) */
          const uint64_t bits = PYMIZU_NA_REAL_BITS;
          memcpy(&x, &bits, 8);
        } else {
          x = PyFloat_AsDouble(v);
          if (x == -1.0 && PyErr_Occurred()) goto nomem_malformed;
        }
        memcpy(c->values + 8 * j, &x, 8);
        break;
      }
      case FCOL_C128: {
        if (!PyComplex_Check(v)) goto nomem_malformed;
        double re = PyComplex_RealAsDouble(v);
        double im = PyComplex_ImagAsDouble(v);
        memcpy(c->values + 16 * j, &re, 8);
        memcpy(c->values + 16 * j + 8, &im, 8);
        break;
      }
      case FCOL_U8: {
        long x = PyLong_AsLong(v);
        if ((x == -1 && PyErr_Occurred()) || x < 0 || x > 255)
          goto nomem_malformed;
        c->values[j] = (uint8_t) x;
        break;
      }
      case FCOL_LGL: {
        int32_t x;
        if (v == Py_None) {
          x = MIZU_NA_INT32;
        } else if (PyBool_Check(v)) {
          x = v == Py_True;
        } else {
          goto nomem_malformed;
        }
        memcpy(c->values + 4 * j, &x, 4);
        break;
      }
      case FCOL_I32: case FCOL_DATE: {
        int32_t x;
        if (v == Py_None) {
          x = MIZU_NA_INT32;
        } else {
          long xl = PyLong_AsLong(v);
          if ((xl == -1 && PyErr_Occurred()) || xl < INT32_MIN ||
              xl > INT32_MAX)
            goto nomem_malformed;
          x = (int32_t) xl;
        }
        memcpy(c->values + 4 * j, &x, 4);
        break;
      }
      case FCOL_I64: case FCOL_TS: case FCOL_TD: {
        int64_t x;
        if (v == Py_None) {
          x = MIZU_NA_INT64;
        } else {
          long long xl = PyLong_AsLongLong(v);
          if (xl == -1 && PyErr_Occurred()) goto nomem_malformed;
          x = (int64_t) xl;
        }
        memcpy(c->values + 8 * j, &x, 8);
        break;
      }
      }
    }
    if (c->kind == FCOL_TS) {
      PyObject *tz = PyTuple_GET_SIZE(t) >= 3 ? PyTuple_GET_ITEM(t, 2) :
        NULL;
      if (tz != NULL && PyUnicode_Check(tz))
        snprintf(c->tz, sizeof(c->tz), "%s", PyUnicode_AsUTF8(tz));
    }
    goto next_col;
  nomem_malformed:
    goto malformed;
  next_col:
    continue;
  }
  fc = frame_cols_new(ncols, nrow);
  if (fc == NULL) goto nomem;
  free(fc->cols);
  fc->cols = cols;
  /* pack the names (unique) */
  size_t name_len = 0;
  for (int i = 0; i < ncols; i++) {
    PyObject *s = PyList_GET_ITEM(names, i);
    if (!PyUnicode_Check(s)) goto fc_malformed;
    Py_ssize_t len;
    const char *u = PyUnicode_AsUTF8AndSize(s, &len);
    if (u == NULL) goto fc_malformed;
    for (int j = 0; j < i; j++)
      if ((size_t) (fc->name_off[j + 1] - fc->name_off[j]) ==
            (size_t) len &&
          memcmp(fc->names + fc->name_off[j], u, (size_t) len) == 0)
        goto fc_malformed;
    char *nb = realloc(fc->names, name_len + (size_t) len + 1);
    if (nb == NULL) goto fc_nomem;
    fc->names = nb;
    memcpy(fc->names + name_len, u, (size_t) len);
    fc->name_off[i] = (int32_t) name_len;
    name_len += (size_t) len;
  }
  fc->name_off[ncols] = (int32_t) name_len;
  /* row names: None, a str list, or an int list (back to the array) */
  PyObject *rownames = NULL;
  if (rn == Py_None) {
    Py_INCREF(Py_None);
    rownames = Py_None;
  } else if (PyList_Check(rn) &&
             (PyList_GET_SIZE(rn) == 0 ||
              PyUnicode_Check(PyList_GET_ITEM(rn, 0)))) {
    if (PyList_GET_SIZE(rn) != nrow) goto fc_malformed;
    for (int64_t j = 0; j < nrow; j++)
      if (!PyUnicode_Check(PyList_GET_ITEM(rn, j)) ||
          PyUnicode_AsUTF8(PyList_GET_ITEM(rn, j)) == NULL)
        goto fc_malformed;
    rownames = rn;
    Py_INCREF(rownames);
  } else if (PyList_Check(rn) && PyList_GET_SIZE(rn) == nrow) {
    int32_t *buf = malloc((size_t) (nrow != 0 ? nrow : 1) * 4);
    if (buf == NULL) goto fc_nomem;
    for (int64_t j = 0; j < nrow; j++) {
      long x = PyLong_AsLong(PyList_GET_ITEM(rn, j));
      if ((x == -1 && PyErr_Occurred()) || x < INT32_MIN || x > INT32_MAX) {
        free(buf);
        goto fc_malformed;
      }
      buf[j] = (int32_t) x;
    }
    rownames = ixr_vec_raw("int32", (const uint8_t *) buf, (uint64_t) nrow,
                           4);
    free(buf);
    if (rownames == NULL) goto fc_fail;
  } else {
    goto fc_malformed;
  }
  {
    MizuFrame *self = (MizuFrame *) MizuFrameType.tp_alloc(&MizuFrameType, 0);
    if (self == NULL) {
      Py_DECREF(rownames);
      goto fc_fail;
    }
    self->fc = fc;
    self->row_names = rownames;
    self->loan = NULL;
    return (PyObject *) self;
  }
fc_malformed:
  frame_cols_decref(fc);
  goto malformed2;
fc_nomem:
  frame_cols_decref(fc);
  goto nomem;
fc_fail:
  frame_cols_decref(fc);
  return NULL;
malformed:
  for (int i = 0; i < ncols; i++) fcol_free(&cols[i]);
  free(cols);
malformed2:
  PyErr_SetString(PyExc_ValueError,
                  "pymizu: malformed frame rebuild state");
  return NULL;
nomem:
  for (int i = 0; i < ncols; i++) fcol_free(&cols[i]);
  free(cols);
  return PyErr_NoMemory();
}

/* The REF staging helper's Frame half (pyinterop.h): the region loan
   anchor of a region-backed Frame, NULL for anything else. */
PyObject *mizu_py_frame_loan(PyObject *obj) {
  if (!PyObject_TypeCheck(obj, &MizuFrameType)) return NULL;
  return ((MizuFrame *) obj)->loan;
}

// The from_arrow constructor and the debug surface --------------------------------

PyDoc_STRVAR(frame_from_arrow_doc,
"from_arrow(obj) -> Frame\n\n\
Construct a frame from an Arrow producer, adopting its buffers\n\
wherever a column's frame layout is the Arrow layout (a single-batch\n\
producer on offset-0 boundaries), so construction copies nothing it\n\
does not have to — nulls included.\n\n\
This is the opt-in zero-copy frame path for Python-to-Python\n\
channels: past the zero-copy floor the frame crosses as one\n\
shared-memory region and arrives region-backed, where a plain Arrow\n\
object pickles (container-exact, but a full copy). The frame reads\n\
back value-exact but type-normalized: a chunked or sliced producer's\n\
columns concatenate, uints widen, nanosecond timestamps rescale to\n\
microseconds, and the received value is a Frame, not the producer's\n\
type.\n\n\
Parameters\n\
----------\n\
obj\n\
    Any ``__arrow_c_stream__`` producer: a pyarrow ``Table`` or\n\
    ``RecordBatchReader``, a polars ``DataFrame``, a duckdb relation.\n\
    ``Table.combine_chunks()`` is the route to the adopt path for a\n\
    chunked table.\n\n\
Returns\n\
-------\n\
    The frame. An adopted column keeps the producer's buffers alive\n\
    for the frame's lifetime (numpy-view semantics: a frame from a\n\
    4 GB table pins it).\n\n\
Raises\n\
------\n\
MizuError\n\
    The Arrow type has no portable home (the column is named), or\n\
    the producer's export failed.\n\n\
Examples\n\
--------\n\
>>> import pyarrow as pa\n\
>>> import pymizu\n\
>>> with pymizu.Channel.create(\n\
...     \"\"\"\n\
... import pymizu\n\
... while True:\n\
...     x = ch.recv()\n\
...     if x is pymizu.CLOSED:\n\
...         break\n\
...     ch.send(x)\n\
... \"\"\"\n\
... ) as ch:\n\
...     f = pymizu.Frame.from_arrow(\n\
...         pa.table({\"x\": pa.array([1.5, None] * 100000)})\n\
...     )\n\
...     ch.send(f)\n\
...     echoed = pa.table(ch.recv(timeout=5))\n\
>>> echoed.column(\"x\").null_count\n\
100000");

static PyObject *Frame_from_arrow(PyObject *Py_UNUSED(cls),
                                  PyObject *const *args,
                                  Py_ssize_t nargs) {
  if (nargs != 1) {
    PyErr_Format(PyExc_TypeError,
                 "from_arrow() takes exactly one argument (%zd given)",
                 nargs);
    return NULL;
  }
  frame_cols *fc = ixs_frame_cols_build(args[0]);
  if (fc == NULL) return NULL;
  MizuFrame *self = (MizuFrame *) MizuFrameType.tp_alloc(&MizuFrameType, 0);
  if (self == NULL) {
    frame_cols_decref(fc);
    return NULL;
  }
  self->fc = fc;
  Py_INCREF(Py_None);
  self->row_names = Py_None;
  self->loan = NULL;
  return (PyObject *) self;
}

/* The test-only introspection surface (the _ShmView.refcount / .flags
   precedent): per-column adopted / known-free flags and the shared
   hold's refcount. */
static PyObject *frame_debug(PyObject *Py_UNUSED(m), PyObject *arg) {
  if (!PyObject_TypeCheck(arg, &MizuFrameType)) {
    PyErr_SetString(PyExc_TypeError, "pymizu: _frame_debug needs a Frame");
    return NULL;
  }
  frame_cols *fc = ((MizuFrame *) arg)->fc;
  PyObject *adopted = PyTuple_New(fc->ncols);
  PyObject *kfree = PyTuple_New(fc->ncols);
  if (adopted == NULL || kfree == NULL) {
    Py_XDECREF(adopted);
    Py_XDECREF(kfree);
    return NULL;
  }
  Py_INCREF(Py_None);
  PyObject *refs = Py_None;
  for (int i = 0; i < fc->ncols; i++) {
    fcol *c = &fc->cols[i];
    PyObject *b = c->ahold != NULL ? Py_True : Py_False;
    Py_INCREF(b);
    PyTuple_SET_ITEM(adopted, i, b);
    b = c->known_free ? Py_True : Py_False;
    Py_INCREF(b);
    PyTuple_SET_ITEM(kfree, i, b);
    if (c->ahold != NULL && refs == Py_None) {
      refs = PyLong_FromSize_t(
        atomic_load_explicit(&c->ahold->refs, memory_order_relaxed));
      if (refs == NULL) {
        Py_DECREF(adopted);
        Py_DECREF(kfree);
        return NULL;
      }
    }
  }
  PyObject *out = PyDict_New();
  if (out == NULL ||
      PyDict_SetItemString(out, "adopted", adopted) < 0 ||
      PyDict_SetItemString(out, "known_free", kfree) < 0 ||
      PyDict_SetItemString(out, "hold_refs", refs) < 0) {
    Py_XDECREF(out);
    Py_DECREF(adopted);
    Py_DECREF(kfree);
    Py_DECREF(refs);
    return NULL;
  }
  Py_DECREF(adopted);
  Py_DECREF(kfree);
  Py_DECREF(refs);
  return out;
}

// The Frame type object --------------------------------------------------------------

static PyMethodDef Frame_methods[] = {
  { "to_dict", (PyCFunction) Frame_to_dict, METH_NOARGS,
    frame_to_dict_doc },
  { "from_arrow", (PyCFunction)(void (*)(void)) Frame_from_arrow,
    METH_CLASS | METH_FASTCALL, frame_from_arrow_doc },
  { "__arrow_c_stream__", (PyCFunction)(void (*)(void))
    Frame_arrow_c_stream, METH_VARARGS | METH_KEYWORDS,
    frame_arrow_stream_doc },
  { "__reduce__", (PyCFunction) Frame_reduce, METH_NOARGS, NULL },
  { NULL, NULL, 0, NULL }
};

static PyGetSetDef Frame_getset[] = {
  { "names", (getter) Frame_names, NULL,
    "The column names (a tuple of str).", NULL },
  { "row_names", (getter) Frame_row_names_get, NULL,
    "The row names metadata: None for automatic, else a list of str or\n"
    "an int32 array. Not carried by to_dict() or the Arrow export.", NULL },
  { NULL, NULL, NULL, NULL, NULL }
};

static PySequenceMethods Frame_as_sequence = {
  .sq_length = (lenfunc) Frame_len,
};

PyTypeObject MizuFrameType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "pymizu.Frame",
  .tp_basicsize = sizeof(MizuFrame),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = "A data.frame's Python home: named columns with a row count.\n"
            "to_dict() gives the column dict without an Arrow library;\n"
            "__arrow_c_stream__ exports to polars / pyarrow / pandas.\n"
            "from_arrow() constructs one from an Arrow producer (the\n"
            "opt-in zero-copy path for Python-to-Python channels).",
  .tp_dealloc = (destructor) Frame_dealloc,
  .tp_repr = (reprfunc) Frame_repr,
  .tp_as_sequence = &Frame_as_sequence,
  .tp_methods = Frame_methods,
  .tp_getset = Frame_getset,
  /* NULL blocks Frame() (a static type on base object never inherits
     tp_new — type_call raises): instances come from a channel receive,
     from_arrow(), or _frame_rebuild */
  .tp_new = NULL,
};

int mizu_py_frame_register(PyObject *m, PyObject *mizu_error) {
  MizuError = mizu_error;
  static PyMethodDef rebuild_def = {
    "_frame_rebuild", frame_rebuild, METH_VARARGS,
    "Rebuild a Frame from its pickle state (facade use only)."
  };
  static PyMethodDef debug_def = {
    "_frame_debug", frame_debug, METH_O,
    "Per-column adopted / known-free flags and the hold refcount "
    "(test-only)."
  };
  PyObject *fn = PyCFunction_New(&rebuild_def, NULL);
  if (fn == NULL) return -1;
  if (PyModule_AddObject(m, "_frame_rebuild", fn) < 0) {
    Py_DECREF(fn);
    return -1;
  }
  frame_rebuild_fn = fn;   /* borrowed stash: __reduce__ returns it */
  fn = PyCFunction_New(&debug_def, NULL);
  if (fn == NULL) return -1;
  if (PyModule_AddObject(m, "_frame_debug", fn) < 0) {
    Py_DECREF(fn);
    return -1;
  }
  if (PyType_Ready(&MizuFrameType) < 0) return -1;
  Py_INCREF(&MizuFrameType);
  return PyModule_AddObject(m, "Frame", (PyObject *) &MizuFrameType);
}
