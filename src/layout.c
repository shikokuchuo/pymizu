/* layout.c — the pymizu -> R layout writers: the shared leaf emitters
 * (an MIZH body plus an optional 'I' attrs blob, or an MIZS four-section
 * block, at a destination pointer) and the top-level SHM_VEC roots built
 * on them. The byte shapes mirror mizu's view.c writers; DESIGN.md's
 * staging policy keeps the string and list-tree layouts binding-side.
 *
 * Value-exactness (the stage_mizh_dim doctrine): a zero-copy tier must
 * be value-identical to what the 'I' writer emits for the same value, so
 * tier choice by size never changes semantics. Anything outside the
 * whitelist declines to the 'I' writer.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <string.h>

#include "pyinterop.h"

static PyObject *MizuErr;   /* pymizu.MizuError (borrowed at registration) */

// The MIZS leaf ------------------------------------------------------------------

/* The probe's byte sum plus the block geometry: the whole block gates
   the tier, not str_bytes alone (1M empty strings are a ~9 MB block of
   offsets). 1 the probe hit, 0 decline. */
static int mizs_leaf_size(PyObject *obj, uint64_t *size) {
  int64_t str_bytes = 0;
  if (!mizu_py_strlist_probe(obj, &str_bytes)) return 0;
  const mizu_mizs_geom g = mizu_mizs_geometry(PyList_GET_SIZE(obj));
  *size = (uint64_t) (g.data + str_bytes);
  return 1;
}

/* The four-section fill at dst (validity, offsets, encoding, data). 0
   written, 1 with MizuError set — pass 1 cached the UTF-8 forms, so the
   re-encode cannot fail, but a write past NULL is not transactional. */
static int mizs_leaf_write(uint8_t *dst, PyObject *obj) {
  const Py_ssize_t n = PyList_GET_SIZE(obj);
  const mizu_mizs_geom g = mizu_mizs_geometry(n);
  uint8_t *validity = dst + g.validity;
  int64_t *offs = (int64_t *) (dst + g.offsets);
  uint8_t *enc = dst + g.encoding;
  uint8_t *data = dst + g.data;
  memset(validity, 0xFF, (size_t) (n + 7) / 8);
  memset(enc, MIZU_CE_UTF8, (size_t) n);
  offs[0] = 0;
  int64_t run = 0;
  for (Py_ssize_t i = 0; i < n; i++) {
    PyObject *elt = PyList_GET_ITEM(obj, i);
    if (elt == Py_None) {
      validity[i >> 3] &= (uint8_t) ~(1u << (i & 7));
      enc[i] = 0;
    } else {
      Py_ssize_t len;
      const char *s = PyUnicode_AsUTF8AndSize(elt, &len);
      if (s == NULL) {
        PyErr_SetString(MizuErr, "pymizu: string staging failed");
        return 1;
      }
      memcpy(data + run, s, (size_t) len);
      run += (int64_t) len;
    }
    offs[i + 1] = run;
  }
  return 0;
}

/* A top-level list[str | None] staged as SHM_VEC: one MIZS layout write
   into a spill region, the region name as the payload. -1 declines to
   the 'I' writer's 0x0b copy; 0 staged; 1 the defensive raise past the
   checkout (the abandoned checkout rolls back at the next verb entry). */
static int mizs_root(PyObject *obj, uint64_t block, mizu_slot_hdr *hdr,
                     uint8_t *payload, uint32_t inline_max, mizu_handle *h) {
  const Py_ssize_t n = PyList_GET_SIZE(obj);
  const uint64_t zc_gate = (uint64_t) inline_max > MIZU_ZC_FLOOR ?
    (uint64_t) inline_max : (uint64_t) MIZU_ZC_FLOOR;
  /* the churn read behind the size gate (the single-read discipline) */
  if (block < zc_gate || mizu_handle_churn(h)) return -1;
  mizu_shm *shm;
  if (mizu_stage_spill_get(h, (size_t) (MIZU_HEADER_SIZE + block),
                           &shm) != MIZU_OK)
    return -1;   /* the copy tier is the fallback (the BUFREF precedent) */
  uint8_t *base = (uint8_t *) shm->addr;
  if (mizs_leaf_write(base + MIZU_HEADER_SIZE, obj) != 0) return 1;
  /* the header last, mizs_write's discipline: the full 64 bytes zeroed
     first (a recycled region carries stale bytes), the fields over it —
     attrs_size / flags / reserved stay zero — then retain_zc stores the
     producer loan over the zeroed refcount word */
  memset(base, 0, MIZU_HEADER_SIZE);
  const uint32_t magic = MIZU_MAGIC_STR;
  const int64_t n64 = (int64_t) n;
  const int64_t sd = (int64_t) block;
  memcpy(base, &magic, 4);
  memcpy(base + 8, &n64, 8);
  memcpy(base + 16, &sd, 8);
  hdr->kind = MIZU_KIND_SHM_VEC;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = mizu_aux_shm_vec(MIZU_TYPE_STR, MIZU_HEADER_SIZE + block);
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain_zc(h, shm);
  return 0;
}

// The MIZH leaf ------------------------------------------------------------------

/* The leaf classification: what one mizh_leaf_* call serves — an
   exact-buffer identity dtype array (any strides, 1..32 dims, an
   int32-bounded shape, an overflow-checked total) or a datetime64 /
   timedelta64 array or scalar (the 'I' writer's unit whitelist, at most
   1-D). The temporal probe runs ahead of any buffer acquisition —
   numpy refuses the buffer protocol for temporal dtypes. */
typedef struct {
  Py_buffer v;          /* identity: the acquired view (held) */
  const cvt_row *row;   /* identity: the dtype's identity row */
  int type;             /* identity: the wire type */
  int temporal;         /* 0 not, 1 datetime64, 2 timedelta64 */
  int unit;             /* temporal: the TU_* unit */
  int nd;
  int32_t dims[32];     /* the {dim} blob's intv form */
  int64_t shape[32];
  int64_t strides[32];  /* bytes */
  uint64_t total;
  int f_contig;         /* the F-order verdict (length-1 axes exempt) */
  /* temporal: the held int64 counts (the .view / .astype dance) */
  PyObject *tview;
  Py_buffer tbv;
  int tgot;
  int64_t tone;
  const int64_t *counts;
  uint64_t nat;         /* the write's NaT count (roots' validity stamp) */
} pymizu_leaf;

static void leaf_release(pymizu_leaf *lf) {
  if (lf->temporal) {
    if (lf->tgot) PyBuffer_Release(&lf->tbv);
    Py_XDECREF(lf->tview);
  } else if (lf->type != 0) {
    PyBuffer_Release(&lf->v);
  }
  lf->type = 0;
  lf->temporal = 0;
}

/* The int64 counts behind a temporal array or scalar: .view("int64")
   with the .astype fallback, the buffer or the index value off the
   result (ixw_numpy_temporal's dance). 0 ok, -1 a value that cannot
   cross (nd > 1 included — the 'I' writer's decline set). */
static int temporal_counts(PyObject *obj, pymizu_leaf *lf) {
  PyObject *view = PyObject_CallMethod(obj, "view", "s", "int64");
  if (view == NULL) {
    PyErr_Clear();
    view = PyObject_CallMethod(obj, "astype", "s", "int64");
    if (view == NULL) {
      PyErr_Clear();
      return -1;
    }
  }
  lf->tview = view;
  lf->counts = &lf->tone;
  lf->total = 1;
  if (PyObject_GetBuffer(view, &lf->tbv, PyBUF_ND | PyBUF_FORMAT) == 0) {
    lf->tgot = 1;
    if (lf->tbv.ndim > 1) return -1;
    lf->counts = (const int64_t *) lf->tbv.buf;
    lf->total = (uint64_t) (lf->tbv.len / 8);
  } else {
    PyErr_Clear();
    PyObject *i = PyNumber_Index(view);
    if (i == NULL) {
      PyErr_Clear();
      return -1;
    }
    lf->tone = PyLong_AsLongLong(i);
    Py_DECREF(i);
  }
  return 0;
}

/* 1 classified (the buffer / the counts view is held; leaf_release),
   0 not layout material (no error). */
static int leaf_classify(PyObject *obj, pymizu_leaf *lf) {
  memset(lf, 0, sizeof *lf);
  int unit = TU_NONE, delta = 0;
  lf->temporal = mizu_py_np_temporal_unit(obj, &unit, &delta);
  if (lf->temporal) {
    if (unit == TU_CAL || unit == TU_SUB || unit == TU_NONE) return 0;
    lf->unit = unit;
    lf->nd = 1;
    if (temporal_counts(obj, lf) < 0) {
      leaf_release(lf);
      return 0;
    }
    return 1;
  }
  if (mizu_py_buffer_subclass_reject(obj)) return 0;
  /* FULL_RO fills strides always (the ixw_buffer precedent) */
  if (PyObject_GetBuffer(obj, &lf->v, PyBUF_FULL_RO) < 0) {
    PyErr_Clear();
    return 0;
  }
  const int nd = lf->v.ndim;
  const int type = mizu_py_wire_type_of_fmt(lf->v.format, lf->v.itemsize);
  if (lf->v.suboffsets != NULL || nd < 1 || nd > 32 || type == 0 ||
      lf->v.len < 0)
    goto fail;
  const uint64_t elt = (uint64_t) mizu_type_elt_size(type);
  if ((uint64_t) lf->v.len % elt != 0) goto fail;  /* exporter padding */
  uint64_t total = 1;
  for (int i = 0; i < nd; i++) {
    if (lf->v.shape[i] < 0 || lf->v.shape[i] > INT32_MAX) goto fail;
    const uint64_t d = (uint64_t) lf->v.shape[i];
    if (d != 0 && total > UINT64_MAX / d) goto fail;
    total *= d;
    lf->dims[i] = (int32_t) lf->v.shape[i];
    lf->shape[i] = (int64_t) lf->v.shape[i];
  }
  if (total != (uint64_t) lf->v.len / elt) goto fail;
  if (lf->v.strides != NULL) {
    for (int i = 0; i < nd; i++)
      lf->strides[i] = (int64_t) lf->v.strides[i];
  } else {
    /* defence against a non-conforming exporter: C-order */
    int64_t acc = (int64_t) lf->v.itemsize;
    for (int i = nd - 1; i >= 0; i--) {
      lf->strides[i] = acc;
      acc *= lf->shape[i];
    }
  }
  /* the F-order verdict: an explicit walk, never a contiguity flag (a
     wire gate must not trust a third-party exporter); axes of length
     <= 1 exempt their stride, numpy's own rule */
  int f_contig = 1;
  int64_t expect = (int64_t) lf->v.itemsize;
  for (int i = 0; i < nd; i++) {
    if (lf->shape[i] > 1 && lf->strides[i] != expect) {
      f_contig = 0;
      break;
    }
    expect *= lf->shape[i];
  }
  lf->type = type;
  lf->row = mizu_py_cvt_for_buffer(lf->v.format, lf->v.itemsize);
  lf->nd = nd;
  lf->total = total;
  lf->f_contig = f_contig;
  return 1;
fail:
  PyBuffer_Release(&lf->v);
  lf->type = 0;
  return 0;
}

// The attribute blobs (complete 'I' streams, the corpus-pinned shapes) -----------

/* The class value's strv form: the 0x04 scalar at length 1
   (ixe_class's shape). */
static size_t blob_class_size(const char *const *names, int n) {
  if (n == 1)
    return mizu_ix_put_str(NULL, names[0], (int32_t) strlen(names[0]));
  size_t sz = mizu_ix_put_strv_begin(NULL, (uint64_t) n);
  for (int i = 0; i < n; i++)
    sz += mizu_ix_put_strelt(NULL, names[i], (int32_t) strlen(names[i]));
  return sz;
}

static size_t blob_class_write(uint8_t *dst, const char *const *names,
                               int n) {
  if (n == 1)
    return mizu_ix_put_str(dst, names[0], (int32_t) strlen(names[0]));
  size_t sz = mizu_ix_put_strv_begin(dst, (uint64_t) n);
  for (int i = 0; i < n; i++)
    sz += mizu_ix_put_strelt(dst + sz, names[i],
                             (int32_t) strlen(names[i]));
  return sz;
}

/* The {dim} blob of an attributed MIZH root (§3.5): byte-identical to
   the 'I' writer's dict portion (the corpus's mat-2x3 form) by
   construction — sized and written through the same put helpers. */
static size_t blob_dim_size(int nd) {
  return mizu_ix_put_header(NULL) + mizu_ix_put_dict_begin(NULL, 1) +
    mizu_ix_put_key(NULL, MIZU_IX_ATTR_DIM, IX_KEYLEN(MIZU_IX_ATTR_DIM)) +
    mizu_ix_put_vec(NULL, MIZU_TYPE_INT, NULL, (uint64_t) nd);
}

static void blob_dim_write(uint8_t *dst, const int32_t *dims, int nd) {
  dst += mizu_ix_put_header(dst);
  dst += mizu_ix_put_dict_begin(dst, 1);
  dst += mizu_ix_put_key(dst, MIZU_IX_ATTR_DIM, IX_KEYLEN(MIZU_IX_ATTR_DIM));
  mizu_ix_put_vec(dst, MIZU_TYPE_INT, dims, (uint64_t) nd);
}

/* The temporal blobs: {class} for Date, {class, tzone = "UTC"} for
   POSIXct, {class, units = "secs"} for difftime — the ixe_date /
   ixe_posixct / ixe_difftime dicts. */
static size_t blob_temporal_size(const pymizu_leaf *lf) {
  static const char *date[1] = { MIZU_IX_CLASS_DATE };
  static const char *posix[2] = { MIZU_IX_CLASS_POSIXCT,
                                  MIZU_IX_CLASS_POSIXT };
  static const char *diff[1] = { MIZU_IX_CLASS_DIFFTIME };
  if (lf->temporal == 2)
    return mizu_ix_put_header(NULL) + mizu_ix_put_dict_begin(NULL, 2) +
      mizu_ix_put_key(NULL, MIZU_IX_ATTR_CLASS,
                      IX_KEYLEN(MIZU_IX_ATTR_CLASS)) +
      blob_class_size(diff, 1) +
      mizu_ix_put_key(NULL, MIZU_IX_ATTR_UNITS,
                      IX_KEYLEN(MIZU_IX_ATTR_UNITS)) +
      mizu_ix_put_str(NULL, MIZU_IX_UNIT_SECS,
                      (int32_t) IX_KEYLEN(MIZU_IX_UNIT_SECS));
  if (lf->unit == TU_DAY || lf->unit == TU_WEEK)
    return mizu_ix_put_header(NULL) + mizu_ix_put_dict_begin(NULL, 1) +
      mizu_ix_put_key(NULL, MIZU_IX_ATTR_CLASS,
                      IX_KEYLEN(MIZU_IX_ATTR_CLASS)) +
      blob_class_size(date, 1);
  return mizu_ix_put_header(NULL) + mizu_ix_put_dict_begin(NULL, 2) +
    mizu_ix_put_key(NULL, MIZU_IX_ATTR_CLASS,
                    IX_KEYLEN(MIZU_IX_ATTR_CLASS)) +
    blob_class_size(posix, 2) +
    mizu_ix_put_key(NULL, MIZU_IX_ATTR_TZONE,
                    IX_KEYLEN(MIZU_IX_ATTR_TZONE)) +
    mizu_ix_put_str(NULL, "UTC", 3);
}

static void blob_temporal_write(uint8_t *dst, const pymizu_leaf *lf) {
  static const char *date[1] = { MIZU_IX_CLASS_DATE };
  static const char *posix[2] = { MIZU_IX_CLASS_POSIXCT,
                                  MIZU_IX_CLASS_POSIXT };
  static const char *diff[1] = { MIZU_IX_CLASS_DIFFTIME };
  dst += mizu_ix_put_header(dst);
  if (lf->temporal == 2) {
    dst += mizu_ix_put_dict_begin(dst, 2);
    dst += mizu_ix_put_key(dst, MIZU_IX_ATTR_CLASS,
                           IX_KEYLEN(MIZU_IX_ATTR_CLASS));
    dst += blob_class_write(dst, diff, 1);
    dst += mizu_ix_put_key(dst, MIZU_IX_ATTR_UNITS,
                           IX_KEYLEN(MIZU_IX_ATTR_UNITS));
    mizu_ix_put_str(dst, MIZU_IX_UNIT_SECS,
                    (int32_t) IX_KEYLEN(MIZU_IX_UNIT_SECS));
    return;
  }
  if (lf->unit == TU_DAY || lf->unit == TU_WEEK) {
    dst += mizu_ix_put_dict_begin(dst, 1);
    dst += mizu_ix_put_key(dst, MIZU_IX_ATTR_CLASS,
                           IX_KEYLEN(MIZU_IX_ATTR_CLASS));
    blob_class_write(dst, date, 1);
    return;
  }
  dst += mizu_ix_put_dict_begin(dst, 2);
  dst += mizu_ix_put_key(dst, MIZU_IX_ATTR_CLASS,
                         IX_KEYLEN(MIZU_IX_ATTR_CLASS));
  dst += blob_class_write(dst, posix, 2);
  dst += mizu_ix_put_key(dst, MIZU_IX_ATTR_TZONE,
                         IX_KEYLEN(MIZU_IX_ATTR_TZONE));
  mizu_ix_put_str(dst, "UTC", 3);
}

/* The leaf sizes: body the MIZH element bytes, blob the attrs stream
   (0 when none — a 1-D identity leaf). */
static uint64_t mizh_leaf_body_size(const pymizu_leaf *lf) {
  return lf->temporal ? lf->total * 8 :
    lf->total * (uint64_t) mizu_type_elt_size(lf->type);
}

static uint64_t mizh_leaf_blob_size(const pymizu_leaf *lf) {
  if (lf->temporal) return blob_temporal_size(lf);
  return lf->nd >= 2 ? blob_dim_size(lf->nd) : 0;
}

/* The body: the identity buffer's F-order flat (the contiguous memcpy
   when the verdict holds, the shared gather otherwise) or the temporal
   counts scaled to double seconds/days — value-identical to the 'I'
   writer's vector body for the same value. NaT (INT64_MIN) becomes the
   NA payload, counted for the root's validity stamp. */
static void mizh_leaf_body_write(uint8_t *dst, pymizu_leaf *lf) {
  if (lf->temporal) {
    double scale;
    if (lf->temporal == 2)   /* a difftime's counts to seconds */
      scale = lf->unit == TU_DAY ? 86400.0 :
        lf->unit == TU_WEEK ? 604800.0 : mizu_py_temporal_scale(lf->unit);
    else if (lf->unit == TU_DAY || lf->unit == TU_WEEK)
      scale = lf->unit == TU_WEEK ? 7.0 : 1.0;   /* Date days */
    else
      scale = mizu_py_temporal_scale(lf->unit);   /* POSIXct seconds */
    lf->nat = 0;
    for (uint64_t i = 0; i < lf->total; i++) {
      if (lf->counts[i] == INT64_MIN) {
        const uint64_t bits = PYMIZU_NA_REAL_BITS;
        memcpy(dst + 8 * i, &bits, 8);
        lf->nat++;
      } else {
        double v = (double) lf->counts[i] * scale;
        memcpy(dst + 8 * i, &v, 8);
      }
    }
    return;
  }
  if (lf->f_contig) {
    memcpy(dst, lf->v.buf,
           (size_t) lf->total * (size_t) mizu_type_elt_size(lf->type));
    return;
  }
  cvt_warn warn;
  memset(&warn, 0, sizeof warn);
  mizu_py_nd_gather(dst, (const uint8_t *) lf->v.buf, lf->nd, lf->shape,
                    lf->strides, lf->row, &warn);
}

static void mizh_leaf_blob_write(uint8_t *dst, const pymizu_leaf *lf) {
  if (lf->temporal) {
    blob_temporal_write(dst, lf);
    return;
  }
  if (lf->nd >= 2) blob_dim_write(dst, lf->dims, lf->nd);
}

/* A top-level buffer or temporal array staged as SHM_VEC: one MIZH
   layout write plus the attrs blob tail — the peer wraps a matrix view
   (no copy, no transpose) or a POSIXct / Date / difftime view. The
   F-order flat the region holds is byte-identical to the 'I' writer's
   vector body for the same value, so tier choice by size never changes
   semantics (the stage_mizh_dim doctrine, generalized past F-contiguity
   by the shared gather). -1 declines to the 'I' writer's value-exact
   copy; 0 staged. */
static int mizh_root(PyObject *obj, pymizu_leaf *lf, mizu_slot_hdr *hdr,
                     uint8_t *payload, uint32_t inline_max, mizu_handle *h) {
  const uint64_t body = mizh_leaf_body_size(lf);
  const uint64_t blob = mizh_leaf_blob_size(lf);
  const uint64_t zc_gate = (uint64_t) inline_max > MIZU_ZC_FLOOR ?
    (uint64_t) inline_max : (uint64_t) MIZU_ZC_FLOOR;
  /* the churn read behind the size gate (the single-read discipline) */
  if (body + blob < zc_gate || mizu_handle_churn(h)) return -1;
  mizu_shm *shm;
  if (mizu_stage_spill_get(h, (size_t) (MIZU_HEADER_SIZE + body + blob),
                           &shm) != MIZU_OK)
    return -1;   /* the copy tier is the fallback (the BUFREF precedent) */
  uint8_t *base = (uint8_t *) shm->addr;
  const int type = lf->temporal ? MIZU_TYPE_REAL : lf->type;
  mizu_mizh_write(base, type, (int64_t) lf->total);
  mizh_leaf_body_write(base + MIZU_HEADER_SIZE, lf);
  /* the validity stamp (roots only): an identity buffer carries no NAs
     (stage_raw's stamp, mizu's type != RAWSXP exclusion); a temporal
     array claims known-NA-free only when the scaling loop saw no NaT —
     otherwise the pair stays absent and R reads the in-band NA payload */
  if (lf->temporal ? lf->nat == 0 : type != MIZU_TYPE_RAW)
    mizu_mizh_validity_set(base, 0, -1);
  mizh_leaf_blob_write(base + MIZU_HEADER_SIZE + body, lf);
  const int64_t asz = (int64_t) blob;   /* attrs_size, over the zeroed word */
  memcpy(base + 16, &asz, 8);
  hdr->kind = MIZU_KIND_SHM_VEC;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = mizu_aux_shm_vec(type, MIZU_HEADER_SIZE + body + blob);
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain_zc(h, shm);
  return 0;
}

/* The top-level layout dispatch (stage_impl's foreign order, after the
   Arrow front-ends, before the 'I' writer): a list[str | None] -> the
   MIZS root; a buffer (any strides) or temporal array -> the MIZH root
   — nd >= 2 in any order, a 1-D strided view, a temporal 1-D array; a
   1-D contiguous identity buffer already took stage_raw. Every decline
   decision returns -1 ahead of the checkout (nothing to roll back); past
   the checkout the only exit is a raised MizuError. */
int pymizu_stage_layout(PyObject *obj, mizu_slot_hdr *hdr,
                        uint8_t *payload, uint32_t inline_max,
                        mizu_handle *h, uint32_t peer_caps) {
  if (PyList_CheckExact(obj)) {
    uint64_t block = 0;
    if ((peer_caps & MIZU_CAP_MIZS) != 0 && mizs_leaf_size(obj, &block))
      return mizs_root(obj, block, hdr, payload, inline_max, h);
    return -1;
  }
  if (!PyObject_CheckBuffer(obj) && mizu_py_np_kind(obj) == 0) return -1;
  pymizu_leaf lf;
  if (!leaf_classify(obj, &lf)) return -1;
  int rc = -1;
  /* the root shapes (a 1-D contiguous identity buffer is stage_raw's) */
  if (lf.temporal || lf.nd >= 2 || !lf.f_contig) {
    const uint64_t blob = mizh_leaf_blob_size(&lf);
    if (blob == 0 || (peer_caps & MIZU_CAP_ATTRS) != 0)
      rc = mizh_root(obj, &lf, hdr, payload, inline_max, h);
  }
  leaf_release(&lf);
  return rc;
}

// The test hook ------------------------------------------------------------------

PyDoc_STRVAR(layout_leaf_doc,
"_layout_leaf(obj) -> (bytes, bytes) | None\n\n\
Test-only: the leaf emitters' (body, blob) bytes for a value — an\n\
MIZS block for a list[str | None], an MIZH body plus attrs blob for a\n\
buffer or temporal array — None on a decline.");

static PyObject *pymizu_layout_leaf_meth(PyObject *Py_UNUSED(m),
                                         PyObject *obj) {
  uint64_t block = 0;
  if (mizs_leaf_size(obj, &block)) {
    uint8_t *buf = (uint8_t *) malloc((size_t) block);
    if (buf == NULL) return PyErr_NoMemory();
    if (mizs_leaf_write(buf, obj) != 0) {
      free(buf);
      return NULL;
    }
    PyObject *body = PyBytes_FromStringAndSize((const char *) buf,
                                               (Py_ssize_t) block);
    free(buf);
    if (body == NULL) return NULL;
    PyObject *empty = PyBytes_FromStringAndSize("", 0);
    PyObject *out = empty != NULL ? PyTuple_Pack(2, body, empty) : NULL;
    Py_XDECREF(empty);
    Py_DECREF(body);
    return out;
  }
  pymizu_leaf lf;
  if (!leaf_classify(obj, &lf)) Py_RETURN_NONE;
  const uint64_t body = mizh_leaf_body_size(&lf);
  const uint64_t blob = mizh_leaf_blob_size(&lf);
  PyObject *out = NULL;
  uint8_t *buf = (uint8_t *) malloc((size_t) (body + blob));
  if (buf == NULL) {
    PyErr_NoMemory();
    goto out;
  }
  mizh_leaf_body_write(buf, &lf);
  mizh_leaf_blob_write(buf + body, &lf);
  {
    PyObject *b = PyBytes_FromStringAndSize((const char *) buf,
                                            (Py_ssize_t) body);
    PyObject *a = PyBytes_FromStringAndSize((const char *) buf + body,
                                            (Py_ssize_t) blob);
    if (b != NULL && a != NULL) out = PyTuple_Pack(2, b, a);
    Py_XDECREF(b);
    Py_XDECREF(a);
  }
  free(buf);
out:
  leaf_release(&lf);
  return out;
}

static PyMethodDef pymizu_layout_methods[] = {
  {"_layout_leaf", pymizu_layout_leaf_meth, METH_O, layout_leaf_doc},
  { NULL, NULL, 0, NULL }
};

int mizu_py_layout_register(PyObject *m, PyObject *mizu_error) {
  MizuErr = mizu_error;
  Py_INCREF(MizuErr);
  for (PyMethodDef *def = pymizu_layout_methods; def->ml_name != NULL;
       def++) {
    PyObject *fn = PyCFunction_New(def, NULL);
    if (fn == NULL || PyModule_AddObject(m, def->ml_name, fn) < 0) {
      Py_XDECREF(fn);
      return -1;
    }
  }
  return 0;
}
