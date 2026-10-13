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

#include <stdatomic.h>
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

// The MIZL tree ------------------------------------------------------------------

/* The 32-byte directory entry (mizu.h's map; the reader's
   mizu_mizl_entry carries the validity pair separately). */
typedef struct {
  int64_t data_offset;
  int64_t data_size;
  int32_t sexptype;
  int32_t attrs_size;
  int64_t length;
} pymizu_mizl_ent;

/* A dict node's names blob: a complete 'I' stream whose value is the
   one-key dict, the strv form scalar at length 1 (ixe_names' shape).
   The keys were UTF-8-validated by the size pass. */
static uint64_t names_blob_size(PyObject *obj) {
  const Py_ssize_t n = PyDict_Size(obj);
  uint64_t sz = mizu_ix_put_header(NULL) +
    mizu_ix_put_dict_begin(NULL, 1) +
    mizu_ix_put_key(NULL, MIZU_IX_ATTR_NAMES,
                    IX_KEYLEN(MIZU_IX_ATTR_NAMES));
  PyObject *k, *v;
  Py_ssize_t pos = 0;
  if (n == 1) {
    PyDict_Next(obj, &pos, &k, &v);
    Py_ssize_t len;
    PyUnicode_AsUTF8AndSize(k, &len);
    sz += mizu_ix_put_str(NULL, NULL, (int32_t) len);
    return sz;
  }
  sz += mizu_ix_put_strv_begin(NULL, (uint64_t) n);
  while (PyDict_Next(obj, &pos, &k, &v)) {
    Py_ssize_t len;
    PyUnicode_AsUTF8AndSize(k, &len);
    sz += mizu_ix_put_strelt(NULL, NULL, (int32_t) len);
  }
  return sz;
}

static void names_blob_write(uint8_t *dst, PyObject *obj) {
  const Py_ssize_t n = PyDict_Size(obj);
  dst += mizu_ix_put_header(dst);
  dst += mizu_ix_put_dict_begin(dst, 1);
  dst += mizu_ix_put_key(dst, MIZU_IX_ATTR_NAMES,
                         IX_KEYLEN(MIZU_IX_ATTR_NAMES));
  PyObject *k, *v;
  Py_ssize_t pos = 0;
  if (n == 1) {
    PyDict_Next(obj, &pos, &k, &v);
    Py_ssize_t len;
    const char *s = PyUnicode_AsUTF8AndSize(k, &len);
    mizu_ix_put_str(dst, s, (int32_t) len);
    return;
  }
  dst += mizu_ix_put_strv_begin(dst, (uint64_t) n);
  while (PyDict_Next(obj, &pos, &k, &v)) {
    Py_ssize_t len;
    const char *s = PyUnicode_AsUTF8AndSize(k, &len);
    dst += mizu_ix_put_strelt(dst, s, (int32_t) len);
  }
}

/* A dict's keys are tree material: every key an exact str, UTF-8
   encodable (a lone surrogate vetoes). */
static int tree_dict_keys_ok(PyObject *obj) {
  PyObject *k, *v;
  Py_ssize_t pos = 0;
  while (PyDict_Next(obj, &pos, &k, &v)) {
    if (!PyUnicode_CheckExact(k)) return 0;
    if (PyUnicode_AsUTF8AndSize(k, NULL) == NULL) {
      PyErr_Clear();
      return 0;
    }
  }
  return 1;
}

/* The element veto/size walk: 1 tree material (size takes the exact
   data span, caps the capability conjunction, remote a direct remote
   leaf — the node's validity-table reserve keys on it), 0 the whole
   tree declines to the 'I' copy. Veto before sum — a decline costs the
   walk prefix, never a data pass. */
static int tree_node_size(PyObject *obj, unsigned depth, uint64_t *size,
                          uint32_t *caps);

static int tree_elt_size(PyObject *elt, unsigned depth, uint64_t *size,
                         uint32_t *caps, int *remote) {
  if (PyBool_Check(elt)) {
    *size = 4;
    return 1;
  }
  if (PyLong_CheckExact(elt)) {
    const long long v = PyLong_AsLongLong(elt);
    if (v == -1 && PyErr_Occurred()) {
      PyErr_Clear();
      return 0;   /* past int64: no portable home */
    }
    /* INT in the R-safe int32 range — INT32_MIN is NA_integer_, so a
       leaf carrying exactly -2^31 would read back NA — else INT64 (the
       'I' reader widens the same values to a class-only integer64;
       INT64_MIN mirrors its NA handling) */
    *size = v > INT32_MIN && v <= INT32_MAX ? 4 : 8;
    return 1;
  }
  if (PyFloat_CheckExact(elt)) {
    *size = 8;
    return 1;
  }
  if (PyComplex_CheckExact(elt)) {
    *size = 16;
    return 1;
  }
  if (PyUnicode_CheckExact(elt)) {
    Py_ssize_t len;
    if (PyUnicode_AsUTF8AndSize(elt, &len) == NULL) {
      PyErr_Clear();   /* a lone surrogate vetoes */
      return 0;
    }
    *size = (uint64_t) (mizu_mizs_geometry(1).data + (int64_t) len);
    *caps |= MIZU_CAP_MIZS;
    return 1;
  }
  if (PyBytes_CheckExact(elt)) {
    *size = (uint64_t) PyBytes_GET_SIZE(elt);
    return 1;
  }
  if (PyList_CheckExact(elt)) {
    uint64_t block = 0;
    if (mizs_leaf_size(elt, &block)) {
      *size = block;
      *caps |= MIZU_CAP_MIZS;
      return 1;
    }
    return tree_node_size(elt, depth + 1, size, caps);
  }
  if (PyTuple_CheckExact(elt) || PyDict_CheckExact(elt))
    return tree_node_size(elt, depth + 1, size, caps);
  /* a re-sendable view crosses as a remote leaf (directory tag 33): the
     identifier span as the data, no local bytes — the referenced
     column's claims resolve at the write. The span's reader bound is
     255 bytes: a longer name vetoes to the by-value copy. */
  {
    mizu_shm *shm = NULL;
    PyObject *view = NULL;
    uint32_t need = 0;
    if (mizu_py_view_ref_probe(elt, &shm, &view, &need)) {
      Py_DECREF(view);
      if (shm->name_len < 1 || shm->name_len > 255) return 0;
      *size = (uint64_t) shm->name_len;
      *caps |= MIZU_CAP_MIZL_REF | need;
      *remote = 1;
      return 1;
    }
  }
  pymizu_leaf lf;
  if (leaf_classify(elt, &lf)) {
    const uint64_t blob = mizh_leaf_blob_size(&lf);
    *size = mizh_leaf_body_size(&lf) + blob;
    if (blob != 0) *caps |= MIZU_CAP_ATTRS;
    leaf_release(&lf);
    return 1;
  }
  return 0;   /* None, non-exact types, conversion dtypes: veto */
}

static int tree_node_size(PyObject *obj, unsigned depth, uint64_t *size,
                          uint32_t *caps) {
  if (depth > MIZU_IX_DEPTH_MAX) return 0;
  const int dict = PyDict_CheckExact(obj);
  if (dict && !tree_dict_keys_ok(obj)) return 0;
  const Py_ssize_t n = dict ? PyDict_Size(obj) :
    PyList_CheckExact(obj) ? PyList_GET_SIZE(obj) : PyTuple_GET_SIZE(obj);
  uint64_t total = MIZU_ALIGN64(MIZU_HEADER_SIZE + 32 * (uint64_t) n);
  uint32_t my_caps = 0;
  int any_remote = 0;
  if (dict) {
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (PyDict_Next(obj, &pos, &k, &v)) {
      uint64_t elt_size = 0;
      int elt_remote = 0;
      if (!tree_elt_size(v, depth, &elt_size, &my_caps, &elt_remote))
        return 0;
      any_remote |= elt_remote;
      total += MIZU_ALIGN64(elt_size);
    }
    total += MIZU_ALIGN64(names_blob_size(obj));
    my_caps |= MIZU_CAP_ATTRS;
  } else {
    for (Py_ssize_t i = 0; i < n; i++) {
      PyObject *elt = PyList_CheckExact(obj) ? PyList_GET_ITEM(obj, i) :
        PyTuple_GET_ITEM(obj, i);
      uint64_t elt_size = 0;
      int elt_remote = 0;
      if (!tree_elt_size(elt, depth, &elt_size, &my_caps, &elt_remote))
        return 0;
      any_remote |= elt_remote;
      total += MIZU_ALIGN64(elt_size);
    }
  }
  /* the validity-table reserve: a remote leaf's claim rides the node's
     n-entry tail (the 63 the alignment slack). The local leaves carry NA
     in-band, so only the remote arm of mizu's reserve ever trips here */
  if (any_remote) total += 63 + 16 * (uint64_t) n;
  *size = total;
  *caps |= my_caps;
  return 1;
}

/* A bare str as a 1-element MIZS block (the string list's fill
   specialized; the UTF-8 form is the size pass's cached one). */
static void mizs_str1_write(uint8_t *dst, const char *s, Py_ssize_t len) {
  const mizu_mizs_geom g = mizu_mizs_geometry(1);
  memset(dst + g.validity, 0xFF, 1);
  memset(dst + g.encoding, MIZU_CE_UTF8, 1);
  int64_t *offs = (int64_t *) (dst + g.offsets);
  offs[0] = 0;
  offs[1] = (int64_t) len;
  memcpy(dst + g.data, s, (size_t) len);
}

/* The element write at base + ent->data_offset, the directory entry
   filled. 0 written, 1 with MizuError set (the defensive re-encode
   check — the size pass validated, so a failure here is a caller bug). */
static int tree_elt_write(uint8_t *base, pymizu_mizl_ent *ent,
                          PyObject *elt);

/* returns the subtree's total bytes (never < 64), 0 with MizuError set */
static uint64_t tree_node_write(uint8_t *base, PyObject *obj);

static int tree_elt_write(uint8_t *base, pymizu_mizl_ent *ent,
                          PyObject *elt) {
  uint8_t *dst = base + ent->data_offset;
  if (PyBool_Check(elt)) {
    const int32_t v = elt == Py_True;
    memcpy(dst, &v, 4);
    ent->sexptype = MIZU_TYPE_LGL;
    ent->attrs_size = 0;
    ent->length = 1;
    ent->data_size = 4;
    return 0;
  }
  if (PyLong_CheckExact(elt)) {
    const long long v = PyLong_AsLongLong(elt);
    if (v > INT32_MIN && v <= INT32_MAX) {
      const int32_t v32 = (int32_t) v;
      memcpy(dst, &v32, 4);
      ent->sexptype = MIZU_TYPE_INT;
      ent->length = 1;
      ent->data_size = 4;
    } else {
      memcpy(dst, &v, 8);
      ent->sexptype = MIZU_TYPE_INT64;
      ent->length = 1;
      ent->data_size = 8;
    }
    ent->attrs_size = 0;
    return 0;
  }
  if (PyFloat_CheckExact(elt)) {
    const double v = PyFloat_AS_DOUBLE(elt);
    memcpy(dst, &v, 8);
    ent->sexptype = MIZU_TYPE_REAL;
    ent->attrs_size = 0;
    ent->length = 1;
    ent->data_size = 8;
    return 0;
  }
  if (PyComplex_CheckExact(elt)) {
    const double re = PyComplex_RealAsDouble(elt);
    const double im = PyComplex_ImagAsDouble(elt);
    memcpy(dst, &re, 8);
    memcpy(dst + 8, &im, 8);
    ent->sexptype = MIZU_TYPE_CPLX;
    ent->attrs_size = 0;
    ent->length = 1;
    ent->data_size = 16;
    return 0;
  }
  if (PyUnicode_CheckExact(elt)) {
    Py_ssize_t len;
    const char *s = PyUnicode_AsUTF8AndSize(elt, &len);
    if (s == NULL) {
      PyErr_SetString(MizuErr, "pymizu: string staging failed");
      return 1;
    }
    mizs_str1_write(dst, s, len);
    ent->sexptype = MIZU_TYPE_STR;
    ent->attrs_size = 0;
    ent->length = 1;
    ent->data_size = mizu_mizs_geometry(1).data + (int64_t) len;
    return 0;
  }
  if (PyBytes_CheckExact(elt)) {
    const Py_ssize_t n = PyBytes_GET_SIZE(elt);
    memcpy(dst, PyBytes_AS_STRING(elt), (size_t) n);
    ent->sexptype = MIZU_TYPE_RAW;
    ent->attrs_size = 0;
    ent->length = n;
    ent->data_size = (int64_t) n;
    return 0;
  }
  if (PyList_CheckExact(elt)) {
    uint64_t block = 0;
    if (mizs_leaf_size(elt, &block)) {
      if (mizs_leaf_write(dst, elt) != 0) return 1;
      ent->sexptype = MIZU_TYPE_STR;
      ent->attrs_size = 0;
      ent->length = PyList_GET_SIZE(elt);
      ent->data_size = (int64_t) block;
      return 0;
    }
  }
  if (PyList_CheckExact(elt) || PyTuple_CheckExact(elt) ||
      PyDict_CheckExact(elt)) {
    const uint64_t written = tree_node_write(dst, elt);
    if (written == 0) return 1;
    ent->sexptype = MIZU_TYPE_VEC;
    ent->attrs_size = 0;
    ent->length = PyDict_CheckExact(elt) ? PyDict_Size(elt) :
      PyList_CheckExact(elt) ? PyList_GET_SIZE(elt) : PyTuple_GET_SIZE(elt);
    ent->data_size = (int64_t) written;
    return 0;
  }
  /* the remote leaf (directory tag 33): the identifier span as the data,
     the referenced column's length / attrs_size resolved off the live
     region, REFHELD OR'd (the holder set widens beyond the direct peer);
     the NA claim lands in the node's validity tail. The size pass
     committed to the span when it counted the identifier, so a
     descriptor that no longer resolves is an error, never a silent
     downgrade to a copy (mizu's discipline). */
  {
    mizu_shm *shm = NULL;
    PyObject *view = NULL;
    uint32_t need = 0;
    if (mizu_py_view_ref_probe(elt, &shm, &view, &need)) {
      int64_t rlength = 0, rattrs = 0;
      int rfree = 0;
      if (mizu_py_root_descriptor(shm, &rlength, &rattrs, &rfree) != 0) {
        Py_DECREF(view);
        PyErr_SetString(MizuErr, "pymizu: a view's identifier no longer "
                        "resolves its region");
        return 1;
      }
      memcpy(dst, shm->name, (size_t) shm->name_len);
      atomic_fetch_or_explicit(mizu_zc_flags_(mizu_shm_addr(shm)),
                               MIZU_ZC_FLAG_REFHELD, memory_order_acq_rel);
      ent->sexptype = PYMIZU_MIZL_TAG_REF;
      ent->attrs_size = (int32_t) rattrs;
      ent->length = rlength;
      ent->data_size = (int64_t) shm->name_len;
      Py_DECREF(view);
      return 0;
    }
  }
  pymizu_leaf lf;
  /* the size pass classified it already — a mismatch is a caller bug */
  if (!leaf_classify(elt, &lf)) {
    PyErr_SetString(MizuErr,
                    "pymizu: the recorded layout leaf is no longer eligible");
    return 1;
  }
  mizh_leaf_body_write(dst, &lf);
  const uint64_t blob = mizh_leaf_blob_size(&lf);
  if (blob != 0)
    mizh_leaf_blob_write(dst + mizh_leaf_body_size(&lf), &lf);
  ent->sexptype = lf.temporal ? MIZU_TYPE_REAL : lf.type;
  ent->attrs_size = (int32_t) blob;
  ent->length = (int64_t) lf.total;
  ent->data_size = (int64_t) (mizh_leaf_body_size(&lf) + blob);
  leaf_release(&lf);
  return 0;
}

/* The validity row's claim for one written element: {0, -1} when the
   element wrote a remote leaf whose referenced column resolves
   known-NA-free off the live region, {0, 0} otherwise (the local leaves
   carry NA in-band — no claim). A descriptor failure keeps the no-claim,
   never an error (mizu's tail tolerance). */
static int64_t tree_remote_claim(const uint8_t *base, Py_ssize_t i,
                                 PyObject *elt) {
  int32_t tag;
  memcpy(&tag, base + MIZU_HEADER_SIZE + 32 * (size_t) i + 16, 4);
  if (tag != PYMIZU_MIZL_TAG_REF) return 0;
  mizu_shm *shm = NULL;
  PyObject *view = NULL;
  uint32_t need = 0;
  int64_t claim = 0;
  if (mizu_py_view_ref_probe(elt, &shm, &view, &need)) {
    int64_t rlength = 0, rattrs = 0;
    int rfree = 0;
    if (mizu_py_root_descriptor(shm, &rlength, &rattrs, &rfree) == 0 &&
        rfree)
      claim = -1;
    Py_DECREF(view);
  }
  return claim;
}

static uint64_t tree_node_write(uint8_t *base, PyObject *obj) {
  const int dict = PyDict_CheckExact(obj);
  const Py_ssize_t n = dict ? PyDict_Size(obj) :
    PyList_CheckExact(obj) ? PyList_GET_SIZE(obj) : PyTuple_GET_SIZE(obj);
  /* the reserved band zeroed (a recycled region carries stale bytes):
     refcount / flags (S4 never set) / the validity pair — {0, 0} absent,
     the tree's leaves carrying NA in-band — / reserved */
  memset(base + 24, 0, MIZU_HEADER_SIZE - 24);
  uint64_t cur = MIZU_ALIGN64(MIZU_HEADER_SIZE + 32 * (uint64_t) n);
  int wrote_remote = 0;
  Py_ssize_t i = 0;
  if (dict) {
    PyObject *k, *v;
    Py_ssize_t pos = 0;
    while (PyDict_Next(obj, &pos, &k, &v)) {
      pymizu_mizl_ent ent;
      ent.data_offset = (int64_t) cur;
      if (tree_elt_write(base, &ent, v) != 0) return 0;
      wrote_remote |= ent.sexptype == PYMIZU_MIZL_TAG_REF;
      memcpy(base + MIZU_HEADER_SIZE + 32 * (size_t) i, &ent, sizeof ent);
      cur += MIZU_ALIGN64((uint64_t) ent.data_size);
      i++;
    }
  } else {
    for (; i < n; i++) {
      PyObject *elt = PyList_CheckExact(obj) ? PyList_GET_ITEM(obj, i) :
        PyTuple_GET_ITEM(obj, i);
      pymizu_mizl_ent ent;
      ent.data_offset = (int64_t) cur;
      if (tree_elt_write(base, &ent, elt) != 0) return 0;
      wrote_remote |= ent.sexptype == PYMIZU_MIZL_TAG_REF;
      memcpy(base + MIZU_HEADER_SIZE + 32 * (size_t) i, &ent, sizeof ent);
      cur += MIZU_ALIGN64((uint64_t) ent.data_size);
    }
  }
  const int64_t attrs_off = (int64_t) cur;
  int64_t attrs_size = 0;
  if (dict) {
    attrs_size = (int64_t) names_blob_size(obj);
    names_blob_write(base + cur, obj);
    cur += MIZU_ALIGN64((uint64_t) attrs_size);
  }
  if (wrote_remote) {
    /* the validity tail: every row the {0, 0} no-claim but a remote
       leaf's {0, -1} upgrade — the table always spent, the header pair
       never speaking for a remote column. */
    int64_t *tab = (int64_t *) malloc(16 * (size_t) n);
    if (tab == NULL) {
      PyErr_NoMemory();
      return 0;
    }
    i = 0;
    if (dict) {
      PyObject *k, *v;
      Py_ssize_t pos = 0;
      while (PyDict_Next(obj, &pos, &k, &v)) {
        tab[2 * i] = 0;
        tab[2 * i + 1] = tree_remote_claim(base, i, v);
        i++;
      }
    } else {
      for (; i < n; i++) {
        PyObject *elt = PyList_CheckExact(obj) ? PyList_GET_ITEM(obj, i) :
          PyTuple_GET_ITEM(obj, i);
        tab[2 * i] = 0;
        tab[2 * i + 1] = tree_remote_claim(base, i, elt);
      }
    }
    const uint64_t tab_off = MIZU_ALIGN64(cur);
    memcpy(base + tab_off, tab, 16 * (size_t) n);
    free(tab);
    mizu_mizh_validity_set(base, (int64_t) tab_off, 0);
    cur = tab_off + 16 * (uint64_t) n;
  }
  uint32_t magic = MIZU_MAGIC_LIST;
  const int32_t n32 = (int32_t) n;
  memcpy(base, &magic, 4);
  memcpy(base + 4, &n32, 4);
  memcpy(base + 8, &attrs_off, 8);
  memcpy(base + 16, &attrs_size, 8);
  return cur;
}

/* A top-level list / tuple / dict staged as SHM_VEC: one MIZL tree
   write into a spill region, the peer wrapping an ALTLIST view — a
   re-sendable view element crossing as a remote leaf (directory tag 33)
   under the peer's MIZU_CAP_MIZL_REF. -1 declines to the 'I' writer's
   value-exact copy (the whitelist veto, a below-floor tree, a short
   peer, churn, a region failure); 0 staged; 1 the defensive raise past
   the checkout. */
static int mizl_root(PyObject *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                     uint32_t inline_max, mizu_handle *h,
                     uint32_t peer_caps) {
  uint64_t total = 0;
  uint32_t caps = MIZU_CAP_MIZL;
  if (!tree_node_size(obj, 0, &total, &caps)) return -1;
  if ((peer_caps & caps) != caps) return -1;
  const uint64_t zc_gate = (uint64_t) inline_max > MIZU_ZC_FLOOR ?
    (uint64_t) inline_max : (uint64_t) MIZU_ZC_FLOOR;
  /* the churn read behind the size gate (the single-read discipline) */
  if (total < zc_gate || mizu_handle_churn(h)) return -1;
  mizu_shm *shm;
  if (mizu_stage_spill_get(h, (size_t) total, &shm) != MIZU_OK)
    return -1;   /* the copy tier is the fallback (the BUFREF precedent) */
  uint8_t *base = (uint8_t *) shm->addr;
  const uint64_t used = tree_node_write(base, obj);
  if (used == 0) return 1;
  hdr->kind = MIZU_KIND_SHM_VEC;
  hdr->len = (uint32_t) shm->name_len;
  /* aux carries the write's actual total — the size pass's reserve
     counts the remote leaves' table slack */
  hdr->aux = mizu_aux_shm_vec(MIZU_TYPE_VEC, used);
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain_zc(h, shm);
  return 0;
}

/* The top-level layout dispatch (stage_impl's foreign order, after the
   Arrow front-ends, before the 'I' writer): a list[str | None] -> the
   MIZS root; a buffer (any strides) or temporal array -> the MIZH root
   — nd >= 2 in any order, a 1-D strided view, a temporal 1-D array; a
   1-D contiguous identity buffer already took stage_raw; a list / tuple
   / dict -> the MIZL tree. Every decline decision returns -1 ahead of
   the checkout (nothing to roll back); past the checkout the only exit
   is a raised MizuError. */
int pymizu_stage_layout(PyObject *obj, mizu_slot_hdr *hdr,
                        uint8_t *payload, uint32_t inline_max,
                        mizu_handle *h, uint32_t peer_caps) {
  if (PyList_CheckExact(obj)) {
    uint64_t block = 0;
    if (mizs_leaf_size(obj, &block)) {
      if ((peer_caps & MIZU_CAP_MIZS) != 0)
        return mizs_root(obj, block, hdr, payload, inline_max, h);
      return -1;   /* a no-cap peer takes the 0x0b copy */
    }
    return mizl_root(obj, hdr, payload, inline_max, h, peer_caps);
  }
  if (PyTuple_CheckExact(obj) || PyDict_CheckExact(obj))
    return mizl_root(obj, hdr, payload, inline_max, h, peer_caps);
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
