/* shmframe.c — the MIZL frame writer (Phase 3.6) and the export-provenance
   registry (Phase 3.8).

   The writer is the mirror of mizu's view-layer layout write (view.c's
   mizu_view_nested_write): an attr-qualified Arrow frame past the zero-copy
   floor stages as one MIZL region — the 64-byte header, one 32-byte
   directory entry per column, the column blobs at 64-byte-aligned offsets,
   the root frame blob, then the validity tail — framed SHM_VEC with the
   exact used byte count. The inline 'I' frame shape stays the tier below
   the floor, and every decline the inline writer has (an unsupported
   column kind) was already found at classification, so the write itself
   never declines. The copy tiers are the fallback on a region failure,
   never an error.

   The registry records, per live Arrow export acquisition of a
   region-backed Frame, the exact ArrowArrays handed out (formats, lengths,
   offsets, buffer pointers, null counts) plus the column names. An outgoing
   frame whose columns all match one acquisition's record — an unmodified
   R -> consumer -> R round trip — stages as the region's REF, zero payload
   bytes; anything less falls through to the layout write. The failure
   direction is always the copy, never wrong data. */

#include <string.h>

#include "pyshmframe.h"

static PyObject *MizuError;
static PyObject *MizuDeclinedError;

void mizu_py_shmframe_register(PyObject *mizu_error,
                               PyObject *declined_error) {
  MizuError = mizu_error;
  MizuDeclinedError = declined_error;
}

/* The MIZL directory entry (mizu.h's wire format; view.c's mizu_view_elem). */
typedef struct {
  int64_t data_offset;   /* 64-byte aligned */
  int64_t data_size;
  int32_t sexptype;
  int32_t attrs_size;
  int64_t length;
} sf_elem;

_Static_assert(sizeof(sf_elem) == 32, "the MIZL directory entry is 32 bytes");

/* R's verbatim NA_real_ bits, for the Date/POSIXct leaves' missing
   sentinels (the corpus's pin; the core's quiet-bit-set twin would read
   as NA too, but a relay compares payloads bitwise). */
static void sf_store_na_r(uint8_t *dst) {
  const uint64_t bits = PYMIZU_NA_REAL_BITS;
  memcpy(dst, &bits, 8);
}

/* A batch's exact null count (Arrow's -1 "not computed" resolved). */
static int64_t sf_batch_nulls(const ArrowArray *a) {
  if (a->null_count == 0 || a->buffers[0] == NULL) return 0;
  if (a->null_count > 0) return a->null_count;
  const uint8_t *bm = (const uint8_t *) a->buffers[0];
  int64_t n = 0;
  for (int64_t k = 0; k < a->length; k++) {
    int64_t i = a->offset + k;
    n += !((bm[i >> 3] >> (i & 7)) & 1);
  }
  return n;
}

// The size pass -----------------------------------------------------------------

typedef struct {
  int64_t body;      /* the leaf's data bytes, blob excluded */
  int64_t blob;      /* the leaf attrs blob size (0 for plain columns) */
  int64_t nulls;     /* Arrow nulls across the column's batches */
  int64_t str_bytes; /* PC_STR: the packed byte total */
  const char *tz;    /* PC_TS: the tzone the leaf blob writes */
  int64_t ref_attrs; /* remote leaf: the referenced leaf's attrs blob size */
  int64_t ref_claim; /* remote leaf: the validity claim (-1 known-NA-free,
                        else 0) */
} sf_col;

/* The packed string byte total: per batch the offset span when no nulls
   (a null's span must collapse, so a batch with nulls walks). */
static int sf_size_str(ixs *x, int col, sf_col *c) {
  const ixs_pcol *pc = &x->cols[col];
  int64_t bytes = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    const ArrowArray *a = x->hold.arrs[b].children[col];
    if (a->length == 0) continue;
    if (pc->str_form != 3 && sf_batch_nulls(a) == 0) {
      if (pc->str_form == 1) {
        const int32_t *offs = (const int32_t *) a->buffers[1];
        int32_t o0 = offs[a->offset], o1 = offs[a->offset + a->length];
        if (o0 < 0 || o1 < o0) return -1;
        bytes += o1 - o0;
      } else {
        const int64_t *offs = (const int64_t *) a->buffers[1];
        int64_t o0 = offs[a->offset], o1 = offs[a->offset + a->length];
        if (o0 < 0 || o1 < o0) return -1;
        bytes += o1 - o0;
      }
      continue;
    }
    for (int64_t k = 0; k < a->length; k++) {
      const uint8_t *s;
      int32_t len;
      int rc = arrow_str_at(a, k, pc->str_form, &s, &len);
      if (rc < 0) return -1;
      if (rc > 0) bytes += len;
    }
  }
  c->str_bytes = bytes;
  return 0;
}

static int sf_size_col(ixs *x, int col, sf_col *c, int same_lang) {
  const ixs_pcol *pc = &x->cols[col];
  memset(c, 0, sizeof(*c));
  switch (pc->kind) {
  case PC_CVT:
    c->body = x->hold.rows * pc->row->w_out;
    break;
  case PC_STR:
    if (sf_size_str(x, col, c) < 0) {
      PyErr_SetString(MizuError, "pymizu: invalid Arrow string export "
                      "(a malformed offset or view record)");
      return -1;
    }
    c->body = mizu_mizs_geometry(x->hold.rows).data + c->str_bytes;
    break;
  case PC_DICT:
    c->body = x->hold.rows * 4;
    c->blob = (int64_t) mizu_py_blob_factor(NULL, x->lev_bytes[col],
                                            x->lev_offs[col], x->nlevs[col]);
    break;
  case PC_DATE:
    c->body = x->hold.rows * 8;
    c->blob = (int64_t) mizu_py_blob_date(NULL);
    break;
  case PC_TS:
    c->body = x->hold.rows * 8;
    /* same-language (a Frame's own stream): the tzone verbatim off the
       format; foreign: the classified tz (naive writes "UTC", §1.0) */
    c->tz = same_lang ? x->schema.children[col]->format + 4 : pc->tz;
    c->blob = (int64_t) mizu_py_blob_ts(NULL, c->tz);
    break;
  case PC_TD:
    c->body = x->hold.rows * 8;
    c->blob = (int64_t) mizu_py_blob_difftime(NULL);
    break;
  default:
    PyErr_SetString(MizuError, "pymizu: unknown frame column kind");
    return -1;
  }
  if (c->blob < 0 || (c->blob == 0 && pc->kind != PC_CVT &&
                      pc->kind != PC_STR))
    return -1;   /* a blob emitter failed (exception set) */
  for (size_t b = 0; b < x->hold.nb; b++)
    c->nulls += sf_batch_nulls(x->hold.arrs[b].children[col]);
  return 0;
}

// The write pass ----------------------------------------------------------------

static void sf_write_cvt(uint8_t *dst, ixs *x, int col, cvt_warn *warn) {
  const ixs_pcol *pc = &x->cols[col];
  size_t done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    const ArrowArray *a = x->hold.arrs[b].children[col];
    size_t n = (size_t) a->length;
    if (n == 0) continue;
    const uint8_t *data = (const uint8_t *) a->buffers[1];
    const uint8_t *valid = a->null_count != 0 ?
      (const uint8_t *) a->buffers[0] : NULL;
    uint64_t off = (uint64_t) a->offset;
    if (pc->row->cvt == CVT_BIT_LGL) {
      mizu_py_cvt_convert(dst + done, data, valid, off, n, pc->row, warn);
    } else {
      mizu_py_cvt_convert(dst + done, data + off * pc->row->w_in, valid,
                          off, n, pc->row, warn);
    }
    done += n * pc->row->w_out;
  }
}

/* The MIZS string block: validity bitmap (present bits), i64 offsets, the
   per-string encoding bytes, the packed bytes — each section 64-byte
   aligned from the block start. Present elements mark CE_UTF8, an NA
   spans zero bytes. */
static void sf_write_str(uint8_t *block, ixs *x, int col, int64_t rows,
                         int has_nulls) {
  const ixs_pcol *pc = &x->cols[col];
  mizu_mizs_geom g = mizu_mizs_geometry(rows);
  uint8_t *validity = block + g.validity;
  int64_t *offs = (int64_t *) (block + g.offsets);
  uint8_t *enc = block + g.encoding;
  uint8_t *data = block + g.data;
  memset(validity, 0xFF, (size_t) (rows + 7) / 8);
  memset(enc, MIZU_CE_UTF8, (size_t) rows);
  int64_t done = 0, run = 0;
  offs[0] = 0;
  if (!has_nulls && pc->str_form != 3) {
    /* no nulls: per-batch rebase, the bytes one memcpy per batch */
    for (size_t b = 0; b < x->hold.nb; b++) {
      const ArrowArray *a = x->hold.arrs[b].children[col];
      if (a->length == 0) continue;
      if (pc->str_form == 1) {
        const int32_t *so = (const int32_t *) a->buffers[1];
        int32_t base0 = so[a->offset];
        for (int64_t k = 0; k < a->length; k++)
          offs[done + k + 1] = run + (so[a->offset + k + 1] - base0);
        int32_t blen = so[a->offset + a->length] - base0;
        memcpy(data + run, (const uint8_t *) a->buffers[2] + base0,
               (size_t) blen);
        run += blen;
      } else {
        const int64_t *so = (const int64_t *) a->buffers[1];
        int64_t base0 = so[a->offset];
        for (int64_t k = 0; k < a->length; k++)
          offs[done + k + 1] = run + (so[a->offset + k + 1] - base0);
        int64_t blen = so[a->offset + a->length] - base0;
        memcpy(data + run, (const uint8_t *) a->buffers[2] + base0,
               (size_t) blen);
        run += blen;
      }
      done += a->length;
    }
    return;
  }
  /* the gather: string_view, or nulls (a null slot's span collapses to
     zero bytes), the encoding bytes and validity cleared in the walk */
  for (size_t b = 0; b < x->hold.nb; b++) {
    const ArrowArray *a = x->hold.arrs[b].children[col];
    for (int64_t k = 0; k < a->length; k++) {
      const uint8_t *s;
      int32_t len;
      /* the size pass already validated every element (a malformed
         record aborts the write), so this is exactly == 1 */
      if (arrow_str_at(a, k, pc->str_form, &s, &len) == 1) {
        memcpy(data + run, s, (size_t) len);
        run += len;
      } else {
        int64_t i = done + k;
        validity[i >> 3] &= (uint8_t) ~(1u << (i & 7));
        enc[i] = 0;
      }
      offs[done + k + 1] = run;
    }
    done += a->length;
  }
}

/* A dictionary column: the INT32 leaf of 1-based R codes (NA = INT_MIN). */
static void sf_write_dict(uint8_t *dst, ixs *x, int col) {
  const ixs_pcol *pc = &x->cols[col];
  int64_t done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    const ArrowArray *a = x->hold.arrs[b].children[col];
    const uint8_t *idx = (const uint8_t *) a->buffers[1];
    for (int64_t k = 0; k < a->length; k++) {
      int32_t v;
      if (!arrow_valid(a, k)) {
        v = MIZU_NA_INT32;
      } else {
        int64_t i = a->offset + k, c;
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
      memcpy(dst + 4 * (done + k), &v, 4);
    }
    done += a->length;
  }
}

/* A date32 column: the REAL leaf of epoch-day doubles (NaT = NA payload). */
static void sf_write_date(uint8_t *dst, ixs *x, int col) {
  int64_t done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    const ArrowArray *a = x->hold.arrs[b].children[col];
    const int32_t *days = (const int32_t *) a->buffers[1];
    for (int64_t k = 0; k < a->length; k++) {
      if (!arrow_valid(a, k)) {
        sf_store_na_r(dst + 8 * (done + k));
      } else {
        double v = (double) days[a->offset + k];
        memcpy(dst + 8 * (done + k), &v, 8);
      }
    }
    done += a->length;
  }
}

/* A timestamp column: the REAL leaf of epoch-second doubles. */
static void sf_write_ts(uint8_t *dst, ixs *x, int col) {
  const ixs_pcol *pc = &x->cols[col];
  int64_t done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    const ArrowArray *a = x->hold.arrs[b].children[col];
    const int64_t *counts = (const int64_t *) a->buffers[1];
    for (int64_t k = 0; k < a->length; k++) {
      if (!arrow_valid(a, k)) {
        sf_store_na_r(dst + 8 * (done + k));
      } else {
        double v = (double) counts[a->offset + k] * pc->ts_scale;
        memcpy(dst + 8 * (done + k), &v, 8);
      }
    }
    done += a->length;
  }
}

/* The duration half of sf_write_ts: the same seconds conversion, the
   difftime blob the only difference (written by the caller). */
static void sf_write_td(uint8_t *dst, ixs *x, int col) {
  sf_write_ts(dst, x, col);
}

/* One leaf's validity bitmap into the section: verbatim for a single
   offset-0 batch, else memset-present and clear at the null positions. */
static void sf_bitmap_write(uint8_t *dst, ixs *x, int col, int64_t rows) {
  const size_t nb = ((size_t) rows + 7) / 8;
  if (x->hold.nb == 1) {
    const ArrowArray *a = x->hold.arrs[0].children[col];
    if (a->offset == 0) {
      memcpy(dst, a->buffers[0], nb);
      return;
    }
  }
  memset(dst, 0xFF, nb);
  int64_t done = 0;
  for (size_t b = 0; b < x->hold.nb; b++) {
    const ArrowArray *a = x->hold.arrs[b].children[col];
    if (a->null_count != 0 && a->buffers[0] != NULL) {
      for (int64_t k = 0; k < a->length; k++) {
        if (!arrow_valid(a, k)) {
          int64_t i = done + k;
          dst[i >> 3] &= (uint8_t) ~(1u << (i & 7));
        }
      }
    }
    done += a->length;
  }
}

// The writer driver ---------------------------------------------------------------

/* The provenance registry's record types, ahead of the writer for the
   remote-leaf claims (the registry itself is below). */
#define PYMIZU_PV_MAX 256
#define PV_NBUFS 4

typedef struct {
  const char *name;      /* borrowed: the export's name store */
  const char *fmt;       /* borrowed: a literal or the export's fmts */
  int64_t length, offset, null_count, n_buffers;
  const void *bufs[PV_NBUFS];
  int has_dict;
  const char *dfmt;
  int64_t dlength, doffset, dnull_count, dn_buffers;
  const void *dbufs[PV_NBUFS];
} pv_col;

typedef struct pv_entry {
  const void *owner;     /* the frame_export (the unregister key) */
  mizu_shm *acq;         /* borrowed: the frame_export owns the mapping */
  long pid;              /* the fork guard */
  int ncols;
  pv_col *cols;
  struct pv_entry *next;
} pv_entry;

/* The identifier span of a remote leaf: name[leaf] with the 1-based leaf
   index — at most MIZU_NAME_MAX + 13 bytes (dst sized for it). */
static size_t sf_ref_span(char *dst, const mizu_shm *acq, int leaf) {
  memcpy(dst, acq->name, (size_t) acq->name_len);
  return (size_t) acq->name_len +
    (size_t) snprintf(dst + acq->name_len, 14, "[%d]", leaf);
}

/* The writer driver. hits (NULL on the same-language path, or when the
   peer is short of MIZU_CAP_MIZL_REF) is pymizu_shmframe_pv_match_cols's
   per-column provenance map: a matched column goes out as a remote leaf
   (directory tag 33) — the identifier span, the referenced leaf's attrs
   size and validity claim as resolved, no body, blob or bitmap — and each
   distinct referenced region takes one REFHELD OR per send. */
int pymizu_shmframe_write(ixs *x, char **names, mizu_slot_hdr *hdr,
                          uint8_t *payload, uint32_t inline_max,
                          mizu_handle *h, int same_lang,
                          struct pv_entry const **hits) {
  const int ncols = x->ncols;
  const int64_t rows = x->hold.rows;
  int rc = 1;
  sf_col *cols = calloc((size_t) ncols, sizeof(*cols));
  int64_t *tab = calloc((size_t) ncols * 2, sizeof(int64_t));
  if (cols == NULL || tab == NULL) {
    PyErr_NoMemory();
    goto out;
  }
  /* the size pass: per-column bodies and blobs, the root blob, the tail */
  size_t cur = MIZU_ALIGN64(MIZU_HEADER_SIZE + 32 * (size_t) ncols);
  int any_atomic_nulls = 0;
  int any_remote_unknown = 0;
  for (int i = 0; i < ncols; i++) {
    if (hits != NULL && hits[i] != NULL) {
      /* a remote leaf: the claims read off the referenced leaf (the match
         pins its export — directly, or via another column of the frame —
         so the mapping is valid) */
      mizu_mizl_entry re;
      if (mizu_mizl_elem(mizu_shm_addr(hits[i]->acq),
                         mizu_shm_size(hits[i]->acq), (int64_t) i,
                         &re) == 0) {
        const int32_t rtag = re.sexptype & ~(int32_t) MIZU_MIZL_S4;
        char spanbuf[MIZU_NAME_MAX + 14];
        cols[i].body = (int64_t) sf_ref_span(spanbuf, hits[i]->acq, i + 1);
        cols[i].blob = 0;
        cols[i].nulls = 0;
        cols[i].ref_attrs = re.attrs_size;
        cols[i].ref_claim = mizu_type_elt_size(rtag) != 0 &&
          re.valid[0] == 0 && re.valid[1] == -1 ? -1 : 0;
        if (cols[i].ref_claim != -1) any_remote_unknown = 1;
      } else {
        hits[i] = NULL;   /* a torn referenced tree: the layout write takes it */
      }
    }
    if (hits == NULL || hits[i] == NULL) {
      if (sf_size_col(x, i, &cols[i], same_lang) < 0) goto out;
    }
    cur += MIZU_ALIGN64((size_t) (cols[i].body + cols[i].blob));
    if (x->cols[i].kind != PC_STR && cols[i].nulls > 0)
      any_atomic_nulls = 1;
  }
  const int64_t root_blob = (int64_t) mizu_py_blob_frame(
    NULL, names, ncols, rows, x->row_names);
  if (root_blob <= 0) goto out;
  const size_t attrs_off = cur;
  cur += MIZU_ALIGN64((size_t) root_blob);
  /* the tail rides whenever a table is emitted: local nulls, or a remote
     column that cannot claim known-NA-free (its row is the claim alone —
     a remote column adds no bitmap and no count to the header validity) */
  if (any_atomic_nulls || any_remote_unknown) {
    for (int i = 0; i < ncols; i++) {
      if (x->cols[i].kind == PC_STR || cols[i].nulls == 0) continue;
      cur = MIZU_ALIGN64(cur) + ((size_t) rows + 7) / 8;
    }
    cur = MIZU_ALIGN64(cur) + 16 * (size_t) ncols;
  }
  const size_t total = cur;

  /* the reserve: one region per frame (the staging seam's checkout) */
  mizu_stage_reap(h);
  mizu_shm *shm;
  if (mizu_stage_spill_get(h, total, &shm) != MIZU_OK) {
    rc = -1;   /* the copy tiers take it from here */
    goto out;
  }
  uint8_t *base = (uint8_t *) mizu_shm_addr(shm);
  /* the reserved header band [24-63]: a recycled region carries stale
     words — the zc refcount included, which retain_zc stores after */
  memset(base + 24, 0, MIZU_HEADER_SIZE - 24);

  cvt_warn warn = { 0, 0 };
  cur = MIZU_ALIGN64(MIZU_HEADER_SIZE + 32 * (size_t) ncols);
  for (int i = 0; i < ncols; i++) {
    const ixs_pcol *pc = &x->cols[i];
    sf_elem entry;
    entry.data_offset = (int64_t) cur;
    entry.length = rows;
    uint8_t *dst = base + cur;
    if (hits != NULL && hits[i] != NULL) {
      /* the remote leaf: the identifier span, the referenced leaf's attrs
         size as resolved — no local bytes */
      entry.sexptype = PYMIZU_MIZL_TAG_REF;
      entry.attrs_size = (int32_t) cols[i].ref_attrs;
      entry.data_size = cols[i].body;
      sf_ref_span((char *) dst, hits[i]->acq, i + 1);
      /* one REFHELD OR per distinct referenced region per send: the
         holder set widens beyond the direct peer (the ref_emit pattern) */
      int seen = 0;
      for (int k = 0; k < i; k++)
        if (hits[k] == hits[i]) seen = 1;
      if (!seen)
        atomic_fetch_or_explicit(
          mizu_zc_flags_(mizu_shm_addr(hits[i]->acq)), MIZU_ZC_FLAG_REFHELD,
          memory_order_acq_rel);
    } else {
      entry.attrs_size = (int32_t) cols[i].blob;
      switch (pc->kind) {
      case PC_CVT:
        entry.sexptype = pc->row->wire;
        sf_write_cvt(dst, x, i, &warn);
        break;
      case PC_STR:
        entry.sexptype = MIZU_TYPE_STR;
        sf_write_str(dst, x, i, rows, cols[i].nulls > 0);
        break;
      case PC_DICT:
        entry.sexptype = MIZU_TYPE_INT;
        sf_write_dict(dst, x, i);
        mizu_py_blob_factor(dst + cols[i].body, x->lev_bytes[i],
                            x->lev_offs[i], x->nlevs[i]);
        break;
      case PC_DATE:
        entry.sexptype = MIZU_TYPE_REAL;
        sf_write_date(dst, x, i);
        mizu_py_blob_date(dst + cols[i].body);
        break;
      case PC_TS:
        entry.sexptype = MIZU_TYPE_REAL;
        sf_write_ts(dst, x, i);
        mizu_py_blob_ts(dst + cols[i].body, cols[i].tz);
        break;
      case PC_TD:
        entry.sexptype = MIZU_TYPE_REAL;
        sf_write_td(dst, x, i);
        mizu_py_blob_difftime(dst + cols[i].body);
        break;
      }
      entry.data_size = cols[i].body + cols[i].blob;
    }
    memcpy(base + MIZU_HEADER_SIZE + 32 * (size_t) i, &entry,
           sizeof(entry));
    cur += MIZU_ALIGN64((size_t) entry.data_size);
  }

  /* the root frame blob (names, class, row.names) */
  mizu_py_blob_frame(base + cur, names, ncols, rows, x->row_names);
  cur += MIZU_ALIGN64((size_t) root_blob);

  /* the validity tail: a bitmap per NA-ful local atomic leaf, then the
     table — a clean run collapses to the header's known-NA-free and no
     tail bytes, but only when every remote column claims known-NA-free
     too (the header never speaks for a remote column); a string leaf's
     nulls ride its block, its entry {0, 0} */
  int64_t total_nulls = 0;
  for (int i = 0; i < ncols; i++) {
    if (hits != NULL && hits[i] != NULL) {
      tab[2 * i] = 0;
      tab[2 * i + 1] = cols[i].ref_claim;
      continue;
    }
    if (x->cols[i].kind == PC_STR) {
      tab[2 * i] = 0;
      tab[2 * i + 1] = 0;
      continue;
    }
    if (cols[i].nulls == 0) {
      tab[2 * i] = 0;
      tab[2 * i + 1] = -1;
      continue;
    }
    size_t off = MIZU_ALIGN64(cur);
    sf_bitmap_write(base + off, x, i, rows);
    tab[2 * i] = (int64_t) off;
    tab[2 * i + 1] = cols[i].nulls;
    total_nulls += cols[i].nulls;
    cur = off + ((size_t) rows + 7) / 8;
  }
  if (total_nulls > 0 || any_remote_unknown) {
    size_t tab_off = MIZU_ALIGN64(cur);
    memcpy(base + tab_off, tab, 16 * (size_t) ncols);
    mizu_mizh_validity_set(base, (int64_t) tab_off, total_nulls);
    cur = tab_off + 16 * (size_t) ncols;
  } else {
    mizu_mizh_validity_set(base, 0, -1);
  }

  /* the header */
  {
    uint32_t magic = MIZU_MAGIC_LIST;
    int32_t n32 = (int32_t) ncols;
    int64_t ao64 = (int64_t) attrs_off;
    int64_t as64 = root_blob;
    memcpy(base, &magic, 4);
    memcpy(base + 4, &n32, 4);
    memcpy(base + 8, &ao64, 8);
    memcpy(base + 16, &as64, 8);
  }

  /* the SHM_VEC frame: the region name, the type tag and the exact used
     byte count in aux */
  hdr->kind = MIZU_KIND_SHM_VEC;
  hdr->len = (uint32_t) shm->name_len;
  hdr->aux = mizu_aux_shm_vec(MIZU_TYPE_VEC, (uint64_t) cur);
  memcpy(payload, shm->name, shm->name_len);
  mizu_stage_retain_zc(h, shm);
  rc = mizu_py_cvt_warn(&warn);
out:
  free(cols);
  free(tab);
  (void) inline_max;
  return rc;
}

// The export-provenance registry (Phase 3.8) ----------------------------------

/* A borrow is not an unmodified column: polars keeps region buffers while
   changing what the column means (new nulls, a new type, a new name, a
   shorter selection; string columns re-view as string_view). So the match
   is the export record, not containment — per acquisition, per leaf: the
   format, the length, the offset, the null count, the column names;
   fixed-width columns by buffer identity, a re-viewed string column by
   pv_col_match_strview's read-only verification — an O(n) scan where the
   rest are O(1) compares, per frame send rather than per element, its
   provenance gate keeping the fresh-buffer failure modes off it. Entries
   unregister at the acquisition's last release, which runs in pure C on
   any thread, so the table takes a lock. The lock is also load-bearing
   for the scan's memory safety: an unregister runs before its mapping
   closes, so the row reads through recorded pointers must hold it against
   a concurrent teardown — that, not the scan's cost, is why verifying
   outside the lock would need a refcount the registry doesn't have.
   Registration is best-effort: a dropped record costs one layout write,
   never data. */

static pv_entry *pv_head;
static int pv_count;
static PyThread_type_lock pv_lock;

void pymizu_shmframe_pv_register(mizu_shm *acq, int ncols, char **names,
                                 const ArrowSchema *col_s,
                                 const ArrowArray *col_a, const void *owner) {
  if (ncols < 1) return;
  pv_entry *e = calloc(1, sizeof(*e));
  pv_col *cols = calloc((size_t) ncols, sizeof(*cols));
  if (e == NULL || cols == NULL) {
    free(e);
    free(cols);
    return;
  }
  e->cols = cols;
  for (int i = 0; i < ncols; i++) {
    pv_col *c = &cols[i];
    const ArrowArray *a = &col_a[i];
    c->name = names[i];
    c->fmt = col_s[i].format;
    c->length = a->length;
    c->offset = a->offset;
    c->null_count = a->null_count;
    c->n_buffers = a->n_buffers;
    if (c->n_buffers > PV_NBUFS) goto drop;
    for (int64_t k = 0; k < a->n_buffers; k++) c->bufs[k] = a->buffers[k];
    if (col_s[i].dictionary != NULL) {
      const ArrowArray *d = a->dictionary;
      if (d == NULL) goto drop;
      c->has_dict = 1;
      c->dfmt = col_s[i].dictionary->format;
      c->dlength = d->length;
      c->doffset = d->offset;
      c->dnull_count = d->null_count;
      c->dn_buffers = d->n_buffers;
      if (c->dn_buffers > PV_NBUFS) goto drop;
      for (int64_t k = 0; k < d->n_buffers; k++) c->dbufs[k] = d->buffers[k];
    }
  }
  e->owner = owner;
  e->acq = acq;
  e->pid = mizu_self_pid();
  e->ncols = ncols;
  if (pv_lock == NULL) pv_lock = PyThread_allocate_lock();
  if (pv_lock == NULL) goto drop;
  PyThread_acquire_lock(pv_lock, 1);
  if (pv_count >= PYMIZU_PV_MAX) {
    PyThread_release_lock(pv_lock);
    goto drop;
  }
  e->next = pv_head;
  pv_head = e;
  pv_count++;
  PyThread_release_lock(pv_lock);
  return;
drop:
  free(cols);
  free(e);
}

void pymizu_shmframe_pv_unregister(const void *owner) {
  if (pv_lock == NULL) return;
  PyThread_acquire_lock(pv_lock, 1);
  pv_entry **pp = &pv_head;
  while (*pp != NULL) {
    if ((*pp)->owner == owner) {
      pv_entry *e = *pp;
      *pp = e->next;
      free(e->cols);
      free(e);
      pv_count--;
      break;
    }
    pp = &(*pp)->next;
  }
  PyThread_release_lock(pv_lock);
}

/* The used bits of two n-row validity bitmaps, offset 0 on both sides; a
   NULL bitmap reads as all-ones (Arrow's null-free form). */
static int pv_bitmap_eq(const uint8_t *x, const uint8_t *y, int64_t n) {
  if (x == NULL && y == NULL) return 1;
  const size_t full = (size_t) n / 8;
  const int rem = (int) (n & 7);
  const uint8_t tail = (uint8_t) ((1u << rem) - 1);
  if (x != NULL && y != NULL) {
    if (full != 0 && memcmp(x, y, full) != 0) return 0;
    return rem == 0 || (x[full] & tail) == (y[full] & tail);
  }
  const uint8_t *bm = x != NULL ? x : y;
  for (size_t i = 0; i < full; i++)
    if (bm[i] != 0xFF) return 0;
  return rem == 0 || (bm[full] & tail) == tail;
}

/* A returned `vu` (string_view) column verified read-only against a
   recorded `u`/`U` column — the polars round trip, whose export re-views
   the strings: <= 12-byte values inline in the 16-byte views, longer ones
   pointing into the recorded leaf's packed bytes. Every check fails
   closed: a miss is the layout write, never wrong data. A pass proves the
   column value-identical to the recorded leaf — inline rows by value,
   long rows by row-byte pointer identity with the row's own span (same
   memory, same bytes), the null set by bitmap equivalence. */
static int pv_col_match_strview(const pv_col *c, const ArrowArray *a) {
  if (c->offset != 0 || a->offset != 0 || a->length != c->length ||
      a->n_buffers < 3 || a->buffers[1] == NULL ||
      c->n_buffers != 3 || c->bufs[1] == NULL)
    return 0;
  const uint8_t *offs = (const uint8_t *) c->bufs[1];
  const uint8_t *data = (const uint8_t *) c->bufs[2];
  const int i64 = c->fmt[0] == 'U';   /* the caller pinned u/U */
  /* the recorded data span: the offsets array's last entry */
  int64_t span;
  if (i64) {
    memcpy(&span, offs + 8 * c->length, 8);
  } else {
    int32_t v;
    memcpy(&v, offs + 4 * c->length, 4);
    span = v;
  }
  if (data == NULL && span != 0) return 0;
  /* The variadic data buffers: slots [2, n_buffers - 1) — the last slot
     is the sizes array the re-viewing producers append (an all-inline
     polars column reports n_buffers == 3: ndata 0, the slot the sizes).
     Each data buffer must start inside the recorded bytes — the O(ndata)
     provenance gate: a recomputed or compacted column arrives with fresh
     buffers and dies here, before any row is read. polars rebases its
     single imported buffer to the first long row, so buffer equality is
     out; the row check below takes the rebase up. */
  const int64_t ndata = a->n_buffers - 3;
  const uintptr_t d0 = (uintptr_t) data;
  for (int64_t j = 0; j < ndata; j++) {
    const uintptr_t b = (uintptr_t) a->buffers[2 + j];
    if (b - d0 > (uintptr_t) span) return 0;   /* wraps when b < d0 */
  }
  /* The null set, following arrow_valid's discipline (a null_count of 0
     or a NULL bitmap reads all-valid). The -1 tolerances are defensive:
     pyarrow normalizes the count at export and polars reports it exact —
     the bitmap proof, never the count metadata, carries acceptance. */
  if (c->null_count == 0) {
    if (a->null_count != 0 && a->buffers[0] != NULL &&
        !pv_bitmap_eq(NULL, (const uint8_t *) a->buffers[0], c->length))
      return 0;
  } else {
    if (c->bufs[0] == NULL || a->buffers[0] == NULL ||
        (a->null_count != c->null_count && a->null_count != -1) ||
        !pv_bitmap_eq((const uint8_t *) c->bufs[0],
                      (const uint8_t *) a->buffers[0], c->length))
      return 0;
  }
  const uint8_t *views = (const uint8_t *) a->buffers[1];
  for (int64_t k = 0; k < c->length; k++) {
    if (!arrow_valid(a, k)) continue;   /* the bitmaps proved the set */
    /* the expected span, off the recorded offsets */
    int64_t lo, hi;
    if (i64) {
      memcpy(&lo, offs + 8 * k, 8);
      memcpy(&hi, offs + 8 * k + 8, 8);
    } else {
      int32_t l, h;
      memcpy(&l, offs + 4 * k, 4);
      memcpy(&h, offs + 4 * k + 4, 4);
      lo = l;
      hi = h;
    }
    const uint8_t *vw = views + 16 * k;
    int32_t len;
    memcpy(&len, vw, 4);
    if (hi - lo != len) return 0;   /* a negative len never matches */
    if (len <= 12) {
      /* inline: the bytes sit in the view — compare by value (a byte
         loop; a memcmp call per short row showed at ~0.7 ms/rt) */
      if (len > 0) {
        const uint8_t *exp = data + lo;
        for (int32_t b = 0; b < len; b++)
          if (vw[4 + b] != exp[b]) return 0;
      }
    } else {
      /* pointer: the view must address exactly this row's recorded
         bytes */
      int32_t bi, off;
      memcpy(&bi, vw + 8, 4);
      memcpy(&off, vw + 12, 4);
      if (bi < 0 || bi >= ndata || off < 0) return 0;
      if ((const uint8_t *) a->buffers[2 + bi] + off != data + lo)
        return 0;
    }
  }
  return 1;
}

/* Whether a matched column pins its record: the per-column write pass
   dereferences the entry after pv_lock drops, and only a column holding a
   byte of the record's memory keeps the export (and the entry) alive for
   it. The pointer-exact match pins by construction (every buffer is the
   record's); a verified string column pins iff it carries a data buffer
   (proven inside the recorded bytes by the match) — an all-inline `vu`
   column's views, and any fresh validity bitmap, hold none of it. Call
   only on a successful pv_col_match. */
static int pv_col_pins(const pv_col *c, const ArrowSchema *sc,
                       const ArrowArray *a) {
  if (strcmp(sc->format, c->fmt) == 0) return 1;
  return a->n_buffers > 3;
}

/* One column's match against a registered record: name, format, length,
   offset, null count, every buffer pointer, and the dictionary
   sub-record. A format mismatch on a string column dispatches to the
   read-only verification: the export's u/U re-viewed by polars comes
   back vu. */
static int pv_col_match(const pv_col *c, const char *name,
                        const ArrowSchema *sc, const ArrowArray *a) {
  if (name == NULL || c->name == NULL || strcmp(name, c->name) != 0)
    return 0;
  if (sc->format == NULL || c->fmt == NULL) return 0;
  if (strcmp(sc->format, c->fmt) != 0)
    return (c->fmt[0] == 'u' || c->fmt[0] == 'U') && c->fmt[1] == '\0' &&
      strcmp(sc->format, "vu") == 0 ? pv_col_match_strview(c, a) : 0;
  if ((sc->dictionary != NULL) != c->has_dict) return 0;
  if (a->length != c->length || a->offset != c->offset ||
      a->null_count != c->null_count || a->n_buffers != c->n_buffers)
    return 0;
  int64_t k;
  for (k = 0; k < c->n_buffers; k++)
    if (a->buffers[k] != c->bufs[k]) return 0;
  if (c->has_dict) {
    const ArrowArray *d = a->dictionary;
    if (d == NULL || sc->dictionary->format == NULL || c->dfmt == NULL ||
        strcmp(sc->dictionary->format, c->dfmt) != 0 ||
        d->length != c->dlength || d->offset != c->doffset ||
        d->null_count != c->dnull_count || d->n_buffers != c->dn_buffers)
      return 0;
    for (k = 0; k < c->dn_buffers; k++)
      if (d->buffers[k] != c->dbufs[k]) return 0;
  }
  return 1;
}

/* The whole-frame match: every column equal to a leaf of the same
   acquisition — name, format, length, offset, null count; fixed-width
   columns by buffer identity, a re-viewed string column by the read-only
   verification — in the same order, none extra or missing. The emit runs
   under the lock, so a by-value string match (which pins nothing) is safe
   here. 0 staged (the region's REF, REFHELD OR'd), -1 anything less. */
int pymizu_shmframe_pv_match(ixs *x, char **names, mizu_slot_hdr *hdr,
                             uint8_t *payload, uint32_t inline_max) {
  if (x->hold.nb != 1 || pv_lock == NULL) return -1;
  const ArrowArray *root = &x->hold.arrs[0];
  const long pid = mizu_self_pid();
  int rc = -1;
  PyThread_acquire_lock(pv_lock, 1);
  for (const pv_entry *e = pv_head; e != NULL; e = e->next) {
    if (e->pid != pid || e->ncols != x->ncols) continue;
    int i;
    for (i = 0; i < e->ncols; i++)
      if (!pv_col_match(&e->cols[i], names[i], x->schema.children[i],
                        root->children[i]))
        break;
    if (i == e->ncols) {
      if (mizu_py_ref_emit(e->acq, hdr, payload, inline_max) == 0) rc = 0;
      break;
    }
  }
  PyThread_release_lock(pv_lock);
  return rc;
}

/* The per-column match (F2): each outgoing column against the same-index
   record of every registered acquisition — the export's child order is
   the MIZL directory order, so a hit references that region's leaf at the
   position + 1 — and columns of one frame may match different
   acquisitions (a frame assembled from two imports, which the whole-frame
   path cannot REF). Fills hits[] with the matched entries (NULL on a
   miss) and returns the hit count. The hits stay borrowed after the lock
   drops: a column whose buffers alias the acquisition's keeps the export
   alive through the stage. A verified string column can pin nothing (an
   all-inline `vu` column references none of the record's buffers), so a
   pinning match is preferred across entries, and a by-value hit stands
   only on an entry another column of the frame pins — else it prunes to
   a layout leaf. */
int pymizu_shmframe_pv_match_cols(ixs *x, char **names,
                                  struct pv_entry const **hits) {
  if (x->hold.nb != 1 || pv_lock == NULL) return 0;
  const ArrowArray *root = &x->hold.arrs[0];
  const long pid = mizu_self_pid();
  int n = 0;
  PyThread_acquire_lock(pv_lock, 1);
  for (int i = 0; i < x->ncols; i++) {
    const pv_entry *byval = NULL;
    for (const pv_entry *e = pv_head; e != NULL; e = e->next) {
      if (e->pid != pid || i >= e->ncols) continue;
      if (!pv_col_match(&e->cols[i], names[i], x->schema.children[i],
                        root->children[i]))
        continue;
      if (pv_col_pins(&e->cols[i], x->schema.children[i],
                      root->children[i])) {
        hits[i] = e;
        break;
      }
      if (byval == NULL) byval = e;   /* prefer a pinning entry */
    }
    if (hits[i] == NULL) hits[i] = byval;
    n += hits[i] != NULL;
  }
  /* the by-value prune (the comment above) */
  for (int i = 0; i < x->ncols; i++) {
    if (hits[i] == NULL ||
        pv_col_pins(&hits[i]->cols[i], x->schema.children[i],
                    root->children[i]))
      continue;
    int pinned = 0;
    for (int j = 0; j < x->ncols; j++) {
      if (j != i && hits[j] == hits[i] &&
          pv_col_pins(&hits[j]->cols[j], x->schema.children[j],
                      root->children[j])) {
        pinned = 1;
        break;
      }
    }
    if (!pinned) {
      hits[i] = NULL;
      n--;
    }
  }
  PyThread_release_lock(pv_lock);
  return n;
}
