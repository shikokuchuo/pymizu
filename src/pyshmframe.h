/* pyshmframe.h — the MIZL frame writer's interface to interop.c: the
 * pulled-batch Arrow stream state (shared with the 'I' inline writer, whose
 * driver interop.c keeps), the attribute-blob emitters, and shmframe.c's own
 * entries — the region writer (Phase 3.6) and the export-provenance registry
 * (Phase 3.8). */

#ifndef PYMIZU_SHMFRAME_H
#define PYMIZU_SHMFRAME_H

#include "pyinterop.h"

// The pulled-batch stream state (interop.c's driver fills) --------------------

/* The held batches of a producer stream (single-consumption: pulled once,
   in the size pass, and written from in the write pass). */
typedef struct {
  ArrowArray *arrs;
  size_t nb, cap;
  int64_t rows;
} ixs_hold;

enum { PC_CVT, PC_STR, PC_DICT, PC_DATE, PC_TS, PC_TD };

typedef struct {
  int kind;
  const cvt_row *row;   /* PC_CVT */
  int str_form;         /* PC_STR/PC_DICT: 1 utf8, 2 large_utf8, 3 view */
  double ts_scale;      /* PC_TS/PC_TD */
  char tz[64];          /* PC_TS */
  char idx_w;           /* PC_DICT: the index width */
  int idx_signed;       /* PC_DICT */
} ixs_pcol;

typedef struct {
  ArrowArrayStream *st;
  PyObject *cap;
  ArrowSchema schema;
  int schema_owned;
  int borrowed_hold;   /* the hold is a borrowed single batch (a capsule
                          pair), never pulled or released */
  ixs_hold hold;
  ixs_pcol *cols;
  int ncols;
  int single;
  /* per-dictionary-column canonical levels (batch 0's), read at
     validation */
  int32_t **lev_offs;
  uint8_t **lev_bytes;
  int64_t *nlevs;
  int64_t *lev_blens;
  uint32_t caps;       /* the peer's capability mask (frame gate) */
  PyObject *row_names; /* a same-language Frame's row_names (borrowed);
                          NULL writes the automatic c(NA, -n) form */
} ixs;

/* The validity bit for element k of a batch (Arrow LSB-first). */
static inline int arrow_valid(const ArrowArray *a, int64_t k) {
  if (a->null_count == 0 || a->buffers[0] == NULL) return 1;
  int64_t i = a->offset + k;
  const uint8_t *bm = (const uint8_t *) a->buffers[0];
  return (bm[i >> 3] >> (i & 7)) & 1;
}

/* The string forms' per-batch element access: utf8 (i32 offsets),
   large_utf8 (i64), string_view (16-byte views). 0 on a null (ptr/len
   unset), -1 on a malformed record. The Arrow C Data Interface carries no
   buffer lengths, so the byte extent stays the producer's word; what is
   checked here are the catastrophic cases — a view's buffer index past
   the pointer array, negative lengths or offsets, and non-monotonic
   offset spans (which would size one way and gather another). */
static inline int arrow_str_at(const ArrowArray *a, int64_t k, int form,
                               const uint8_t **ptr, int32_t *len) {
  if (!arrow_valid(a, k)) return 0;
  int64_t i = a->offset + k;
  if (form == 1) {
    const int32_t *offs = (const int32_t *) a->buffers[1];
    if (offs[i] < 0 || offs[i + 1] < offs[i]) return -1;
    *ptr = (const uint8_t *) a->buffers[2] + offs[i];
    *len = offs[i + 1] - offs[i];
  } else if (form == 2) {
    const int64_t *offs = (const int64_t *) a->buffers[1];
    if (offs[i] < 0 || offs[i + 1] < offs[i] ||
        offs[i + 1] - offs[i] > INT32_MAX)
      return -1;
    *ptr = (const uint8_t *) a->buffers[2] + offs[i];
    *len = (int32_t) (offs[i + 1] - offs[i]);
  } else {
    const uint8_t *vw = (const uint8_t *) a->buffers[1] + i * 16;
    int32_t l;
    memcpy(&l, vw, 4);
    if (l < 0) return -1;
    if (l <= 12) {
      *ptr = vw + 4;
    } else {
      int32_t bi, off;
      memcpy(&bi, vw + 8, 4);
      memcpy(&off, vw + 12, 4);
      if (bi < 0 || 2 + (int64_t) bi >= a->n_buffers || off < 0) return -1;
      *ptr = (const uint8_t *) a->buffers[2 + bi] + off;
    }
    *len = l;
  }
  return 1;
}

// The attribute-blob emitters (interop.c's; complete 'I' streams) -------------

/* Each emits a complete 'I' stream (the layout attribute blob form) whose
   value is the attribute dict; dst NULL sizes, a real dst writes behind the
   size pass's count. 0 with an exception set on failure (blob sizes are
   never 0). */
size_t mizu_py_blob_frame(uint8_t *dst, char **names, int ncols, int64_t rows,
                          PyObject *row_names);
size_t mizu_py_blob_factor(uint8_t *dst, const uint8_t *bytes,
                           const int32_t *offs, int64_t nlev);
size_t mizu_py_blob_date(uint8_t *dst);
size_t mizu_py_blob_ts(uint8_t *dst, const char *tz);
size_t mizu_py_blob_difftime(uint8_t *dst);

// shmframe.c -------------------------------------------------------------------

/* Exception stash, mirroring mizu_py_interop_register. */
void mizu_py_shmframe_register(PyObject *mizu_error,
                               PyObject *declined_error);

/* The MIZL layout write over the pulled frame: one spill region — header,
   directory, column blobs, the root frame blob, the validity tail — staged
   SHM_VEC (MIZU_TYPE_VEC). same_lang selects the same-language rules (the
   tzone string verbatim; the caller's row_names). hits is the per-column
   provenance map (NULL: every column a layout leaf): a matched column
   goes out as a remote leaf (tag 33). 0 staged, 1 an exception is set,
   -1 the region was unavailable (the caller falls back to the copy tiers;
   no exception). */
struct pv_entry;   /* shmframe.c's provenance record */
int pymizu_shmframe_write(ixs *x, char **names, mizu_slot_hdr *hdr,
                          uint8_t *payload, uint32_t inline_max,
                          mizu_handle *h, int same_lang,
                          struct pv_entry const **hits);

/* The export-provenance registry (Phase 3.8): a region-backed Frame's Arrow
   export records what each acquisition handed out; an outgoing frame whose
   columns all match one acquisition's record stages as the region's REF. */
void pymizu_shmframe_pv_register(mizu_shm *acq, int ncols, char **names,
                                 const ArrowSchema *col_s,
                                 const ArrowArray *col_a, const void *owner);
void pymizu_shmframe_pv_unregister(const void *owner);
/* 0 staged (REF), -1 no whole-frame match (the caller writes MIZL). */
int pymizu_shmframe_pv_match(ixs *x, char **names, mizu_slot_hdr *hdr,
                             uint8_t *payload, uint32_t inline_max);
/* The per-column match (F2): hits filled with the matched entries (NULL
   on a miss), the hit count returned. */
int pymizu_shmframe_pv_match_cols(ixs *x, char **names,
                                  struct pv_entry const **hits);

#endif
