/* pyframe.h — the pymizu.Frame columnar home, shared by interop.c (the
 * wire side: the frame reader, the MIZL tree wrap, the 'I' writer's
 * ixe_frame, the same-language MIZL branch) and interop_frame.c (the
 * Python-facing side: the type object, methods, the __arrow_c_stream__
 * export, the pickle form). The column block is C-owned (no PyObject
 * inside — the Arrow export's release callbacks are pure C, callable
 * from any thread) and refcounted between the Frame and its exported
 * streams. */

#ifndef PYMIZU_FRAME_H
#define PYMIZU_FRAME_H

#include "pyinterop.h"

enum {
  FCOL_F64, FCOL_I32, FCOL_I64, FCOL_U8, FCOL_C128,
  FCOL_LGL,   /* i32 0/1, MIZU_NA_INT32 null — exports Arrow bool */
  FCOL_STR,   /* values = i32 offsets (n+1); bytes packed */
  FCOL_DICT,  /* values = i32 codes 0-based (MIZU_NA_INT32 null) + levels */
  FCOL_DATE,  /* i32 days, MIZU_NA_INT32 null — Arrow date32 */
  FCOL_TS,    /* i64 counts of unit, MIZU_NA_INT64 null — Arrow timestamp */
  FCOL_TD,    /* i64 counts of unit, MIZU_NA_INT64 null — Arrow duration */
  FCOL_STR64, /* a region MIZS block (the tree wrap): values = the block,
                 validity/i64 offsets/bytes in place — Arrow large_utf8 */
  FCOL_U64    /* u64 lanes, MIZU_NA_INT64 a *value* (the verbatim uint64
                 form): nulls bitmap-only, never sentinel-derived */
};

/* The adoption hold: one per single-batch Frame.from_arrow construction.
   The batch struct's release covers its children and their dictionaries
   (the C Data Interface's discipline), so adopted values, codes, level
   buffers and validity bitmaps all ride the one hold; the last decref runs
   release pure-C from any thread (the frame_export discipline) and frees
   the hold. */
typedef struct {
  ArrowArray arr;
  _Atomic size_t refs;
} mizu_py_arrow_hold;

static inline void ahold_decref(mizu_py_arrow_hold *h) {
  if (atomic_fetch_sub_explicit(&h->refs, 1, memory_order_acq_rel) != 1)
    return;
  if (h->arr.release != NULL) h->arr.release(&h->arr);
  free(h);
}

typedef struct {
  int kind;
  int64_t n;
  uint8_t *values;    /* the data / i32 offsets (STR) / i32 codes (DICT) */
  uint8_t *bytes;     /* STR: packed utf8; DICT: packed level bytes */
  int64_t bytes_len;
  int32_t *lev_off;   /* DICT: level offsets, nlev + 1 */
  int64_t nlev;       /* DICT */
  char tz[48];        /* TS: the tzone name ("" is naive) */
  char unit;          /* TS/TD: the Arrow unit s/m/u/n */
  uint8_t *valid;     /* validity bitmap: the region's section (borrowed)
                         or built lazily at export (owned) */
  int64_t vnulls;     /* valid's null count, recorded at the read or the
                         bitmap build — the export serves it, never
                         re-counts */
  uint8_t *bits;      /* LGL: the bit-packed values, built at export */
  int32_t *codes0;    /* DICT codes1: the 0-based shift, built at export */
  uint8_t borrowed;   /* the tree wrap's: values/valid point into the
                         region (never freed here) */
  uint8_t valid_owned;/* valid was built here (freed even when borrowed) */
  uint8_t known_free; /* the region's {0, -1}: the sentinel scan never
                         runs (the LGL bit-pack still builds) */
  uint8_t codes1;     /* DICT: the region's 1-based R codes — the export
                         shifts to 0-based into codes0 */
  uint8_t ordered;    /* DICT: an ordered factor — the class strv's
                         two-element form, the export's ORDERED flag */
  uint8_t enc_ok;     /* STR64: the encoding check ran (once, at export) */
  mizu_shm *hold;     /* a remote column's referenced-region mapping,
                         borrowed from the column block's deduped holds —
                         the frame's loan does not cover it */
  mizu_py_arrow_hold *ahold;  /* a Frame.from_arrow adopted column: values,
                              valid, bytes and lev_off borrow the producer's
                              batch buffers (freed by the hold's release,
                              never here); borrowed stays 0, so the region
                              arms never take it */
} fcol;

/* The column block: C-owned (no PyObject inside — the Arrow export's
   release callbacks are pure C, callable from any thread) and refcounted
   between the Frame and its exported streams. */
typedef struct frame_cols {
  _Atomic size_t refs;
  int ncols;
  int64_t nrow;
  fcol *cols;
  char *names;        /* the packed column names (utf8) */
  int32_t *name_off;  /* ncols + 1 offsets into names */
  mizu_shm **holds;   /* the remote columns' referenced regions, deduped by
                         name at the read: one mapping + counted loan per
                         distinct region, however many columns borrow it */
  char (*hold_names)[MIZU_NAME_MAX];
  size_t nholds, holds_cap;
  long holds_pid;     /* the holds' fork guard (all acquisitions are
                         same-process) */
  char *cnames;       /* the NUL-terminated name store, built lazily at the
                         first Arrow export and borrowed by every later one
                         (ArrowSchema.name wants C strings; names is
                         packed) */
  char **cname_ptrs;  /* [ncols] */
} frame_cols;

/* The f64 NA test, payload-aware: a NaN whose low word is R's 1954
   (0x7A2) — ISNA's own test, so a genuine NaN stays a value. */
static inline int is_na_r(uint64_t bits) {
  return ((bits >> 52) & 0x7FF) == 0x7FF && (uint32_t) bits == 0x7A2;
}

/* The validity bitmap's bit i (LSB-first, Arrow's order). */
static inline int bitmap_at(const uint8_t *bm, int64_t i) {
  return (bm[i >> 3] >> (i & 7)) & 1;
}

/* The lazy validity bitmap: starts NULL (all-valid implied); the first
   null at lane i allocates the bitmap over n lanes and backfills the
   preceding (all-valid) lanes. 0 ok, -1 allocation failure. */
static inline int bitmap_mark_null(uint8_t **valid, uint64_t n, int64_t i) {
  if (*valid != NULL) return 0;
  uint8_t *bm = mizu_py_bitmap_calloc(n);
  if (bm == NULL) return -1;
  for (int64_t j = 0; j < i; j++) bm[j >> 3] |= 1 << (j & 7);
  *valid = bm;
  return 0;
}

/* The MIZU_TYPE -> FCOL correspondence for the fixed-width vector kinds;
   -1 a type with no column kind (unreachable behind the cursor's closed
   type set and the element-size gates). */
static inline int fcol_of_type(int wire_type) {
  switch (wire_type) {
  case MIZU_TYPE_REAL: return FCOL_F64;
  case MIZU_TYPE_INT: return FCOL_I32;
  case MIZU_TYPE_INT64: return FCOL_I64;
  case MIZU_TYPE_RAW: return FCOL_U8;
  case MIZU_TYPE_CPLX: return FCOL_C128;
  case MIZU_TYPE_LGL: return FCOL_LGL;
  default: return -1;
  }
}

/* The inverse: kind -> wire type, 0 a kind with no view form. */
static inline int fcol_type_of(int kind) {
  switch (kind) {
  case FCOL_F64: return MIZU_TYPE_REAL;
  case FCOL_I32: return MIZU_TYPE_INT;
  case FCOL_I64: return MIZU_TYPE_INT64;
  case FCOL_U8: return MIZU_TYPE_RAW;
  case FCOL_C128: return MIZU_TYPE_CPLX;
  default: return 0;
  }
}

/* One column name into the block's packed store: name_off[i] stamped ahead
   of the append (the span of name i-1 ends at it), the duplicate scan
   against the i names already packed when check_dups. Returns
   FRAME_PACK_OK packed, FRAME_PACK_DUP a duplicate, FRAME_PACK_OVERFLOW
   past the int32 offset range, FRAME_PACK_NOMEM an allocation failure
   (raised). */
enum {
  FRAME_PACK_OK = 0,
  FRAME_PACK_DUP,
  FRAME_PACK_OVERFLOW,
  FRAME_PACK_NOMEM
};
static inline int frame_name_pack(frame_cols *fc, int i, size_t *name_len,
                                  const char *s, size_t len, int check_dups) {
  fc->name_off[i] = (int32_t) *name_len;
  if (check_dups)
    for (int j = 0; j < i; j++)
      if ((size_t) (fc->name_off[j + 1] - fc->name_off[j]) == len &&
          memcmp(fc->names + fc->name_off[j], s, len) == 0)
        return FRAME_PACK_DUP;
  if (len > (size_t) INT32_MAX - *name_len) return FRAME_PACK_OVERFLOW;
  char *nb = realloc(fc->names, *name_len + len + 1);
  if (nb == NULL) {
    PyErr_NoMemory();
    return FRAME_PACK_NOMEM;
  }
  fc->names = nb;
  memcpy(fc->names + *name_len, s, len);
  *name_len += len;
  return FRAME_PACK_OK;
}

static inline int span_has_na32(const int32_t *p, size_t n) {
  for (size_t i = 0; i < n; i++)
    if (p[i] == MIZU_NA_INT32) return 1;
  return 0;
}

/* A packed byte store's geometric growth: double on overflow from 64, one
   exact-size shrink at the end (a failed shrink keeps the slack). The +1
   slack keeps the block non-NULL for an empty store. -1 without an error
   set, the frame_cols_new discipline. */
static inline int bytes_reserve(uint8_t **bytes, size_t *cap, size_t need) {
  if (need <= *cap) return 0;
  size_t ncap = *cap != 0 ? *cap : 64;
  while (ncap < need) {
    if (ncap > SIZE_MAX / 2) {
      ncap = need;
      break;
    }
    ncap *= 2;
  }
  uint8_t *nb = realloc(*bytes, ncap);
  if (nb == NULL) return -1;
  *bytes = nb;
  *cap = ncap;
  return 0;
}

static inline void bytes_shrink(uint8_t **bytes, size_t len) {
  if (*bytes == NULL) return;
  uint8_t *nb = realloc(*bytes, len + 1);
  if (nb != NULL) *bytes = nb;
}

typedef struct {
  PyObject_HEAD
  frame_cols *fc;
  PyObject *row_names;   /* Py_None | list[str] | numpy int32 array */
  PyObject *loan;        /* region-backed: the tree wrap's _ShmLoan anchor
                            (NULL for a copy-backed Frame) */
  uint8_t loan_root;     /* the frame is its region's root (not an element
                            of a list tree): it re-sends by the region's
                            name */
  PyObject *r_attrs;     /* NULL, or the extended frame's (class_tuple,
                            extras) stash: extras a list of (key, bytes)
                            — the extra attributes' raw wire spans,
                            spliced verbatim on a pristine echo, never
                            decoded (DESIGN.md's Frame immutability
                            invariant: any future Frame mutator must
                            clear it first) */
  struct frame_stage *stage_exp;  /* the cached stage construction:
                            interop_frame.c's, built at the first
                            same-language send, freed at dealloc */
} MizuFrame;

typedef struct { const int32_t *p; uint64_t n; } i32_span;

// interop_frame.c's (the lifecycle and the export entry) ----------------------

extern PyTypeObject MizuFrameType;

void fcol_free(fcol *c);
void frame_cols_decref(frame_cols *fc);
frame_cols *frame_cols_new(int ncols, int64_t nrow);
int fcol_fixed_size(int kind);

/* The FRAMEREF gate's data-size lower bound (the task codec's size pass):
   the column-body bytes only. */
uint64_t frame_data_size(const frame_cols *fc);

/* The __arrow_c_stream__ method body (the public export — always a fresh
   stream; the same-language MIZL branch rides frame_stage_fill instead). */
PyObject *Frame_arrow_c_stream(MizuFrame *self, PyObject *args,
                               PyObject *kw);

/* The cached stage construction: the pulled Arrow state (root schema and
   one batch, copied into the outs — children and buffers borrowed, owned
   by the frame) over the frame's own mapping, built at the first call and
   reused by later same-language sends. The Frame is immutable, so the
   cache key is the frame itself; the per-send root-blob/tz overrides and
   the caps word live outside it. 0 ok, -1 with an error set (a complex
   column, OOM) — the caller clears and declines. */
int frame_stage_fill(MizuFrame *f, ArrowSchema *schema_out,
                     ArrowArray *batch_out);

/* Exception stash, the Frame type, and _frame_rebuild — mirroring
   mizu_py_interop_register. */
int mizu_py_frame_register(PyObject *m, PyObject *mizu_error);

// interop.c's (the builder helpers the consumer half reuses) ------------------

PyObject *ixr_memoryview(const void *src, size_t n);
void conv_lgl_bool(uint8_t *dst, const void *ctx);
void conv_days_i64(uint8_t *dst, const void *ctx);
PyObject *ixr_vec_conv(const char *dt, const uint8_t *ptr, uint64_t count,
                       size_t elt, void (*conv)(uint8_t *, const void *),
                       const void *ctx);
PyObject *ixr_vec_raw(const char *dt, const uint8_t *ptr, uint64_t count,
                      size_t elt);

/* to_dict's null-fidelity reads (fcol_to_obj's fixed-width arms): a
   Frame.from_arrow adopted column carries nulls bitmap-only — garbage at
   null slots — so valid != NULL masks; NULL is the owned sentinel
   invariant and reduces to the fused ixr_vec policy. One mechanism, both
   callers. */
PyObject *ixr_masked_f64(const uint8_t *values, const uint8_t *valid,
                         uint64_t n);
PyObject *ixr_masked_i32(const uint8_t *values, const uint8_t *valid,
                         uint64_t n);
PyObject *ixr_masked_i64(const uint8_t *values, const uint8_t *valid,
                         uint64_t n);
PyObject *ixr_masked_u64(const uint8_t *values, const uint8_t *valid,
                         uint64_t n);
PyObject *ixr_masked_days(const uint8_t *values, const uint8_t *valid,
                          uint64_t n);
PyObject *ixr_masked_us(const char *dt, const uint8_t *values,
                        const uint8_t *valid, uint64_t n);

#endif
