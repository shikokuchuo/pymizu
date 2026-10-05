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
  FCOL_TS,    /* i64 us, MIZU_NA_INT64 null — Arrow timestamp[us] */
  FCOL_TD,    /* i64 us, MIZU_NA_INT64 null — Arrow duration[us] */
  FCOL_STR64  /* a region MIZS block (the tree wrap): values = the block,
                 validity/i64 offsets/bytes in place — Arrow large_utf8 */
};

typedef struct {
  int kind;
  int64_t n;
  uint8_t *values;    /* the data / i32 offsets (STR) / i32 codes (DICT) */
  uint8_t *bytes;     /* STR: packed utf8; DICT: packed level bytes */
  int64_t bytes_len;
  int32_t *lev_off;   /* DICT: level offsets, nlev + 1 */
  int64_t nlev;       /* DICT */
  char tz[48];        /* TS: the tzone name ("" is naive) */
  uint8_t *valid;     /* validity bitmap: the region's section (borrowed)
                         or built lazily at export (owned) */
  int64_t vnulls;     /* a borrowed valid's null count */
  uint8_t *bits;      /* LGL: the bit-packed values, built at export */
  int32_t *codes0;    /* DICT codes1: the 0-based shift, built at export */
  uint8_t borrowed;   /* the tree wrap's: values/valid point into the
                         region (never freed here) */
  uint8_t valid_owned;/* valid was built here (freed even when borrowed) */
  uint8_t known_free; /* the region's {0, -1}: the sentinel scan never
                         runs (the LGL bit-pack still builds) */
  uint8_t codes1;     /* DICT: the region's 1-based R codes — the export
                         shifts to 0-based into codes0 */
  uint8_t enc_ok;     /* STR64: the encoding check ran (once, at export) */
  mizu_shm *hold;     /* a remote column's own mapping of the referenced
                         region (the frame's loan does not cover it) — its
                         counted loan is released here, the frame_export
                         acquisition's pure-C discipline */
  long hold_pid;      /* the fork guard */
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

static inline int span_has_na32(const int32_t *p, size_t n) {
  for (size_t i = 0; i < n; i++)
    if (p[i] == MIZU_NA_INT32) return 1;
  return 0;
}

typedef struct {
  PyObject_HEAD
  frame_cols *fc;
  PyObject *row_names;   /* Py_None | list[str] | numpy int32 array */
  PyObject *loan;        /* region-backed: the tree wrap's _ShmLoan anchor
                            (NULL for a copy-backed Frame) */
} MizuFrame;

typedef struct { const int32_t *p; uint64_t n; } i32_span;

// interop_frame.c's (the lifecycle and the export entry) ----------------------

extern PyTypeObject MizuFrameType;

void fcol_free(fcol *c);
void frame_cols_decref(frame_cols *fc);
frame_cols *frame_cols_new(int ncols, int64_t nrow);
int fcol_fixed_size(int kind);

/* The __arrow_c_stream__ method body: interop.c's same-language MIZL
   branch drives it directly (pymizu_frame_stage_mizl). */
PyObject *Frame_arrow_c_stream(MizuFrame *self, PyObject *args,
                               PyObject *kw);

/* Exception stash, the Frame type, and _frame_rebuild — mirroring
   mizu_py_interop_register. */
int mizu_py_frame_register(PyObject *m, PyObject *mizu_error);

// interop.c's (the builder helpers the consumer half reuses) ------------------

PyObject *ixr_memoryview(const void *src, size_t n);
void conv_lgl_bool(uint8_t *dst, const void *ctx);
PyObject *ixr_vec_conv(const char *dt, const uint8_t *ptr, uint64_t count,
                       size_t elt, void (*conv)(uint8_t *, const void *),
                       const void *ctx);
PyObject *ixr_vec_raw(const char *dt, const uint8_t *ptr, uint64_t count,
                      size_t elt);

#endif
