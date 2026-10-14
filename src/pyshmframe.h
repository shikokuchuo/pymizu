/* pyshmframe.h — the pulled-batch Arrow stream state shared by interop.c's
 * frame paths (the 'I' inline writer and the core's MIZL writer, which
 * moved upstream to libmizu's arrow.c), the custom-row_names blob emitter,
 * and shmframe.c's export-provenance registry (Phase 3.8). */

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
  mizu_arrow *ar;      /* the open core handle while the emit writers run
                          (the run functions own) */
  uint32_t caps;       /* the peer's capability mask (frame gate) */
  uint32_t lang;       /* the peer's language (the sentinel warnings gate) */
  PyObject *row_names; /* a same-language Frame's row_names (borrowed);
                          NULL writes the automatic c(NA, -n) form */
  cvt_warn warn;       /* the 'I' frame emit's conversion warnings,
                          raised after the write completes (the MIZL
                          writer keeps its own) */
} ixs;

// The root-blob override emitter (interop.c's; a complete 'I' stream) -----------

/* The frame dict {names, class = "data.frame", row.names} with a custom
   (int32 or character) row_names — the override handed to the core
   writer's mizu_arrow_root_blob. The automatic c(NA, -n) form is the
   core's. dst NULL sizes, a real dst writes behind the size pass's count.
   0 with an exception set on a malformed row_names. */
size_t mizu_py_blob_frame(uint8_t *dst, char **names, int ncols, int64_t rows,
                          PyObject *row_names);

// The Frame.from_arrow builder (interop.c's; interop_frame.c wraps) -----------

/* Drain an __arrow_c_stream__ producer into a fresh column block: a single
   batch's columns whose fcol layout IS the Arrow layout adopt the
   producer's buffers behind one mizu_arrow_hold (nulls included — the
   validity bitmap adopts with the values); the rest copy per column
   (multi-batch concat, typed conversions, string gathers). Names from the
   schema children; nrow the drained row count. NULL with an exception set
   (MizuError — an unsupported type names its column, a producer/export
   failure the export's own text). */
struct frame_cols *ixs_frame_cols_build(PyObject *obj);

// shmframe.c: the export-provenance registry (Phase 3.8) -------------------------

/* Exception stash, mirroring mizu_py_interop_register. */
void mizu_py_shmframe_register(PyObject *mizu_error,
                               PyObject *declined_error);

/* A region-backed Frame's Arrow export records what each acquisition
   handed out; an outgoing frame whose columns all match one acquisition's
   record stages as the region's REF. */
void pymizu_shmframe_pv_register(mizu_shm *acq, int ncols, char **names,
                                 const ArrowSchema *col_s,
                                 const ArrowArray *col_a, const void *owner);
void pymizu_shmframe_pv_unregister(const void *owner);
/* 0 staged (REF), -1 no whole-frame match (the caller writes MIZL). */
struct pv_entry;   /* shmframe.c's provenance record */
int pymizu_shmframe_pv_match(ixs *x, char **names, mizu_slot_hdr *hdr,
                             uint8_t *payload, uint32_t inline_max);
/* The per-column match (F2): hits filled with the matched entries (NULL
   on a miss), the hit count returned. */
int pymizu_shmframe_pv_match_cols(ixs *x, char **names,
                                  struct pv_entry const **hits);

/* A matched entry's region (mizu_shm_addr / mizu_shm_size inputs to the
   core writer's mizu_arrow_remote) and its name span. */
mizu_shm *pymizu_shmframe_pv_acq(struct pv_entry const *e);

#endif
