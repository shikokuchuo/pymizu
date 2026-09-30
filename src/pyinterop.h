/* pyinterop.h — the 'I' interchange codec's interface to _pymizu.c, and the
 * shared declarations it needs from there (the Arrow C Data Interface
 * structs, the conversion table, and a handful of mizu_py_* wrappers over
 * _pymizu.c's static machinery, following the mizu_py_wire_type_of
 * precedent in pymap.h). */

#ifndef PYMIZU_INTEROP_H
#define PYMIZU_INTEROP_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "mizu.h"
#include "mizu_ext.h"

// Arrow C Data Interface ---------------------------------------------------------

/* The stable C ABI structs, defined locally per the spec (the layout is
   frozen; no headers, no dependency). The import front-ends (a producer's
   __arrow_c_array__ / __arrow_c_stream__) and the view's and Frame's export
   dunders use them. */
#ifndef PYMIZU_ARROW_SCHEMA_DEFINED
#define PYMIZU_ARROW_SCHEMA_DEFINED
typedef struct ArrowSchema {
  const char *format;
  const char *name;
  const char *metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema **children;
  struct ArrowSchema *dictionary;
  void (*release)(struct ArrowSchema *);
  void *private_data;
} ArrowSchema;

typedef struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void **buffers;
  struct ArrowArray **children;
  struct ArrowArray *dictionary;
  void (*release)(struct ArrowArray *);
  void *private_data;
} ArrowArray;

typedef struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *out);
  int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *out);
  const char *(*get_last_error)(struct ArrowArrayStream *);
  void (*release)(struct ArrowArrayStream *);
  void *private_data;
} ArrowArrayStream;
#endif

#define PYMIZU_ARROW_FLAG_NULLABLE 2
#define PYMIZU_ARROW_FLAG_DICTIONARY_ORDERED 1

/* R's verbatim NA_real_ bits (the corpus pins them; the core's
   MIZU_NA_REAL_BITS is the quiet-bit-set twin — ISNA reads either, but an
   R->Python->R relay compares NaN payloads bitwise). */
#define PYMIZU_NA_REAL_BITS 0x7FF00000000007A2ULL

// The conversion table (shared with _pymizu.c's conversion pass) ---------------

enum {
  CVT_COPY = 0,   /* identity: one memcpy per run */
  CVT_I8_INT,     /* sign-widen */
  CVT_I16_INT,
  CVT_U16_INT,    /* zero-widen */
  CVT_U32_REAL,   /* exact */
  CVT_U64_REAL,
  CVT_F32_REAL,   /* exact */
  CVT_BOOL8_LGL,  /* one byte per source lane (numpy '?') -> int32 0/1 */
  CVT_BIT_LGL,    /* one bit per source lane (Arrow 'b') -> int32 0/1 */
  CVT_C64_CPLX    /* float re/im -> double re/im */
};

typedef struct {
  int wire;         /* the MIZU_TYPE_* the row produces */
  int cvt;
  uint8_t w_in;     /* source element bytes (0: bit-packed, CVT_BIT_LGL) */
  uint8_t w_out;
} cvt_row;

/* The warning counts, emitted only after the write half completes. */
typedef struct {
  uint64_t n_range;   /* uint64 past 2^53 -> NA_real_ */
  uint64_t n_intmin;  /* masked int32 only: a genuine INT_MIN reads as NA */
} cvt_warn;

// What interop.c calls in _pymizu.c -------------------------------------------

int mizu_py_stage_bytes(const uint8_t *src, size_t n, mizu_slot_hdr *hdr,
                        uint8_t *payload, uint32_t inline_max, mizu_handle *h);
PyObject *mizu_py_numpy_module(void);   /* the module, Py_None, or NULL+err */
int mizu_py_buffer_subclass_reject(PyObject *obj);
/* 1 exact ndarray, 2 a numpy scalar, 0 neither. */
int mizu_py_np_kind(PyObject *obj);
int mizu_py_wire_type_of(const Py_buffer *v);
const cvt_row *mizu_py_cvt_for_buffer(const char *f, Py_ssize_t itemsize);
const cvt_row *mizu_py_cvt_for_arrow(const char *f);
/* One batch's convert into a reserved destination: the BIT_LGL / masked /
   plain dispatch of _pymizu.c's stage_convert. */
void mizu_py_cvt_convert(uint8_t *dst, const uint8_t *src,
                         const uint8_t *valid, uint64_t off, size_t n,
                         const cvt_row *row, cvt_warn *warn);
/* The conversion warnings, raised after the write half completes (never
   mid-write). 0 ok, 1 a warning raised as an error. */
int mizu_py_cvt_warn(const cvt_warn *warn);

// What _pymizu.c calls in interop.c -------------------------------------------

/* The 'I' writer onto the tiers (foreign handles only): sizes and validates
   in one pass, then writes and stages INLINE/ARENA/SHM_RAW. 0 staged,
   1 declined (DeclinedError set) or another exception. */
int pymizu_ix_stage(PyObject *obj, mizu_slot_hdr *hdr, uint8_t *payload,
                    uint32_t inline_max, mizu_handle *h);

/* This binding's identity word, the one binding fill (channel and pool). */
#define MIZU_PY_IDENT \
  MIZU_IDENT(MIZU_LANG_PYTHON, MIZU_CAP_MIZS | MIZU_CAP_ATTRS | MIZU_CAP_MIZL)

/* The task stream (0x12) writer (Phase 4): the header fields then the
   code string, the positional list and the named dict emitted off the
   spec's components, elements through the generic writer; the target is
   the pool word's language byte. 0 staged, 1 error (DeclinedError for a
   non-portable argument — a foreign task with non-portable args can
   never run, so no fallback — MizuError otherwise). */
int pymizu_ix_stage_task(PyObject *spec, mizu_slot_hdr *hdr,
                         uint8_t *payload, uint32_t inline_max,
                         mizu_handle *h, uint64_t ident);

/* The exec-hook decode and run of a task stream (Phase 4): validates the
   header and the per-kind shape off the cursor — ident_out takes the
   submitter identity ahead of every field read — then executes: name
   kind by importlib resolution and vectorcall straight off the cursor,
   source kind by the ast split with the arguments bound in a fresh
   namespace. Returns the result, or NULL with an exception set (the exec
   hook publishes it — every path is catching). */
PyObject *pymizu_ix_task_run(const uint8_t *src, size_t n,
                             uint64_t *ident_out);

/* The test hooks' halves: the task writer as bytes, and the decode as
   components (target, kind, ident, code, positional list, named dict) —
   the golden corpus drives them. */
PyObject *pymizu_ix_write_task_stream(PyObject *code, long kind,
                                      PyObject *args, PyObject *kwargs,
                                      uint32_t target, uint64_t ident);
PyObject *pymizu_ix_read_task_components(const uint8_t *src, size_t n);

/* The __arrow_c_stream__ front-end (foreign handles only): struct schemas
   write the frame shape, non-struct the bare-bytes tiers / 0x0b / the
   temporal and factor shapes; past the zero-copy floor a frame the peer's
   capability mask admits stages as one MIZL region (or REF on an
   unmodified round trip). 0 staged, 1 error (DeclinedError for a value
   without a portable home), -1 not a stream producer. */
int pymizu_ix_stage_arrow_stream(PyObject *obj, mizu_slot_hdr *hdr,
                                 uint8_t *payload, uint32_t inline_max,
                                 mizu_handle *h, uint32_t peer_caps);

/* The same-language MIZL branch (stage_impl's, ahead of pickle): an exact
   pymizu.Frame past the zero-copy floor stages as one MIZL region
   (container-exact). 0 staged, -1 fall back to pickle (never an error). */
int pymizu_frame_stage_mizl(PyObject *obj, mizu_slot_hdr *hdr,
                            uint8_t *payload, uint32_t inline_max,
                            mizu_handle *h);

/* The REF emit shared by the whole-view re-sends and the 3.8 provenance
   match: REFHELD OR'd, the region name the payload. 0 staged, -1 the name
   cannot ride the payload. */
int mizu_py_ref_emit(mizu_shm *shm, mizu_slot_hdr *hdr, uint8_t *payload,
                     uint32_t inline_max);

/* The 'I' builder: one stream -> one Python object. NULL with an
   exception set (the cursor's informative text, the no-home decline, or
   MemoryError). */
PyObject *pymizu_ix_read(const uint8_t *src, size_t n);

/* The err tag (0x11) framer: the three bare strings and the optional
   element index, truncated at UTF-8 boundaries to fit inline_max by
   construction (type past 128 bytes, message past half the budget, detail
   past what remains) — the writer cannot fail. Returns the stream size.
   Serves the peer shim's _send_error and Phase 4's ERR publish. */
size_t pymizu_ix_write_err(uint8_t *dst, uint32_t inline_max,
                           const char *type, size_t type_n,
                           const char *msg, size_t msg_n,
                           const char *detail, size_t detail_n,
                           int has_index, uint64_t index);

/* The _write_stream test hook: the writer, as bytes (DeclinedError for a
   value outside the portable subset). */
PyObject *pymizu_ix_write_stream(PyObject *obj);

/* Module init: ready the Frame type, stash the shared exceptions, add
   Frame to the module. */
int mizu_py_interop_register(PyObject *m, PyObject *mizu_error,
                             PyObject *declined_error);

// What interop.c calls in _pymizu.c -------------------------------------------

/* The one TaskError builder: (type name, message, traceback, optional
   element index) -> the exception object, text "type: message", the
   fields as remote_type / remote_traceback / index. Borrowed references;
   NULL with an exception set on allocation failure. */
PyObject *mizu_py_task_error_build(PyObject *tn, PyObject *ms,
                                   PyObject *tbs, PyObject *eidx);

// The MIZL tree wrap (§3.5) --------------------------------------------------------

/* What interop.c calls in _pymizu.c: the tree-borrow constructors. owner
   is a _ShmOwner; the loan anchor (mizu_py_loan_new) holds the tree's
   one zc loan, referenced by every element view the walk builds, so the
   sub fires at the last view's release. valid/nulls are the leaf's
   validity verdict (bitmap + count, NULL + -1 known-NA-free, NULL + 0
   the lazy scan) — a borrowed view never reads the region root's words. */
PyObject *mizu_py_loan_new(PyObject *owner);
PyObject *mizu_py_view_borrow(PyObject *owner, PyObject *loan,
                              uint8_t *data, Py_ssize_t len, int type,
                              const uint8_t *valid, int64_t nulls);
PyObject *mizu_py_strview_borrow(PyObject *owner, PyObject *loan,
                                 const uint8_t *block, int64_t n,
                                 int64_t str_bytes);
mizu_shm *mizu_py_loan_shm(PyObject *loan);
PyObject *mizu_py_loan_owner(PyObject *loan);   /* borrowed ref */

/* The live export acquisitions' mapping spans (test-only; _pymizu.c's). */
void mizu_py_debug_span_add(void *base, size_t size);
void mizu_py_debug_span_remove(void *base);

/* The attributed-atomic home shared by an MIZH root and an MIZL leaf:
   the blob's shape decision (factor / dim / Date / POSIXct / no-home). */
PyObject *mizu_py_atomic_home(PyObject *owner, PyObject *loan, int type,
                              uint8_t *data, int64_t n,
                              const uint8_t *valid, int64_t nulls,
                              const uint8_t *blob, size_t blob_size);

/* The MIZL walk and shape decision: one counted zc-ref per root (the
   loan anchor), element views borrowed over the shared mapping. Returns
   the tree's home — a plain list (no attributes), a dict (names only),
   or a region-backed Frame (the data.frame shape) — NULL with an
   exception on a decline or a corrupt region. */
PyObject *pymizu_tree_wrap(PyObject *owner, PyObject *loan,
                           const uint8_t *base, size_t size, int64_t n,
                           int64_t attrs_off, int64_t attrs_size);

/* A path REF's resolution: "[i,j,...]" (1-based hops, the R identifier's
   form) walked to the element's wrap. */
PyObject *pymizu_tree_walk_path(PyObject *owner, PyObject *loan,
                                const uint8_t *base, size_t size,
                                const char *path);

/* The REF staging helper's Frame half: the region loan anchor of a
   region-backed Frame, NULL for anything else (a copy-backed Frame
   included). Borrowed reference. */
PyObject *mizu_py_frame_loan(PyObject *obj);

#endif
