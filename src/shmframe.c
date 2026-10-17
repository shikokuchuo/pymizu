/* shmframe.c — the export-provenance registry (Phase 3.8).

   The MIZL frame writer moved upstream to libmizu's arrow.c (the code
   moves, the wire bytes are pinned by the cross-language suite); what
   stays here is the registry recording, per live Arrow export acquisition
   of a region-backed Frame, the exact ArrowArrays handed out (formats,
   lengths, offsets, buffer pointers, null counts) plus the column names.
   An outgoing frame whose columns all match one acquisition's record —
   an unmodified R -> consumer -> R round trip — stages as the region's
   REF, zero payload bytes; a per-column match goes out as a remote leaf
   through the core writer's mizu_arrow_remote. The failure direction is
   always the copy, never wrong data. */

#include <string.h>

#include "pyshmframe.h"

static PyObject *MizuError;
static PyObject *MizuDeclinedError;

void mizu_py_shmframe_register(PyObject *mizu_error,
                               PyObject *declined_error) {
  MizuError = mizu_error;
  MizuDeclinedError = declined_error;
}

/* The provenance registry's record types. */
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

mizu_shm *pymizu_shmframe_pv_acq(struct pv_entry const *e) {
  return e->acq;
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
   recorded `U` column — the polars round trip, whose export re-views
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
  /* the recorded column is always a region MIZS block (i64 offsets): a
     copy-backed frame's 'u' column never registers — its export carries
     no acquisition */
  int64_t span;
  memcpy(&span, offs + 8 * c->length, 8);
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
    if (!mizu_arrow_valid(a, k)) continue;   /* the bitmaps proved the set */
    /* the expected span, off the recorded offsets */
    int64_t lo, hi;
    memcpy(&lo, offs + 8 * k, 8);
    memcpy(&hi, offs + 8 * k + 8, 8);
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
   read-only verification: the export's U re-viewed by polars comes
   back vu. */
static int pv_col_match(const pv_col *c, const char *name,
                        const ArrowSchema *sc, const ArrowArray *a) {
  if (name == NULL || c->name == NULL || strcmp(name, c->name) != 0)
    return 0;
  if (sc->format == NULL || c->fmt == NULL) return 0;
  if (strcmp(sc->format, c->fmt) != 0)
    return c->fmt[0] == 'U' && c->fmt[1] == '\0' &&
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
