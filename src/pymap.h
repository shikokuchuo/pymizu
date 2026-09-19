/* pymap.h — the Pool.map region layer's interface to _pymizu.c.
 *
 * map.c is language-neutral over the vendored core (no module state of its
 * own beyond the two exception classes handed over at registration); the
 * hand-offs are the wire-type gate (defined in _pymizu.c) and the capsule
 * name the pool-signal handle travels under.
 */

#ifndef PYMIZU_MAP_H
#define PYMIZU_MAP_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

/* The capsule name of the worker-local pool-signal handle (a malloc'd
   mizu_pool_sig copy created by _Pool._signals()). */
#define MIZU_PY_SIG_CAPSULE "pymizu.pool_signals"

/* The capsule name of a map context (stage- or worker-side). */
#define MIZU_PY_MAP_CAPSULE "pymizu.map_ctx"

/* _pymizu.c's O(1) raw-tier gate over a Py_buffer (0 = pickle). */
int mizu_py_wire_type_of(const Py_buffer *v);

/* Register map.c's module functions on the _pymizu module; the exception
   objects are borrowed from _pymizu.c's module state (incref'd here). */
int mizu_py_map_register(PyObject *m, PyObject *mizu_error,
                        PyObject *shm_error);

#endif
