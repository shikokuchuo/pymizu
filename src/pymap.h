/* pymap.h — the Pool.map region layer's interface to _pyrei.c.
 *
 * map.c is language-neutral over the vendored core (no module state of its
 * own beyond the two exception classes handed over at registration); the
 * hand-offs are the wire-type gate (defined in _pyrei.c) and the capsule
 * name the pool-signal handle travels under.
 */

#ifndef PYREI_MAP_H
#define PYREI_MAP_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

/* The capsule name of the worker-local pool-signal handle (a malloc'd
   rei_pool_sig copy created by _Pool._signals()). */
#define REI_PY_SIG_CAPSULE "pyrei.pool_signals"

/* The capsule name of a map context (stage- or worker-side). */
#define REI_PY_MAP_CAPSULE "pyrei.map_ctx"

/* _pyrei.c's O(1) raw-tier gate over a Py_buffer (0 = pickle). */
int rei_py_wire_type_of(const Py_buffer *v);

/* Register map.c's module functions on the _pyrei module; the exception
   objects are borrowed from _pyrei.c's module state (incref'd here). */
int rei_py_map_register(PyObject *m, PyObject *rei_error,
                        PyObject *shm_error);

#endif
