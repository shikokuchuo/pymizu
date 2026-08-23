/* pyrei — Python binding for librei (raw CPython C API).
 *
 * The extension compiles the vendored core sources directly, so the module
 * is self-contained: no system librei is required or consulted.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "rei.h"

#define REI_STR_(x) #x
#define REI_STR(x) REI_STR_(x)
#define REI_VERSION_STRING                                                \
  REI_STR(REI_VERSION_MAJOR) "." REI_STR(REI_VERSION_MINOR) "."           \
  REI_STR(REI_VERSION_PATCH)

PyDoc_STRVAR(abi_version_doc,
"abi_version() -> int\n\n\
Wire-format ABI version of the compiled-in librei core.");

static PyObject *
pyrei_abi_version(PyObject *Py_UNUSED(module), PyObject *Py_UNUSED(args))
{
  return PyLong_FromUnsignedLong((unsigned long) REI_ABI_VERSION);
}

static PyMethodDef pyrei_methods[] = {
  {"abi_version", pyrei_abi_version, METH_NOARGS, abi_version_doc},
  {NULL, NULL, 0, NULL}
};

PyDoc_STRVAR(pyrei_module_doc,
"Lock-free shared-memory IPC: SPSC channels and work-stealing task pools\n\
(the librei core, compiled in).");

static struct PyModuleDef pyrei_module = {
  PyModuleDef_HEAD_INIT, "_pyrei", pyrei_module_doc, -1, pyrei_methods,
  NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC
PyInit__pyrei(void)
{
  PyObject *m = PyModule_Create(&pyrei_module);
  if (m == NULL)
    return NULL;
  if (PyModule_AddStringConstant(m, "__core_version__",
                                 REI_VERSION_STRING) < 0) {
    Py_DECREF(m);
    return NULL;
  }
  return m;
}
