#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "nanoparquet.h"

static PyMethodDef parquet_methods[] = {
//    {"read", nanoparquet_read, METH_VARARGS, "Read a parquet file from disk."}, {NULL, NULL, 0, NULL} /* Sentinel */
};

static struct PyModuleDef nanoparquetmodule = {
  PyModuleDef_HEAD_INIT,
  "nanoparquet", /* name of module */
  nullptr,       /* module documentation, may be NULL */
  -1,            /* size of per-interpreter state of the module,
                   or -1 if the module keeps state in global variables. */
  parquet_methods
};

PyMODINIT_FUNC PyInit_nanoparquet(void) {
	return PyModule_Create(&nanoparquetmodule);
}
