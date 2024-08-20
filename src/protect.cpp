#include "protect.h"

void throw_error(void *err, Rboolean jump) {
  if (jump) {
    struct np_error *rerr = (struct np_error*) err;
    throw *rerr;
  }
}

SEXP wrapped_rawsxp(void *len) {
  R_xlen_t *xlen = (R_xlen_t*) len;
  return Rf_allocVector(RAWSXP, *xlen);
}

SEXP wrapped_intsxp(void *len) {
  R_xlen_t *xlen = (R_xlen_t*) len;
  return Rf_allocVector(INTSXP, *xlen);
}

SEXP wrapped_realsxp(void *len) {
  R_xlen_t *xlen = (R_xlen_t*) len;
  return Rf_allocVector(REALSXP, *xlen);
}

SEXP wrapped_strsxp(void *len) {
  R_xlen_t *xlen = (R_xlen_t*) len;
  return Rf_allocVector(STRSXP, *xlen);
}

SEXP wrapped_vecsxp(void *len) {
  R_xlen_t *xlen = (R_xlen_t*) len;
  return Rf_allocVector(VECSXP, *xlen);
}

SEXP wrapped_mkchar(void *data) {
  const char **c = (const char **) data;
  return Rf_mkChar(*c);
}

SEXP wrapped_mkstring(void *data) {
  const char **c = (const char **) data;
  return Rf_mkString(*c);
}

SEXP wrapped_scalarinteger(void *data) {
  int *n = (int*) data;
  return Rf_ScalarInteger(*n);
}

SEXP wrapped_scalarreal(void *data) {
  double *n = (double*) data;
  return Rf_ScalarReal(*n);
}

SEXP wrapped_scalarlogical(void *data) {
  int *n = (int*) data;
  return Rf_ScalarLogical(*n);
}

SEXP wrapped_scalarstring(void *data) {
  SEXP *x = (SEXP*) data;
  return Rf_ScalarString(*x);
}

SEXP wrapped_mknamed_vec(void *data) {
  const char ***rdata = (const char ***) data;
  return Rf_mkNamed(VECSXP, *rdata);
}
