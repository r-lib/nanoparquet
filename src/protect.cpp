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

SEXP wrapped_mkchar(void *data) {
  const char **c = (const char **) data;
  return Rf_mkChar(*c);
}

SEXP wrapped_mknamed_vec(void *data) {
  const char ***rdata = (const char ***) data;
  return Rf_mkNamed(VECSXP, *rdata);
}
