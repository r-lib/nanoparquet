#include <cstring>
#include <cmath>

#include "r-nanoparquet.h"

extern "C" {

Int96 int32_to_int96(int32_t x) noexcept {
  if (x >= 0) {
    Int96 r = { 0, 0, 0 };
    int32_t *p = (int32_t*) r.value;
    *p = x;
    return r;
  } else {
    Int96 r = { 0, 0xffffffff, 0xffffffff };
    int32_t *p = (int32_t*) r.value;
    *p = x;
    return r;
  }
}

Int96 double_to_int96(double x) noexcept {
  x = trunc(x);
  Int96 r = { 0, 0, 0 };
  bool neg = x < 0;
  x = neg ? -x : x;

  // absolute value first
  // TODO: what to do if too big?
  r.value[0] = fmod(x, 0xffffffff);
  x = trunc(x / 0xffffffff);
  r.value[1] = fmod(x, 0xffffffff);
  x = trunc(x / 0xffffffff);
  r.value[2] = x;

  // TODO: what to do if too small?
  if (neg) {
    r.value[0] = ~r.value[0] + 1;
    r.value[1] = ~r.value[1];
    r.value[2] = ~r.value[2];
    if (r.value[0] == 0) {
      r.value[1] += 1;
      if (r.value[1] == 0) {
        r.value[2] += 1;
      }
    }
  }
  return r;
}

uint16_t double_to_float16(double x) noexcept {
  if (x == R_PosInf) {
    return 0x7c00;
  } else if (x == R_NegInf) {
    return 0xfc00;
  } else if (R_IsNaN(x)) {
    return 0x7C80;
  } else if (x > 65504) {
    return 0x7c00;
  } else if (x < -65504) {
    return 0xfc00;
  } else if (x >= 0 && x < 0.000000059604645) {
    return 0x0000;
  } else if (x <= 0 && x > -0.000000059604645) {
    return 0x0000;
  }
  float f = x;
  uint32_t fi32 = *((uint32_t*) &f);
  uint16_t fi16 = (fi32 >> 31) << 5;
  uint16_t tmp = (fi32 >> 23) & 0xff;
  tmp = (tmp - 0x70) & ((uint32_t)((int32_t)(0x70 - tmp) >> 4) >> 27);
  fi16 = (fi16 | tmp) << 10;
  fi16 |= (fi32 >> 13) & 0x3ff;
  return fi16;
}

SEXP rf_get_list_element(SEXP list, const char *str) noexcept {
  SEXP elmt = R_NilValue;
  SEXP names = PROTECT(Rf_getAttrib(list, R_NamesSymbol));

  for (R_xlen_t i = 0; i < Rf_xlength(list); i++) {
    if (strcmp(CHAR(STRING_ELT(names, i)), str) == 0) {
       elmt = VECTOR_ELT(list, i);
       break;
    }
  }
  UNPROTECT(1);
  return elmt;
}

SEXP rf_nanoparquet_any_na(SEXP x) noexcept {
  R_xlen_t l = Rf_xlength(x);
  double *ptr = REAL(x);
  double *end = ptr + l;
  for (; ptr < end; ptr++) {
    if (R_IsNA(*ptr)) {
      return Rf_ScalarLogical(1);
    }
  }

  return Rf_ScalarLogical(0);
}

SEXP rf_nanoparquet_any_na_int64(SEXP x) noexcept {
  R_xlen_t l = Rf_xlength(x);
  int64_t *ptr = (int64_t *) REAL(x);
  int64_t *end = ptr + l;
  for (; ptr < end; ptr++) {
    if (*ptr == INT64_MIN) {
      return Rf_ScalarLogical(1);
    }
  }
  return Rf_ScalarLogical(0);
}

SEXP rf_nanoparquet_any_null(SEXP x) noexcept {
  R_xlen_t l = Rf_xlength(x);
  for (R_xlen_t i = 0; i < l; i++) {
    if (Rf_isNull(VECTOR_ELT(x, i))) {
      return Rf_ScalarLogical(1);
    }
  }

  return Rf_ScalarLogical(0);
}

SEXP nanoparquet_repeated_positions(
  SEXP data,
  SEXP replevels,
  SEXP deflevels,
  SEXP nrows
) {
  SEXP res = Rf_protect(Rf_allocVector(INTSXP, INTEGER(nrows)[0]));
  // first repetition level is always 0
  INTEGER(res)[0] = 1;
  for (R_xlen_t i = 1, r = 0; i < Rf_xlength(replevels); i++) {
    if (RAW(replevels)[i] > 0) {
      INTEGER(res)[r]++;
    } else {
      r++;
      if (r >= INTEGER(nrows)[0]) {
        Rf_error("Repetition levels indicate more rows than expected, broken Parquet file?");
        break;
      }
      INTEGER(res)[r] = RAW(deflevels)[i] ? 1 : 0;
    }
  }
  Rf_unprotect(1);
  return res;
}

SEXP nanoparquet_enlist_sizes_req_req(SEXP rep, SEXP def, SEXP nrows) {
  R_xlen_t len = Rf_xlength(rep);
  if (Rf_length(def) != len) {
    Rf_error("Length of repetition and definition levels must be the same");
  }
  R_xlen_t listlen = REAL(nrows)[0];
  Rbyte *crep = RAW(rep), *cdef = RAW(def);
  SEXP res = Rf_protect(Rf_allocVector(INTSXP, listlen));
  R_xlen_t p = -1;
  for (Rbyte *r = crep, *d = cdef; r < crep + len; r++, d++) {
    if (*r == 0) {
      INTEGER(res)[++p] = *d == 0 ? 0 : 1;
    } else {
      INTEGER(res)[p]++;
    }
  }

  Rf_unprotect(1);
  return res;
}

inline void set_list_element(SEXP list, R_xlen_t idx, SEXP x, R_xlen_t xidx) {
  switch (TYPEOF(list)) {
  case VECSXP:
    SET_VECTOR_ELT(list, idx, VECTOR_ELT(x, xidx));
    break;
  case STRSXP:
    SET_STRING_ELT(list, idx, STRING_ELT(x, xidx));
    break;
  case LGLSXP:
    LOGICAL(list)[idx] = LOGICAL(x)[xidx];
    break;
  case INTSXP:
    INTEGER(list)[idx] = INTEGER(x)[xidx];
    break;
  case REALSXP:
    REAL(list)[idx] = REAL(x)[xidx];
    break;
  default:
    Rf_error("Unsupported list element type");
  }
}

inline void set_list_element_na(SEXP list, R_xlen_t idx) {
  switch (TYPEOF(list)) {
  case VECSXP:
    SET_VECTOR_ELT(list, idx, R_NilValue);
    break;
  case LGLSXP:
    LOGICAL(list)[idx] = NA_LOGICAL;
    break;
  case INTSXP:
    INTEGER(list)[idx] = NA_INTEGER;
    break;
  case REALSXP:
    REAL(list)[idx] = NA_REAL;
    break;
  case STRSXP:
    SET_STRING_ELT(list, idx, NA_STRING);
    break;
  default:
    Rf_error("Unsupported list element type");
  }
}

SEXP nanoparquet_enlist_req_opt(SEXP x, SEXP rep, SEXP def, SEXP nrows) {
  R_xlen_t len = Rf_xlength(rep);
  if (Rf_length(def) != len) {
    Rf_error("Length of repetition and definition levels must be the same");
  }
  R_xlen_t listlen = REAL(nrows)[0];
  SEXP lengths = Rf_protect(Rf_allocVector(REALSXP, listlen));
  double *clengths = REAL(lengths);
  SEXP res = Rf_protect(Rf_allocVector(VECSXP, listlen));

  // calculate the length of each list element. we need this first to
  // avoid reallocations
  Rbyte *crep = RAW(rep), *cdef = RAW(def);
  R_xlen_t lstidx = -1;
  for (Rbyte *r = crep, *d = cdef; r < crep + len; r++, d++) {
    if (*r == 0) {
      // new list element
      if (*d == 0) {
        REAL(lengths)[++lstidx] = 0;
      } else {
        REAL(lengths)[++lstidx] = 1;
      }
    } else {
      REAL(lengths)[lstidx]++;

    }
  }

  // now fill in the list elements
  lstidx = -1;
  R_xlen_t xidx = 0, eltidx = 0;
  for (Rbyte *r = crep, *d = cdef; r < crep + len; r++, d++) {
    if (*r == 0) {
      lstidx++;
      SET_VECTOR_ELT(res, lstidx, Rf_allocVector(TYPEOF(x), clengths[lstidx]));
      eltidx = 0;
    }
    // def = 0 => empty list, 1 => NA, 2 => non-NA element
    if (*d == 1) {
      // NA
      set_list_element_na(VECTOR_ELT(res, lstidx), eltidx);
      eltidx++;
    } else if (*d == 2) {
      // non-NA element
      set_list_element(VECTOR_ELT(res, lstidx), eltidx, x, xidx);
      eltidx++;
      xidx++;
    }
  }

  Rf_unprotect(2);
  return res;
}

SEXP nanoparquet_enlist_opt_req(SEXP x, SEXP rep, SEXP def, SEXP nrows) {
  R_xlen_t len = Rf_xlength(rep);
  if (Rf_length(def) != len) {
    Rf_error("Length of repetition and definition levels must be the same");
  }
  R_xlen_t listlen = REAL(nrows)[0];
  SEXP lengths = Rf_protect(Rf_allocVector(REALSXP, listlen));
  double *clengths = REAL(lengths);
  SEXP res = Rf_protect(Rf_allocVector(VECSXP, listlen));

  // calculate the length of each list element. we need this first to
  // avoid reallocations
  Rbyte *crep = RAW(rep), *cdef = RAW(def);
  R_xlen_t lstidx = -1;
  for (Rbyte *r = crep, *d = cdef; r < crep + len; r++, d++) {
    if (*r == 0) {
      // new list element
      if (*d <= 1) {
        // NULL or list()
        REAL(lengths)[++lstidx] = 0;
      } else {
        REAL(lengths)[++lstidx] = 1;
      }
    } else {
      REAL(lengths)[lstidx]++;

    }
  }

  // now fill in the list elements
  lstidx = -1;
  R_xlen_t xidx = 0, eltidx = 0;
  for (Rbyte *r = crep, *d = cdef; r < crep + len; r++, d++) {
    if (*r == 0) {
      lstidx++;
      eltidx = 0;
      if (*d == 0) {
        // NULL
        SET_VECTOR_ELT(res, lstidx, R_NilValue);
      } else {
        // list() or non-empty list element
        SET_VECTOR_ELT(res, lstidx, Rf_allocVector(TYPEOF(x), clengths[lstidx]));
      }
    }
    if (*d == 2) {
      // non-NA element
      set_list_element(VECTOR_ELT(res, lstidx), eltidx, x, xidx);
      eltidx++;
      xidx++;
    }
  }

  Rf_unprotect(2);
  return res;
}

SEXP nanoparquet_enlist_opt_opt(SEXP x, SEXP rep, SEXP def, SEXP nrows) {
  R_xlen_t len = Rf_xlength(rep);
  if (Rf_length(def) != len) {
    Rf_error("Length of repetition and definition levels must be the same");
  }
  R_xlen_t listlen = REAL(nrows)[0];
  SEXP lengths = Rf_protect(Rf_allocVector(REALSXP, listlen));
  double *clengths = REAL(lengths);
  SEXP res = Rf_protect(Rf_allocVector(VECSXP, listlen));

  // calculate the length of each list element. we need this first to
  // avoid reallocations
  Rbyte *crep = RAW(rep), *cdef = RAW(def);
  R_xlen_t lstidx = -1;
  for (Rbyte *r = crep, *d = cdef; r < crep + len; r++, d++) {
    if (*r == 0) {
      // new list element
      if (*d <= 1) {
        // NULL or list()
        REAL(lengths)[++lstidx] = 0;
      } else {
        REAL(lengths)[++lstidx] = 1;
      }
    } else {
      REAL(lengths)[lstidx]++;
    }
  }

  // now fill in the list elements
  lstidx = -1;
  R_xlen_t xidx = 0, eltidx = 0;
  for (Rbyte *r = crep, *d = cdef; r < crep + len; r++, d++) {
    if (*r == 0) {
      lstidx++;
      eltidx = 0;
      if (*d == 0) {
        // NULL
        SET_VECTOR_ELT(res, lstidx, R_NilValue);
      } else {
        // list() or non-empty list element
        SET_VECTOR_ELT(res, lstidx, Rf_allocVector(TYPEOF(x), clengths[lstidx]));
      }
    }
    if (*d == 2) {
      // NA
      set_list_element_na(VECTOR_ELT(res, lstidx), eltidx);
      eltidx++;
    } else if (*d == 3) {
      // non-NA element
      set_list_element(VECTOR_ELT(res, lstidx), eltidx, x, xidx);
      eltidx++;
      xidx++;
    }
  }

  Rf_unprotect(2);
  return res;
}

} // extern "C"
