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

SEXP nanoparquet_any_na(SEXP x) noexcept {
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

SEXP nanoparquet_any_null(SEXP x) noexcept {
  R_xlen_t l = Rf_xlength(x);
  for (R_xlen_t i = 0; i < l; i++) {
    if (Rf_isNull(VECTOR_ELT(x, i))) {
      return Rf_ScalarLogical(1);
    }
  }

  return Rf_ScalarLogical(0);
}

} // extern "C"
