#include <cstdlib>

#include <Rdefines.h>

#include "base64.h"

extern "C" {

// TODO: exceptions?

SEXP miniparquet_base64_decode(SEXP x) {
  const char *input;
  size_t len;
  if (TYPEOF(x) == STRSXP) {
    input = (const char*) CHAR(STRING_ELT(x, 0));
    len = strlen(input);
  } else if (TYPEOF(x) == RAWSXP) {
    input = (const char*) RAW(x);
    len = XLENGTH(x);
  } else {
    Rf_error("Invalid input in base64 decoder");
  }

  size_t olen = base64::maximal_binary_length_from_base64(
    input,
    len
  );
  SEXP rres = PROTECT(Rf_allocVector(RAWSXP, olen));

  base64::result res = base64::base64_to_binary(
    input,
    len,
    (char*) RAW(rres)
  );
  if(res.error != base64::error_code::SUCCESS) {
    Rf_error("Base64 decoding error at position %zu", res.count);
  }

  if (res.count < olen) {
    rres = Rf_xlengthgets(rres, res.count);
  }

  UNPROTECT(1);
  return rres;
}

// TODO: exceptions?

SEXP miniparquet_base64_encode(SEXP x) {
  const char *input;
  size_t len;
  if (TYPEOF(x) == STRSXP) {
    input = (const char*) CHAR(STRING_ELT(x, 0));
    len = strlen(input);
  } else if (TYPEOF(x) == RAWSXP) {
    input = (const char*) RAW(x);
    len = XLENGTH(x);
  } else {
    Rf_error("Invalid input in base64 encoder");
  }

  size_t olen = base64::base64_length_from_binary(len);
  SEXP rtmp = PROTECT(Rf_allocVector(RAWSXP, olen));
  size_t truelen = base64::binary_to_base64(
    input,
    len,
    (char*) RAW(rtmp)
  );
  SEXP rres = PROTECT(Rf_allocVector(STRSXP, 1));
  SET_STRING_ELT(rres, 0, Rf_mkCharLen((const char*) RAW(rtmp), truelen));

  UNPROTECT(2);
  return rres;
}

}
