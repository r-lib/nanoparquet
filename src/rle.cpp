#include "lib/RleBpDecoder.h"
#include "lib/RleBpEncoder.h"

#include <Rdefines.h>

extern "C" {

SEXP nanoparquet_rle_decode_int(SEXP x, SEXP bit_width,
                                SEXP includes_length, SEXP length) {
  int prot = 0;
  uint8_t *buf = (uint8_t *) RAW(x);
  R_xlen_t len = Rf_xlength(x);
  uint32_t num_values;
  if (LOGICAL(includes_length)[0]) {
    if (len < 4) {
      Rf_error("RLE encoded data too short to include length");
    }
    num_values = ((int32_t*)buf)[0];
    buf += 4;
  } else {
    num_values = INTEGER(length)[0];
  }

  char error_buffer[8192];
  error_buffer[0] = '\0';

  try {

    RleBpDecoder decoder(buf, len, INTEGER(bit_width)[0]);
    SEXP res = PROTECT(Rf_allocVector(INTSXP, num_values)); prot++;

    decoder.GetBatch((uint32_t*) INTEGER(res), num_values);

    UNPROTECT(prot);
    return res;

  } catch (std::exception &ex) {
    strncpy(error_buffer, ex.what(), sizeof(error_buffer) - 1); // # nocov
  }

  if (error_buffer[0] != '\0') {         // # nocov
    Rf_error("%s", error_buffer);        // # nocov
  }                                      // # nocov

  // never reached
  UNPROTECT(prot);
  return R_NilValue; // # nocov
}

SEXP nanoparquet_rle_encode_int(SEXP x, SEXP bit_width) {
  int prot = 0;
  int *input = INTEGER(x);
  R_xlen_t input_len = Rf_xlength(x);
  uint8_t bw = INTEGER(bit_width)[0];

  char error_buffer[8192];
  error_buffer[0] = '\0';

  try {

    size_t os = MaxRleBpSize(input, input_len, bw);
    SEXP res = PROTECT(Rf_allocVector(RAWSXP, os)); prot++;
    uint8_t *output = (uint8_t *) RAW(res);
    size_t rs = RleBpEncode(input, input_len, bw, output, os);

    if (rs < os) {
      res = Rf_lengthgets(res, rs);
    }

    UNPROTECT(prot);
    return res;

  } catch (std::exception &ex) {
    strncpy(error_buffer, ex.what(), sizeof(error_buffer) - 1); // # nocov
  }

  if (error_buffer[0] != '\0') {         // # nocov
    Rf_error("%s", error_buffer);        // # nocov
  }                                      // # nocov

  // never reached
  UNPROTECT(prot);
  return R_NilValue; // # nocov
}

}
