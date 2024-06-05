#include "lib/RleBpDecoder.h"
#include "lib/RleBpEncoder.h"

#include <Rdefines.h>

#include "protect.h"

extern "C" {

SEXP nanoparquet_rle_decode_int(SEXP x, SEXP bit_width, SEXP includes_length,
                                SEXP length) {
  uint8_t *buf = (uint8_t *)RAW(x);
  R_xlen_t len = Rf_xlength(x);
  uint32_t num_values;
  if (LOGICAL(includes_length)[0]) {
    if (len < 4) {
      Rf_error("RLE encoded data too short to include length");
    }
    num_values = ((int32_t *)buf)[0];
    buf += 4;
  } else {
    num_values = INTEGER(length)[0];
  }

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();
  SEXP res = PROTECT(safe_allocvector_int(num_values, &uwtoken));
  RleBpDecoder decoder(buf, len, INTEGER(bit_width)[0]);
  decoder.GetBatch((uint32_t *)INTEGER(res), num_values);
  UNPROTECT(2);
  return res;
  R_API_END();
}

SEXP nanoparquet_rle_encode_int(SEXP x, SEXP bit_width) {
  int *input = INTEGER(x);
  R_xlen_t input_len = Rf_xlength(x);
  uint8_t bw = INTEGER(bit_width)[0];

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();
  size_t os = MaxRleBpSize(input, input_len, bw);
  SEXP res = PROTECT(safe_allocvector_raw(os, &uwtoken));
  uint8_t *output = (uint8_t *) RAW(res);
  size_t rs = RleBpEncode(input, input_len, bw, output, os);

  if (rs < os) {
    res = Rf_lengthgets(res, rs);
  }

  UNPROTECT(2);
  return res;
  R_API_END()
}

SEXP nanoparquet_dbp_decode_int(SEXP x) {

  return R_NilValue;
}

SEXP nanoparquet_dbp_encode_int(SEXP x) {

  return R_NilValue;
}


}
