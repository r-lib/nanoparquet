#include <Rdefines.h>

#include "snappy/snappy.h"

extern "C" {

SEXP snappy_compress_raw(SEXP x) {
  R_xlen_t data_size = Rf_xlength(x);
  size_t ms = snappy::MaxCompressedLength(data_size);
  size_t cs;
  SEXP res = PROTECT(Rf_allocVector(RAWSXP, ms));
  snappy::RawCompress(
    (const char*) RAW(x),
    data_size,
    (char*) RAW(res),
    &cs
  );
  res = Rf_lengthgets(res, cs);
  UNPROTECT(1);
  return res;
}

SEXP snappy_uncompress_raw(SEXP x) {
  R_xlen_t data_size = Rf_xlength(x);
  size_t us;
  snappy::GetUncompressedLength(
    (const char*) RAW(x),
    data_size,
    &us
  );
  SEXP res = PROTECT(Rf_allocVector(RAWSXP, us));
  auto ok = snappy::RawUncompress(
    (const char *) RAW(x),
    data_size,
    (char*) RAW(res)
  );

  if (!ok) {
    Rf_error("Snappy Uncompression failure");
  }

  UNPROTECT(1);
  return res;
}

}
