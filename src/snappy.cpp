#include <Rdefines.h>

#include "snappy/snappy.h"
#include "miniz/miniz_wrapper.hpp"
#include "zstd.h"

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

SEXP gzip_compress_raw(SEXP x) {
  R_xlen_t data_size = Rf_xlength(x);
  miniz::MiniZStream mzs;
  size_t maxcsize = mzs.MaxCompressedLength(data_size);
  SEXP res = PROTECT(Rf_allocVector(RAWSXP, maxcsize));
  mzs.Compress((const char*) RAW(x), data_size, (char*) RAW(res), &maxcsize);
  res = Rf_lengthgets(res, maxcsize);
  UNPROTECT(1);
  return res;
}

SEXP gzip_uncompress_raw(SEXP x, SEXP ucl) {
  R_xlen_t data_size = Rf_xlength(x);
  size_t cusl = INTEGER(ucl)[0];
  miniz::MiniZStream mzs;
  SEXP res = PROTECT(Rf_allocVector(RAWSXP, cusl));
  mzs.Decompress((const char*) RAW(x), data_size, (char*) RAW(res), cusl);
  UNPROTECT(1);
  return res;
}

SEXP zstd_compress_raw(SEXP x) {
  R_xlen_t data_size = Rf_xlength(x);
  miniz::MiniZStream mzs;
  size_t maxcsize = zstd::ZSTD_compressBound(data_size);
  SEXP res = PROTECT(Rf_allocVector(RAWSXP, maxcsize));
  size_t tgt_size = zstd::ZSTD_compress(
    (char*) RAW(res),
    maxcsize,
    (const char*) RAW(x),
    data_size,
    ZSTD_CLEVEL_DEFAULT
  );
  res = Rf_lengthgets(res, tgt_size);
  UNPROTECT(1);
  return res;
}

SEXP zstd_uncompress_raw(SEXP x, SEXP ucl) {
  R_xlen_t data_size = Rf_xlength(x);
  size_t cusl = INTEGER(ucl)[0];
  SEXP res = PROTECT(Rf_allocVector(RAWSXP, cusl));
  zstd::ZSTD_decompress(RAW(res), cusl, RAW(x), data_size);
  UNPROTECT(1);
  return res;
}

}
