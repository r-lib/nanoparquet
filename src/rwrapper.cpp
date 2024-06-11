#include <Rdefines.h>

extern "C" {

SEXP nanoparquet_read(SEXP filesxp);
SEXP nanoparquet_write(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression,
  SEXP metadata,
  SEXP required
);
SEXP nanoparquet_read_metadata(SEXP filesxp);
SEXP nanoparquet_read_schema(SEXP filesxp);
SEXP nanoparquet_read_pages(SEXP filesxp);
SEXP nanoparquet_read_page(SEXP filesxp, SEXP page);
SEXP nanoparquet_parse_arrow_schema(SEXP rbuf);
SEXP nanoparquet_encode_arrow_schema(SEXP schema);

SEXP nanoparquet_rle_decode_int(SEXP x, SEXP bit_width, SEXP
                                includes_length, SEXP length);
SEXP nanoparquet_rle_encode_int(SEXP x, SEXP bit_width);
SEXP nanoparquet_dbp_decode_int32(SEXP x);
SEXP nanoparquet_dbp_encode_int32(SEXP x);
SEXP nanoparquet_dbp_decode_int64(SEXP x);
SEXP nanoparquet_dbp_encode_int64(SEXP x);
SEXP nanoparquet_unpack_bits_int32(SEXP x, SEXP bit_width, SEXP n);
SEXP nanoparquet_pack_bits_int32(SEXP x, SEXP bit_width);

SEXP nanoparquet_create_dict(SEXP x, SEXP l);
SEXP nanoparquet_create_dict_idx(SEXP x);
SEXP nanoparquet_avg_run_length(SEXP x, SEXP len);

SEXP nanoparquet_base64_decode(SEXP x);
SEXP nanoparquet_base64_encode(SEXP x);

SEXP snappy_compress_raw(SEXP x);
SEXP snappy_uncompress_raw(SEXP x);
SEXP gzip_compress_raw(SEXP x);
SEXP gzip_uncompress_raw(SEXP x, SEXP ucl);
SEXP zstd_compress_raw(SEXP x);
SEXP zstd_uncompress_raw(SEXP x, SEXP ucl);

SEXP test_memstream();

SEXP is_asan_() {
#if defined(__has_feature)
#   if __has_feature(address_sanitizer) // for clang
#       define __SANITIZE_ADDRESS__ // GCC already sets this
#   endif
#endif

#ifdef __SANITIZE_ADDRESS__
  return Rf_ScalarLogical(1);
#else
  return Rf_ScalarLogical(0);
#endif
}

// R native routine registration
#define CALLDEF(name, n) \
  { #name, (DL_FUNC)&name, n }

static const R_CallMethodDef R_CallDef[] = {
  CALLDEF(nanoparquet_read, 1),
  CALLDEF(nanoparquet_write, 6),
  CALLDEF(nanoparquet_read_metadata, 1),
  CALLDEF(nanoparquet_read_schema, 1),
  CALLDEF(nanoparquet_read_pages, 1),
  CALLDEF(nanoparquet_read_page, 2),
  CALLDEF(nanoparquet_parse_arrow_schema, 1),
  CALLDEF(nanoparquet_encode_arrow_schema, 1),
  CALLDEF(nanoparquet_rle_decode_int, 4),
  CALLDEF(nanoparquet_rle_encode_int, 2),
  CALLDEF(nanoparquet_dbp_decode_int32, 1),
  CALLDEF(nanoparquet_dbp_encode_int32, 1),
  CALLDEF(nanoparquet_dbp_decode_int64, 1),
  CALLDEF(nanoparquet_dbp_encode_int64, 1),
  CALLDEF(nanoparquet_unpack_bits_int32, 3),
  CALLDEF(nanoparquet_pack_bits_int32, 2),
  CALLDEF(nanoparquet_create_dict, 2),
  CALLDEF(nanoparquet_create_dict_idx, 1),
  CALLDEF(nanoparquet_avg_run_length, 2),
  CALLDEF(nanoparquet_base64_decode, 1),
  CALLDEF(nanoparquet_base64_encode, 1),
  CALLDEF(snappy_compress_raw, 1),
  CALLDEF(snappy_uncompress_raw, 1),
  CALLDEF(gzip_compress_raw, 1),
  CALLDEF(gzip_uncompress_raw, 2),
  CALLDEF(zstd_compress_raw, 1),
  CALLDEF(zstd_uncompress_raw, 2),

  CALLDEF(test_memstream, 0),

  CALLDEF(is_asan_, 0),
  {NULL, NULL, 0}
};

void R_init_nanoparquet(DllInfo *dll) {
  R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}

}
