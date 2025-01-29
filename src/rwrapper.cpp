#include <cstdint>

#include <Rdefines.h>

extern "C" {

SEXP nanoparquet_call = R_NilValue;

SEXP nanoparquet_read2(SEXP filesxp, SEXP options, SEXP cols, SEXP call);
SEXP nanoparquet_read_row_group(
  SEXP filesxp,
  SEXP rc,
  SEXP options,
  SEXP call
);
SEXP nanoparquet_read_col_names(SEXP filesxp);
SEXP nanoparquet_read_column_chunk(
  SEXP filesxp,
  SEXP rc,
  SEXP col,
  SEXP options,
  SEXP call
);
SEXP rf_nanoparquet_write(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression,
  SEXP metadata,
  SEXP required,
  SEXP options,
  SEXP schema,
  SEXP encoding,
  SEXP row_group_starts,
  SEXP mycall
);
SEXP rf_nanoparquet_append(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression,
  SEXP required,
  SEXP options,
  SEXP schema,
  SEXP encoding,
  SEXP row_group_starts,
  SEXP overwrite_last_row_group,
  SEXP mycall
);
SEXP rf_nanoparquet_map_to_parquet_types(SEXP df, SEXP options);
SEXP rf_nanoparquet_logical_to_converted(SEXP logical_type);
SEXP nanoparquet_read_metadata(SEXP filesxp);
SEXP nanoparquet_read_schema(SEXP filesxp);
SEXP nanoparquet_read_pages(SEXP filesxp);
SEXP nanoparquet_read_page(SEXP filesxp, SEXP page);
SEXP nanoparquet_parse_arrow_schema(SEXP rbuf);
SEXP nanoparquet_encode_arrow_schema(SEXP schema);

SEXP rf_nanoparquet_any_null(SEXP x);
SEXP rf_nanoparquet_any_na(SEXP x);

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
SEXP nanoparquet_create_dict_idx(SEXP x, SEXP from, SEXP until, SEXP call);
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

SEXP read_float(SEXP x) {
  float *f = (float*) RAW(x);
  double d = *f;
  return Rf_ScalarReal(d);
}

SEXP read_int64(SEXP x) {
  int64_t *f = (int64_t*) RAW(x);
  double d = *f;
  return Rf_ScalarReal(d);
}

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

SEXP is_ubsan_() {
#if defined(__has_feature)
#   if __has_feature(undefined_behavior_sanitizer)
#       define HAS_UBSAN 1
#   endif
#endif

#ifdef HAS_UBSAN
  return Rf_ScalarLogical(1);
#else
  return Rf_ScalarLogical(0);
#endif
}

// R native routine registration
#define CALLDEF(name, n) \
  { #name, (DL_FUNC)&name, n }

static const R_CallMethodDef R_CallDef[] = {
  CALLDEF(nanoparquet_read2, 4),
  CALLDEF(nanoparquet_read_row_group, 4),
  CALLDEF(nanoparquet_read_col_names, 1),
  CALLDEF(nanoparquet_read_column_chunk, 5),
  CALLDEF(rf_nanoparquet_write, 11),
  CALLDEF(rf_nanoparquet_append, 11),
  CALLDEF(rf_nanoparquet_map_to_parquet_types, 2),
  CALLDEF(rf_nanoparquet_logical_to_converted, 1),
  CALLDEF(nanoparquet_read_metadata, 1),
  CALLDEF(nanoparquet_read_schema, 1),
  CALLDEF(nanoparquet_read_pages, 1),
  CALLDEF(nanoparquet_read_page, 2),
  CALLDEF(nanoparquet_parse_arrow_schema, 1),
  CALLDEF(nanoparquet_encode_arrow_schema, 1),
  CALLDEF(rf_nanoparquet_any_null, 1),
  CALLDEF(rf_nanoparquet_any_na, 1),
  CALLDEF(nanoparquet_rle_decode_int, 4),
  CALLDEF(nanoparquet_rle_encode_int, 2),
  CALLDEF(nanoparquet_dbp_decode_int32, 1),
  CALLDEF(nanoparquet_dbp_encode_int32, 1),
  CALLDEF(nanoparquet_dbp_decode_int64, 1),
  CALLDEF(nanoparquet_dbp_encode_int64, 1),
  CALLDEF(nanoparquet_unpack_bits_int32, 3),
  CALLDEF(nanoparquet_pack_bits_int32, 2),
  CALLDEF(nanoparquet_create_dict, 2),
  CALLDEF(nanoparquet_create_dict_idx, 4),
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
  CALLDEF(read_float, 1),
  CALLDEF(read_int64, 1),

  CALLDEF(is_asan_, 0),
  CALLDEF(is_ubsan_, 0),
  {NULL, NULL, 0}
};

void R_init_nanoparquet(DllInfo *dll) {
  R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}

}
