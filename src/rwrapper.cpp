#include <Rdefines.h>

extern "C" {

SEXP miniparquet_read(SEXP filesxp);
SEXP miniparquet_write(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression,
  SEXP metadata
);
SEXP miniparquet_read_metadata(SEXP filesxp);
SEXP miniparquet_read_schema(SEXP filesxp);
SEXP miniparquet_read_pages(SEXP filesxp);
SEXP miniparquet_read_page(SEXP filesxp, SEXP page);
SEXP miniparquet_parse_arrow_schema(SEXP rbuf);
SEXP miniparquet_encode_arrow_schema(SEXP schema);

SEXP miniparquet_base64_decode(SEXP x);
SEXP miniparquet_base64_encode(SEXP x);

SEXP snappy_compress_raw(SEXP x);
SEXP snappy_uncompress_raw(SEXP x);

// R native routine registration
#define CALLDEF(name, n) \
  { #name, (DL_FUNC)&name, n }

static const R_CallMethodDef R_CallDef[] = {
  CALLDEF(miniparquet_read, 1),
  CALLDEF(miniparquet_read_metadata, 1),
  CALLDEF(miniparquet_read_schema, 1),
  CALLDEF(miniparquet_read_pages, 1),
  CALLDEF(miniparquet_read_page, 2),
  CALLDEF(miniparquet_write, 5),
  CALLDEF(miniparquet_parse_arrow_schema, 1),
  CALLDEF(miniparquet_encode_arrow_schema, 1),
  CALLDEF(miniparquet_base64_decode, 1),
  CALLDEF(miniparquet_base64_encode, 1),
  CALLDEF(snappy_compress_raw, 1),
  CALLDEF(snappy_uncompress_raw, 1),
  {NULL, NULL, 0}
};

void R_init_miniparquet(DllInfo *dll) {
  R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}

}
