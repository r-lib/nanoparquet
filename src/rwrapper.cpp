#include <Rdefines.h>

extern "C" {

SEXP miniparquet_read(SEXP filesxp);
SEXP miniparquet_write(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression);
SEXP miniparquet_read_metadata(SEXP filesxp);

// R native routine registration
#define CALLDEF(name, n) \
  { #name, (DL_FUNC)&name, n }

static const R_CallMethodDef R_CallDef[] = {
  CALLDEF(miniparquet_read, 1),
  CALLDEF(miniparquet_read_metadata, 1),
  CALLDEF(miniparquet_write, 4),
  {NULL, NULL, 0}
};

void R_init_miniparquet(DllInfo *dll) {
  R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}

}
