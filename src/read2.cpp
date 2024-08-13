#include <cmath>
#include <iostream>

#include "lib/nanoparquet.h"
#undef ERROR
#include <Rdefines.h>
#include "protect.h"

#include "RParquetReader.h"

extern SEXP nanoparquet_call;

extern "C" {

SEXP nanoparquet_read_(SEXP filesxp, SEXP options) {
  const char *sfname = CHAR(STRING_ELT(filesxp, 0));

  try {
    std::string fname = sfname;
    RParquetReader reader(fname);
    reader.read_all_columns();
    reader.convert_columns_to_r();
    reader.create_df();
    return reader.columns;
  } catch (std::exception &ex) {
    Rf_error("%s", ex.what());
  }
}

struct nanoparquet_read_data {
  SEXP filesxp;
  SEXP options;
};

SEXP nanoparquet_read_wrapped(void *data) {
  nanoparquet_read_data *rdata = (struct nanoparquet_read_data*) data;
  SEXP filesxp = rdata->filesxp;
  SEXP options = rdata->options;
  return nanoparquet_read_(filesxp, options);
}

SEXP nanoparquet_read2(SEXP filesxp,
                       SEXP options,
                       SEXP call) {

  SEXP uwt = PROTECT(R_MakeUnwindCont());
  R_API_START(call);

  struct nanoparquet_read_data data = {
    filesxp, options
  };

  SEXP ret = R_UnwindProtect(
    nanoparquet_read_wrapped,
    &data,
    throw_error,
    &uwt,
    uwt
  );

  UNPROTECT(1);
  return ret;

  R_API_END();
}

} // extern "C"
