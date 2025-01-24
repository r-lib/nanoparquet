#include <cmath>
#include <iostream>

#include "lib/nanoparquet.h"
#undef ERROR
#include <Rdefines.h>
#include "protect.h"

#include "RParquetReader.h"

extern SEXP nanoparquet_call;

extern "C" {

static char _np_error[4096];

SEXP nanoparquet_read_(SEXP filesxp, SEXP rcols, SEXP options) {
  const char *sfname = CHAR(STRING_ELT(filesxp, 0));
  SEXP res = R_NilValue;

  try {
    std::string fname = sfname;
    RParquetFilter filter;
    if (!Rf_isNull(rcols)) {
      filter.filter_columns = true;
      R_xlen_t nc = Rf_length(rcols);
      filter.columns.resize(nc);
      for (auto i = 0; i < nc; i++) {
        filter.columns[i] = INTEGER(rcols)[i] - 1;
      }
    }
    RParquetReader reader(fname, filter);
    reader.read_columns();
    reader.convert_columns_to_r();
    reader.create_df();
    PROTECT(res = Rf_allocVector(VECSXP, 3));
    SET_VECTOR_ELT(res, 0, reader.columns);
    SET_VECTOR_ELT(res, 1, reader.facdicts);
    SET_VECTOR_ELT(res, 2, reader.types);
    UNPROTECT(1);
    return res;
  } catch (std::exception &ex) {
    strncpy(_np_error, ex.what(), sizeof(_np_error) - 1);
    _np_error[sizeof(_np_error) - 1] = '\0';
  }
  Rf_error("%s", _np_error);
}

struct nanoparquet_read_data {
  SEXP filesxp;
  SEXP cols;
  SEXP options;
  SEXP rc;
  SEXP col;
};

SEXP nanoparquet_read_wrapped(void *data) {
  nanoparquet_read_data *rdata = (struct nanoparquet_read_data*) data;
  SEXP filesxp = rdata->filesxp;
  SEXP cols = rdata->cols;
  SEXP options = rdata->options;
  return nanoparquet_read_(filesxp, cols, options);
}

SEXP nanoparquet_read2(SEXP filesxp,
                       SEXP cols,
                       SEXP options,
                       SEXP call) {

  SEXP uwt = PROTECT(R_MakeUnwindCont());
  R_API_START(call);

  struct nanoparquet_read_data data = {
    filesxp, options, cols
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

SEXP nanoparquet_read_row_group_(
  SEXP filesxp,
  SEXP rgsxp,
  SEXP options
) {
  const char *sfname = CHAR(STRING_ELT(filesxp, 0));
  uint32_t rg = INTEGER(rgsxp)[0];
  SEXP res = R_NilValue;

  try {
    std::string fname = sfname;
    RParquetFilter row_filter;
    row_filter.filter_row_groups = true;
    row_filter.row_groups.resize(1);
    row_filter.row_groups[0] = rg;
    RParquetReader reader(fname, row_filter);
    reader.read_row_group(rg);
    reader.convert_columns_to_r();
    reader.create_df();
    PROTECT(res = Rf_allocVector(VECSXP, 3));
    SET_VECTOR_ELT(res, 0, reader.columns);
    SET_VECTOR_ELT(res, 1, reader.facdicts);
    SET_VECTOR_ELT(res, 2, reader.types);
    UNPROTECT(1);
    return res;
  } catch (std::exception &ex) {
    Rf_error("%s", ex.what());
  }
}

SEXP nanoparquet_read_row_group_wrapped(void *data) {
  nanoparquet_read_data *rdata = (struct nanoparquet_read_data*) data;
  SEXP filesxp = rdata->filesxp;
  SEXP rc = rdata->rc;
  SEXP options = rdata->options;
  return nanoparquet_read_row_group_(filesxp, rc, options);
}

SEXP nanoparquet_read_row_group(
  SEXP filesxp,
  SEXP rg,
  SEXP options,
  SEXP call
) {
  SEXP uwt = PROTECT(R_MakeUnwindCont());
  R_API_START(call);

  struct nanoparquet_read_data data = {
    filesxp, /* cols */ R_NilValue, options, rg
  };

  SEXP ret = R_UnwindProtect(
    nanoparquet_read_row_group_wrapped,
    &data,
    throw_error,
    &uwt,
    uwt
  );

  UNPROTECT(1);
  return ret;

  R_API_END();
}

SEXP nanoparquet_read_column_chunk_(
  SEXP filesxp,
  SEXP rgsxp,
  SEXP colsxp,
  SEXP options) {

  const char *sfname = CHAR(STRING_ELT(filesxp, 0));
  uint32_t rg = INTEGER(rgsxp)[0];
  uint32_t col = INTEGER(colsxp)[0] + 1L; // skip meta column
  SEXP res = R_NilValue;

  try {
    std::string fname = sfname;
    RParquetFilter row_filter;
    row_filter.filter_row_groups = true;
    row_filter.row_groups.resize(1);
    row_filter.row_groups[0] = rg;
    RParquetReader reader(fname, row_filter);
    reader.read_column_chunk(rg, col);
    reader.convert_columns_to_r();
    reader.create_df();
    PROTECT(res = Rf_allocVector(VECSXP, 3));
    SET_VECTOR_ELT(res, 0, reader.columns);
    SET_VECTOR_ELT(res, 1, reader.facdicts);
    SET_VECTOR_ELT(res, 2, reader.types);
    UNPROTECT(1);
    return res;
  } catch (std::exception &ex) {
    Rf_error("%s", ex.what());
  }
}

SEXP nanoparquet_read_column_chunk_wrapped(void *data) {
  nanoparquet_read_data *rdata = (struct nanoparquet_read_data*) data;
  SEXP filesxp = rdata->filesxp;
  SEXP rc = rdata->rc;
  SEXP col = rdata->col;
  SEXP options = rdata->options;
  return nanoparquet_read_column_chunk_(filesxp, rc, col, options);
}

SEXP nanoparquet_read_column_chunk(
  SEXP filesxp,
  SEXP rg,
  SEXP col,
  SEXP options,
  SEXP call
) {
  SEXP uwt = PROTECT(R_MakeUnwindCont());
  R_API_START(call);

  struct nanoparquet_read_data data = {
    filesxp, /* cols */ R_NilValue, options, rg, col
  };

  SEXP ret = R_UnwindProtect(
    nanoparquet_read_column_chunk_wrapped,
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
