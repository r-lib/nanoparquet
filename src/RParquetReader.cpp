#include "RParquetReader.h"

RParquetReader::RParquetReader(std::string filename)
  : ParquetReader(filename) {
  parquet::FileMetaData fmt = get_file_meta_data();
  uint32_t num_cols = fmt.schema.size();
  uint32_t num_row_groups = fmt.row_groups.size();

  columns = Rf_allocVector(VECSXP, num_cols);
  R_PreserveObject(columns);
  for (auto i = 0; i < num_cols; i++) {
    // skip non-leaf columns
    if (fmt.schema[i].__isset.num_children) continue;
    SEXP rg = Rf_allocVector(VECSXP, num_row_groups);
    SET_VECTOR_ELT(columns, i, rg);
  }

  const char *meta_named[] = {
    "num_rows",
    "row_group_num_rows",
    "col_name",
    "type",
    "converted_type",
    "logical_type",
    ""
  };
  metadata = Rf_mkNamed(VECSXP, meta_named);
  R_PreserveObject(metadata);
  SET_VECTOR_ELT(metadata, 0, Rf_ScalarReal(fmt.num_rows));
  SEXP rgnr = PROTECT(Rf_allocVector(REALSXP, fmt.row_groups.size()));
  for (auto i = 0; i < fmt.row_groups.size(); i++) {
    REAL(rgnr)[i] = fmt.row_groups[i].num_rows;
  }
  SET_VECTOR_ELT(metadata, 1, rgnr);
  UNPROTECT(1);
  SEXP colnames = PROTECT(Rf_allocVector(STRSXP, num_cols));
  for (auto i = 0; i < num_cols; i++) {
    SET_STRING_ELT(
      colnames,
      i,
      Rf_mkCharCE(fmt.schema[i].name.c_str(), CE_UTF8)
    );
  }
  SET_VECTOR_ELT(metadata, 2, colnames);
  UNPROTECT(1);
}

RParquetReader::~RParquetReader() {
  if (!Rf_isNull(columns)) {
    R_ReleaseObject(columns);
  }
  if (!Rf_isNull(metadata)) {
    R_ReleaseObject(metadata);
  }
}

void RParquetReader::add_dict_page_int32(
  uint32_t column,
  uint32_t row_group,
  int32_t **dict,
  uint32_t dict_len) {

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[row_group];
    SEXP val = Rf_allocVector(VECSXP, 2);
    SET_VECTOR_ELT(VECTOR_ELT(columns, column), row_group, val);
    SET_VECTOR_ELT(val, 0, Rf_allocVector(INTSXP, dict_len));
    SET_VECTOR_ELT(val, 1, Rf_allocVector(INTSXP, nr));
    *dict = INTEGER(VECTOR_ELT(val, 0));
  } else {
    Rf_error("Dictionary already set");
  }
}

void RParquetReader::add_data_page_int32(
  uint32_t column,
  uint32_t row_group,
  uint32_t page,
  int32_t **data,
  int32_t **present,
  uint64_t len,
  uint64_t from,
  uint64_t to) {

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[row_group];
    x = Rf_allocVector(INTSXP, nr);
    SET_VECTOR_ELT(VECTOR_ELT(columns, column), row_group, x);
  }
  *data = INTEGER(x) + from;
  // TODO: present
}

void RParquetReader::add_dict_page_double(
  uint32_t column,
  uint32_t row_group,
  double **dict,
  uint32_t dict_len) {

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[row_group];
    SEXP val = Rf_allocVector(VECSXP, 2);
    SET_VECTOR_ELT(VECTOR_ELT(columns, column), row_group, val);
    SET_VECTOR_ELT(val, 0, Rf_allocVector(REALSXP, dict_len));
    SET_VECTOR_ELT(val, 1, Rf_allocVector(INTSXP, nr));
    *dict = REAL(VECTOR_ELT(val, 0));
  } else {
    Rf_error("Dictionary already set");
  }
}

void RParquetReader::add_data_page_double(
  uint32_t column,
  uint32_t row_group,
  uint32_t page,
  double **data,
  int32_t **present,
  uint64_t len,
  uint64_t from,
  uint64_t to) {

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[row_group];
    x = Rf_allocVector(REALSXP, nr);
    SET_VECTOR_ELT(VECTOR_ELT(columns, column), row_group, x);
  }
  *data = REAL(x) + from;
  // TODO: present
}

void RParquetReader::add_dict_indices(
  uint32_t column,
  uint32_t row_group,
  uint32_t page,
  uint32_t **dict_idx,
  int32_t **present,
  uint64_t len,
  uint64_t from,
  uint64_t to) {

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  *dict_idx = (uint32_t*) INTEGER(VECTOR_ELT(x, 1));
  // TODO: present
}
