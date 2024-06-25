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
    for (auto j = 0; j < Rf_length(rg); j++) {
      // allocate memory for 10 pages, might need to extend this vector
      SET_VECTOR_ELT(rg, j, Rf_allocVector(VECSXP, 10));
    }
  }

  const char *meta_named[] = { "num_rows", "col_names", "" };
  metadata = Rf_mkNamed(VECSXP, meta_named);
  R_PreserveObject(metadata);
  SET_VECTOR_ELT(metadata, 0, Rf_ScalarReal(fmt.num_rows));
  SEXP colnames = PROTECT(Rf_allocVector(STRSXP, num_cols));
  for (auto i = 0; i < num_cols; i++) {
    SET_STRING_ELT(
      colnames,
      i,
      Rf_mkCharCE(fmt.schema[i].name.c_str(), CE_UTF8)
    );
  }
  SET_VECTOR_ELT(metadata, 1, colnames);
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
  SEXP v = Rf_allocVector(INTSXP, dict_len);
  SET_VECTOR_ELT(x, 0, v);
  *dict = INTEGER(v);
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
  SEXP v = Rf_allocVector(VECSXP, 4);
  SET_VECTOR_ELT(x, page, v);
  SET_VECTOR_ELT(v, 0, Rf_allocVector(INTSXP, len));
  *data = INTEGER(VECTOR_ELT(v, 0));
  if (present) {
    SET_VECTOR_ELT(v, 1, Rf_allocVector(INTSXP, len));
    *present = INTEGER(VECTOR_ELT(v, 1));
  }
  SET_VECTOR_ELT(v, 2, Rf_ScalarReal(from));
  SET_VECTOR_ELT(v, 3, Rf_ScalarReal(to));
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

}
