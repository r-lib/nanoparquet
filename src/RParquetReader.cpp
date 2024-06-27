#include "RParquetReader.h"

constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;
constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

static int64_t impala_timestamp_to_nanoseconds(const int96_t &impala_timestamp) {
  int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;

  int64_t nanoseconds;
  memcpy(&nanoseconds, impala_timestamp.value, sizeof(nanoseconds));
  return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

// ------------------------------------------------------------------------

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

// ------------------------------------------------------------------------

void RParquetReader::convert_columns_to_r() {
  for (auto cn = 0; cn < file_meta_data_.schema.size(); cn++) {
    parquet::SchemaElement &sel = file_meta_data_.schema[cn];
    if (sel.__isset.num_children) {
      continue;
    }
    for (auto rg = 0; rg < file_meta_data_.row_groups.size(); rg++) {
      switch (sel.type) {
      case parquet::Type::INT64: {
        SEXP col = VECTOR_ELT(VECTOR_ELT(columns, cn), rg);
        convert_int64_to_double(col);
        break;
      }
      case parquet::Type::INT96: {
        SEXP col = VECTOR_ELT(columns, cn);
        convert_int96_to_double(col, rg);
        break;
      }
      case parquet::Type::FLOAT: {
        SEXP col = VECTOR_ELT(VECTOR_ELT(columns, cn), rg);
        convert_float_to_double(col);
        break;
      }
      default:
        // others are ok
        break;
      }
    }
  }
}

void RParquetReader::convert_int64_to_double(SEXP x) {
  if (TYPEOF(x) == VECSXP) {
    // dictionary
    x = VECTOR_ELT(x, 0);
  } else {
    // plain
  }
  double *dp = REAL(x);
  int64_t *ip = (int64_t*) REAL(x);
  R_xlen_t l = Rf_xlength(x);
  for (R_xlen_t i = 0; i < l; i++, dp++, ip++) {
    *dp = static_cast<double>(*ip);
  }
}

void RParquetReader::convert_int96_to_double(SEXP v, uint32_t idx) {
  SEXP x = VECTOR_ELT(v, idx);
  if (TYPEOF(x) == VECSXP) {
    // dictionary, transform v[idx][0]
    v = x;
    idx = 0;
    x = VECTOR_ELT(x, 0);
  } else {
    // plain, transform v[idx]
  }

  R_xlen_t l3 = Rf_xlength(x);
  R_xlen_t l = l3 / 3;
  SEXP y = PROTECT(Rf_allocVector(REALSXP, l));
  int96_t *ip = (int96_t*) INTEGER(x);
  double *dp = REAL(y);

  for (R_xlen_t i = 0; i < l; i++, ip++, dp++) {
    *dp = impala_timestamp_to_nanoseconds(*ip) / 1000000;
  }
}

void RParquetReader::convert_float_to_double(SEXP x) {
  if (TYPEOF(x) == VECSXP) {
    // dictionary
    x = VECTOR_ELT(x, 0);
  } else {
    // plain
  }
  R_xlen_t l = Rf_xlength(x);
  double *start = REAL(x);
  double *end = start + l;
  float *fstart = (float*) REAL(x);
  float *fend = fstart + l;
  while (end > start) {
    end--; fend--;
    *end = *fend;
    *end = static_cast<double>(*fend);
  }
}

// ------------------------------------------------------------------------

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

void RParquetReader::add_dict_page_int64(
  uint32_t column,
  uint32_t row_group,
  int64_t **dict,
  uint32_t dict_len) {

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[row_group];
    SEXP val = Rf_allocVector(VECSXP, 2);
    SET_VECTOR_ELT(VECTOR_ELT(columns, column), row_group, val);
    SET_VECTOR_ELT(val, 0, Rf_allocVector(REALSXP, dict_len));
    SET_VECTOR_ELT(val, 1, Rf_allocVector(INTSXP, nr));
    *dict = (int64_t*) REAL(VECTOR_ELT(val, 0));
  } else {
    Rf_error("Dictionary already set");
  }
}

void RParquetReader::add_data_page_int64(
  uint32_t column,
  uint32_t row_group,
  uint32_t page,
  int64_t **data,
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
  *data = (int64_t*) REAL(x) + from;
  // TODO: present
}

void RParquetReader::add_dict_page_int96(
  uint32_t column,
  uint32_t row_group,
  int96_t **dict,
  uint32_t dict_len) {

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[row_group];
    SEXP val = Rf_allocVector(VECSXP, 2);
    SET_VECTOR_ELT(VECTOR_ELT(columns, column), row_group, val);
    SET_VECTOR_ELT(val, 0, Rf_allocVector(INTSXP, dict_len * 3));
    SET_VECTOR_ELT(val, 1, Rf_allocVector(INTSXP, nr));
    *dict = (int96_t*) INTEGER(VECTOR_ELT(val, 0));
  } else {
    Rf_error("Dictionary already set");
  }
}

void RParquetReader::add_data_page_int96(
  uint32_t column,
  uint32_t row_group,
  uint32_t page,
  int96_t **data,
  int32_t **present,
  uint64_t len,
  uint64_t from,
  uint64_t to) {

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[row_group];
    x = Rf_allocVector(INTSXP, nr * 3);
    SET_VECTOR_ELT(VECTOR_ELT(columns, column), row_group, x);
  }
  *data = ((int96_t*) INTEGER(x)) + from;
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

void RParquetReader::add_dict_page_float(
  uint32_t column,
  uint32_t row_group,
  float **dict,
  uint32_t dict_len) {

  // We allocate doubles, that's what we'll need eventually

  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, column), row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[row_group];
    SEXP val = Rf_allocVector(VECSXP, 2);
    SET_VECTOR_ELT(VECTOR_ELT(columns, column), row_group, val);
    SET_VECTOR_ELT(val, 0, Rf_allocVector(REALSXP, dict_len));
    SET_VECTOR_ELT(val, 1, Rf_allocVector(INTSXP, nr));
    *dict = (float*) REAL(VECTOR_ELT(val, 0));
  } else {
    Rf_error("Dictionary already set");
  }
}

void RParquetReader::add_data_page_float(
  uint32_t column,
  uint32_t row_group,
  uint32_t page,
  float **data,
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
  *data = ((float*) REAL(x)) + from;
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
