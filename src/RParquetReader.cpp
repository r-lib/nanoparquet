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

void RParquetReader::add_dict_page(DictPage &dict) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, dict.cc.column), dict.cc.row_group);
  if (!Rf_isNull(x)) {
    throw std::runtime_error("Duplicate dictionary page");
  }

  int rtype;
  R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[dict.cc.row_group];
  R_xlen_t al = nr;
  switch (dict.cc.type) {
  case parquet::Type::INT32:
    rtype = INTSXP;
    break;
  case parquet::Type::INT64:
    rtype = REALSXP;
    break;
  case parquet::Type::INT96:
    rtype = INTSXP;
    al *= 3;
    break;
  case parquet::Type::FLOAT:
    rtype = REALSXP;
    break;
  case parquet::Type::DOUBLE:
    rtype = REALSXP;
    break;
  default:
    throw std::runtime_error("Type not implemented yet");
  }

  SEXP val = Rf_allocVector(VECSXP, 2);
  SET_VECTOR_ELT(VECTOR_ELT(columns, dict.cc.column), dict.cc.row_group, val);
  SET_VECTOR_ELT(val, 0, Rf_allocVector(rtype, al));
  SET_VECTOR_ELT(val, 1, Rf_allocVector(INTSXP, nr));
  dict.dict = (uint8_t*) DATAPTR(VECTOR_ELT(val, 0));
}

void RParquetReader::add_data_page_int32(DataPage<int32_t> &data) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[data.row_group];
    x = Rf_allocVector(VECSXP, 2);
    SET_VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group, x);
    SET_VECTOR_ELT(x, 0, Rf_allocVector(INTSXP, nr));
    if (data.optional) {
      SET_VECTOR_ELT(x, 1, Rf_allocVector(INTSXP, nr));
    }
  }
  data.data = INTEGER(VECTOR_ELT(x, 0)) + data.from;
  if (data.optional) {
    data.present = INTEGER(VECTOR_ELT(x, 1)) + data.from;
  }
}

void RParquetReader::add_data_page_int64(DataPage<int64_t> &data) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[data.row_group];
    x = Rf_allocVector(REALSXP, nr);
    SET_VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group, x);
  }
  data.data = (int64_t*) REAL(x) + data.from;
  // TODO: present
}

void RParquetReader::add_data_page_int96(DataPage<int96_t> &data) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[data.row_group];
    x = Rf_allocVector(INTSXP, nr * 3);
    SET_VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group, x);
  }
  data.data = ((int96_t*) INTEGER(x)) + data.from;
  // TODO: present
}

void RParquetReader::add_data_page_float(DataPage<float> &data) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[data.row_group];
    x = Rf_allocVector(REALSXP, nr);
    SET_VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group, x);
  }
  data.data = ((float*) REAL(x)) + data.from;
  // TODO: present
}

void RParquetReader::add_data_page_double(DataPage<double> &data) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[data.row_group];
    x = Rf_allocVector(REALSXP, nr);
    SET_VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group, x);
  }
  data.data = REAL(x) + data.from;
  // TODO: present
}

void RParquetReader::add_dict_page_byte_array(BADictPage &dict) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, dict.cc.column), dict.cc.row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[dict.cc.row_group];
    SEXP val = Rf_allocVector(VECSXP, 2);
    SET_VECTOR_ELT(VECTOR_ELT(columns, dict.cc.column), dict.cc.row_group, val);
    SET_VECTOR_ELT(val, 0, Rf_allocVector(STRSXP, dict.dict_len));
    SET_VECTOR_ELT(val, 1, Rf_allocVector(INTSXP, nr));
    SEXP d = VECTOR_ELT(val, 0);
    for (uint32_t i = 0; i < dict.strs.len; i++) {
      const char *s = dict.strs.buf + dict.strs.offsets[i];
      uint32_t l = dict.strs.lengths[i];
      SET_STRING_ELT(d, i, Rf_mkCharLenCE(s, l, CE_UTF8));
    }
  } else {
    Rf_error("Dictionary already set");
  }
}

void RParquetReader::add_data_page_byte_array(BADataPage &data) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group);
  if (Rf_isNull(x)) {
    R_xlen_t nr = REAL(VECTOR_ELT(metadata, 1))[data.row_group];
    x = Rf_allocVector(STRSXP, nr);
    SET_VECTOR_ELT(VECTOR_ELT(columns, data.column), data.row_group, x);
  }
  for (uint32_t i = 0; i < data.strs.len; i++) {
    const char *s = data.strs.buf + data.strs.offsets[i];
    uint32_t l = data.strs.lengths[i];
    SET_STRING_ELT(x, data.from + i, Rf_mkCharLenCE(s, l, CE_UTF8));
  }
}

void RParquetReader::add_dict_index_page(DictIndexPage &idx) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, idx.column), idx.row_group);
  idx.dict_idx = (uint32_t*) INTEGER(VECTOR_ELT(x, 1));
  // TODO: present
}
