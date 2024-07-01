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
        SEXP col = VECTOR_ELT(VECTOR_ELT(columns, cn), rg);
        convert_int96_to_double(col);
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
  SEXP meta = VECTOR_ELT(x, 0);
  bool dict = LOGICAL(VECTOR_ELT(meta, 4))[0];
  SEXP inp = VECTOR_ELT(x, dict ? 1 : 2);

  double *dp = REAL(inp);
  int64_t *ip = (int64_t*) REAL(inp);
  R_xlen_t l = Rf_xlength(x);
  for (R_xlen_t i = 0; i < l; i++, dp++, ip++) {
    *dp = static_cast<double>(*ip);
  }
}

void RParquetReader::convert_int96_to_double(SEXP x) {
  SEXP meta = VECTOR_ELT(x, 0);
  bool dict = LOGICAL(VECTOR_ELT(meta, 4))[0];
  int idx = dict ? 1 : 2;
  SEXP inp = VECTOR_ELT(x, idx);

  R_xlen_t l3 = Rf_xlength(inp);
  R_xlen_t l = l3 / 3;
  SEXP out = PROTECT(Rf_allocVector(REALSXP, l));
  int96_t *ip = (int96_t*) INTEGER(inp);
  double *dp = REAL(out);

  for (R_xlen_t i = 0; i < l; i++, ip++, dp++) {
    *dp = impala_timestamp_to_nanoseconds(*ip) / 1000000;
  }
  SET_VECTOR_ELT(x, idx, out);
  UNPROTECT(1);
}

void RParquetReader::convert_float_to_double(SEXP x) {
  SEXP meta = VECTOR_ELT(x, 0);
  bool dict = LOGICAL(VECTOR_ELT(meta, 4))[0];
  SEXP inp = dict ? VECTOR_ELT(x, 1) : VECTOR_ELT(x, 2);

  R_xlen_t l = Rf_xlength(inp);
  double *start = REAL(inp);
  double *end = start + l;
  float *fstart = (float*) REAL(inp);
  float *fend = fstart + l;
  while (end > start) {
    end--; fend--;
    *end = *fend;
    *end = static_cast<double>(*fend);
  }
}

// ------------------------------------------------------------------------

void RParquetReader::alloc_column_chunk(ColumnChunk &cc)  {
  // R type for the chunk, and its numer of bytes
  int rtype;
  int elsize = -1;
  // number of rows in the column chunks
  R_xlen_t num_rows = cc.num_rows;
  // how many R elements we need for one parquet element
  int rsize = 1;
  bool dict = cc.has_dictionary;
  bool byte_array = false;

  switch (cc.sel.type) {
  case parquet::Type::BOOLEAN:
    rtype = LGLSXP;
    elsize = sizeof(int);
    break;
  case parquet::Type::INT32:
    rtype = INTSXP;
    elsize = sizeof(int);
    break;
  case parquet::Type::INT64:
    rtype = REALSXP;
    elsize = sizeof(double);
    break;
  case parquet::Type::INT96:
    rtype = INTSXP;
    elsize = sizeof(int) * 3;
    rsize = 3;
    break;
  case parquet::Type::FLOAT:
    rtype = REALSXP;
    elsize = sizeof(double);
    break;
  case parquet::Type::DOUBLE:
    rtype = REALSXP;
    elsize = sizeof(double);
    break;
  case parquet::Type::BYTE_ARRAY:
  case parquet::Type::FIXED_LEN_BYTE_ARRAY:
    rtype = STRSXP;
    byte_array = true;
    break;
  default:
    throw std::runtime_error("Type not implemented yet");
  }

  const char *nms[] = { "metadata", "dict", "data", "present", "" };
  SEXP x = Rf_mkNamed(VECSXP, nms);
  SET_VECTOR_ELT(VECTOR_ELT(columns, cc.column), cc.row_group, x);
  const char *nms2[] = { "rtype", "elsize", "num_rows", "rsize", "dict", "" };
  SEXP metadata = Rf_mkNamed(VECSXP, nms2);
  SET_VECTOR_ELT(x, 0, metadata);
  SET_VECTOR_ELT(metadata, 0, Rf_ScalarInteger(rtype));
  SET_VECTOR_ELT(metadata, 1, Rf_ScalarInteger(elsize));
  SET_VECTOR_ELT(metadata, 2, Rf_ScalarReal(num_rows));
  SET_VECTOR_ELT(metadata, 3, Rf_ScalarInteger(rsize));
  SET_VECTOR_ELT(metadata, 4, Rf_ScalarLogical(dict));

  // dictionary is allocated later, at the dict page

  // data is allocaed later for byte arrays
  if (dict) {
    SET_VECTOR_ELT(x, 2, Rf_allocVector(INTSXP, num_rows));
  } else if (!byte_array) {
    SET_VECTOR_ELT(x, 2, Rf_allocVector(rtype, num_rows * rsize));
  }

  if (cc.optional) {
    SET_VECTOR_ELT(x, 3, Rf_allocVector(LGLSXP, num_rows));
  }
}

// ------------------------------------------------------------------------

void RParquetReader::alloc_dict_page(DictPage &dict) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, dict.cc.column), dict.cc.row_group);
  SEXP meta = VECTOR_ELT(x, 0);
  int rtype = INTEGER(VECTOR_ELT(meta, 0))[0];
  int rsize = INTEGER(VECTOR_ELT(meta, 3))[0];

  if (dict.cc.sel.type == parquet::Type::BYTE_ARRAY ||
      dict.cc.sel.type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    SEXP vals = Rf_allocVector(VECSXP, 4);
    SET_VECTOR_ELT(x, 1, vals);
    SET_VECTOR_ELT(vals, 0, Rf_ScalarInteger(dict.dict_len));
    SET_VECTOR_ELT(vals, 1, Rf_allocVector(RAWSXP, dict.strs.total_len));
    SET_VECTOR_ELT(vals, 2, Rf_allocVector(INTSXP, dict.dict_len));
    SET_VECTOR_ELT(vals, 3, Rf_allocVector(INTSXP, dict.dict_len));
    dict.strs.buf = (char*) RAW(VECTOR_ELT(vals, 1));
    dict.strs.offsets = (uint32_t*) INTEGER(VECTOR_ELT(vals, 2));
    dict.strs.lengths = (uint32_t*) INTEGER(VECTOR_ELT(vals, 3));
  } else {
    SEXP vals = Rf_allocVector(rtype, dict.dict_len * rsize);
    SET_VECTOR_ELT(x, 1, vals);
    dict.dict = (uint8_t*) DATAPTR(vals);
  }
}

// ------------------------------------------------------------------------

void RParquetReader::alloc_data_page(DataPage &data) {
  SEXP x = VECTOR_ELT(VECTOR_ELT(columns, data.cc.column), data.cc.row_group);
  SEXP meta = VECTOR_ELT(x, 0);
  int elsize = INTEGER(VECTOR_ELT(meta, 3))[0];
  int dict = LOGICAL(VECTOR_ELT(meta, 4))[0];

  if (dict) {
    data.data = (uint8_t *) INTEGER(VECTOR_ELT(x, 2)) + data.from;

  } else if (data.cc.sel.type == parquet::Type::BYTE_ARRAY ||
             data.cc.sel.type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    SEXP v = VECTOR_ELT(x, 2);
    R_xlen_t len = Rf_xlength(v);
    if (Rf_length(v) <= data.page) {
      R_xlen_t new_len = len * 1.5 + 5;
      SEXP nv = PROTECT(Rf_allocVector(VECSXP, new_len));
      for (R_xlen_t i = 0; i < len; i++) {
        SET_VECTOR_ELT(nv, i, VECTOR_ELT(v, i));
      }
      SET_VECTOR_ELT(x, 2, nv);
      UNPROTECT(1);
      v = nv;
    }

    SEXP p = Rf_allocVector(VECSXP, 4);
    SET_VECTOR_ELT(v, data.page, p);
    SET_VECTOR_ELT(p, 0, Rf_ScalarInteger(data.len));
    SET_VECTOR_ELT(p, 1, Rf_allocVector(RAWSXP, data.strs.total_len));
    SET_VECTOR_ELT(p, 2, Rf_allocVector(INTSXP, data.len));
    SET_VECTOR_ELT(p, 3, Rf_allocVector(INTSXP, data.len));
    data.strs.buf = (char*) RAW(VECTOR_ELT(p, 1));
    data.strs.offsets = (uint32_t*) INTEGER(VECTOR_ELT(p, 2));
    data.strs.lengths = (uint32_t*) INTEGER(VECTOR_ELT(p, 3));

  } else {
    data.data = ((uint8_t*) DATAPTR(VECTOR_ELT(x, 2))) + data.from * elsize;
  }

  if (data.cc.optional) {
    data.present = INTEGER(VECTOR_ELT(x, 3)) + data.from;
  }
}
