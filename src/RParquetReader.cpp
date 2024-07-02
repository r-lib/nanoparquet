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

// We create a VECSXP for every column chunk, with elements:
// 0 metadata
// 1 dictionary
// 2 data
// 3 present
//
// metadata:
// 0 rtype: the R type, e.g. INTSXP
// 1 elsize: parquet element size in bytes
// 2 num_rows: number of rows in the row group (REALSXP)
// 3 rsize: how many R <rtype> makes up a Parquet type
// 4 dict: whether there is a dictionary page
// 5 num_present: number of non-missing values (REALSXP)
//
// dictionary:
// - non-dictionary columns: NULL
// - non byte arrays: dictionary values
// - byte arrays: VECSXP:
//   - number of dictionary values (INTSXP)
//   - raw buffer containing all strings
//   - integer offsets within the buffer
//   - integer lengths for the strings
//
// data:
// - for columns with a dictionary: integer indices (w/o NAs)
// - for non byte-array columns: actual values (w/o NAs)
// - for byte-array columns: VECSXP:
//   - number of values
//   - raw buffer of all strings (w/o NAs)
//   - integer offsets within the buffer
//   - integer lengths for the strings
//
// present:
// - NULL if the column is REQUIRED
// - LGLSXP of num_values, whether a value is present or not

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
  const char *nms2[] = {
    "rtype",
    "elsize",
    "num_rows",
    "rsize",
    "dict",
    "num_pressent",
    ""
  };
  SEXP metadata = Rf_mkNamed(VECSXP, nms2);
  SET_VECTOR_ELT(x, 0, metadata);
  SET_VECTOR_ELT(metadata, 0, Rf_ScalarInteger(rtype));
  SET_VECTOR_ELT(metadata, 1, Rf_ScalarInteger(elsize));
  SET_VECTOR_ELT(metadata, 2, Rf_ScalarReal(num_rows));
  SET_VECTOR_ELT(metadata, 3, Rf_ScalarInteger(rsize));
  SET_VECTOR_ELT(metadata, 4, Rf_ScalarLogical(dict));
  SET_VECTOR_ELT(metadata, 5, Rf_ScalarReal(0));

  // dictionary is allocated later, at the dict page

  // data is allocaed later for byte arrays
  if (dict) {
    SET_VECTOR_ELT(x, 2, Rf_allocVector(INTSXP, num_rows));
  } else if (!byte_array) {
    SET_VECTOR_ELT(x, 2, Rf_allocVector(rtype, num_rows * rsize));
  }

  if (cc.optional) {
    SET_VECTOR_ELT(x, 3, Rf_allocVector(RAWSXP, num_rows));
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
    SET_VECTOR_ELT(p, 0, Rf_ScalarInteger(data.num_present));
    SET_VECTOR_ELT(p, 1, Rf_allocVector(RAWSXP, data.strs.total_len));
    SET_VECTOR_ELT(p, 2, Rf_allocVector(INTSXP, data.num_present));
    SET_VECTOR_ELT(p, 3, Rf_allocVector(INTSXP, data.num_present));
    data.strs.buf = (char*) RAW(VECTOR_ELT(p, 1));
    data.strs.offsets = (uint32_t*) INTEGER(VECTOR_ELT(p, 2));
    data.strs.lengths = (uint32_t*) INTEGER(VECTOR_ELT(p, 3));

  } else {
    data.data = ((uint8_t*) DATAPTR(VECTOR_ELT(x, 2))) + data.from * elsize;
  }

  if (data.cc.optional) {
    REAL(VECTOR_ELT(meta, 5))[0] += data.num_present;
    data.present = (uint8_t*) RAW(VECTOR_ELT(x, 3)) + data.from;
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
      case parquet::Type::BYTE_ARRAY:
      case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        SEXP col = VECTOR_ELT(VECTOR_ELT(columns, cn), rg);
        convert_buffer_to_string(col);
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

void RParquetReader::convert_buffer_to_string(SEXP x) {
  SEXP meta = VECTOR_ELT(x, 0);
  bool isdict = LOGICAL(VECTOR_ELT(meta, 4))[0];

  if (isdict) {
    SEXP dict = VECTOR_ELT(x, 1);
    R_xlen_t dict_len = INTEGER(VECTOR_ELT(dict, 0))[0];
    SEXP nv = PROTECT(Rf_allocVector(STRSXP, dict_len));
    R_xlen_t idx = 0;
    convert_buffer_to_string1(dict, nv, idx);
    SET_VECTOR_ELT(x, 1, nv);
    UNPROTECT(1);
  } else {
    R_xlen_t nr = REAL(VECTOR_ELT(meta, 2))[0];
    SEXP nv = PROTECT(Rf_allocVector(STRSXP, nr));
    R_xlen_t idx = 0;
    SEXP data = VECTOR_ELT(x, 2);
    R_xlen_t len = Rf_length(data);
    for (R_xlen_t i = 0; i < len; i++) {
      SEXP v = VECTOR_ELT(data, i);
      if (Rf_isNull(v)) break;
      convert_buffer_to_string1(v, nv, idx);
    }
    SET_VECTOR_ELT(x, 2, nv);
    UNPROTECT(1);
  }
}

void RParquetReader::convert_buffer_to_string1(SEXP x, SEXP nv, R_xlen_t &idx) {
  R_xlen_t nr = INTEGER(VECTOR_ELT(x, 0))[0];
  char *buf = (char*) RAW(VECTOR_ELT(x, 1));
  uint32_t *offsets = (uint32_t*) INTEGER(VECTOR_ELT(x, 2));
  uint32_t *lengths = (uint32_t*) INTEGER(VECTOR_ELT(x, 3));
  for (R_xlen_t i = 0; i < nr; i++, offsets++, lengths++) {
    char *s = buf + *offsets;
    SET_STRING_ELT(nv, idx++, Rf_mkCharLenCE(s, *lengths, CE_UTF8));
  }
}

// ------------------------------------------------------------------------

void RParquetReader::decode_dicts() {
  for (auto cn = 0; cn < file_meta_data_.schema.size(); cn++) {
    parquet::SchemaElement &sel = file_meta_data_.schema[cn];
    if (sel.__isset.num_children) {
      continue;
    }
    for (auto rg = 0; rg < file_meta_data_.row_groups.size(); rg++) {
      SEXP col = VECTOR_ELT(VECTOR_ELT(columns, cn), rg);
      SEXP meta = VECTOR_ELT(col, 0);
      bool dict = LOGICAL(VECTOR_ELT(meta, 4))[0];
      if (!dict) {
        continue;
      }
      SEXP nv = subset_vector(VECTOR_ELT(col, 1), VECTOR_ELT(col, 2));
      SET_VECTOR_ELT(col, 2, nv);
      SET_VECTOR_ELT(col, 1, R_NilValue);
    }
  }
}

SEXP RParquetReader::subset_vector(SEXP x, SEXP idx) {
  int rtype = TYPEOF(x);
  R_xlen_t len = Rf_xlength(idx);
  SEXP nv = Rf_allocVector(rtype, len);
  uint32_t *cidx = (uint32_t*) INTEGER(idx);

  if (rtype == INTSXP) {
    int *d = INTEGER(x);
    int *val = INTEGER(nv);
    for (R_xlen_t i = 0; i < len; i++) {
      val[i] = d[cidx[i]];
    }

  } else if (rtype == REALSXP) {
    double *d = REAL(x);
    double *val = REAL(nv);
    for (R_xlen_t i = 0; i < len; i++) {
      val[i] = d[cidx[i]];
    }

  } else if (rtype == LGLSXP) {
    int *d = LOGICAL(x);
    int *val = LOGICAL(nv);
    for (R_xlen_t i = 0; i < len; i++) {
      val[i] = d[cidx[i]];
    }

  } else if (rtype == STRSXP) {
    for (R_xlen_t i = 0; i < len; i++) {
      SET_STRING_ELT(nv, i, STRING_ELT(x, cidx[i]));
    }
  }

  return nv;
}

void RParquetReader::handle_missing() {
  for (auto cn = 0; cn < file_meta_data_.schema.size(); cn++) {
    parquet::SchemaElement &sel = file_meta_data_.schema[cn];
    if (sel.__isset.num_children) {
      continue;
    }
    if (sel.repetition_type == parquet::FieldRepetitionType::REQUIRED) {
      continue;
    }
    for (auto rg = 0; rg < file_meta_data_.row_groups.size(); rg++) {
      SEXP col = VECTOR_ELT(VECTOR_ELT(columns, cn), rg);
      SEXP meta = VECTOR_ELT(col, 0);
      SEXP data = VECTOR_ELT(col, 2);
      SEXP pres = VECTOR_ELT(col, 3);
      bool isdict = LOGICAL(VECTOR_ELT(meta, 4))[0];
      R_xlen_t num_values = REAL(VECTOR_ELT(meta, 2))[0];
      R_xlen_t num_present = REAL(VECTOR_ELT(meta, 5))[0];
      R_xlen_t num_missing = num_values - num_present;
      int rtype = INTEGER(VECTOR_ELT(meta, 0))[0];

      // Need to do this backwards to be able to do it in place
      if (isdict || rtype == INTSXP) {
        int *src = INTEGER(data) + num_present - 1;
        int *tgt = INTEGER(data) + num_values - 1;
        uint8_t *p = (uint8_t*) RAW(pres) + num_values - 1;
        while (num_missing > 0) {
          if (*p) {
            *tgt-- = *src--;
            p--;
          } else {
            *tgt-- = NA_INTEGER;
            p--;
            num_missing--;
          }
        }

      } else if (rtype == REALSXP) {
        double *src = REAL(data) + num_present - 1;
        double *tgt = REAL(data) + num_values - 1;
        uint8_t *p = (uint8_t*) RAW(pres) + num_values - 1;
        while (num_missing > 0) {
          if (*p) {
            *tgt-- = *src--;
            p--;
          } else {
            *tgt-- = NA_REAL;
            p--;
            num_missing--;
          }
        }

      } else if (rtype == LGLSXP) {
        int *src = LOGICAL(data) + num_present - 1;
        int *tgt = LOGICAL(data) + num_values - 1;
        uint8_t *p = (uint8_t*) RAW(pres) + num_values - 1;
        while (num_missing > 0) {
          if (*p) {
            *tgt-- = *src--;
            p--;
          } else {
            *tgt-- = NA_LOGICAL;
            p--;
            num_missing--;
          }
        }

      } else if (rtype == STRSXP) {
        R_xlen_t src = num_present - 1;
        R_xlen_t tgt = num_values - 1;
        uint8_t *p = (uint8_t*) RAW(pres) + num_values - 1;

        while (num_missing > 0) {
          if (*p) {
            SET_STRING_ELT(data, tgt--, STRING_ELT(data, src--));
            p--;
          } else {
            SET_STRING_ELT(data, tgt--, R_NaString);
            p--;
            num_missing--;
          }
        }
      }

      SET_VECTOR_ELT(col, 3, R_NilValue);
    }
  }
}

// ------------------------------------------------------------------------

void RParquetReader::rbind_row_groups() {
  R_xlen_t nc = Rf_xlength(columns);
  R_xlen_t nr = file_meta_data_.num_rows;
  for (auto c = 0; c < nc; c++) {
    SEXP col = VECTOR_ELT(columns, c);
    if (Rf_isNull(col)) {
      continue;
    }
    // TODO: make this work with zero rows. I.e. we need to assign the
    // R types when we parse the Parquet metadata, not when we add a
    // column chunk.
    R_xlen_t rgn = Rf_xlength(col);
    if (rgn == 0) {
      Rf_error("No rown in Parquet file");
    }
    int rtype = TYPEOF(VECTOR_ELT(VECTOR_ELT(col, 0), 2));
    SEXP nv = PROTECT(Rf_allocVector(rtype, nr));
    R_xlen_t idx = 0;
    for (auto rgi = 0; rgi < rgn; rgi++) {
      SEXP rg = VECTOR_ELT(col, rgi);
      SEXP data = VECTOR_ELT(rg, 2);
      R_xlen_t dl = Rf_xlength(data);

      if (rtype == INTSXP) {
        memcpy(INTEGER(nv) + idx, INTEGER(data), dl * sizeof(int));
      } else if (rtype == REALSXP) {
        memcpy(REAL(nv) + idx, REAL(data), dl * sizeof(double));
      } else if (rtype == LGLSXP) {
        memcpy(LOGICAL(nv) + idx, LOGICAL(data), dl * sizeof(int));
      } else if (rtype == STRSXP) {
        for (auto i = 0; i < dl; i++) {
          SET_STRING_ELT(nv, idx++, STRING_ELT(data, i));
        }
      }

      idx += dl;
    }

    SET_VECTOR_ELT(columns, c, nv);
    UNPROTECT(1);
  }
}
