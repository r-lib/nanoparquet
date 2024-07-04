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

  // metadata
  create_metadata();

  // columns
  columns = Rf_allocVector(VECSXP, metadata.num_cols);
  R_PreserveObject(columns);
  // temporary data for dictionaries
  tmpdata = Rf_allocVector(VECSXP, metadata.num_cols);
  R_PreserveObject(tmpdata);

  for (auto i = 0; i < metadata.num_cols; i++) {
    // skip non-leaf columns
    if (file_meta_data_.schema[i].__isset.num_children) continue;
    int t = metadata.r_types[i].tmptype;
    int rsize = metadata.r_types[i].rsize;
    if (t != NILSXP) {
      // If we can directly read into a simple type
      SET_VECTOR_ELT(columns, i, Rf_allocVector(t, metadata.num_rows * rsize));
      // A place to put dictionaries, should we have any
      SET_VECTOR_ELT(tmpdata, i, Rf_allocVector(VECSXP, metadata.num_row_groups));
    } else {
      // For byte arrays we need a temporary structure
      SET_VECTOR_ELT(tmpdata, i, Rf_allocVector(VECSXP, metadata.num_row_groups));
    }
  }
}

RParquetReader::~RParquetReader() {
  if (!Rf_isNull(columns)) {
    R_ReleaseObject(columns);
  }
  if (!Rf_isNull(tmpdata)) {
    R_ReleaseObject(tmpdata);
  }
}

// ------------------------------------------------------------------------

void RParquetReader::create_metadata() {
  parquet::FileMetaData fmt = get_file_meta_data();
  metadata.num_rows = fmt.num_rows;
  metadata.num_cols = fmt.schema.size();
  metadata.num_row_groups = fmt.row_groups.size();
  metadata.row_group_num_rows.resize(metadata.num_row_groups);
  metadata.row_group_offsets.resize(metadata.num_row_groups);

  for (auto i = 0; i < fmt.row_groups.size(); i++) {
    metadata.row_group_num_rows[i] = fmt.row_groups[i].num_rows;
    metadata.row_group_offsets[i] = 0;
    if (i > 0) {
      metadata.row_group_offsets[i] =
        metadata.row_group_offsets[i-1] + metadata.row_group_num_rows[i-1];
    }
  }

  metadata.r_types.resize(metadata.num_cols);
  for (auto i = 0; i < metadata.num_cols; i++) {
    if (fmt.schema[i].__isset.num_children) {
      continue;
    }
    rtype rt(fmt.schema[i]);
    metadata.r_types[i] = rt;
  }
}

// ------------------------------------------------------------------------

rtype::rtype(parquet::SchemaElement &sel) {
  switch (sel.type) {
  case parquet::Type::BOOLEAN:
    type = tmptype = LGLSXP;
    elsize = sizeof(int);
    break;
  case parquet::Type::INT32:
    type = tmptype = INTSXP;
    elsize = sizeof(int);
    if ((sel.__isset.logicalType && sel.logicalType.__isset.DATE) ||
        (sel.__isset.converted_type &&
         sel.converted_type == parquet::ConvertedType::DATE)) {
      classes.push_back("Date");
    } else if ((sel.__isset.logicalType && sel.logicalType.__isset.TIME &&
                sel.logicalType.TIME.unit.__isset.MILLIS) ||
               (sel.__isset.converted_type &&
                sel.converted_type == parquet::ConvertedType::TIME_MILLIS)) {
      classes.push_back("hms");
      classes.push_back("difftime");
      units.push_back("secs");
    }
    break;
  case parquet::Type::INT64:
    type = tmptype = REALSXP;
    type_conversion = INT64_DOUBLE;
    elsize = sizeof(double);
    if ((sel.__isset.logicalType &&
         sel.logicalType.__isset.TIMESTAMP &&
         (sel.logicalType.TIMESTAMP.unit.__isset.MILLIS ||
          sel.logicalType.TIMESTAMP.unit.__isset.MICROS ||
          sel.logicalType.TIMESTAMP.unit.__isset.NANOS)) ||
        (sel.__isset.converted_type &&
         (sel.converted_type == parquet::ConvertedType::TIMESTAMP_MILLIS ||
          sel.converted_type == parquet::ConvertedType::TIMESTAMP_MICROS))) {
      classes.push_back("POSIXct");
      classes.push_back("POSIXt");
      if (sel.__isset.logicalType &&
          sel.logicalType.__isset.TIMESTAMP) {
        if (sel.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
          time_fct = 1;
        } else if (sel.logicalType.TIMESTAMP.unit.__isset.MICROS) {
          time_fct = 1000;
        } else if (sel.logicalType.TIMESTAMP.unit.__isset.NANOS) {
          time_fct = 1000 * 1000;
        }
      } else if (sel.__isset.converted_type) {
        if (sel.converted_type == parquet::ConvertedType::TIMESTAMP_MILLIS) {
          time_fct = 1;
        } else if (sel.converted_type == parquet::ConvertedType::TIMESTAMP_MICROS) {
          time_fct = 1000;
        }
      }
      if (!sel.__isset.logicalType ||
          (sel.logicalType.__isset.TIMESTAMP &&
           sel.logicalType.TIMESTAMP.isAdjustedToUTC)) {
        tzone = "UTC";
      }
    } else if ((sel.__isset.logicalType &&
                sel.logicalType.__isset.TIME &&
                (sel.logicalType.TIME.unit.__isset.MICROS ||
                   sel.logicalType.TIME.unit.__isset.NANOS)) ||
                 (sel.__isset.converted_type &&
                  sel.converted_type == parquet::ConvertedType::TIME_MICROS)) {
        // can be MICROS or NANOS currently, other values read as INT64
      if (sel.__isset.logicalType &&
          sel.logicalType.__isset.TIME) {
        if (sel.logicalType.TIME.unit.__isset.MICROS) {
          time_fct = 1000;
        } else if (sel.logicalType.TIME.unit.__isset.NANOS) {
          time_fct = 1000 * 1000;
        }
      } else if (sel.converted_type == parquet::ConvertedType::TIME_MICROS) {
        time_fct = 1000;
      }
      classes.push_back("hms");
      classes.push_back("difftime");
      units.push_back("secs");
    }
    break;
  case parquet::Type::INT96:
    type = REALSXP;
    tmptype = INTSXP;
    type_conversion = INT96_DOUBLE;
    elsize = sizeof(int) * 3;
    rsize = 3;
    classes.push_back("POSIXct");
    classes.push_back("POSIXt");
    tzone = "UTC";
    break;
  case parquet::Type::FLOAT:
    type = tmptype = REALSXP;
    type_conversion = FLOAT_DOUBLE;
    elsize = sizeof(double);
    break;
  case parquet::Type::DOUBLE:
    type = tmptype = REALSXP;
    elsize = sizeof(double);
    break;
  case parquet::Type::BYTE_ARRAY:
  case parquet::Type::FIXED_LEN_BYTE_ARRAY:
    byte_array = true;
    if ((sel.__isset.logicalType &&
         (sel.logicalType.__isset.STRING ||
          sel.logicalType.__isset.ENUM ||
          sel.logicalType.__isset.UUID)) ||
        (sel.__isset.converted_type &&
         sel.converted_type == parquet::ConvertedType::UTF8)) {
      type = STRSXP;
      tmptype = NILSXP;
      type_conversion = BA_STRING;
    } else if (sel.__isset.converted_type &&
               sel.converted_type == parquet::ConvertedType::DECIMAL) {
      type = REALSXP;
      tmptype = NILSXP;
      type_conversion = BA_DECIMAL;
    } else {
      type = VECSXP;
      tmptype = NILSXP;
      type_conversion = BA_RAW;
    }
    break;
  }
}

// ------------------------------------------------------------------------

// We only need this for byte array columns, because we cannot
// fill them in immediately in place. It would be costly to
// (safely) create an R string for every element, so we
// will only do it later, when no exceptions can happen.
//
// For byte array columns we create a VECSXP for every column chunk,
// with elements:
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
  // Only need this for byte arrays
  if (!metadata.r_types[cc.column].byte_array) {
    return;
  }

  const char *nms[] = { "metadata", "dict", "data", "present", "" };
  SEXP x = Rf_mkNamed(VECSXP, nms);
  SET_VECTOR_ELT(VECTOR_ELT(tmpdata, cc.column), cc.row_group, x);
  bool dict = cc.has_dictionary;
  const char *nms2[] = {
    "num_rows",
    "dict",
    "num_pressent",
    ""
  };
  SEXP metadata = Rf_mkNamed(VECSXP, nms2);
  SET_VECTOR_ELT(x, 0, metadata);
  SET_VECTOR_ELT(metadata, 0, Rf_ScalarReal(cc.num_rows));
  SET_VECTOR_ELT(metadata, 1, Rf_ScalarLogical(dict));
  SET_VECTOR_ELT(metadata, 2, Rf_ScalarReal(0));

  // dictionary is allocated later, at the dict page

  // data is allocaed later for byte arrays
  if (dict) {
    SET_VECTOR_ELT(x, 2, Rf_allocVector(INTSXP, cc.num_rows));
  }
}

// ------------------------------------------------------------------------

void RParquetReader::alloc_dict_page(DictPage &dict) {
  if (metadata.r_types[dict.cc.column].byte_array) {
    SEXP x = VECTOR_ELT(VECTOR_ELT(tmpdata, dict.cc.column), dict.cc.row_group);
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
    SEXP x = VECTOR_ELT(tmpdata, dict.cc.column);
    int rtype = metadata.r_types[dict.cc.column].tmptype;
    int rsize = metadata.r_types[dict.cc.column].rsize;
    SEXP d = Rf_allocVector(VECSXP, 3);
    SET_VECTOR_ELT(x, dict.cc.row_group, d);
    SET_VECTOR_ELT(d, 0, Rf_allocVector(rtype, dict.dict_len * rsize));
    SET_VECTOR_ELT(d, 2, Rf_allocVector(INTSXP, dict.cc.num_rows));
    dict.dict = (uint8_t*) DATAPTR(VECTOR_ELT(d, 0));
  }
}

// ------------------------------------------------------------------------

void RParquetReader::alloc_data_page(DataPage &data) {
  bool ba = metadata.r_types[data.cc.column].byte_array;
  bool dict = data.cc.has_dictionary;

  if (ba) {
    SEXP x = VECTOR_ELT(VECTOR_ELT(tmpdata, data.cc.column), data.cc.row_group);
    SEXP meta = VECTOR_ELT(x, 0);
    if (data.cc.optional) {
      REAL(VECTOR_ELT(meta, 5))[0] += data.num_present;
      data.present = (uint8_t*) RAW(VECTOR_ELT(x, 3)) + data.from;
    }
    if (dict) {
      data.data = (uint8_t *) (INTEGER(VECTOR_ELT(x, 2)) + data.from);
    } else {
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
    }

  } else if (dict) {
    SEXP x = VECTOR_ELT(VECTOR_ELT(tmpdata, data.cc.column), data.cc.row_group);
    data.data = (uint8_t *) (INTEGER(VECTOR_ELT(x, 2)) + data.from);

  } else {
    SEXP x = VECTOR_ELT(columns, data.cc.column);
    int64_t off = metadata.row_group_offsets[data.cc.column];
    int elsize = metadata.r_types[data.cc.column].elsize;
    data.data = ((uint8_t*) DATAPTR(x)) + (off + data.from) * elsize;
  }
}

void RParquetReader::add_data_page(DataPage &data) {
  // if this is not a byte array page then we need to use the NA values
  if (metadata.r_types[data.cc.column].byte_array) {
    return;
  }
  if (!data.cc.optional) {
    return;
  }
  bool dict = data.cc.has_dictionary;
  SEXP x = R_NilValue;

  if (dict) {
    x = VECTOR_ELT(
      VECTOR_ELT(VECTOR_ELT(tmpdata, data.cc.column), data.cc.row_group),
      2
    );
  } else {
    x = VECTOR_ELT(columns, data.cc.column);
  }

  int rsize = metadata.r_types[data.cc.column].rsize;
  int rtype = TYPEOF(x);
  int64_t off = metadata.row_group_offsets[data.cc.column];
  uint32_t num_values = data.num_values;
  uint32_t num_present = data.num_present;
  R_xlen_t num_missing = num_values - num_present;
  uint8_t *p = data.present + num_values - 1;

  if (dict || (rtype == INTSXP && rsize == 1)) {
    int *src = INTEGER(x) + (off + data.from);
    int *tgt = src + num_values - 1;
    src += num_present - 1;
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

  } else if (rtype == INTSXP && rsize == 3) {
    int *src = INTEGER(x) + (off + data.from) * rsize;
    int *tgt = src + (num_values - 1) * rsize;
    src += (num_present - 1) * rsize;
    while (num_missing > 0) {
      if (*p) {
        *tgt-- = *src--;
        *tgt-- = *src--;
        *tgt-- = *src--;
        p--;
      } else {
        *tgt-- = NA_INTEGER;
        *tgt-- = NA_INTEGER;
        *tgt-- = NA_INTEGER;
        p--;
        num_missing--;
      }
    }

  } else if (rtype == REALSXP) {
    double *src = REAL(x) + (off + data.from);
    double *tgt = src + num_values - 1;
    src += num_present - 1;
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
    int *src = LOGICAL(x) + (off + data.from);
    int *tgt = src + num_values - 1;
    src += num_present - 1;
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
  }
}

// ------------------------------------------------------------------------

void RParquetReader::convert_columns_to_r() {
  for (auto cn = 0; cn < metadata.num_cols; cn++) {
    rtype rt = metadata.r_types[cn];
    if (rt.type == NILSXP) {
      // non-leaf column
      continue;
    }

    switch (rt.type_conversion) {
    case NONE:
      convert_column_to_r_dicts(cn);
      break;
    case INT64_DOUBLE:
      convert_column_to_r_int64(cn);
      break;
    case INT96_DOUBLE:
      convert_column_to_r_int96(cn);
      break;
    case FLOAT_DOUBLE:
      convert_column_to_r_float(cn);
      break;
    case BA_STRING:
      convert_column_to_r_ba_string(cn);
      break;
    case BA_DECIMAL:
      convert_column_to_r_ba_decimal(cn);
      break;
    case BA_RAW:
      convert_column_to_r_ba_raw(cn);
      break;
    default:
      break;
    }
  }
}

void RParquetReader::convert_column_to_r_dicts(uint32_t cn) {
  for (auto rg = 0; rg < metadata.num_row_groups; rg++) {
    SEXP dict = VECTOR_ELT(VECTOR_ELT(tmpdata, cn), rg);
    if (Rf_isNull(dict)) {
      continue;
    }
    R_xlen_t from = metadata.row_group_offsets[rg];
    uint32_t *didx = (uint32_t*) INTEGER(VECTOR_ELT(dict, 2));
    switch (metadata.r_types[cn].type) {
      case INTSXP: {
        int32_t *tgt = INTEGER(VECTOR_ELT(columns, cn)) + from;
        int32_t *end = tgt + metadata.row_group_num_rows[rg];
        int32_t *dval = INTEGER(VECTOR_ELT(dict, 0));
        while (tgt < end) {
          *tgt++ = dval[*didx++];
        }
        break;
      }
      case REALSXP: {
        double *tgt = REAL(VECTOR_ELT(columns, cn)) + from;
        double *end = tgt + metadata.row_group_num_rows[rg];
        double *dval = REAL(VECTOR_ELT(dict, 0));
        while (tgt < end) {
          *tgt++ = dval[*didx++];
        }
        break;
      }
    }
    SET_VECTOR_ELT(VECTOR_ELT(tmpdata, cn), rg, R_NilValue);
  }
}

void RParquetReader::convert_column_to_r_int64(uint32_t cn) {
  for (auto rg = 0; rg < metadata.num_row_groups; rg++) {
    SEXP dict = VECTOR_ELT(VECTOR_ELT(tmpdata, cn), rg);
    R_xlen_t from = metadata.row_group_offsets[rg];
    double *tgt = REAL(VECTOR_ELT(columns, cn)) + from;
    double *end = tgt + metadata.row_group_num_rows[rg];
    if (Rf_isNull(dict)) {
      // no dict, do it in place
      int64_t *src = (int64_t*) tgt;
      while (tgt < end) {
        *tgt++ = static_cast<double>(*src++);
      }
    } else {
      // dict, convert the dict values
      double *dtgt = REAL(VECTOR_ELT(dict, 0));
      double *dend = dtgt + Rf_length(VECTOR_ELT(dict, 0));
      int64_t *dsrc = (int64_t*) tgt;
      while (dtgt < dend) {
        *dtgt++ = static_cast<double>(*dsrc++);
      }
      // index
      double *dval = REAL(VECTOR_ELT(dict, 0));
      uint32_t *didx = (uint32_t*) INTEGER(VECTOR_ELT(dict, 2));
      while (tgt < end) {
        *tgt++ = dval[*didx++];
      }
    }
  }
}

void RParquetReader::convert_column_to_r_int96(uint32_t cn) {
  SEXP nv = PROTECT(Rf_allocVector(REALSXP, metadata.num_rows));
  for (auto rg = 0; rg < metadata.num_row_groups; rg++) {
    SEXP dict = VECTOR_ELT(VECTOR_ELT(tmpdata, cn), rg);
    R_xlen_t from = metadata.row_group_offsets[rg];
    double *tgt = REAL(nv) + from;
    double *end = tgt + metadata.row_group_num_rows[rg];
    if (Rf_isNull(dict)) {
      // no dict, do it in place
      int96_t *src = ((int96_t*) INTEGER(VECTOR_ELT(columns, cn))) + from;
      while (tgt < end) {
        *tgt++ = impala_timestamp_to_nanoseconds(*src++) / 1000000;
      }
    } else {
      //dict
      R_xlen_t dlen = Rf_length(VECTOR_ELT(dict, 0));
      SEXP nd = PROTECT(Rf_allocVector(REALSXP, dlen));
      double *dtgt = REAL(VECTOR_ELT(dict, 0));
      double *dend = dtgt + dlen;
      int96_t *dsrc = (int96_t*) VECTOR_ELT(dict, 0);
      while (dtgt < dend) {
        *dtgt++ = impala_timestamp_to_nanoseconds(*dsrc++) / 1000000;
      }
      // index
      double *dval = REAL(nd);
      uint32_t *didx = (uint32_t*) INTEGER(VECTOR_ELT(dict, 2));
      while (tgt < end) {
        *tgt++ = dval[*didx++];
      }
      UNPROTECT(1);
    }
  }
  SET_VECTOR_ELT(columns, cn, nv);
  UNPROTECT(1);
}

void RParquetReader::convert_column_to_r_float(uint32_t cn) {
  for (auto rg = 0; rg < metadata.num_row_groups; rg++) {
    SEXP dict = VECTOR_ELT(VECTOR_ELT(tmpdata, cn), rg);
    R_xlen_t from = metadata.row_group_offsets[rg];
    if (Rf_isNull(dict)) {
      // no dict
      double *beg = REAL(VECTOR_ELT(columns, cn)) + from;
      double *tgt = beg + metadata.row_group_num_rows[rg] - 1;
      float *src = ((float*) beg) + metadata.row_group_num_rows[rg] - 1;
      while (tgt >= beg) {
        *tgt-- = static_cast<double>(*src--);
      }
    } else {
      // dict
      R_xlen_t dlen = Rf_length(VECTOR_ELT(dict, 0));
      double *dbeg = REAL(VECTOR_ELT(dict, 0));
      double *dtgt = dbeg + dlen - 1;
      float *dsrc = ((float*) dbeg) + dlen - 1;
      while (dtgt >= dbeg) {
        *dtgt-- = static_cast<double>(*dsrc--);
      }
      // index
      double *dval = REAL(VECTOR_ELT(dict, 0));
      uint32_t *didx = (uint32_t*) INTEGER(VECTOR_ELT(dict, 2));
      double *tgt = REAL(VECTOR_ELT(columns, cn)) + from;
      double *end = tgt + metadata.row_group_num_rows[rg];
      while (tgt < end) {
        *tgt++ = dval[*didx++];
      }
    }
  }
}

void RParquetReader::convert_column_to_r_ba_string(uint32_t cn) {

}

void RParquetReader::convert_column_to_r_ba_decimal(uint32_t cn) {

}

void RParquetReader::convert_column_to_r_ba_raw(uint32_t cn) {

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
