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

  tmpdata.resize(metadata.num_cols);
  dicts.resize(metadata.num_cols);
  byte_arrays.resize(metadata.num_cols);
  present.resize(metadata.num_cols);

  for (auto i = 0; i < metadata.num_cols; i++) {
    // skip non-leaf columns
    if (file_meta_data_.schema[i].__isset.num_children) continue;

    rtype rt = metadata.r_types[i];
    SET_VECTOR_ELT(columns, i, Rf_allocVector(rt.type, metadata.num_rows));
    metadata.dataptr[i] = (uint8_t*) DATAPTR(VECTOR_ELT(columns, i));
    if (rt.type != rt.tmptype && rt.tmptype != NILSXP) {
      tmpdata[i].resize(metadata.num_rows * rt.elsize);
    }
  }
}

RParquetReader::~RParquetReader() {
  if (!Rf_isNull(columns)) {
    R_ReleaseObject(columns);
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
  metadata.dataptr.resize(metadata.num_cols);

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

// The alloc_*() functions do not allowed to call the R API, so they
// can run concurrently.

void RParquetReader::alloc_column_chunk(ColumnChunk &cc)  {
  if (metadata.r_types[cc.column].byte_array) {
    if (byte_arrays[cc.column].size() == 0) {
      byte_arrays[cc.column].resize(metadata.num_row_groups);
    }
  }

  if (cc.optional) {
    if (present[cc.column].size() == 0) {
      present[cc.column].resize(metadata.num_row_groups);
    }
    present[cc.column][cc.row_group].num_present = 0;
    present[cc.column][cc.row_group].map.resize(cc.num_rows);
  }
}

void RParquetReader::alloc_dict_page(DictPage &dict) {
  auto cl = dict.cc.column;
  auto rg = dict.cc.row_group;

  if (dicts[cl].size() == 0) {
    dicts[cl].resize(metadata.num_row_groups);
  }

  rtype rt = metadata.r_types[cl];

  dicts[cl][rg].dict_len = dict.dict_len;
  dicts[cl][rg].indices.resize(dict.cc.num_rows);
  if (rt.byte_array) {
    dicts[cl][rg].bytes.buffer.resize(dict.strs.total_len);
    dicts[cl][rg].bytes.offsets.resize(dict.dict_len);
    dicts[cl][rg].bytes.lengths.resize(dict.dict_len);
    dict.strs.buf = (char*) dicts[cl][rg].bytes.buffer.data();
    dict.strs.offsets = dicts[cl][rg].bytes.offsets.data();
    dict.strs.lengths = dicts[cl][rg].bytes.lengths.data();
  } else {
    dicts[cl][rg].buffer.resize(dict.dict_len * rt.elsize);
    dict.dict = dicts[cl][rg].buffer.data();
  }
}

void RParquetReader::alloc_data_page(DataPage &data) {
  auto cl = data.cc.column;
  auto rg = data.cc.row_group;
  rtype rt = metadata.r_types[cl];

  // If there are missing values, then the page offset is defined by
  // the number of present values. I.e. within each column chunk we put the
  // present values at the beginning of the memory allocated for that
  // column chunk.
  uint32_t page_off = data.from;
  if (data.cc.optional) {
    page_off = present[cl][rg].num_present;
    present[cl][rg].num_present += data.num_present;
    data.present = present[cl][rg].map.data() + data.from;
  }

  if (data.cc.has_dictionary) {
    data.data = (uint8_t*) dicts[cl][rg].indices.data() + page_off;

  } else if (!rt.byte_array) {
    int64_t off = metadata.row_group_offsets[rg];
    data.data = metadata.dataptr[cl] + (off + page_off) * rt.elsize;

  } else {
    tmpbytes bapage;
    bapage.buffer.resize(data.strs.total_len);
    bapage.offsets.resize(data.num_present);
    bapage.lengths.resize(data.num_present);
    data.strs.buf = (char*) bapage.buffer.data();
    data.strs.offsets = bapage.offsets.data();
    data.strs.lengths = bapage.lengths.data();
    byte_arrays[cl][rg].push_back(bapage);
  }
}

// ------------------------------------------------------------------------

// UnwindProtect is simpler if this is not a member function

struct postprocess {
public:
  SEXP columns;
  rmetadata &metadata;
  std::vector<std::vector<uint8_t>> &tmpdata;
  std::vector<std::vector<tmpdict>> &dicts;
  std::vector<std::vector<std::vector<tmpbytes>>> &byte_arrays;
  std::vector<std::vector<presentmap>> &present;
};

void convert_column_to_r_dicts(postprocess *pp, uint32_t cl) {
  if (pp->dicts[cl].size() == 0) return;
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    // In theory some row groups might be non dictionary encoded
    if (pp->dicts[cl][rg].dict_len == 0) {
      continue;
    }
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    int64_t from = pp->metadata.row_group_offsets[cl];
    SEXP x = VECTOR_ELT(pp->columns, cl);
    switch (TYPEOF(x)) {
    case INTSXP: {
      int *beg = INTEGER(x) + from;
      int *end = beg + num_values;
      int *dict = (int*) pp->dicts[cl][rg].buffer.data();
      uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data();
      while (beg < end) {
        *beg++ = dict[*idx++];
      }
      break;
    }
    case REALSXP: {
      double *beg = REAL(x) + from;
      double *end = beg + num_values;
      double *dict = (double*) pp->dicts[cl][rg].buffer.data();
      uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data();
      while (beg < end) {
        *beg++ = dict[*idx++];
      }
      break;
    }
    case LGLSXP: {
      int *beg = LOGICAL(x) + from;
      int *end = beg + num_values;
      int *dict = (int*) pp->dicts[cl][rg].buffer.data();
      uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data();
      while (beg < end) {
        *beg++ = dict[*idx++];
      }
      break;
    }
    }
  }
}

void convert_column_to_r_dicts_na(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    // in theory some row groups might be dict encoded, some not
    bool hasdict = hasdict0 && pp->dicts[cl][rg].dict_len > 0;
    uint32_t num_present = pp->present[cl][rg].num_present;
    bool hasmiss = num_present != num_values;
    if (!hasdict && !hasmiss) {
      continue;
    }
    if (!hasdict && hasmiss) {
      // missing values in place
      int64_t from = pp->metadata.row_group_offsets[cl];
      SEXP x = VECTOR_ELT(pp->columns, cl);
      switch (TYPEOF(x)) {
      case INTSXP: {
        int *beg = INTEGER(x) + from;
        int *end = beg + num_values - 1;
        int *pend = beg + num_present - 1;
        uint8_t *pres = pp->present[cl][rg].map.data() + num_values - 1;
        uint32_t num_miss = num_values - num_present;
        while (num_miss > 0) {
          if (*pres) {
            *end-- = *pend--;
            pres--;
          } else {
            *end = NA_INTEGER;
            pres--;
            num_miss--;
          }
        }
        break;
      }
      case REALSXP: {
        double *beg = REAL(x) + from;
        double *end = beg + num_values - 1;
        double *pend = beg + num_present - 1;
        uint8_t *pres = pp->present[cl][rg].map.data() + num_values - 1;
        uint32_t num_miss = num_values - num_present;
        while (num_miss > 0) {
          if (*pres) {
            *end-- = *pend--;
            pres--;
          } else {
            *end = NA_REAL;
            pres--;
            num_miss--;
          }
        }
        break;
      }
      case LGLSXP: {
        int *beg = LOGICAL(x) + from;
        int *end = beg + num_values - 1;
        int *pend = beg + num_present - 1;
        uint8_t *pres = pp->present[cl][rg].map.data() + num_values - 1;
        uint32_t num_miss = num_values - num_present;
        while (num_miss > 0) {
          if (*pres) {
            *end-- = *pend--;
            pres--;
          } else {
            *end = NA_LOGICAL;
            pres--;
            num_miss--;
          }
        }
        break;
      }
      default:
        throw std::runtime_error("Unknown type when processing dictionaries");
      }
    }

    if (hasdict && !hasmiss) {
      // only dict
      int64_t from = pp->metadata.row_group_offsets[cl];
      SEXP x = VECTOR_ELT(pp->columns, cl);
      switch (TYPEOF(x)) {
      case INTSXP: {
        int *beg = INTEGER(x) + from;
        int *end = beg + num_values;
        int *dict = (int*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data();
        while (beg < end) {
          *beg++ = dict[*idx++];
        }
        break;
      }
      case REALSXP: {
        double *beg = REAL(x) + from;
        double *end = beg + num_values;
        double *dict = (double*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data();
        while (beg < end) {
          *beg++ = dict[*idx++];
        }
        break;
      }
      case LGLSXP: {
        // BOOLEAN dictionaries are not really possible...
        int *beg = LOGICAL(x) + from;
        int *end = beg + num_values;
        int *dict = (int*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data();
        while (beg < end) {
          *beg++ = dict[*idx++];
        }
        break;
      }
      default:
        throw std::runtime_error("Unknown type when processing dictionaries");
      }
    }

    if (hasdict && hasmiss) {
      // dict + missing values
      int64_t from = pp->metadata.row_group_offsets[cl];
      SEXP x = VECTOR_ELT(pp->columns, cl);
      switch (TYPEOF(x)) {
      case INTSXP: {
        int *beg = INTEGER(x) + from;
        int *end = beg + num_values - 1;
        int *dict = (int*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx =
          (uint32_t*) pp->dicts[cl][rg].indices.data() + num_present - 1;
        uint8_t *pres = pp->present[cl][rg].map.data() + num_values - 1;
        while (end >= beg) {
          if (*pres) {
            *end-- = dict[*idx--];
            pres--;
          } else {
            *end-- = NA_INTEGER;
            pres--;
          }
        }
        break;
      }
      case REALSXP: {
        double *beg = REAL(x) + from;
        double *end = beg + num_values - 1;
        double *dict = (double*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx =
          (uint32_t*) pp->dicts[cl][rg].indices.data() + num_present - 1;
        uint8_t *pres = pp->present[cl][rg].map.data() + num_values - 1;
        while (end >= beg) {
          if (*pres) {
            *end-- = dict[*idx--];
            pres--;
          } else {
            *end = NA_INTEGER;
            pres--;
          }
        }
        break;
      }
      case LGLSXP: {
        // BOOLEAN dictionaries are not really possible...
        int *beg = LOGICAL(x) + from;
        int *end = beg + num_values - 1;
        int *dict = (int*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx =
          (uint32_t*) pp->dicts[cl][rg].indices.data() + num_present - 1;
        uint8_t *pres = pp->present[cl][rg].map.data() + num_values - 1;
        while (end >= beg) {
          if (*pres) {
            *end-- = dict[*idx--];
            pres--;
          } else {
            *end-- = NA_LOGICAL;
            pres--;
          }
        }
        break;
      }
      default:
        throw std::runtime_error("Unknown type when processing dictionaries");
      }
    }
  }
}

void convert_column_to_r_int64_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  double *beg = REAL(x);
  double *end = REAL(x) + pp->metadata.num_rows;
  int64_t *ibeg = (int64_t*) beg;
  while (beg < end) {
    *beg++ = static_cast<double>(*ibeg++);
  }
}

void convert_column_to_r_int64_dict_nomiss(postprocess *pp, uint32_t cl) {

}

void convert_column_to_r_int64_nodict_miss(postprocess *pp, uint32_t cl) {

}

void convert_column_to_r_int64_dict_miss(postprocess *pp, uint32_t cl) {

}

void convert_column_to_r_int64(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_int64_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_int64_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_int64_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_int64_dict_miss(pp, cl);
  }
}

void convert_columns_to_r_(postprocess *pp) {

  for (auto cl = 0; cl < pp->metadata.num_cols; cl++) {
    rtype rt = pp->metadata.r_types[cl];
    if (rt.type == NILSXP) {
      // non-leaf column
      continue;
    }

    switch (rt.type_conversion) {
    case NONE:
      if (pp->present[cl].size() == 0) {
        convert_column_to_r_dicts(pp, cl);
      } else {
        convert_column_to_r_dicts_na(pp, cl);
      }
      break;
    case INT64_DOUBLE:
      convert_column_to_r_int64(pp, cl);
      break;
    case INT96_DOUBLE:
      // convert_column_to_r_int96(cl);
      break;
    case FLOAT_DOUBLE:
      // convert_column_to_r_float(cl);
      break;
    case BA_STRING:
      // convert_column_to_r_ba_string(cl);
      break;
    case BA_DECIMAL:
      // convert_column_to_r_ba_decimal(cl);
      break;
    case BA_RAW:
      // convert_column_to_r_ba_raw(cl);
      break;
    default:
      break;
    }
  }
}

void RParquetReader::convert_columns_to_r() {
  postprocess pp = {
    columns,
    metadata,
    tmpdata,
    dicts,
    byte_arrays,
    present
  };
  convert_columns_to_r_(&pp);
}
