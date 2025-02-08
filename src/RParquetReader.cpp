#include <cmath>

#include "RParquetReader.h"
#include "protect.h"

constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;

static int64_t impala_timestamp_to_milliseconds(const int96_t &impala_timestamp) {
  int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;
  int64_t nanoseconds;
  memcpy(&nanoseconds, impala_timestamp.value, sizeof(nanoseconds));
  return days_since_epoch * kMillisecondsInADay + nanoseconds / 1000000LL;
}

static uint32_t as_uint(const float x) {
  return *(uint32_t*) &x;
}

static float as_float(const uint32_t x) {
  return *(float*) &x;
}

static double float16_to_double(uint16_t x) {
  bool neg = (x & 0x8000) != 0;
  if ((x & 0x7fff) == 0x7c00) {
    return neg ? R_NegInf : R_PosInf;
  } else if ((x & 0x7fff) > 0x7c00) {
    return R_NaN;
  } else {
    // Accuracy and performance of the lattice Boltzmann method with
    // 64-bit, 32-bit, and customized 16-bit number formats
    // Moritz Lehmann, Mathias J. Krause, Giorgio Amati, Marcello Sega,
    // Jens Harting, and Stephan Gekle
    // Phys. Rev. E 106, 015308
    // https://journals.aps.org/pre/abstract/10.1103/PhysRevE.106.015308
    const uint32_t e = (x & 0x7C00) >> 10;
    const uint32_t m = (x & 0x03FF) << 13;
    const uint32_t v = as_uint((float) m) >> 23;
    // ASAN does not like the large << shift, which is ok and faster
#if defined(__has_feature)
#   if __has_feature(address_sanitizer) || __has_feature(undefined_behavior_sanitizer) // for clang
#       define __SANITIZE_ADDRESS__ // GCC already sets this
#   endif
#endif
#ifdef __SANITIZE_ADDRESS__
    const uint32_t m0 = v < 118 ? 0 : (m << (150-v));
    float f = as_float((x & 0x8000) << 16 |
      (e != 0) * ((e + 112) << 23 | m) |
      ((e == 0) & (m != 0)) * ((v - 37) << 23 | (m0 & 0x007FE000)));
    return f;
#else
    float f = as_float((x & 0x8000) << 16 |
      (e != 0) * ((e + 112) << 23 | m) |
      ((e == 0) & (m != 0)) * ((v - 37) << 23 | ((m << (150 - v)) & 0x007FE000)));
    return f;
#endif
  }
  return 0;
}

// ------------------------------------------------------------------------

// readwrite == true is for appending
RParquetReader::RParquetReader(std::string filename, bool readwrite)
  : ParquetReader(filename, readwrite) {
   if (readwrite) {
     filter.filter_row_groups = true;
     create_metadata(filter);
   } else {
    init(filter);
   }
}

RParquetReader::RParquetReader(std::string filename, RParquetFilter &filter)
  : ParquetReader(filename), filter(filter) {
  init(filter);
}

void RParquetReader::init(RParquetFilter &filter) {
  // metadata
  create_metadata(filter);

  // columns
  columns = Rf_allocVector(VECSXP, metadata.num_cols_to_read);
  R_PreserveObject(columns);
  facdicts = Rf_allocVector(VECSXP, metadata.num_cols_to_read);
  R_PreserveObject(facdicts);
  types = Rf_allocVector(INTSXP, metadata.num_cols_to_read);
  R_PreserveObject(types);
  arrow_metadata = Rf_allocVector(STRSXP, 1);
  R_PreserveObject(arrow_metadata);

  tmpdata.resize(metadata.num_cols_to_read);
  dicts.resize(metadata.num_cols_to_read);
  chunk_parts.resize(metadata.num_cols_to_read);
  byte_arrays.resize(metadata.num_cols_to_read);
  present.resize(metadata.num_cols_to_read);

  for (auto i = 0, idx = 0; i < metadata.num_cols; i++) {
    // skip non-leaf columns
    if (file_meta_data_.schema[i].__isset.num_children &&
        file_meta_data_.schema[i].num_children > 0) {
      continue;
    }
    // skips columns the reader does not want
    if (filter.filter_columns && colmap[i] == 0) {
      continue;
    }
    rtype rt = metadata.r_types[idx];
    SET_VECTOR_ELT(columns, idx, Rf_allocVector(rt.type, metadata.num_rows));
    metadata.dataptr[idx] = (uint8_t*) DATAPTR(VECTOR_ELT(columns, idx));
    if (rt.type != rt.tmptype && rt.tmptype != NILSXP) {
      tmpdata[idx].resize(metadata.num_rows * rt.elsize);
    }
    INTEGER(types)[idx] = file_meta_data_.schema[i].type;
    idx++;
  }
}

RParquetReader::~RParquetReader() {
  if (!Rf_isNull(columns)) {
    R_ReleaseObject(columns);
  }
  if (!Rf_isNull(facdicts)) {
    R_ReleaseObject(facdicts);
  }
  if (!Rf_isNull(types)) {
    R_ReleaseObject(types);
  }
  if (!Rf_isNull(arrow_metadata)) {
    R_ReleaseObject(arrow_metadata);
  }
}

// ------------------------------------------------------------------------

void RParquetReader::create_metadata(RParquetFilter &filter) {
  parquet::FileMetaData fmt = get_file_meta_data();
  metadata.num_rows = fmt.num_rows;
  metadata.num_cols = fmt.schema.size();
  metadata.num_leaf_cols = num_leaf_cols;
  if (filter.filter_columns) {
    metadata.num_cols_to_read = filter.columns.size();
  } else {
    metadata.num_cols_to_read = num_leaf_cols;
  }
  metadata.num_row_groups = fmt.row_groups.size();
  metadata.row_group_num_rows.resize(metadata.num_row_groups);
  metadata.row_group_offsets.resize(metadata.num_row_groups);
  metadata.dataptr.resize(metadata.num_cols_to_read);

  if (!filter.filter_row_groups) {
    for (auto i = 0; i < fmt.row_groups.size(); i++) {
      metadata.row_group_num_rows[i] = fmt.row_groups[i].num_rows;
      metadata.row_group_offsets[i] = 0;
      if (i > 0) {
        metadata.row_group_offsets[i] =
          metadata.row_group_offsets[i-1] + metadata.row_group_num_rows[i-1];
      }
    }
  } else {
    // only a subset of row groups? Then calculate number of rows and
    // row group offsets
    metadata.num_rows = 0;
    std::fill(
      metadata.row_group_num_rows.begin(),
      metadata.row_group_num_rows.end(),
      0
    );
    int64_t offs = 0;
    for (auto i = 0; i < filter.row_groups.size(); i++) {
      uint32_t rg = filter.row_groups[i];
      metadata.row_group_num_rows[rg] = fmt.row_groups[rg].num_rows;
      int64_t rgs = metadata.row_group_num_rows[rg];
      metadata.num_rows += rgs;
      metadata.row_group_offsets[rg] = offs;
      offs += rgs;
    }
  }

  // Map leaf Parquet columns to R columns, the ones we actually read
  colmap.resize(metadata.num_cols, 0);
  if (filter.filter_columns) {
    for (auto i = 0; i < filter.columns.size(); i++) {
      if (filter.columns[i] >= num_leaf_cols) {
        throw std::runtime_error(
          "Unvalid (too large) column selected from Parquet file"
        );
      }
      colmap[filter.columns[i] + 1] = i + 1;
    }
  } else {
    for (auto i = 1, idx = 0; i < metadata.num_cols; i++) {
      if (fmt.schema[i].__isset.num_children &&
          fmt.schema[i].num_children > 0) {
        continue;
      }
      colmap[i] = idx++ + 1;
    }
  }

  // Only consider R types of columns that we read, the rest is NILSXP
  metadata.r_types.resize(metadata.num_cols_to_read);
  for (auto i = 0; i < metadata.num_cols; i++) {
    // skips internals plus columns the reader does not want
    if (colmap[i] == 0) {
      continue;
    }
    rtype rt(fmt.schema[i]);
    metadata.r_types[colmap[i] - 1] = rt;
  }
}

// ------------------------------------------------------------------------

void RParquetReader::read_columns() {
  if (filter.filter_columns) {
    for (auto i = 0; i < filter.columns.size(); i++) {
      read_column(filter.columns[i] + 1);
    }
  } else {
    for (auto i = 1; i < metadata.num_cols; i++) {
      read_column(i);
    }
  }
}

rtype::rtype(parquet::SchemaElement &sel) {
  switch (sel.type) {
  case parquet::Type::BOOLEAN:
    type = tmptype = LGLSXP;
    elsize = sizeof(int);
    psize = 0; // not really true or course...
    break;
  case parquet::Type::INT32:
    type = tmptype = INTSXP;
    elsize = sizeof(int);
    psize = 4;
    if ((sel.__isset.logicalType && sel.logicalType.__isset.DATE) ||
        (sel.__isset.converted_type &&
         sel.converted_type == parquet::ConvertedType::DATE)) {
      classes.push_back("Date");
    } else if (sel.__isset.logicalType && sel.logicalType.__isset.TIME) {
      classes.push_back("hms");
      classes.push_back("difftime");
      units.push_back("secs");
      auto unit = sel.logicalType.TIME.unit;
      if (unit.__isset.MILLIS) {
        time_fct = 1;
      } else if (unit.__isset.MICROS) {           // not possible for INT32
        time_fct = 1000;                          // # nocov
      } else if (unit.__isset.NANOS) {            // # nocov
        time_fct = 1000 * 1000;                   // # nocov
      }
    } else if (sel.__isset.converted_type &&
               sel.converted_type == parquet::ConvertedType::TIME_MILLIS) {
      classes.push_back("hms");
      classes.push_back("difftime");
      units.push_back("secs");
      time_fct = 1;
    } else if (sel.__isset.converted_type &&
               sel.converted_type == parquet::ConvertedType::TIME_MICROS) {
                                                  // not possible for INT32
      classes.push_back("hms");                   // # nocov
      classes.push_back("difftime");              // # nocov
      units.push_back("secs");                    // # nocov
      time_fct = 1000;                            // # nocov
    } else if ((sel.__isset.logicalType && sel.logicalType.__isset.DECIMAL) ||
               (sel.__isset.converted_type &&
                sel.converted_type == parquet::ConvertedType::DECIMAL)) {
      type = tmptype = REALSXP;
      elsize = sizeof(double);
      type_conversion = INT32_DECIMAL;
      scale = sel.scale;
      if (sel.__isset.logicalType) {
        scale = sel.logicalType.DECIMAL.scale;
      }
    }
    break;
  case parquet::Type::INT64:
    type = tmptype = REALSXP;
    type_conversion = INT64_DOUBLE;
    elsize = sizeof(double);
    psize = 8;
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
    } else if ((sel.__isset.logicalType && sel.logicalType.__isset.DECIMAL) ||
               (sel.__isset.converted_type &&
                sel.converted_type == parquet::ConvertedType::DECIMAL)) {
      type_conversion = INT64_DECIMAL;
      scale = sel.scale;
      if (sel.__isset.logicalType) {
        scale = sel.logicalType.DECIMAL.scale;
      }
    }
    break;
  case parquet::Type::INT96:
    type = REALSXP;
    tmptype = INTSXP;
    type_conversion = INT96_DOUBLE;
    elsize = sizeof(int) * 3;
    psize = 8 * 3;
    rsize = 3;
    classes.push_back("POSIXct");
    classes.push_back("POSIXt");
    tzone = "UTC";
    break;
  case parquet::Type::FLOAT:
    type = tmptype = REALSXP;
    type_conversion = FLOAT_DOUBLE;
    elsize = sizeof(double);
    psize = 4;
    break;
  case parquet::Type::DOUBLE:
    type = tmptype = REALSXP;
    elsize = sizeof(double);
    psize = 8;
    break;
  case parquet::Type::BYTE_ARRAY:
  case parquet::Type::FIXED_LEN_BYTE_ARRAY:
    byte_array = true;
    if ((sel.__isset.logicalType &&
         (sel.logicalType.__isset.STRING ||
          sel.logicalType.__isset.ENUM ||
          sel.logicalType.__isset.JSON ||
          sel.logicalType.__isset.BSON)) ||
        (sel.__isset.converted_type &&
         sel.converted_type == parquet::ConvertedType::UTF8)) {
      type = STRSXP;
      tmptype = NILSXP;
      type_conversion = BA_STRING;
    } else if (sel.__isset.logicalType && sel.logicalType.__isset.UUID) {
      type = STRSXP;
      tmptype = NILSXP;
      type_conversion = BA_UUID;
    } else if (sel.__isset.logicalType && sel.logicalType.__isset.FLOAT16) {
      type = REALSXP;
      tmptype = NILSXP;
      type_conversion = BA_FLOAT16;
    } else if ((sel.__isset.logicalType &&
                (sel.logicalType.__isset.DECIMAL)) ||
               (sel.__isset.converted_type &&
                sel.converted_type == parquet::ConvertedType::DECIMAL)) {
      type = REALSXP;
      tmptype = NILSXP;
      type_conversion = BA_DECIMAL;
      scale = sel.scale;
      if (sel.__isset.logicalType) {
        scale = sel.logicalType.DECIMAL.scale;
      }
    } else {
      type = VECSXP;
      tmptype = NILSXP;
      type_conversion = BA_RAW;
    }
    break;
  }
}

// ------------------------------------------------------------------------

// The alloc_*() functions are not allowed to call the R API, so they
// can run concurrently for column chunks.

void RParquetReader::alloc_column_chunk(ColumnChunk &cc)  {
  uint32_t cl = colmap[cc.column] - 1;
  if (chunk_parts[cl].size() == 0) {
    // first row group of this column
    chunk_parts[cl].resize(metadata.num_row_groups);
  }

  if (metadata.r_types[cl].byte_array) {
    if (byte_arrays[cl].size() == 0) {
      byte_arrays[cl].resize(metadata.num_row_groups);
    }
  }

  if (cc.optional) {
    if (present[cl].size() == 0) {
      present[cl].resize(metadata.num_row_groups);
    }
    present[cl][cc.row_group].num_present = 0;
    present[cl][cc.row_group].map.resize(cc.num_rows);
  }
}

void RParquetReader::alloc_dict_page(DictPage &dict) {
  auto cl = colmap[dict.cc.column] - 1;
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
  auto cl = colmap[data.cc.column] - 1;
  auto rg = data.cc.row_group;
  rtype rt = metadata.r_types[cl];

  bool has_dict = data.cc.has_dictionary;
  bool is_index = has_dict &&
    (data.encoding == parquet::Encoding::RLE_DICTIONARY ||
     data.encoding == parquet::Encoding::PLAIN_DICTIONARY);

  // A non-dict-index page in a column chunk that has a
  // dictionary page. Should be rare, but arrow does write
  // these: https://github.com/r-lib/nanoparquet/issues/110
  uint32_t page_off = data.from;
  std::vector<chunk_part> &cps = chunk_parts[cl][rg];
  if (cps.size() == 0) {
    cps.push_back({ data.from, data.num_values, data.num_present, is_index });
  } else {
    chunk_part &last = cps.back();
    if (is_index == last.dict) {
      // same as last, extend chunk part
      if (data.cc.optional) {
        page_off = last.offset + last.num_present;
      }
      last.num_values += data.num_values;
      last.num_present += data.num_present;
    } else {
      // new chunk part
      cps.push_back({ data.from, data.num_values, data.num_present, is_index});
    }
  }

  if (data.cc.optional) {
    present[cl][rg].num_present += data.num_present;
    data.present = present[cl][rg].map.data() + data.from;
  }

  if (is_index) {
      data.data = (uint8_t*) (dicts[cl][rg].indices.data() + page_off);

  } else if (!rt.byte_array) {
    int64_t off = metadata.row_group_offsets[rg];
    if (tmpdata[cl].size() > 0) {
      // only for int96 currently
      data.data = tmpdata[cl].data() + off * rt.elsize + page_off * rt.psize;
    } else {
      data.data = metadata.dataptr[cl] + off * rt.elsize + page_off * rt.psize;
    }
  } else {
    tmpbytes bapage;
    bapage.from = metadata.row_group_offsets[rg] + page_off;
    bapage.buffer.resize(data.strs.total_len);
    bapage.offsets.resize(data.num_present);
    bapage.lengths.resize(data.num_present);
    data.strs.buf = (char*) bapage.buffer.data();
    data.strs.offsets = bapage.offsets.data();
    data.strs.lengths = bapage.lengths.data();
    byte_arrays[cl][rg].push_back(std::move(bapage));
  }
}

// ------------------------------------------------------------------------

// UnwindProtect is simpler if this is not a member function

struct postprocess {
public:
  SEXP columns;
  SEXP facdicts;
  SEXP types;
  rmetadata &metadata;
  std::vector<std::vector<uint8_t>> &tmpdata;
  std::vector<std::vector<tmpdict>> &dicts;
  std::vector<std::vector<std::vector<chunk_part>>> &chunk_parts;
  std::vector<std::vector<std::vector<tmpbytes>>> &byte_arrays;
  std::vector<std::vector<presentmap>> &present;
};

void convert_column_to_r_dicts(postprocess *pp, uint32_t cl) {
  if (pp->dicts[cl].size() == 0) return;
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    if (pp->dicts[cl][rg].dict_len == 0) continue;
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      if (!cps[cpi].dict) continue;
      int64_t cp_offset = cps[cpi].offset;
      int64_t cp_num_values = cps[cpi].num_values;
      SEXP x = VECTOR_ELT(pp->columns, cl);
      switch (TYPEOF(x)) {
      case INTSXP: {
        int *beg = INTEGER(x) + rg_offset + cp_offset;
        int *end = beg + cp_num_values;
        int *dict = (int*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset;
        while (beg < end) {
          *beg++ = dict[*idx++];
        }
        break;
      }
      case REALSXP: {
        double *beg = REAL(x) + rg_offset + cp_offset;
        double *end = beg + cp_num_values;
        double *dict = (double*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset;
        while (beg < end) {
          *beg++ = dict[*idx++];
        }
        break;
      }
      case LGLSXP: {                                                     // # nocov start
        int *beg = LOGICAL(x) + rg_offset + cp_offset;
        int *end = beg + cp_num_values;
        int *dict = (int*) pp->dicts[cl][rg].buffer.data();
        uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset;
        while (beg < end) {
          *beg++ = dict[*idx++];
        }
        break;                                                           // # nocov end
      }
      }
    }
  }
}

void convert_column_to_r_dicts_na(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_values = cps[cpi].num_values;
      int64_t cp_num_present = cps[cpi].num_present;
      bool hasmiss = cp_num_present != cp_num_values;
      bool hasdict = cps[cpi].dict;
      if (!hasdict && !hasmiss) {
        continue;
      } else if (!hasdict && hasmiss) {
        // missing values in place
        switch (TYPEOF(x)) {
        case INTSXP: {
          int *beg = INTEGER(x) + rg_offset + cp_offset;
          int *endm1 = beg + cp_num_values - 1;
          int *pendm1 = beg + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          uint32_t num_miss = cp_num_values - cp_num_present;
          while (num_miss > 0) {
            if (*presm1 != 0) {
              *endm1-- = *pendm1--;
              presm1--;
            } else {
              *endm1-- = NA_INTEGER;
              presm1--;
              num_miss--;
            }
          }
          break;
        }
        case REALSXP: {
          double *beg = REAL(x) + rg_offset + cp_offset;
          double *endm1 = beg + cp_num_values - 1;
          double *pendm1 = beg + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          uint32_t num_miss = cp_num_values - cp_num_present;
          while (num_miss > 0) {
            if (*presm1) {
              *endm1-- = *pendm1--;
              presm1--;
            } else {
              *endm1-- = NA_REAL;
              presm1--;
              num_miss--;
            }
          }
          break;
        }
        case LGLSXP: {
          int *beg = LOGICAL(x) + rg_offset + cp_offset;
          int *endm1 = beg + cp_num_values - 1;
          int *pendm1 = beg + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          uint32_t num_miss = cp_num_values - cp_num_present;
          while (num_miss > 0) {
            if (*presm1) {
              *endm1-- = *pendm1--;
              presm1--;
            } else {
              *endm1-- = NA_LOGICAL;
              presm1--;
              num_miss--;
            }
          }
          break;
        }
        default:
          throw std::runtime_error("Unknown type when processing dictionaries"); // # nocov
        }
      } else if (hasdict && !hasmiss) {
        // only dict
        SEXP x = VECTOR_ELT(pp->columns, cl);
        switch (TYPEOF(x)) {
        case INTSXP: {
          int *beg = INTEGER(x) + rg_offset + cp_offset;
          int *end = beg + cp_num_values;
          int *dict = (int*) pp->dicts[cl][rg].buffer.data();
          uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset;
          while (beg < end) {
            *beg++ = dict[*idx++];
          }
          break;
        }
        case REALSXP: {
          double *beg = REAL(x) + rg_offset + cp_offset;
          double *end = beg + cp_num_values;
          double *dict = (double*) pp->dicts[cl][rg].buffer.data();
          uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset;
          while (beg < end) {
            *beg++ = dict[*idx++];
          }
          break;
        }
        case LGLSXP: {                                                   // # nocov start
          // BOOLEAN dictionaries are not really possible...
          int *beg = LOGICAL(x) + rg_offset + cp_offset;
          int *end = beg + cp_num_values;
          int *dict = (int*) pp->dicts[cl][rg].buffer.data();
          uint32_t *idx = (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset;
          while (beg < end) {
            *beg++ = dict[*idx++];
          }
          break;                                                         // # nocov end
        }
        default:
          throw std::runtime_error("Unknown type when processing dictionaries"); // # nocov
        }
      } else if (hasdict && hasmiss) {
        // dict + missing values
        switch (TYPEOF(x)) {
        case INTSXP: {
          int *beg = INTEGER(x) + rg_offset + cp_offset;
          int *endm1 = beg + cp_num_values - 1;
          int *dict = (int*) pp->dicts[cl][rg].buffer.data();
          uint32_t *idxm1 =
            (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (endm1 >= beg) {
            if (*presm1) {
              *endm1-- = dict[*idxm1--];
              presm1--;
            } else {
              *endm1-- = NA_INTEGER;
              presm1--;
            }
          }
          break;
        }
        case REALSXP: {
          double *beg = REAL(x) + rg_offset + cp_offset;
          double *endm1 = beg + cp_num_values - 1;
          double *dict = (double*) pp->dicts[cl][rg].buffer.data();
          uint32_t *idxm1 =
            (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (endm1 >= beg) {
            if (*presm1) {
              *endm1-- = dict[*idxm1--];
              presm1--;
            } else {
              *endm1-- = NA_REAL;
              presm1--;
            }
          }
          break;
        }
        case LGLSXP: {
          // BOOLEAN dictionaries are not really possible... // # nocov start
          int *beg = LOGICAL(x) + rg_offset + cp_offset;
          int *endm1 = beg + cp_num_values - 1;
          int *dict = (int*) pp->dicts[cl][rg].buffer.data();
          uint32_t *idxm1 =
            (uint32_t*) pp->dicts[cl][rg].indices.data() + cp_offset + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (endm1 >= beg) {
            if (*presm1) {
              *endm1-- = dict[*idxm1--];
              presm1--;
            } else {
              *endm1-- = NA_LOGICAL;
              presm1--;
            }
          }
          break;                                             // # nocov end
        }
        default:
          throw std::runtime_error("Unknown type when processing dictionaries"); // # nocov
        }
      }
    }
  }
}

// ------------------------------------------------------------------------

void convert_column_to_r_int64_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  double *beg = REAL(x);
  double *end = beg + pp->metadata.num_rows;
  int64_t *ibeg = (int64_t*) beg;
  while (beg < end) {
    *beg++ = static_cast<double>(*ibeg++);
  }
}

void convert_column_to_r_int64_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_values = cps[cpi].num_values;
      bool hasdict = cps[cpi].dict;
      double *beg = REAL(x) + rg_offset + cp_offset;
      double *end = beg + cp_num_values;
      if (!hasdict) {
        int64_t *ibeg = (int64_t*) beg;
        while (beg < end) {
          *beg++ = static_cast<double>(*ibeg++);
        }
      } else {
        // first convert tbe dict values
        uint32_t dict_len = pp->dicts[cl][rg].dict_len;
        if (!rg_dict_converted && dict_len > 0) {
          rg_dict_converted = true;
          double *dbeg = (double*) pp->dicts[cl][rg].buffer.data();
          double *dend = dbeg + dict_len;
          int64_t *idbeg = (int64_t *) dbeg;
          while (dbeg < dend) {
            *dbeg++ = static_cast<double>(*idbeg++);
          }
        }
        double *dict = (double*) pp -> dicts[cl][rg].buffer.data();
        uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
        while (beg < end) {
          *beg++ = dict[*didx++];
        }
      }
    }
  }
}

void convert_column_to_r_int64_nodict_miss(postprocess *pp, uint32_t cl) {
  // Need to process this by row group, because the present values are
  // stored at the beginning of each row group.
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    double *beg = REAL(x) + pp->metadata.row_group_offsets[rg];
    int64_t *ibeg = (int64_t*) beg;
    uint32_t num_present = pp->present[cl][rg].num_present;
    bool hasmiss = num_present != num_values;
    if (!hasmiss) {
      double *end = beg + num_values;
      while (beg < end) {
        *beg++ = static_cast<double>(*ibeg++);
      }
    } else {
      double *endm1 = beg + num_values - 1;
      int64_t *pendm1 = ibeg + num_present - 1;
      uint8_t *presm1 = pp->present[cl][rg].map.data() + num_values - 1;
      while (beg <= endm1) {
        if (*presm1) {
          *endm1-- = static_cast<double>(*pendm1--);
          presm1--;
        } else {
          *endm1-- = NA_REAL;
          presm1--;
        }
      }
    }
  }
}

void convert_column_to_r_int64_dict_miss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_values = cps[cpi].num_values;
      uint32_t cp_num_present = cps[cpi].num_present;
      bool hasdict = cps[cpi].dict;
      bool hasmiss = cp_num_present != cp_num_values;
      double *beg = REAL(x) + rg_offset + cp_offset;
      if (!hasdict) {
        int64_t *ibeg = (int64_t *)beg;
        if (!hasmiss) {
          double *end = beg + cp_num_values;
          while (beg < end) {
            *beg++ = static_cast<double>(*ibeg++);
          }
        } else {
          double *endm1 = beg + cp_num_values - 1;
          int64_t *pendm1 = ibeg + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (beg <= endm1) {
            if (*presm1) {
              *endm1-- = static_cast<double>(*pendm1--);
              presm1--;
            } else {
              *endm1-- = NA_REAL;
              presm1--;
            }
          }
        }

      } else {
        // convert dict values first, if not yet done
        uint32_t dict_len = pp->dicts[cl][rg].dict_len;
        if (!rg_dict_converted && dict_len > 0) {
          rg_dict_converted = true;
          double *dbeg = (double *)pp->dicts[cl][rg].buffer.data();
          double *dend = dbeg + dict_len;
          int64_t *idbeg = (int64_t *)dbeg;
          while (dbeg < dend) {
            *dbeg++ = static_cast<double>(*idbeg++);
          }
        }
        double *dict = (double *)pp->dicts[cl][rg].buffer.data();
        if (!hasmiss) {
          double *end = beg + cp_num_values;
          uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
          while (beg < end) {
            *beg++ = dict[*didx++];
          }
        } else {
          double *endm1 = beg + cp_num_values - 1;
          uint32_t *dendm1 = pp->dicts[cl][rg].indices.data() + cp_offset + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (beg <= endm1) {
            if (*presm1) {
              *endm1-- = dict[*dendm1--];
              presm1--;
            } else {
              *endm1-- = NA_REAL;
              presm1--;
            }
          }
        }
      }
    }
  }
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

// ------------------------------------------------------------------------

void convert_column_to_r_float_nodict_nomiss(postprocess *pp, uint32_t cl) {
  // Need to do this by row group because the values are at the beginning
  // of the memory area of the row group
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    int64_t off = pp->metadata.row_group_offsets[rg];
    double *beg = REAL(x) + off;
    double *end = beg + num_values - 1;
    float *fend = ((float*) beg) + num_values - 1;
    while (beg <= end) {
      *end-- = static_cast<double>(*fend--);
    }
  }
}

void convert_column_to_r_float_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (auto cp = cps.rbegin(); cp != cps.rend(); ++cp) {
      double *beg = REAL(x) + rg_offset + cp->offset;
      // In theory we might dictionary encode a subset of the columns only
      if (!cp->dict) {
        double *end = beg + cp->num_values - 1;
        float *fend = ((float*) (REAL(x) + rg_offset)) + cp->offset + cp->num_values - 1;
        while (beg <= end) {
          *end-- = static_cast<double>(*fend--);
        }
      } else {
        // Convert the dictionary first
        uint32_t dict_len = pp->dicts[cl][rg].dict_len;
        if (!rg_dict_converted && dict_len > 0) {
          rg_dict_converted = true;
          double *dbeg = (double*) pp->dicts[cl][rg].buffer.data();
          double *dend = dbeg + dict_len - 1;
          float *fdend = ((float*) dbeg) + dict_len - 1;
          while (dbeg <= dend) {
            *dend-- = static_cast<double>(*fdend--);
          }
        }

        // fill in the dict
        double *end = beg + cp->num_values;
        double *dict = (double*) pp->dicts[cl][rg].buffer.data();
        uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp->offset;
        while (beg < end) {
          *beg++ = dict[*didx++];
        }
      }
    }
  }
}

void convert_column_to_r_float_nodict_miss(postprocess *pp, uint32_t cl) {
  // Need to process this by row group, because the present values are
  // stored at the beginning of each row group.
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    double *beg = REAL(x) + pp->metadata.row_group_offsets[rg];
    double *endm1 = beg + num_values - 1;
    uint32_t num_present = pp->present[cl][rg].num_present;
    float *fendm1 = ((float*) beg) + num_present - 1;
    bool hasmiss = num_present != num_values;
    if (!hasmiss) {
      while (beg <= endm1) {
        *endm1-- = static_cast<double>(*fendm1--);
      }
    } else {
      uint8_t *presm1 = pp->present[cl][rg].map.data() + num_values - 1;
      while (beg <= endm1) {
        if (*presm1) {
          *endm1-- = static_cast<double>(*fendm1--);
          presm1--;
        } else {
          *endm1-- = NA_REAL;
          presm1--;
        }
      }
    }
  }
}

void convert_column_to_r_float_dict_miss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (auto cp = cps.rbegin(); cp != cps.rend(); ++cp) {
      int64_t cp_offset = cp->offset;
      uint32_t cp_num_values = cp->num_values;
      uint32_t cp_num_present = cp->num_present;
      bool hasdict = cp->dict;
      bool hasmiss = cp_num_present != cp_num_values;
      double *beg = REAL(x) + rg_offset + cp_offset;
      if (!hasdict) {
        if (!hasmiss) {
          double *endm1 = beg + cp_num_values - 1;
          float *fendm1 = ((float*) (REAL(x) + rg_offset)) + cp_offset + cp_num_values - 1;
          while (beg <= endm1) {
            *endm1-- = static_cast<double>(*fendm1--);
          }
        } else {
          // nodict, miss
          double *endm1 = beg + cp_num_values - 1;
          float *fendm1 = ((float*) (REAL(x) + rg_offset)) + cp_offset + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (beg <= endm1) {
            if (*presm1) {
              *endm1-- = static_cast<double>(*fendm1--);
              presm1--;
            } else {
              *endm1-- = NA_REAL;
              presm1--;
            }
          }
        }

      } else {
        // convert dict values first
        uint32_t dict_len = pp->dicts[cl][rg].dict_len;
        if (!rg_dict_converted && dict_len > 0) {
          rg_dict_converted = true;
          double *dbeg = (double *)pp->dicts[cl][rg].buffer.data();
          double *dendm1 = dbeg + dict_len - 1;
          float *fdendm1 = ((float*) dbeg) + dict_len - 1;
          while (dbeg <= dendm1) {
            *dendm1-- = static_cast<double>(*fdendm1--);
          }
        }
        // fill in values
        double *dict = (double *)pp->dicts[cl][rg].buffer.data();
        if (!hasmiss) {
          double *end = beg + cp_num_values;
          uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
          while (beg < end) {
            *beg++ = dict[*didx++];
          }
        } else {
          double *endm1 = beg + cp_num_values - 1;
          uint32_t *dendm1 = pp->dicts[cl][rg].indices.data() + cp_offset + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (beg <= endm1) {
            if (*presm1) {
              *endm1-- = dict[*dendm1--];
              presm1--;
            } else {
              *endm1-- = NA_REAL;
              presm1--;
            }
          }
        }
      }
    }
  }
}

void convert_column_to_r_float(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_float_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_float_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_float_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_float_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

void convert_column_to_r_int96_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int96_t *src = (int96_t*) pp->tmpdata[cl].data();
  double *beg = REAL(x);
  double *end = beg + pp->metadata.num_rows;
  while (beg < end) {
    *beg++ = impala_timestamp_to_milliseconds(*src++);
  }
}

void convert_column_to_r_int96_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int96_t *src0 = (int96_t*) pp->tmpdata[cl].data();
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_values = cps[cpi].num_values;
      bool hasdict = cps[cpi].dict;
      double *beg = REAL(x) + rg_offset + cp_offset;
      double *end = beg + cp_num_values;
      if (!hasdict) {
        int96_t *src = src0 + rg_offset + cp_offset;
        while (beg < end) {
          *beg++ = impala_timestamp_to_milliseconds(*src++);
        }
      } else {
        // convert dict values in place
        uint32_t dict_len = pp->dicts[cl][rg].dict_len;
        if (!rg_dict_converted && dict_len > 0) {
          rg_dict_converted = true;
          double *dbeg = (double*) pp->dicts[cl][rg].buffer.data();
          double *dend = dbeg + dict_len;
          int96_t *idbeg = (int96_t*) dbeg;
          while (dbeg < dend) {
            *dbeg++ = impala_timestamp_to_milliseconds(*idbeg++);
          }
        }
        double *dict = (double*) pp->dicts[cl][rg].buffer.data();
        uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
        while (beg < end) {
          *beg++ = dict[*didx++];
        }
      }
    }
  }
}

void convert_column_to_r_int96_nodict_miss(postprocess *pp, uint32_t cl) {
  // Need to process this by row group, because the present values are
  // stored at the beginning of each row group.
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int96_t *src0 = (int96_t*) pp->tmpdata[cl].data();
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    int64_t from = pp->metadata.row_group_offsets[rg];
    double *beg = REAL(x) + from;
    int96_t *ibeg = src0 + from;
    uint32_t num_present = pp->present[cl][rg].num_present;
    bool hasmiss = num_present != num_values;
    if (!hasmiss) {
      double *end = beg + num_values;
      while (beg < end) {
        *beg++ = impala_timestamp_to_milliseconds(*ibeg++);
      }
    } else {
      double *endm1 = beg + num_values - 1;
      int96_t *pendm1 = ibeg + num_present - 1;
      uint8_t *presm1 = pp->present[cl][rg].map.data() + num_values - 1;
      while (beg <= endm1) {
        if (*presm1) {
          *endm1-- = impala_timestamp_to_milliseconds(*pendm1--);
          presm1--;
        } else {
          *endm1-- = NA_REAL;
          presm1--;
        }
      }
    }
  }

}

void convert_column_to_r_int96_dict_miss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int96_t *src0 = (int96_t*) pp->tmpdata[cl].data();
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_values = cps[cpi].num_values;
      uint32_t cp_num_present = cps[cpi].num_present;
      bool hasdict = cps[cpi].dict;
      bool hasmiss = cp_num_present != cp_num_values;
      double *beg = REAL(x) + rg_offset + cp_offset;
      if (!hasdict) {
        int96_t *ibeg = src0 + rg_offset + cp_offset;
        if (!hasmiss) {
          double *end = beg + cp_num_values;
          while (beg < end) {
            *beg++ = impala_timestamp_to_milliseconds(*ibeg++);
          }
        } else {
          double *endm1 = beg + cp_num_values - 1;
          int96_t *pendm1 = ibeg + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (beg <= endm1) {
            if (*presm1) {
              *endm1-- = impala_timestamp_to_milliseconds(*pendm1--);
              presm1--;
            } else {
              *endm1-- = NA_REAL;
              presm1--;
            }
          }
        }

      } else {
        // convert dict values first
        uint32_t dict_len = pp->dicts[cl][rg].dict_len;
        if (!rg_dict_converted && dict_len > 0) {
          rg_dict_converted = true;
          double *dbeg = (double *)pp->dicts[cl][rg].buffer.data();
          double *dend = dbeg + dict_len;
          int96_t *idbeg = (int96_t *) dbeg;
          while (dbeg < dend) {
            *dbeg++ = impala_timestamp_to_milliseconds(*idbeg++);
          }
        }
        double *dict = (double *)pp->dicts[cl][rg].buffer.data();
        if (!hasmiss) {
          double *end = beg + cp_num_values;
          uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
          while (beg < end) {
            *beg++ = dict[*didx++];
          }
        } else {
          double *endm1 = beg + cp_num_values - 1;
          uint32_t *dendm1 = pp->dicts[cl][rg].indices.data() + cp_offset + cp_num_present - 1;
          uint8_t *presm1 = pp->present[cl][rg].map.data() + cp_offset + cp_num_values - 1;
          while (beg <= endm1) {
            if (*presm1) {
              *endm1-- = dict[*dendm1--];
              presm1--;
            } else {
              *endm1-- = NA_REAL;
              presm1--;
            }
          }
        }
      }
    }
  }
}

void convert_column_to_r_int96(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_int96_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_int96_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_int96_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_int96_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

void convert_column_to_r_ba_string_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    int64_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
    for (auto it = rgba.begin(); it != rgba.end(); ++it) {
      int64_t from = it->from;
      for (auto i = 0; i < it->offsets.size(); i++) {
        SEXP xi = Rf_mkCharLenCE(
          (char*) it->buffer.data() + it->offsets[i],
          it->lengths[i],
          CE_UTF8
        );
        SET_STRING_ELT(x, from, xi);
        from++;
      }
    }
  }
}

void convert_column_to_r_ba_string_dict_nomiss(postprocess *pp, uint32_t cl) {
  uint32_t lcl = cl;
  SEXP x = VECTOR_ELT(pp->columns, lcl);
  SET_VECTOR_ELT(pp->facdicts, lcl, Rf_allocVector(VECSXP, pp->metadata.num_row_groups));
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    if (pp->byte_arrays[cl].size() > 0) {
      // first the non-dict parts, if any
      std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
      for (auto it = rgba.begin(); it != rgba.end(); ++it) {
        int64_t from = it->from;
        for (auto i = 0; i < it->offsets.size(); i++) {
          SEXP xi = Rf_mkCharLenCE(
            (char*) it->buffer.data() + it->offsets[i],
            it->lengths[i],
            CE_UTF8
          );
          SET_STRING_ELT(x, from, xi);
          from++;
        }
      }
    }

    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    SEXP tmp = R_NilValue;
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_present = cps[cpi].num_present;
      bool hasdict = cps[cpi].dict;
      if (!hasdict) continue;
      // convert dictionary first
      uint32_t dict_len = pp->dicts[cl][rg].dict_len;
      if (!rg_dict_converted && dict_len > 0) {
        rg_dict_converted = true;
        tmp = PROTECT(Rf_allocVector(STRSXP, dict_len));
        tmpbytes &ba = pp->dicts[cl][rg].bytes;
        for (uint32_t i = 0; i < dict_len; i++) {
          SEXP xi = Rf_mkCharLenCE(
            (char*) ba.buffer.data() + ba.offsets[i],
            ba.lengths[i],
            CE_UTF8
          );
          SET_STRING_ELT(tmp, i, xi);
        }
        SET_VECTOR_ELT(VECTOR_ELT(pp->facdicts, lcl), rg, tmp);
      }

      // fill in
      uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
      uint32_t *end = didx + cp_num_present;
      int64_t from = rg_offset + cp_offset;
      while (didx < end) {
        SET_STRING_ELT(x, from, STRING_ELT(tmp, *didx));
        from++;
        didx++;
      }
    }
    if (!Rf_isNull(tmp)) UNPROTECT(1);
  }
}

void convert_column_to_r_ba_string_miss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    uint32_t num_present = pp->present[cl][rg].num_present;
    bool hasmiss = num_present != num_values;
    if (hasmiss) {
      // need to rewrite
      int64_t beg = pp->metadata.row_group_offsets[rg];
      int64_t endm1 = beg + num_values - 1;
      int64_t pendm1 = beg + num_present -1;
      uint8_t *presm1 = pp->present[cl][rg].map.data() + num_values - 1;
      while (beg <= endm1) {
        if (*presm1) {
          SET_STRING_ELT(x, endm1--, STRING_ELT(x, pendm1--));
          presm1--;
        } else {
          SET_STRING_ELT(x, endm1--, NA_STRING);
          presm1--;
        }
      }
    }
  }
}

void convert_column_to_r_ba_string_nodict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_string_nodict_nomiss(pp, cl);
  convert_column_to_r_ba_string_miss(pp, cl);
}

void convert_column_to_r_ba_string_dict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_string_dict_nomiss(pp, cl);
  convert_column_to_r_ba_string_miss(pp, cl);
}

void convert_column_to_r_ba_string(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_string_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_string_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_ba_string_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_ba_string_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

inline double parse_decimal(uint8_t *d, uint32_t len) {
  if (len == 0) return 0;
  uint64_t val = 0;
  bool neg = d[0] >= 128;
  if (neg) {
    for (auto j = 0; j < len; j++) {
      uint8_t n = ~ d[j];
      val = (val << 8) | n;
    }
    return - ((double) val + 1);

  } else {
    for (auto j = 0; j < len; j++) {
      val = (val << 8) | d[j];
    }
    return val;
  }
}

void convert_column_to_r_ba_decimal_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int32_t scale = pp->metadata.r_types[cl].scale;
  double fct = std::pow(10.0, scale);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
    for (auto it = rgba.begin(); it != rgba.end(); ++it) {
      int64_t from = it->from;
      double *beg = REAL(x) + from;
      for (auto i = 0; i < it->offsets.size(); i++) {
        beg[i] = parse_decimal(it->buffer.data() + it->offsets[i], it->lengths[i]) / fct;
      }
    }
  }
}

void convert_column_to_r_ba_decimal_miss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    uint32_t num_present = pp->present[cl][rg].num_present;
    bool hasmiss = num_present != num_values;
    if (hasmiss) {
      // need to rewrite
      double *beg = REAL(x) + pp->metadata.row_group_offsets[rg];
      double *endm1 = beg + num_values - 1;
      double *pendm1 = beg + num_present -1;
      uint8_t *presm1 = pp->present[cl][rg].map.data() + num_values - 1;
      while (beg <= endm1) {
        if (*presm1) {
          *endm1-- = *pendm1--;
          presm1--;
        } else {
          *endm1-- = NA_REAL;
          presm1--;
        }
      }
    }
  }
}

void convert_column_to_r_ba_decimal_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int32_t scale = pp->metadata.r_types[cl].scale;
  double fct = std::pow(10.0, scale);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    if (pp->byte_arrays[cl].size() > 0) {
      // first the non-dict parts, if any
      std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
      for (auto it = rgba.begin(); it != rgba.end(); ++it) {
        int64_t from = it->from;
        double *beg = REAL(x) + from;
        for (auto i = 0; i < it->offsets.size(); i++) {
          beg[i] = parse_decimal(it->buffer.data() + it->offsets[i], it->lengths[i]) / fct;
        }
      }
    }

    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    SEXP tmp = R_NilValue;
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_present = cps[cpi].num_present;
      bool hasdict = cps[cpi].dict;
      if (!hasdict) continue;
      // convert dictionary first
      uint32_t dict_len = pp->dicts[cl][rg].dict_len;
      if (!rg_dict_converted && dict_len > 0) {
        rg_dict_converted = true;
        tmp = PROTECT(Rf_allocVector(REALSXP, dict_len));
        tmpbytes &ba = pp->dicts[cl][rg].bytes;
        for (uint32_t i = 0; i < dict_len; i++) {
          REAL(tmp)[i] = parse_decimal(ba.buffer.data() + ba.offsets[i], ba.lengths[i]) / fct;
        }
      }

      // fill in
      uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
      uint32_t *end = didx + cp_num_present;
      int64_t from = rg_offset + cp_offset;
      while (didx < end) {
        REAL(x)[from++] = REAL(tmp)[*didx++];
      }
    }
    if (!Rf_isNull(tmp)) UNPROTECT(1);
  }
}

void convert_column_to_r_ba_decimal_nodict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_decimal_nodict_nomiss(pp, cl);
  convert_column_to_r_ba_decimal_miss(pp, cl);
}

void convert_column_to_r_ba_decimal_dict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_decimal_dict_nomiss(pp, cl);
  convert_column_to_r_ba_decimal_miss(pp, cl);
}

void convert_column_to_r_ba_decimal(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_decimal_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_decimal_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_ba_decimal_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_ba_decimal_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

void convert_column_to_r_ba_raw_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
    for (auto it = rgba.begin(); it != rgba.end(); ++it) {
      int64_t from = it->from;
      for (auto i = 0; i < it->offsets.size(); i++) {
        SEXP xi = Rf_allocVector(RAWSXP, it->lengths[i]);
        memcpy(RAW(xi), it->buffer.data() + it->offsets[i], it->lengths[i]);
        SET_VECTOR_ELT(x, from, xi);
        from++;
      }
    }
  }
}

void convert_column_to_r_ba_raw_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    if (pp->byte_arrays[cl].size() > 0) {
      // first the non-dict parts, if any
      std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
      for (auto it = rgba.begin(); it != rgba.end(); ++it) {
        int64_t from = it->from;
        for (auto i = 0; i < it->offsets.size(); i++) {
          SEXP xi = Rf_allocVector(RAWSXP, it->lengths[i]);
          memcpy(RAW(xi), it->buffer.data() + it->offsets[i], it->lengths[i]);
          SET_VECTOR_ELT(x, from, xi);
          from++;
        }
      }
    }

    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    SEXP tmp = R_NilValue;
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_present = cps[cpi].num_present;
      bool hasdict = cps[cpi].dict;
      if (!hasdict) continue;
      // convert dictionary first
      uint32_t dict_len = pp->dicts[cl][rg].dict_len;
      if (!rg_dict_converted && dict_len > 0) {
        rg_dict_converted = true;
        tmp = PROTECT(Rf_allocVector(VECSXP, dict_len));
        tmpbytes &ba = pp->dicts[cl][rg].bytes;
        for (uint32_t i = 0; i < dict_len; i++) {
          SEXP xi = Rf_allocVector(RAWSXP, ba.lengths[i]);
          memcpy(RAW(xi), ba.buffer.data() + ba.offsets[i], ba.lengths[i]);
          SET_VECTOR_ELT(tmp, i, xi);
        }
      }

      // fill in
      uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
      uint32_t *end = didx + cp_num_present;
      int64_t from = rg_offset + cp_offset;
      while (didx < end) {
        SET_VECTOR_ELT(x, from, VECTOR_ELT(tmp, *didx));
        from++;
        didx++;
      }
    }
    if (!Rf_isNull(tmp)) UNPROTECT(1);
  }
}

void convert_column_to_r_ba_raw_miss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    uint32_t num_present = pp->present[cl][rg].num_present;
    bool hasmiss = num_present != num_values;
    if (hasmiss) {
      // need to rewrite
      int64_t beg = pp->metadata.row_group_offsets[rg];
      int64_t endm1 = beg + num_values - 1;
      int64_t pendm1 = beg + num_present -1;
      uint8_t *presm1 = pp->present[cl][rg].map.data() + num_values - 1;
      while (beg <= endm1) {
        if (*presm1) {
          SET_VECTOR_ELT(x, endm1--, VECTOR_ELT(x, pendm1--));
          presm1--;
        } else {
          SET_VECTOR_ELT(x, endm1--, R_NilValue);
          presm1--;
        }
      }
    }
  }
}

void convert_column_to_r_ba_raw_nodict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_raw_nodict_nomiss(pp, cl);
  convert_column_to_r_ba_raw_miss(pp, cl);
}

void convert_column_to_r_ba_raw_dict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_raw_dict_nomiss(pp, cl);
  convert_column_to_r_ba_raw_miss(pp, cl);
}

void convert_column_to_r_ba_raw(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_raw_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_raw_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_ba_raw_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_ba_raw_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

void convert_column_to_r_ba_uuid_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  char uuid[37];
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
    for (auto it = rgba.begin(); it != rgba.end(); ++it) {
      int64_t from = it->from;
      for (auto i = 0; i < it->offsets.size(); i++) {
        unsigned char *s = (unsigned char*) it->buffer.data() + it->offsets[i];
        snprintf(
          uuid, 37,
          "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
          s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9],
          s[10], s[11], s[12], s[13], s[14], s[15]
        );
        SET_STRING_ELT(x, from, Rf_mkCharLenCE(uuid, 36, CE_UTF8));
        from++;
      }
    }
  }
}

void convert_column_to_r_ba_uuid_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  char uuid[37];
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    if (pp->byte_arrays[cl].size() > 0) {
      // first the non-dict parts, if any
      std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
      for (auto it = rgba.begin(); it != rgba.end(); ++it) {
        int64_t from = it->from;
        for (auto i = 0; i < it->offsets.size(); i++) {
          unsigned char *s = (unsigned char*) it->buffer.data() + it->offsets[i];
          snprintf(
            uuid, 37,
            "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9],
            s[10], s[11], s[12], s[13], s[14], s[15]
          );
          SET_STRING_ELT(x, from, Rf_mkCharLenCE(uuid, 36, CE_UTF8));
          from++;
        }
      }
    }

    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    SEXP tmp = R_NilValue;
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_present = cps[cpi].num_present;
      bool hasdict = cps[cpi].dict;
      if (!hasdict) continue;
      // convert dictionary first
      uint32_t dict_len = pp->dicts[cl][rg].dict_len;
      if (!rg_dict_converted && dict_len > 0) {
        rg_dict_converted = true;
        tmp = PROTECT(Rf_allocVector(STRSXP, dict_len));
        tmpbytes &ba = pp->dicts[cl][rg].bytes;
        for (uint32_t i = 0; i < dict_len; i++) {
          unsigned char *s = (unsigned char*) ba.buffer.data() + ba.offsets[i];
          snprintf(
            uuid, 37,
            "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9],
            s[10], s[11], s[12], s[13], s[14], s[15]
          );
          SET_STRING_ELT(tmp, i, Rf_mkCharLenCE(uuid, 36, CE_UTF8));
        }
      }

      // fill in
      uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
      uint32_t *end = didx + cp_num_present;
      int64_t from = rg_offset + cp_offset;
      while (didx < end) {
        SET_STRING_ELT(x, from, STRING_ELT(tmp, *didx));
        from++;
        didx++;
      }
    }
    if (!Rf_isNull(tmp)) UNPROTECT(1);
  }
}

void convert_column_to_r_ba_uuid_nodict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_uuid_nodict_nomiss(pp, cl);
  convert_column_to_r_ba_string_miss(pp, cl);
}

void convert_column_to_r_ba_uuid_dict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_uuid_dict_nomiss(pp, cl);
  convert_column_to_r_ba_string_miss(pp, cl);
}

void convert_column_to_r_ba_uuid(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_uuid_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_uuid_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_ba_uuid_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_ba_uuid_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

void convert_column_to_r_ba_float16_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
    for (auto it = rgba.begin(); it != rgba.end(); ++it) {
      int64_t from = it->from;
      for (auto i = 0; i < it->offsets.size(); i++) {
        uint16_t *f = (uint16_t*) (it->buffer.data() + it->offsets[i]);
        REAL(x)[from] = float16_to_double(*f);
        from++;
      }
    }
  }
}

void convert_column_to_r_ba_float16_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    if (pp->byte_arrays[cl].size() > 0) {
      // first the non-dict parts, if any
      std::vector<tmpbytes> rgba = pp->byte_arrays[cl][rg];
      for (auto it = rgba.begin(); it != rgba.end(); ++it) {
        int64_t from = it->from;
        for (auto i = 0; i < it->offsets.size(); i++) {
          uint16_t *f = (uint16_t*) (it->buffer.data() + it->offsets[i]);
          REAL(x)[from] = float16_to_double(*f);
          from++;
        }
      }
    }

    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    SEXP tmp = R_NilValue;
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_present = cps[cpi].num_present;
      bool hasdict = cps[cpi].dict;
      if (!hasdict) continue;
      // convert dictionary first
      uint32_t dict_len = pp->dicts[cl][rg].dict_len;
      if (!rg_dict_converted && dict_len > 0) {
        rg_dict_converted = true;
        tmp = PROTECT(Rf_allocVector(REALSXP, dict_len));
        tmpbytes &ba = pp->dicts[cl][rg].bytes;
        for (uint32_t i = 0; i < dict_len; i++) {
          uint16_t *f = (uint16_t*) (ba.buffer.data() + ba.offsets[i]);
          REAL(tmp)[i] = float16_to_double(*f);
        }
      }

      // fill in
      uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
      uint32_t *end = didx + cp_num_present;
      int64_t from = rg_offset + cp_offset;
      while (didx < end) {
        REAL(x)[from] = REAL(tmp)[*didx];
        from++;
        didx++;
      }
    }
    if (!Rf_isNull(tmp)) UNPROTECT(1);
  }
}

void convert_column_to_r_ba_float16_nodict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_float16_nodict_nomiss(pp, cl);
  convert_column_to_r_ba_decimal_miss(pp, cl);
}

void convert_column_to_r_ba_float16_dict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_ba_float16_dict_nomiss(pp, cl);
  convert_column_to_r_ba_decimal_miss(pp, cl);
}

void convert_column_to_r_ba_float16(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_float16_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_ba_float16_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_ba_float16_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_ba_float16_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

void convert_column_to_r_int32_decimal_nodict_nomiss(postprocess *pp, uint32_t cl) {
  // Need to do this by row group because the values are at the beginning
  // of the memory area of the row group
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int32_t scale = pp->metadata.r_types[cl].scale;
  double fct = std::pow(10.0, scale);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    uint32_t num_values = pp->metadata.row_group_num_rows[rg];
    if (num_values == 0) continue;
    int64_t off = pp->metadata.row_group_offsets[rg];
    double *beg = REAL(x) + off;
    double *end = beg + num_values - 1;
    int32_t *fend = ((int32_t*) beg) + num_values - 1;
    while (beg <= end) {
      *end-- = static_cast<double>(*fend--) / fct;
    }
  }
}

void convert_column_to_r_int32_decimal_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int32_t scale = pp->metadata.r_types[cl].scale;
  double fct = std::pow(10.0, scale);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (uint32_t cpi = 0; cpi < cps.size(); cpi++) {
      int64_t cp_offset = cps[cpi].offset;
      uint32_t cp_num_values = cps[cpi].num_values;
      bool hasdict = cps[cpi].dict;
      double *beg = REAL(x) + rg_offset + cp_offset;
      if (!hasdict) {
        double *end = beg + cp_num_values - 1;
        int32_t *fend = ((int32_t*) beg) + cp_num_values - 1;
        while (beg <= end) {
          *end-- = static_cast<double>(*fend--) / fct;
        }
      } else {
        // Convert the dictionary first
        uint32_t dict_len = pp->dicts[cl][rg].dict_len;
        if (!rg_dict_converted && dict_len > 0) {
          rg_dict_converted = true;
          double *dbeg = (double*) pp->dicts[cl][rg].buffer.data();
          double *dend = dbeg + dict_len - 1;
          int32_t *fdend = ((int32_t*) dbeg) + dict_len - 1;
          while (dbeg <= dend) {
            *dend-- = static_cast<double>(*fdend--) / fct;
          }
        }

        // fill in the dict
        double *end = beg + cp_num_values;
        double *dict = (double*) pp->dicts[cl][rg].buffer.data();
        uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
        while (beg < end) {
          *beg++ = dict[*didx++];
        }
      }
    }
  }
}

void convert_column_to_r_int32_decimal_nodict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_int32_decimal_nodict_nomiss(pp, cl);
  convert_column_to_r_ba_decimal_miss(pp, cl);
}

void convert_column_to_r_int32_decimal_dict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_int32_decimal_dict_nomiss(pp, cl);
  convert_column_to_r_ba_decimal_miss(pp, cl);
}

void convert_column_to_r_int32_decimal(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_int32_decimal_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_int32_decimal_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_int32_decimal_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_int32_decimal_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

void convert_column_to_r_int64_decimal_nodict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int32_t scale = pp->metadata.r_types[cl].scale;
  double fct = std::pow(10.0, scale);
  double *beg = REAL(x);
  double *end = beg + pp->metadata.num_rows;
  int64_t *ibeg = (int64_t*) beg;
  while (beg < end) {
    *beg++ = static_cast<double>(*ibeg++) / fct;
  }
}

void convert_column_to_r_int64_decimal_dict_nomiss(postprocess *pp, uint32_t cl) {
  SEXP x = VECTOR_ELT(pp->columns, cl);
  int32_t scale = pp->metadata.r_types[cl].scale;
  double fct = std::pow(10.0, scale);
  for (auto rg = 0; rg < pp->metadata.num_row_groups; rg++) {
    std::vector<chunk_part> &cps = pp->chunk_parts[cl][rg];
    bool rg_dict_converted = false;
    int64_t rg_offset = pp->metadata.row_group_offsets[rg];
    for (auto cp = cps.begin(); cp != cps.end(); ++cp) {
      int64_t cp_offset = cp->offset;
      uint32_t cp_num_values = cp->num_values;
      bool hasdict = cp->dict;
      double *beg = REAL(x) + rg_offset + cp_offset;
      double *end = beg + cp_num_values;
      if (!hasdict) {
        int64_t *ibeg = (int64_t*) beg;
        while (beg < end) {
          *beg++ = static_cast<double>(*ibeg++) / fct;
        }
      } else {
        // convert dictionary first
        uint32_t dict_len = pp->dicts[cl][rg].dict_len;
        if (!rg_dict_converted && dict_len > 0) {
          rg_dict_converted = true;
          double *dbeg = (double*) pp->dicts[cl][rg].buffer.data();
          double *dend = dbeg + dict_len;
          int64_t *idbeg = (int64_t *) dbeg;
          while (dbeg < dend) {
            *dbeg++ = static_cast<double>(*idbeg++) / fct;
          }
        }
        double *dict = (double*) pp -> dicts[cl][rg].buffer.data();
        uint32_t *didx = pp->dicts[cl][rg].indices.data() + cp_offset;
        while (beg < end) {
          *beg++ = dict[*didx++];
        }
      }
    }
  }
}

void convert_column_to_r_int64_decimal_nodict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_int64_decimal_nodict_nomiss(pp, cl);
  convert_column_to_r_ba_decimal_miss(pp, cl);
}

void convert_column_to_r_int64_decimal_dict_miss(postprocess *pp, uint32_t cl) {
  convert_column_to_r_int64_decimal_dict_nomiss(pp, cl);
  convert_column_to_r_ba_decimal_miss(pp, cl);
}

void convert_column_to_r_int64_decimal(postprocess *pp, uint32_t cl) {
  bool hasdict0 = pp->dicts[cl].size() > 0;
  bool hasmiss0 = pp->present[cl].size() > 0;
  if (!hasdict0 && !hasmiss0) {
    convert_column_to_r_int64_decimal_nodict_nomiss(pp, cl);
  } else if (hasdict0 && !hasmiss0) {
    convert_column_to_r_int64_decimal_dict_nomiss(pp, cl);
  } else if (!hasdict0 && hasmiss0) {
    convert_column_to_r_int64_decimal_nodict_miss(pp, cl);
  } else if (hasdict0 && hasmiss0) {
    convert_column_to_r_int64_decimal_dict_miss(pp, cl);
  }
}

// ------------------------------------------------------------------------

void convert_columns_to_r_(postprocess *pp) {

  for (auto cl = 0; cl < pp->metadata.num_cols_to_read; cl++) {
    rtype rt = pp->metadata.r_types[cl];

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
      convert_column_to_r_int96(pp, cl);
      break;
    case FLOAT_DOUBLE:
      convert_column_to_r_float(pp, cl);
      break;
    case BA_STRING:
      convert_column_to_r_ba_string(pp, cl);
      break;
    case BA_DECIMAL:
      convert_column_to_r_ba_decimal(pp, cl);
      break;
    case BA_RAW:
      convert_column_to_r_ba_raw(pp, cl);
      break;
    case BA_UUID:
      convert_column_to_r_ba_uuid(pp, cl);
      break;
    case BA_FLOAT16:
      convert_column_to_r_ba_float16(pp, cl);
      break;
    case INT32_DECIMAL:
      convert_column_to_r_int32_decimal(pp, cl);
      break;
    case INT64_DECIMAL:
      convert_column_to_r_int64_decimal(pp, cl);
      break;
    default:
      break;        // # nocov
    }

    // add classes, if any
    size_t nc = rt.classes.size();
    if (nc > 0) {
      SEXP x = VECTOR_ELT(pp->columns, cl);
      SEXP cls = PROTECT(Rf_allocVector(STRSXP, nc));
      for (size_t i = 0; i < nc; i++) {
        SET_STRING_ELT(cls, i, Rf_mkCharCE(rt.classes[i].c_str(), CE_UTF8));
      }
      SET_CLASS(x, cls);
      UNPROTECT(1);
    }

    // add time zone attribute, if any
    if (rt.tzone != "") {
      SEXP x = VECTOR_ELT(pp->columns, cl);
      Rf_setAttrib(x, Rf_install("tzone"), Rf_mkString(rt.tzone.c_str()));
    }

    // add unit
    size_t nu = rt.units.size();
    if (nu > 0) {
      SEXP x = VECTOR_ELT(pp->columns, cl);
      SEXP units = PROTECT(Rf_allocVector(STRSXP, nu));
      for (size_t i = 0; i < nu; i++) {
        SET_STRING_ELT(units, i, Rf_mkCharCE(rt.units[i].c_str(), CE_UTF8));
      }
      Rf_setAttrib(x, Rf_install("units"), units);
      UNPROTECT(1);
    }

    // use multiplier, if any
    if (rt.time_fct != 1.0) {
      SEXP x = VECTOR_ELT(pp->columns, cl);
      if (TYPEOF(x) == INTSXP) {                       // does not happen
        int32_t *ptr = INTEGER(x);                     // # nocov
        int32_t *end = ptr + Rf_xlength(x);            // # nocov
        for (; ptr < end; ptr++) {                     // # nocov
          *ptr /= rt.time_fct;                         // # nocov
        }
      } else if (TYPEOF(x) == REALSXP) {
        double *ptr = REAL(x);
        double *end = ptr + Rf_xlength(x);
        for (; ptr < end; ptr++) {
          *ptr /= rt.time_fct;
        }
      } else {
        Rf_error("Internal nanoparquet error, cannot multiply non-numeric"); // # nocov
      }
    }
  }
}

void RParquetReader::convert_columns_to_r() {
  postprocess pp = {
    columns,
    facdicts,
    types,
    metadata,
    tmpdata,
    dicts,
    chunk_parts,
    byte_arrays,
    present
  };
  convert_columns_to_r_(&pp);
}

// ------------------------------------------------------------------------

void RParquetReader::create_df() {
  SEXP nms = PROTECT(Rf_allocVector(STRSXP, metadata.num_cols_to_read));
  for (R_xlen_t i = 0; i < metadata.num_cols; i++) {
    // skip columns that were not requested
    if (colmap[i] == 0) {
      continue;
    }
    SET_STRING_ELT(
      nms, colmap[i] - 1,
      Rf_mkCharCE(file_meta_data_.schema[i].name.c_str(), CE_UTF8)
    );
  }
  Rf_setAttrib(columns, R_NamesSymbol, nms);
  UNPROTECT(1);

  SEXP rnms = PROTECT(Rf_allocVector(INTSXP, 2));
  INTEGER(rnms)[0] = NA_INTEGER;
  INTEGER(rnms)[1] = - metadata.num_rows;
  Rf_setAttrib(columns, R_RowNamesSymbol, rnms);
  UNPROTECT(1);

  SEXP cls = PROTECT(Rf_allocVector(STRSXP, 1));
  SET_STRING_ELT(cls, 0, Rf_mkCharCE("data.frame", CE_UTF8));
  Rf_setAttrib(columns, R_ClassSymbol, cls);
  UNPROTECT(1);
}

// ------------------------------------------------------------------------

void RParquetReader::read_arrow_metadata() {
  if (file_meta_data_.__isset.key_value_metadata) {
    std::vector<parquet::KeyValue> &kvm = file_meta_data_.key_value_metadata;
    for (auto i = 0; i < kvm.size(); i++) {
      parquet::KeyValue &kv = kvm[i];
      if (kv.__isset.value) {
        if (kv.key == "ARROW:schema") {
          SET_STRING_ELT(arrow_metadata, 0, Rf_mkChar(kv.value.c_str()));
          return;
        }
      }
    }
  }
  SET_STRING_ELT(arrow_metadata, 0, R_NaString);
  return;
}
