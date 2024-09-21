#include <cmath>
#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>

#include "lib/nanoparquet.h"
#include "lib/bitpacker.h"

#include <Rdefines.h>

#include "lib/memstream.h"

#include "protect.h"

using namespace nanoparquet;
using namespace std;

extern SEXP nanoparquet_call;

inline Int96 int32_to_int96(int32_t x) {
  if (x >= 0) {
    Int96 r = { 0, 0, 0 };
    int32_t *p = (int32_t*) r.value;
    *p = x;
    return r;
  } else {
    Int96 r = { 0, 0xffffffff, 0xffffffff };
    int32_t *p = (int32_t*) r.value;
    *p = x;
    return r;
  }
}

inline Int96 double_to_int96(double x) {
  x = trunc(x);
  Int96 r = { 0, 0, 0 };
  bool neg = x < 0;
  x = neg ? -x : x;

  // absolute value first
  // TODO: what to do if too big?
  r.value[0] = fmod(x, 0xffffffff);
  x = trunc(x / 0xffffffff);
  r.value[1] = fmod(x, 0xffffffff);
  x = trunc(x / 0xffffffff);
  r.value[2] = x;

  // TODO: what to do if too small?
  if (neg) {
    r.value[0] = ~r.value[0] + 1;
    r.value[1] = ~r.value[1];
    r.value[2] = ~r.value[2];
    if (r.value[0] == 0) {
      r.value[1] += 1;
      if (r.value[1] == 0) {
        r.value[2] += 1;
      }
    }
  }
  return r;
}

static uint16_t double_to_float16(double x) {
  if (x == R_PosInf) {
    return 0x7c00;
  } else if (x == R_NegInf) {
    return 0xfc00;
  } else if (R_IsNaN(x)) {
    return 0x7C80;
  } else if (x > 65504) {
    return 0x7c00;
  } else if (x < -65504) {
    return 0xfc00;
  } else if (x >= 0 && x < 0.000000059604645) {
    return 0x0000;
  } else if (x <= 0 && x > -0.000000059604645) {
    return 0x0000;
  }
  float f = x;
  uint32_t fi32 = *((uint32_t*) &f);
  uint16_t fi16 = (fi32 >> 31) << 5;
  uint16_t tmp = (fi32 >> 23) & 0xff;
  tmp = (tmp - 0x70) & ((uint32_t)((int32_t)(0x70 - tmp) >> 4) >> 27);
  fi16 = (fi16 | tmp) << 10;
  fi16 |= (fi32 >> 13) & 0x3ff;
  return fi16;
}

extern "C" {
SEXP nanoparquet_create_dict(SEXP x, SEXP rlen);
SEXP nanoparquet_create_dict_idx_(SEXP x, SEXP from, SEXP until);
SEXP nanoparquet_avg_run_length(SEXP x, SEXP rlen);
static SEXP get_list_element(SEXP list, const char *str);
}

class RParquetOutFile : public ParquetOutFile {
public:
  RParquetOutFile(
    std::string filename,
    parquet::CompressionCodec::type codec,
    int compression_level,
    std::vector<int64_t> &row_groups
  );
  RParquetOutFile(
    std::ostream &stream,
    parquet::CompressionCodec::type codec,
    int compsession_level,
    std::vector<int64_t> &row_groups
  );
  void write_row_group(uint32_t group);
  void write_int32(std::ostream &file, uint32_t idx, uint32_t group,
                   uint32_t page, uint64_t from, uint64_t until,
                   parquet::SchemaElement &sel);
  void write_int64(std::ostream &file, uint32_t idx, uint32_t group,
                   uint32_t page, uint64_t from, uint64_t until,
                   parquet::SchemaElement &sel);
  void write_int96(std::ostream &file, uint32_t idx, uint32_t group,
                   uint32_t page, uint64_t from, uint64_t until,
                   parquet::SchemaElement &sel);
  void write_float(std::ostream &file, uint32_t idx, uint32_t group,
                   uint32_t page, uint64_t from, uint64_t until,
                   parquet::SchemaElement &sel);
  void write_double(std::ostream &file, uint32_t idx, uint32_t group,
                    uint32_t page, uint64_t from, uint64_t until,
                    parquet::SchemaElement &sel);
  void write_byte_array(std::ostream &file, uint32_t idx, uint32_t group,
                        uint32_t page, uint64_t from, uint64_t until,
                        parquet::SchemaElement &sel);
  void write_fixed_len_byte_array(std::ostream &file, uint32_t id,
                                  uint32_t group, uint32_t page, uint64_t from,
                                  uint64_t until, parquet::SchemaElement &sel);
  uint32_t get_size_byte_array(uint32_t idx, uint32_t num_present,
                               uint64_t from, uint64_t until);
  void write_boolean(std::ostream &file, uint32_t idx, uint32_t group,
                     uint32_t page, uint64_t from, uint64_t until);
  void write_boolean_as_int(std::ostream &file, uint32_t idx, uint32_t group,
                            uint32_t page, uint64_t from, uint64_t until);

  uint32_t write_present(std::ostream &file, uint32_t idx, uint64_t from,
                         uint64_t until);
  void write_present_boolean(std::ostream &file, uint32_t idx,
                             uint32_t num_present, uint64_t from,
                             uint64_t until);
  void write_present_boolean_as_int(std::ostream &file, uint32_t idx,
                                    uint32_t num_present, uint64_t from,
                                    uint64_t until);

  // for dictionaries
  uint32_t get_num_values_dictionary(uint32_t idx,
                                     parquet::SchemaElement &sel,
                                     int64_t form, int64_t until);
  uint32_t get_size_dictionary(uint32_t idx, parquet::SchemaElement &type,
                               int64_t from, int64_t until);
  void write_dictionary(std::ostream &file, uint32_t idx,
                        parquet::SchemaElement &sel, int64_t from,
                        int64_t until);
  void write_dictionary_indices(std::ostream &file, uint32_t idx,
                                int64_t rg_from, int64_t rg_until,
                                uint64_t page_from, uint64_t page_until);

  // statistics
  bool get_group_minmax_values(uint32_t idx, uint32_t group,
                               parquet::SchemaElement &sel,
                               std::string &min_value,
                               std::string &max_value);

  void write(
    SEXP dfsxp,
    SEXP dim,
    SEXP metadata,
    SEXP rrequired,
    SEXP options,
    SEXP schema,
    SEXP encoding
  );

private:
  SEXP df = R_NilValue;
  SEXP required = R_NilValue;
  SEXP dicts = R_NilValue;
  SEXP dicts_from = R_NilValue;
  ByteBuffer present;

  bool write_minmax_values;
  std::vector<bool> is_minmax_supported;
  std::vector<std::string> min_values;
  std::vector<std::string> max_values;
  std::vector<bool> has_minmax_value;

  void create_dictionary(uint32_t idx, int64_t from, int64_t until,
                         parquet::SchemaElement &sel);
  // for LGLSXP this mean RLE encoding
  bool should_use_dict_encoding(uint32_t idx);
  parquet::Encoding::type
  detect_encoding(uint32_t idx, parquet::SchemaElement &sel, int32_t renc);

  void write_integer_int32(std::ostream &file, SEXP col, uint32_t idx,
                           uint64_t from, uint64_t until,
                           parquet::SchemaElement &sel);
  void write_double_int32_time(std::ostream &file, SEXP col, uint32_t idx,
                               uint64_t from, uint64_t until,
                               parquet::SchemaElement &sel, double factor);
  void write_double_int32(std::ostream &file, SEXP col, uint32_t idx,
                          uint64_t from, uint64_t until,
                          parquet::SchemaElement &sel);
};

RParquetOutFile::RParquetOutFile(
  std::string filename,
  parquet::CompressionCodec::type codec,
  int compression_level,
  std::vector<int64_t> &row_groups) :
    ParquetOutFile(filename, codec, compression_level, row_groups) {
}

RParquetOutFile::RParquetOutFile(
  std::ostream &stream,
  parquet::CompressionCodec::type codec,
  int compression_level,
  std::vector<int64_t> &row_groups) :
    ParquetOutFile(stream, codec, compression_level, row_groups) {
}

static bool is_time(parquet::SchemaElement &sel, double &factor) {
  factor = 1.0;
  if (sel.__isset.logicalType && sel.logicalType.__isset.TIME) {
    auto unit = sel.logicalType.TIME.unit;
    if (unit.__isset.MILLIS) {
      factor = 1000;
    } else if (unit.__isset.MICROS) {
      factor = 1000 * 1000;
    } else if (unit.__isset.NANOS) {
      factor = 1000 * 1000 * 1000;
    }
    return true;
  } else if (sel.__isset.converted_type) {
    if (sel.converted_type == parquet::ConvertedType::TIME_MILLIS) {
      factor = 1000;
      return true;
    } else if (sel.converted_type == parquet::ConvertedType::TIME_MICROS) {
      factor = 1000 * 1000;
      return true;
    }
  }
  return false;
}

void RParquetOutFile::create_dictionary(uint32_t idx, int64_t from,
                                        int64_t until,
                                        parquet::SchemaElement &sel) {
  if (!Rf_isNull(VECTOR_ELT(dicts, idx)) &&
      INTEGER(dicts_from)[idx] == from) {
    return;
  }

  SEXP col = VECTOR_ELT(df, idx);
  SEXP sfrom = PROTECT(Rf_ScalarInteger(from));
  SEXP suntil = PROTECT(Rf_ScalarInteger(until));
  SEXP d = PROTECT(nanoparquet_create_dict_idx_(col, sfrom, suntil));
  SET_VECTOR_ELT(dicts, idx, d);
  INTEGER(dicts_from)[idx] = from;
  UNPROTECT(3);
  if (write_minmax_values && Rf_length(d) == 4 &&
      is_minmax_supported[idx] && Rf_xlength(col) > 0 &&
      !Rf_isNull(VECTOR_ELT(d, 2)) && !Rf_isNull(VECTOR_ELT(d, 3))) {
    has_minmax_value[idx] = true;
    if (TYPEOF(VECTOR_ELT(d, 2)) == INTSXP) {
      min_values[idx] = std::string((const char*) INTEGER(VECTOR_ELT(d, 2)), sizeof(int32_t));
      max_values[idx] = std::string((const char*) INTEGER(VECTOR_ELT(d, 3)), sizeof(int32_t));
    } else if (TYPEOF(VECTOR_ELT(d, 2)) == REALSXP) {
      double factor;
      bool istime = is_time(sel, factor);
      if (istime) {
        if (sel.type == parquet::Type::INT32) {
          int32_t min = REAL(VECTOR_ELT(d, 2))[0] * factor;
          int32_t max = REAL(VECTOR_ELT(d, 3))[0] * factor;
          min_values[idx] = std::string((const char*) &min, sizeof(int32_t));
          max_values[idx] = std::string((const char*) &max, sizeof(int32_t));
        } else {
          int64_t min = REAL(VECTOR_ELT(d, 2))[0] * factor;
          int64_t max = REAL(VECTOR_ELT(d, 3))[0] * factor;
          min_values[idx] = std::string((const char*) &min, sizeof(int64_t));
          max_values[idx] = std::string((const char*) &max, sizeof(int64_t));
        }
      } else if (sel.type == parquet::Type::INT32) {
        int32_t min = REAL(VECTOR_ELT(d, 2))[0];
        int32_t max = REAL(VECTOR_ELT(d, 3))[0];
        min_values[idx] = std::string((const char*) &min, sizeof(int32_t));
        max_values[idx] = std::string((const char*) &max, sizeof(int32_t));
      } else if (sel.type == parquet::Type::DOUBLE) {
        min_values[idx] = std::string((const char*) REAL(VECTOR_ELT(d, 2)), sizeof(double));
        max_values[idx] = std::string((const char*) REAL(VECTOR_ELT(d, 3)), sizeof(double));
      } else if (sel.type == parquet::Type::FLOAT) {
        float min = REAL(VECTOR_ELT(d, 2))[0];
        float max = REAL(VECTOR_ELT(d, 3))[0];
        min_values[idx] = std::string((const char*) &min, sizeof(float));
        max_values[idx] = std::string((const char*) &max, sizeof(float));
      }
    }
  }
}

static const char *enc_[] = {
  "PLAIN",
  "GROUP_VAR_INT",
  "PLAIN_DICTIONARY",
  "RLE",
  "BIT_PACKED",
  "DELTA_BINARY_PACKED",
  "DELTA_LENGTH_BYTE_ARRAY",
  "DELTA_BYTE_ARRAY",
  "RLE_DICTIONARY",
  "BYTE_STREAM_SPLIT"
};

parquet::Encoding::type
RParquetOutFile::detect_encoding(uint32_t idx, parquet::SchemaElement &sel,
                                 int32_t renc) {
  if (renc == NA_INTEGER) {
    bool dict = should_use_dict_encoding(idx);
    if (dict) {
      if (sel.type == parquet::Type::BOOLEAN) {
        return parquet::Encoding::RLE;
      } else {
        return parquet::Encoding::RLE_DICTIONARY;
      }
    } else {
      return parquet::Encoding::PLAIN;
    }
  }

  if (renc >= 10) {
    Rf_error("Unknown Praquet encoding code: %d", renc);
  }

  // otherwise we need to check if the encoding is allowed and implemented
  switch (sel.type) {
  case parquet::Type::BOOLEAN:
    if (renc == parquet::Encoding::RLE_DICTIONARY ||
        renc == parquet::Encoding::BIT_PACKED) {
      Rf_errorcall(
        nanoparquet_call,
        "Unimplemented encoding for BOOLEAN column: %s", enc_[renc]
      );
    }
    if (renc != parquet::Encoding::RLE && renc != parquet::Encoding::PLAIN) {
      Rf_errorcall(
        nanoparquet_call,
        "Unsupported encoding for BOOLEAN column: %s", enc_[renc]
      );
    }
    break;
  case parquet::Type::INT32:
    if (renc == parquet::Encoding::DELTA_BINARY_PACKED ||
        renc == parquet::Encoding::BYTE_STREAM_SPLIT) {
      Rf_errorcall(
        nanoparquet_call,
        "Unimplemented encoding for INT32 column: %s", enc_[renc]
      );
    }
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      Rf_errorcall(
        nanoparquet_call,
        "Unsupported encoding for INT32 column: %s", enc_[renc]
      );
    }
    break;
  case parquet::Type::INT64:
    if (renc == parquet::Encoding::DELTA_BINARY_PACKED ||
        renc == parquet::Encoding::BYTE_STREAM_SPLIT) {
      Rf_errorcall(
        nanoparquet_call,
        "Unimplemented encoding for INT64 column: %s", enc_[renc]
      );
    }
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      Rf_errorcall(
        nanoparquet_call,
        "Unsupported encoding for INT64 column: %s", enc_[renc]
      );
    }
    break;
  case parquet::Type::INT96:
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      Rf_errorcall(
        nanoparquet_call,
        "Unsupported encoding for INT96 column: %s", enc_[renc]
      );
    }
    break;
  case parquet::Type::FLOAT:
    if (renc == parquet::Encoding::BYTE_STREAM_SPLIT) {
      Rf_errorcall(
        nanoparquet_call,
        "Unimplemented encoding for FLOAT column: %s", enc_[renc]
      );
    }
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      Rf_errorcall(
        nanoparquet_call,
        "Unsupported encoding for FLOAT column: %s", enc_[renc]
      );
    }
    break;
  case parquet::Type::DOUBLE:
    if (renc == parquet::Encoding::BYTE_STREAM_SPLIT) {
      Rf_errorcall(
        nanoparquet_call,
        "Unimplemented encoding for DOUBLE column: %s", enc_[renc]
      );
    }
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      Rf_errorcall(
        nanoparquet_call,
        "Unsupported encoding for DOUBLE column: %s", enc_[renc]
      );
    }
    break;
  case parquet::Type::BYTE_ARRAY: {
    SEXP col = VECTOR_ELT(df, idx);
    if (TYPEOF(col) == VECSXP) {
      if (renc == parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY||
          renc == parquet::Encoding::DELTA_BYTE_ARRAY ||
          renc == parquet::Encoding::RLE_DICTIONARY ||
          renc == parquet::Encoding::PLAIN_DICTIONARY) {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for list(raw) BYTE_ARRAY column: %s",
          enc_[renc]
        );
      }
      if (renc != parquet::Encoding::PLAIN) {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for list(raw) BYTE_ARRAY column: %s", enc_[renc]
        );
      }
    } else {
      if (renc == parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY||
          renc == parquet::Encoding::DELTA_BYTE_ARRAY) {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for BYTE_ARRAY column: %s",
          enc_[renc]
        );
      }
      if (renc != parquet::Encoding::RLE_DICTIONARY &&
          renc != parquet::Encoding::PLAIN_DICTIONARY &&
          renc != parquet::Encoding::PLAIN) {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for BYTE_ARRAY column: %s", enc_[renc]
        );
      }
    }
    break;
  }
  case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
    SEXP col = VECTOR_ELT(df, idx);
    if (TYPEOF(col) == VECSXP) {
      if (renc == parquet::Encoding::DELTA_BYTE_ARRAY ||
          renc == parquet::Encoding::BYTE_STREAM_SPLIT ||
          renc == parquet::Encoding::RLE_DICTIONARY ||
          renc == parquet::Encoding::PLAIN_DICTIONARY) {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for list(raw) FIXED_LEN_BYTE_ARRAY column: %s",
          enc_[renc]
        );
      }
      if (renc != parquet::Encoding::PLAIN) {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for list(raw) FIXED_LEN_BYTE_ARRAY column: %s",
          enc_[renc]
        );
      }
    } else {
      if (renc == parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY||
          renc == parquet::Encoding::DELTA_BYTE_ARRAY) {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for FIXED_LEN_BYTE_ARRAY column: %s",
          enc_[renc]
        );
      }
      if (renc != parquet::Encoding::RLE_DICTIONARY &&
          renc != parquet::Encoding::PLAIN_DICTIONARY &&
          renc != parquet::Encoding::PLAIN) {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for FIXED_LEN_BYTE_ARRAY column: %s",
          enc_[renc]
        );
      }
    }
    break;
  }
  default:
    Rf_errorcall(nanoparquet_call, "Unsupported Parquet type"); // # nocov
  }

  return (parquet::Encoding::type) renc;
}

bool RParquetOutFile::should_use_dict_encoding(uint32_t idx) {
  SEXP col = VECTOR_ELT(df, idx);
  int rtype = TYPEOF(col);
  // this has to a dictionaery
  if (rtype == INTSXP && Rf_inherits(col, "factor")) {
    return true;
  }
  if (getenv("NANOPARQUET_FORCE_PLAIN")) {
    return false;
  }
  if (getenv("NANOPARQUET_FORCE_RLE")) {
    return true;
  }
  switch (rtype) {
    case LGLSXP: {
      R_xlen_t len = Rf_length(col);
      if (len > 10000) {
        len = 10000;
      }
      SEXP l = PROTECT(Rf_ScalarInteger(len));
      SEXP rl = PROTECT(nanoparquet_avg_run_length(col, l));
      bool ans = INTEGER(rl)[0] >= 15;
      UNPROTECT(2);
      return ans;
      break;
    }
    case INTSXP:
    case REALSXP:
    case STRSXP: {
      R_xlen_t len = Rf_xlength(col);
      if (len > 10000) {
        len = 10000;
      }
      SEXP l = PROTECT(Rf_ScalarInteger(len));
      SEXP n = PROTECT(nanoparquet_create_dict(col, l));
      bool ans = INTEGER(n)[0] < INTEGER(l)[0] / 3;
      UNPROTECT(2);
      return ans;
      break;
    }
    default:
      return false;
      break;
  }
}

static const char *type_names[] = {
  "NULL",
  "a symbol",
  "a pairlist",
  "a closure",
  "an environment",
  "a promise",
  "a language object",
  "a special function",
  "a builtin function",
  "an internal character string",
  "a logical vector",
  "",
  "",
  "an integer vector",
  "a double vector",
  "a complex vector",
  "a character vector",
  "a dot-dot-dot object",
  "an \"any\" object",
  "a list",
  "an expression",
  "a byte code object",
  "an external pointer",
  "a weak reference",
  "a raw vector",
  "an S4 object"
};

void RParquetOutFile::write_row_group(uint32_t group) {
  if (write_minmax_values) {
    std::fill(min_values.begin(), min_values.end(), std::string());
    std::fill(max_values.begin(), max_values.end(), std::string());
    std::fill(has_minmax_value.begin(), has_minmax_value.end(), false);
  }
}

static bool is_decimal(parquet::SchemaElement &sel, int32_t &precision,
                       int32_t &scale) {
  if (sel.__isset.logicalType && sel.logicalType.__isset.DECIMAL) {
    precision = sel.logicalType.DECIMAL.precision;
    scale = sel.logicalType.DECIMAL.scale;
    return true;
  } else if (sel.__isset.converted_type &&
             sel.converted_type == parquet::ConvertedType::DECIMAL) {
    if (!sel.__isset.precision) {
      Rf_errorcall(
        nanoparquet_call,
        "Invalid Parquet file: precision is not set for DECIMAL converted type"
      );
    }
    if (!sel.__isset.scale) {
      Rf_errorcall(
        nanoparquet_call,
        "Invalid Parquet file: scale is not set for DECIMAL converted type"
      );
    }
    precision = sel.precision;
    scale = sel. scale;
    return true;
  } else {
    return false;
  }
}

void write_integer_int32_dec(std::ostream & file, SEXP col, uint64_t from,
                             uint64_t until, int32_t precision,
                             int32_t scale) {

  if (precision > 9) {
    Rf_errorcall(
      nanoparquet_call,
      "Internal nanoparquet error, precision to high for INT32 DECIMAL"
    );
  }
  int32_t fact = pow(10, scale);
  int32_t max = ((int32_t)pow(10, precision)) / fact;
  int32_t min = -max;
  for (uint64_t i = from; i < until; i++) {
    int32_t val = INTEGER(col)[i];
    if (val == NA_INTEGER) continue;
    if (val <= min) {
      Rf_errorcall(
        nanoparquet_call,
        "Value too small for INT32 DECIMAL with precision %d, scale %d: %d",
        precision, scale, val);
    }
    if (val >= max) {
      Rf_errorcall(
        nanoparquet_call,
        "Value too large for INT32 DECIMAL with precision %d, scale %d: %d",
        precision, scale, val);
    }
    val *= fact;
    file.write((const char *)&val, sizeof(int32_t));
  }
}

#define GRAB_MIN(idx, t) ((t*) min_values[idx].data())
#define GRAB_MAX(idx, t) ((t*) max_values[idx].data())
#define SAVE_MIN(idx, val, t) do {                               \
  min_values[idx] = std::string((const char*) &val, sizeof(t));  \
  min_value = (t*) min_values[idx].data(); } while (0)
#define SAVE_MAX(idx, val, t) do {                               \
  max_values[idx] = std::string((const char*) &val, sizeof(t));  \
  max_value = (t*) max_values[idx].data(); } while (0)

void RParquetOutFile::write_integer_int32(std::ostream &file, SEXP col,
                                          uint32_t idx,
                                          uint64_t from, uint64_t until,
                                          parquet::SchemaElement &sel) {
  bool is_signed = TRUE;
  int bit_width = 32;
  if (sel.__isset.logicalType && sel.logicalType.__isset.INTEGER) {
    is_signed = sel.logicalType.INTEGER.isSigned;
    bit_width = sel.logicalType.INTEGER.bitWidth;
  }

  bool minmax = write_minmax_values && is_minmax_supported[idx];
  int32_t *min_value = 0, *max_value = 0;
  if (minmax && has_minmax_value[idx]) {
    min_value = GRAB_MIN(idx, int32_t);
    max_value = GRAB_MAX(idx, int32_t);
  }

  if (bit_width == 32) {
    if (!minmax &&
        sel.repetition_type == parquet::FieldRepetitionType::REQUIRED) {
      uint64_t len = until - from;
      file.write((const char *) (INTEGER(col) + from), sizeof(int) * len);
    } else {
      for (uint64_t i = from; i < until; i++) {
        int32_t val = INTEGER(col)[i];
        if (val == NA_INTEGER) continue;
        if (minmax && (min_value == 0 || val < *min_value)) {
          SAVE_MIN(idx, val, int32_t);
        }
        if (minmax && (max_value == 0 || val > *max_value)) {
          SAVE_MAX(idx, val, int32_t);
        }
        file.write((const char*) &val, sizeof(int32_t));
      }
    }
  } else {
    int min, max;
    if (bit_width == 8) {
      max = is_signed ? 127 : 255;
    } else if (bit_width == 16) {
      max = is_signed ? 256 * 128 - 1 : 256 * 256 - 1;
    } else {
      Rf_errorcall(
        nanoparquet_call,
        "Invalid bit width for INT32: %d", bit_width
      );
    }
    min = is_signed ? -max-1 : 0;
    for (uint64_t i = from; i < until; i++) {
      int32_t val = INTEGER(col)[i];
      if (val == NA_INTEGER) continue;
      const char *w = val < min ? "small" : (val > max ? "large" : "");
      if (w[0]) {
        Rf_errorcall(
          nanoparquet_call,
          "Integer value too %s for %sINT with bit width %d: %d"
          " at column %u, row %" PRIu64 ":",
          w, (is_signed ? "" : "U"), bit_width, val, idx + 1, i + 1
        );
      }
      if (minmax && (min_value == 0 || val < *min_value)) {
        SAVE_MIN(idx, val, int32_t);
      }
      if (minmax && (max_value == 0 || val > *max_value)) {
        SAVE_MAX(idx, val, int32_t);
      }
      file.write((const char *) &val, sizeof(int32_t));
    }
  }
  has_minmax_value[idx] = has_minmax_value[idx] || min_value != 0;
}

void write_double_int32_dec(std::ostream &file, SEXP col, uint64_t from,
                            uint64_t until, int32_t precision,
                            int32_t scale) {

  if (precision > 9) {
    Rf_errorcall(
      nanoparquet_call,
      "Internal nanoparquet error, precision to high for INT32 DECIMAL"
    );
  }
  int32_t fact = pow(10, scale);
  double max = (pow(10, precision)) / fact;
  double min = -max;
  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    if (val <= min) {
      Rf_errorcall(
        nanoparquet_call,
        "Value too small for INT32 DECIMAL with precision %d, scale %d: %f",
        precision, scale, round(val * fact) / fact);
    }
    if (val >= max) {
      Rf_errorcall(
        nanoparquet_call,
        "Value too large for INT32 DECIMAL with precision %d, scale %d: %f",
        precision, scale, round(val * fact)/ fact);
    }
    int32_t ival = val * fact;
    file.write((const char *)&ival, sizeof(int32_t));
  }
}

void RParquetOutFile::write_double_int32_time(std::ostream &file, SEXP col,
                                              uint32_t idx, uint64_t from,
                                              uint64_t until,
                                              parquet::SchemaElement &sel,
                                              double factor) {
  int32_t *min_value = 0, *max_value = 0;
  bool minmax = write_minmax_values && is_minmax_supported[idx];
  if (minmax && has_minmax_value[idx]) {
    min_value = GRAB_MIN(idx, int32_t);
    max_value = GRAB_MAX(idx, int32_t);
  }

  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    int32_t ival = val * factor;
    if (minmax && (min_value == 0 || ival < *min_value)) {
      SAVE_MIN(idx, ival, int32_t);
    }
    if (minmax && (max_value == 0 || ival > *max_value)) {
      SAVE_MAX(idx, ival, int32_t);
    }
    file.write((const char *)&ival, sizeof(int32_t));
  }
  has_minmax_value[idx] = has_minmax_value[idx] || min_value != 0;
}

void RParquetOutFile::write_double_int32(std::ostream &file, SEXP col,
                                         uint32_t idx, uint64_t from,
                                         uint64_t until,
                                         parquet::SchemaElement &sel) {
  bool is_signed = TRUE;
  int bit_width = 32;
  if (sel.__isset.logicalType && sel.logicalType.__isset.INTEGER) {
    is_signed = sel.logicalType.INTEGER.isSigned;
    bit_width = sel.logicalType.INTEGER.bitWidth;
  }
  if (is_signed) {
    int32_t *min_value = 0, *max_value = 0;
    bool minmax = write_minmax_values && is_minmax_supported[idx];
    if (minmax && has_minmax_value[idx]) {
      min_value = GRAB_MIN(idx, int32_t);
      max_value = GRAB_MAX(idx, int32_t);
    }

    int32_t min, max;
    switch (bit_width) {
    case 8:
      max = 0x7f;
      break;
    case 16:
      max = 0x7fff;
      break;
    case 32:
      max = 0x7fffffff;
      break;
    default:
      Rf_errorcall(nanoparquet_call, "Invalid bit width for INT32: %d", bit_width);
    }
    min = -max - 1;
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      const char *w = val < min ? "small" : (val > max ? "large" : "");
      if (w[0]) {
        Rf_errorcall(
          nanoparquet_call,
          "Integer value too %s for INT with bit width %d: %f"
          " at column %u, row %" PRIu64 ":",
          w, bit_width, val, idx + 1, i + 1
        );
      }
      int32_t ival = val;
      if (minmax && (min_value == 0 || ival < *min_value)) {
        SAVE_MIN(idx, ival, int32_t);
      }
      if (minmax && (max_value == 0 || ival > *max_value)) {
        SAVE_MAX(idx, ival, int32_t);
      }
      file.write((const char *)&ival, sizeof(int32_t));
    }
    has_minmax_value[idx] = has_minmax_value[idx] || min_value != 0;
  } else {
    uint32_t *min_value = 0, *max_value = 0;
    bool minmax = write_minmax_values && is_minmax_supported[idx];
    if (minmax && has_minmax_value[idx]) {
      min_value = GRAB_MIN(idx, uint32_t);
      max_value = GRAB_MAX(idx, uint32_t);
    }

    uint32_t max;
    switch (bit_width) {
    case 8:
      max = 0xff;
      break;
    case 16:
      max = 0xffff;
      break;
    case 32:
      max = 0xffffffff;
      break;
    default:
      Rf_errorcall(
        nanoparquet_call,
        "Invalid bit width for INT32: %d", bit_width
      );
    }
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      if (val > max) {
        Rf_errorcall(
          nanoparquet_call,
          "Integer value too large for INT with bit width %d: %f"
          " at column %u, row %" PRIu64 ".",
          bit_width, val, idx + 1, i + 1
        );
      }
      if (val < 0 ) {
        Rf_errorcall(
          nanoparquet_call,
          "Negative values are not allowed in unsigned INT column:"
          "%f at column %u, row %"  PRIu64 ".",
          val, idx + 1, i + 1
        );
      }
      uint32_t uival = val;
      if (minmax && (min_value == 0 || uival < *min_value)) {
        SAVE_MIN(idx, uival, uint32_t);
      }
      if (minmax && (max_value == 0 || uival > *max_value)) {
        SAVE_MAX(idx, uival, uint32_t);
      }
      file.write((const char *)&uival, sizeof(uint32_t));
    }
    has_minmax_value[idx] = has_minmax_value[idx] || min_value != 0;
  }
}

void RParquetOutFile::write_int32(std::ostream &file, uint32_t idx,
                                  uint32_t group, uint32_t page,
                                  uint64_t from, uint64_t until,
                                  parquet::SchemaElement &sel) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                          // # nocov
      nanoparquet_call,                                    // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  int32_t precision, scale;
  bool isdec = is_decimal(sel, precision, scale);
  double factor = 1;
  bool istime = is_time(sel, factor);
  switch (TYPEOF(col)) {
  case INTSXP:
    if (isdec) {
      write_integer_int32_dec(file, col, from, until, precision, scale);
    } else {
      write_integer_int32(file, col, idx, from, until, sel);
    }
    break;
  case REALSXP:
    if (isdec) {
      write_double_int32_dec(file, col, from, until, precision, scale);
    } else if (istime) {
      write_double_int32_time(file, col, idx, from, until, sel, factor);
    } else {
      write_double_int32(file, col, idx, from, until, sel);
    }
    break;
  default:
    Rf_errorcall(
      nanoparquet_call,
      "Cannot write %s as a Parquet INT32 type.",
      type_names[TYPEOF(col)]
    );
  }
}

void write_integer_int64_dec(std::ostream &file, SEXP col, uint64_t from,
                             uint64_t until, int32_t precision,
                             int32_t scale) {
  if (precision > 18) {
    Rf_errorcall(
      nanoparquet_call,
      "Internal nanoparquet error, precision to high for INT64 DECIMAL"
    );
  }
  int64_t fact = pow(10, scale);
  int64_t max = ((int64_t)pow(10, precision)) / fact;
  int64_t min = -max;
  for (uint64_t i = from; i < until; i++) {
    int32_t val = INTEGER(col)[i];
    if (val == NA_INTEGER) continue;
    int64_t ival = val;
    if (ival <= min) {
      Rf_errorcall(
        nanoparquet_call,
        "Value too small for INT64 DECIMAL with precision %d, scale "
        "%d: %" PRId64,
        precision, scale, ival
      );
    }
    if (ival >= max) {
      Rf_errorcall(
        nanoparquet_call,
        "Value too large for INT64 DECIMAL with precision %d, scale "
        "%d: %" PRId64,
        precision, scale, ival
      );
    }
    ival *= fact;
    file.write((const char *)&ival, sizeof(int64_t));
  }
}

void write_integer_int64(std::ostream &file, SEXP col, uint64_t from,
                         uint64_t until) {

  for (uint64_t i = from; i < until; i++) {
    int32_t val = INTEGER(col)[i];
    if (val == NA_INTEGER) continue;
    int64_t el = val;
    file.write((const char*) &el, sizeof(int64_t));
   }
 }

 void write_double_int64_dec(std::ostream &file, SEXP col, uint64_t from,
                             uint64_t until, int32_t precision,
                             int32_t scale) {
  if (precision > 18) {
    Rf_errorcall(
      nanoparquet_call,
      "Internal nanoparquet error, precision to high for INT64 DECIMAL"
    );
  }
  int64_t fact = pow(10, scale);
  double max = (pow(10, precision)) / fact;
  double min = -max;
  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    if (val <= min) {
      Rf_errorcall(
        nanoparquet_call,
        "Value too small for INT64 DECIMAL with precision %d, scale %d: %f",
        precision, scale, round(val * fact) / fact);
    }
    if (val >= max) {
      Rf_errorcall(
        nanoparquet_call,
        "Value too large for INT64 DECIMAL with precision %d, scale %d: %f",
        precision, scale, round(val * fact)/ fact);
    }
    int64_t ival = val * fact;
    file.write((const char *)&ival, sizeof(int64_t));
  }
}

void write_double_int64_time(std::ostream &file, SEXP col, uint32_t idx,
                             uint64_t from, uint64_t until,
                             parquet::SchemaElement &sel, double factor) {
  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    int64_t ival = val * factor;
    file.write((const char *)&ival, sizeof(int64_t));
  }
}

void write_double_int64(std::ostream &file, SEXP col, uint32_t idx,
                        uint64_t from, uint64_t until,
                        parquet::SchemaElement &sel) {
  if (Rf_inherits(col, "POSIXct")) {
    int64_t fact = 1;
    if (sel.__isset.logicalType && sel.logicalType.__isset.TIMESTAMP) {
      auto &unit = sel.logicalType.TIMESTAMP.unit;
      if (unit.__isset.MILLIS) {
        fact = 1000;
      } else if (unit.__isset.MICROS) {
        fact = 1000 * 1000;
      } else if (unit.__isset.NANOS) {
        fact = 1000 * 1000 * 1000;
      }
    } else if (sel.__isset.converted_type) {
      if (sel.converted_type == parquet::ConvertedType::TIMESTAMP_MILLIS) {
        fact = 1000;
      } else if (sel.converted_type == parquet::ConvertedType::TIMESTAMP_MICROS) {
        fact = 1000 * 1000;
      }
    }
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      int64_t el = val * fact;
      file.write((const char *)&el, sizeof(int64_t));
    }
  } else if (Rf_inherits(col, "difftime")) {
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      int64_t el = val * 1000 * 1000 * 1000;
      file.write((const char *)&el, sizeof(int64_t));
    }
  } else {
    bool is_signed = TRUE;
    int bit_width = 64;
    if (sel.__isset.logicalType && sel.logicalType.__isset.INTEGER) {
      is_signed = sel.logicalType.INTEGER.isSigned;
      bit_width = sel.logicalType.INTEGER.bitWidth;
    }
    if (bit_width != 64) {
      Rf_errorcall(
        nanoparquet_call,
        "Invalid bit width for INT64 INT type: %d", bit_width
      );
    }
    if (is_signed) {
      double min = -pow(2, 63), max = -(min+1);
      for (uint64_t i = from; i < until; i++) {
        double val = REAL(col)[i];
        if (R_IsNA(val)) continue;
        const char *w = val < min ? "small" : (val > max ? "large" : "");
        if (w[0]) {
          Rf_errorcall(
            nanoparquet_call,
            "Integer value too %s for %sINT with bit width %d: %f"
            " at column %u, row %" PRIu64 ".",
            w, (is_signed ? "" : "U"), bit_width, val, idx + 1, i + 1
          );
        }
        int64_t el = val;
        file.write((const char *)&el, sizeof(int64_t));
      }
    } else {
      double max = pow(2, 64) - 1;
      for (uint64_t i = from; i < until; i++) {
        double val = REAL(col)[i];
        if (R_IsNA(val)) continue;
        if (val > max) {
          Rf_errorcall(
            nanoparquet_call,
            "Integer value too large for unsigned INT with bit width %d: %f"
            " at column %u, row %" PRIu64 ".",
            bit_width, val, idx + 1, i + 1
          );
        }
        if (val < 0) {
          Rf_errorcall(
            nanoparquet_call,
            "Negative values are not allowed in unsigned INT column:"
            "%f at column %u, row %"  PRIu64 ".",
            val, idx + 1, i + 1
          );
        }
        uint64_t el = val;
        file.write((const char *)&el, sizeof(uint64_t));
      }
    }
  }
}

void RParquetOutFile::write_int64(std::ostream &file, uint32_t idx,
                                  uint32_t group, uint32_t page,
                                  uint64_t from, uint64_t until,
                                  parquet::SchemaElement &sel) {
  // This is double in R, so we need to convert
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                          // # nocov
      nanoparquet_call,                                    // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  int32_t precision, scale;
  bool isdec = is_decimal(sel, precision, scale);
  double factor;
  bool istime = is_time(sel, factor);
  switch (TYPEOF(col)) {
  case INTSXP:
    if (isdec) {
      write_integer_int64_dec(file, col, from, until, precision, scale);
    } else {
      write_integer_int64(file, col, from, until);
    }
    break;
  case REALSXP:
    if (isdec) {
      write_double_int64_dec(file, col, from, until, precision, scale);
    } else if (istime) {
      write_double_int64_time(file, col, idx, from, until, sel, factor);
    } else {
      write_double_int64(file, col, idx, from, until, sel);
    }
    break;
  default:
    Rf_errorcall(
      nanoparquet_call,
      "Cannot write %s as a Parquet INT64 type.",
      type_names[TYPEOF(col)]
    );
  }
}

void RParquetOutFile::write_int96(std::ostream &file, uint32_t idx,
                                  uint32_t group, uint32_t page,
                                  uint64_t from, uint64_t until,
                                  parquet::SchemaElement &sel) {
  // This is double in R, so we need to convert
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                         // # nocov
      nanoparquet_call,                                   // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  switch (TYPEOF(col)) {
  case INTSXP: {
    for (uint64_t i = from; i < until; i++) {
      int32_t val = INTEGER(col)[i];
      if (val == NA_INTEGER) continue;
      Int96 el = int32_to_int96(val);
      file.write((const char*) &el, sizeof(Int96));
    }
    break;
  }
  case REALSXP: {
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      Int96 el = double_to_int96(val);
      file.write((const char*) &el, sizeof(Int96));
    }
    break;
  }
  default:
    Rf_errorcall(
      nanoparquet_call,
      "Cannot write %s as a Parquet INT96 type.",
      type_names[TYPEOF(col)]
    );
  }
}

void RParquetOutFile::write_float(std::ostream &file, uint32_t idx,
                                  uint32_t group, uint32_t page,
                                  uint64_t from, uint64_t until,
                                  parquet::SchemaElement &sel) {
  SEXP col = VECTOR_ELT(df, idx);
  if (TYPEOF(col) != REALSXP) {
    Rf_errorcall(
      nanoparquet_call,
      "Cannot write %s as a Parquet FLOAT type.",
      type_names[TYPEOF(col)]
    );
  }
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                         // # nocov
      nanoparquet_call,                                   // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }

  bool minmax = write_minmax_values && is_minmax_supported[idx];
  float *min_value = 0, *max_value = 0;
  if (minmax && has_minmax_value[idx]) {
    min_value = GRAB_MIN(idx, float);
    max_value = GRAB_MAX(idx, float);
  }

  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    float el = val;
    if (minmax && (min_value == 0 || el< *min_value)) {
      SAVE_MIN(idx, el, float);
    }
    if (minmax && (max_value == 0 || el > *max_value)) {
      SAVE_MAX(idx, el, float);
    }
    file.write((const char*) &el, sizeof(float));
  }
  has_minmax_value[idx] = has_minmax_value[idx] || min_value != 0;
}

void RParquetOutFile::write_double(std::ostream &file, uint32_t idx,
                                   uint32_t group, uint32_t page,
                                   uint64_t from, uint64_t until,
                                   parquet::SchemaElement &sel) {
  SEXP col = VECTOR_ELT(df, idx);
  if (TYPEOF(col) != REALSXP) {
    Rf_errorcall(
      nanoparquet_call,
      "Cannot write %s as a Parquet DOUBLE type.",
      type_names[TYPEOF(col)]
    );
  }
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                         // # nocov
      nanoparquet_call,                                   // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }

  bool minmax = write_minmax_values && is_minmax_supported[idx];
  double *min_value = 0, *max_value = 0;
  if (minmax && has_minmax_value[idx]) {
    min_value = GRAB_MIN(idx, double);
    max_value = GRAB_MAX(idx, double);
  }

  if (!minmax &&
      sel.repetition_type == parquet::FieldRepetitionType::REQUIRED) {
    uint64_t len = until - from;
    file.write((const char *) (REAL(col) + from), sizeof(double) * len);
  } else {
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      if (minmax && (min_value == 0 || val < *min_value)) {
        SAVE_MIN(idx, val, double);
      }
      if (minmax && (max_value == 0 || val > *max_value)) {
        SAVE_MAX(idx, val, double);
      }
      file.write((const char*) &val, sizeof(double));
    }
    has_minmax_value[idx] = has_minmax_value[idx] || min_value != 0;
  }
}

void RParquetOutFile::write_byte_array(std::ostream &file, uint32_t idx,
                                       uint32_t group, uint32_t page,
                                       uint64_t from, uint64_t until,
                                       parquet::SchemaElement &sel) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                         // # nocov
      nanoparquet_call,                                   // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }

  switch (TYPEOF(col)) {
  case STRSXP: {
    for (uint64_t i = from; i < until; i++) {
      SEXP el = STRING_ELT(col, i);
      if (el == NA_STRING) {
        continue;
      }
      const char *c = CHAR(el);
      uint32_t len1 = strlen(c);
      file.write((const char *)&len1, 4);
      file.write(c, len1);
    }
    break;
  }
  case VECSXP: {
    for (uint64_t i = from; i < until; i++) {
      SEXP el = VECTOR_ELT(col, i);
      if (Rf_isNull(el)) {
        continue;
      }
      if (TYPEOF(el) != RAWSXP) {
        Rf_errorcall(                                                       // # nocov
          nanoparquet_call,                                                 // # nocov
          "Cannot write %s as a Parquet BYTE_ARRAY element when writing a " // # nocov
          "list column of RAW vectors.", type_names[TYPEOF(el)]             // # nocov
        );
      }
      uint32_t len1 = Rf_xlength(el);
      file.write((const char*) &len1, sizeof(uint32_t));
      file.write((const char*) RAW(el), len1);
    }
    break;
  }
  default:
    Rf_errorcall(                                       // # nocov
      nanoparquet_call,                                 // # nocov
      "Cannot write %s as a Parquet BYTE_ARRAY type.",  // # nocov
      type_names[TYPEOF(col)]                           // # nocov
    );
    break;
  }
}

uint32_t RParquetOutFile::get_size_byte_array(
  uint32_t idx,
  uint32_t num_present,
  uint64_t from,
  uint64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                          // # nocov
      nanoparquet_call,                                    // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  uint32_t size = 0;
  switch (TYPEOF(col)) {
  case STRSXP: {
    for (uint64_t i = from; i < until; i++) {
      SEXP csxp = STRING_ELT(col, i);
      if (csxp != NA_STRING) {
        const char *c = CHAR(csxp);
        size += strlen(c) + 4;
      }
    }
    break;
  }
  case VECSXP: {
    for (uint64_t i = from; i < until; i++) {
      SEXP el = VECTOR_ELT(col, i);
      if (Rf_isNull(el)) {
        continue;
      }
      if (TYPEOF(el) != RAWSXP) {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot write %s as a Parquet BYTE_ARRAY element when writing a "
          "list column of RAW vectors.", type_names[TYPEOF(el)]
        );
      }
      size += Rf_xlength(el) + 4;
    }
    break;
  }
  default:
    Rf_errorcall(
      nanoparquet_call,
      "Cannot write %s as a Parquet BYTE_ARRAY type.",
      type_names[TYPEOF(col)]
    );
  }

  return size;
}

static bool parse_uuid(const char *c, char *u, char *t) {
  if (strlen(c) != 36) {
    return false;
  }

  uint32_t *p1 = (uint32_t*) t;
  uint16_t *p2 = (uint16_t*) (t + 4);
  uint16_t *p3 = (uint16_t*) (t + 6);
  uint16_t *p4 = (uint16_t*) (t + 8);
  uint64_t *p5 = (uint64_t*) (t + 10);

  *p1 = std::strtoul(c, NULL, 16);
  *p2 = std::strtoul(c + 9, NULL, 16);
  *p3 = std::strtoul(c + 14, NULL, 16);
  *p4 = std::strtoul(c + 19, NULL, 16);
  *p5 = std::strtoull(c + 24, NULL, 16);

  // TODO: fix for big endian, this is little endian swap
  u[0] = t[3]; u[1] = t[2]; u[2] = t[1]; u[3] = t[0];
  u[4] = t[5]; u[5] = t[4];
  u[6] = t[7]; u[7] = t[6];
  u[8] = t[9]; u[9] = t[8];
  u[10] = t[15]; u[11] = t[14];
  u[12] = t[13]; u[13] = t[12];
  u[14] = t[11]; u[15] = t[10];

  return true;
}

void RParquetOutFile::write_fixed_len_byte_array(
  std::ostream &file,
  uint32_t idx,
  uint32_t group, uint32_t page,
  uint64_t from, uint64_t until,
  parquet::SchemaElement &sel) {

  uint32_t type_length = sel.type_length;
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                          // # nocov
      nanoparquet_call,                                    // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  bool is_uuid = sel.__isset.logicalType && sel.logicalType.__isset.UUID;
  bool is_f16 = sel.__isset.logicalType && sel.logicalType.__isset.FLOAT16;
  if (is_uuid) {
    if (TYPEOF(col) != STRSXP) {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet UUID (FIXED_LEN_BYTE_ARRAY) type.",
        type_names[TYPEOF(col)]
      );
    }
    char u[16], tmp[18];  // need to be longer, for easier conversion
    for (uint64_t i = from; i < until; i++) {
      SEXP s = STRING_ELT(col, i);
      if (s == NA_STRING) continue;
      const char *c = CHAR(s);
      if (!parse_uuid(c, u, tmp)) {
        Rf_errorcall(
          nanoparquet_call,
          "Invalid UUID value in column %d, row %" PRIu64,
          idx + 1, i + 1
        );
      }
      file.write(u, 16);
    }

  } else if (is_f16) {
    if (TYPEOF(col) != REALSXP) {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet FLOAT16 type.",
        type_names[TYPEOF(col)]
      );
    }
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      uint16_t f16val = double_to_float16(val);
      file.write((const char*) &f16val, sizeof(uint16_t));
    }

  } else {
    switch (TYPEOF(col)) {
    case STRSXP: {
      for (uint64_t i = from; i < until; i++) {
        SEXP s = STRING_ELT(col, i);
        if (s == NA_STRING) {
          continue;
        }
        const char *c = CHAR(s);
        uint32_t len1 = strlen(c);
        if (len1 != type_length) {
          Rf_errorcall(
            nanoparquet_call,
            "Invalid string length: %d, expenting %d for "
            "FIXED_LEN_TYPE_ARRAY", len1, type_length
          );
        }
        file.write(c, type_length);
      }
      break;
    }
    case VECSXP: {
      for (uint64_t i = from; i < until; i++) {
        SEXP el = VECTOR_ELT(col, i);
        if (Rf_isNull(el)) {
          continue;
        }
        if (TYPEOF(el) != RAWSXP) {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot write %s as a Parquet BYTE_ARRAY element when writing a "
            "list column of RAW vectors.", type_names[TYPEOF(el)]
          );
        }
        uint32_t len1 = Rf_xlength(el);
        if (len1 != type_length) {
          Rf_errorcall(
            nanoparquet_call,
            "Invalid string length: %d, expenting %d for "
            "FIXED_LEN_TYPE_ARRAY", len1, type_length
          );
        }
        file.write((const char *)RAW(el), len1);
      }
      break;
    }
    default:
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet FIXED_LEN_BYTE_ARRAY type.",
        type_names[TYPEOF(col)]
      );
    }
  }
}

void write_boolean_impl(std::ostream &file, SEXP col,
                        uint64_t from, uint64_t until) {
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                          // # nocov
      nanoparquet_call,                                    // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  uint64_t len = until - from;
  int *p = LOGICAL(col) + from;
  int *end = p + len;
  while (p + 8 < end) {
    char b = 0;
    b += (*p++);
    b += (*p++) << 1;
    b += (*p++) << 2;
    b += (*p++) << 3;
    b += (*p++) << 4;
    b += (*p++) << 5;
    b += (*p++) << 6;
    b += (*p++) << 7;
    file.write(&b, 1);
  }
  if (p < end) {
    char b = 0;
    while (end > p) {
      b = (b << 1) + (*--end);
    }
    file.write(&b, 1);
  }
}

void RParquetOutFile::write_boolean(std::ostream &file, uint32_t idx,
                                    uint32_t group, uint32_t page,
                                    uint64_t from, uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (TYPEOF(col) != LGLSXP) {
    Rf_errorcall(
      nanoparquet_call,
      "Cannot write %s as a Parquet BOOLEAN type.",
      type_names[TYPEOF(col)]
    );
  }
  write_boolean_impl(file, col, from, until);
}

void RParquetOutFile::write_boolean_as_int(std::ostream &file,
                                           uint32_t idx,
                                           uint32_t group,
                                           uint32_t page,
                                           uint64_t from,
                                           uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                          // # nocov
      nanoparquet_call,                                    // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  uint64_t len = until - from;
  file.write((const char *) (LOGICAL(col) + from), sizeof(int) * len);
}

uint32_t RParquetOutFile:: write_present(std::ostream &file, uint32_t idx,
                                         uint64_t from, uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                         // # nocov
      nanoparquet_call,                                   // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  uint64_t len = until - from;
  uint64_t num_pres = 0;
  present.reset(len * sizeof(int));
  int *presptr = (int*) present.ptr;
  switch(TYPEOF(col)) {
  case INTSXP: {
    int *intptr = INTEGER(col) + from;
    int *end = intptr + len;
    for (; intptr < end; intptr++, presptr++) {
      *presptr = (*intptr != NA_INTEGER);
      num_pres += *presptr;
    }
    break;
  }
  case REALSXP: {
    double *dblptr = REAL(col) + from;
    double *end = dblptr + len;
    for (; dblptr < end; dblptr++, presptr++) {
      *presptr = ! R_IsNA(*dblptr);
      num_pres += *presptr;
    }
    break;
  }
  case STRSXP: {
    for (auto i = from; i < until; i++, presptr++) {
      *presptr = (STRING_ELT(col, i) != R_NaString);
      num_pres += *presptr;
    }
    break;
  }
  case LGLSXP: {
    int *lglptr = LOGICAL(col) + from;
    int *end = lglptr + len;
    for (; lglptr < end; lglptr++, presptr++) {
      *presptr = (*lglptr != NA_LOGICAL);
      num_pres += *presptr;
    }
    break;
  }
  case VECSXP: {
    for (auto i = from; i < until; i++, presptr++) {
      *presptr = ! Rf_isNull(VECTOR_ELT(col, i));
      num_pres += *presptr;
    }
    break;
  }
  default:
    Rf_errorcall(nanoparquet_call, "Uninmplemented R type");   // # nocov
  }

  file.write(present.ptr, len * sizeof(int));

  return num_pres;
}

void RParquetOutFile::write_present_boolean_as_int(std::ostream &file,
                                                   uint32_t idx,
                                                   uint32_t num_present,
                                                   uint64_t from,
                                                   uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (TYPEOF(col) != LGLSXP) {
    Rf_errorcall(                                         // # nocov
      nanoparquet_call,                                   // # nocov
      "Cannot write %s as a Parquet BOOLEAN type.",       // # nocov
      type_names[TYPEOF(col)]                             // # nocov
    );
  }
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                         // # nocov
      nanoparquet_call,                                   // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  for (uint64_t i = from; i < until; i++) {
    int el = LOGICAL(col)[i];
    if (el != NA_LOGICAL) {
      file.write((const char*) &el, sizeof(int));
    }
  }
}

void RParquetOutFile::write_present_boolean(
  std::ostream &file,
  uint32_t idx,
  uint32_t num_present,
  uint64_t from,
  uint64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  if (TYPEOF(col) != LGLSXP) {
    Rf_errorcall(
      nanoparquet_call,
      "Cannot write %s as a Parquet BOOLEAN type.", type_names[TYPEOF(col)]
    );
  }
  SEXP col2 = PROTECT(Rf_allocVector(LGLSXP, num_present));
  uint64_t i, o;
  if (until > Rf_xlength(col)) {
    Rf_errorcall(                                         // # nocov
      nanoparquet_call,                                   // # nocov
      "Internal nanoparquet error, row index too large"
    );
  }
  for (i = from, o = 0; i < until; i++) {
    if (LOGICAL(col)[i] != NA_LOGICAL) {
      LOGICAL(col2)[o++] = LOGICAL(col)[i];
    }
  }
  write_boolean_impl(file, col2, 0, num_present);

  UNPROTECT(1);
}

uint32_t RParquetOutFile::get_num_values_dictionary(
    uint32_t idx,
    parquet::SchemaElement &sel,
    int64_t from,
    int64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (Rf_inherits(col, "factor")) {
    return Rf_nlevels(col);
  } else {
    create_dictionary(idx, from, until, sel);
    return Rf_length(VECTOR_ELT(VECTOR_ELT(dicts, idx), 0));
  }
}

uint32_t RParquetOutFile::get_size_dictionary(
  uint32_t idx,
  parquet::SchemaElement &sel,
  int64_t from,
  int64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  parquet::Type::type type = sel.type;

  switch (TYPEOF(col)) {
  case INTSXP: {
    if (Rf_inherits(col, "factor")) {
      SEXP levels = PROTECT(Rf_getAttrib(col, R_LevelsSymbol));
      R_xlen_t len = Rf_xlength(levels);
      uint32_t size = len * 4; // 4 bytes of length for each CHARSXP
      for (R_xlen_t i = 0; i < len; i++) {
        const char *c = CHAR(STRING_ELT(levels, i));
        size += strlen(c);
      }
      UNPROTECT(1);
      return size;
    } else {
      create_dictionary(idx, from, until, sel);
      SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
      if (type == parquet::Type::INT32) {
        return Rf_xlength(dictidx) * sizeof(int);
      } else if (type == parquet::Type::INT64) {
        return Rf_xlength(dictidx) * sizeof(int64_t);
      } else if (type == parquet::Type::INT96) {
        return Rf_xlength(dictidx) * sizeof(Int96);
      } else {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot convert an integer vector to Parquet type %s.",
         parquet::_Type_VALUES_TO_NAMES.at(type)
        );
      }
    }
    break;
  }
  case REALSXP: {
    create_dictionary(idx, from, until, sel);
    SEXP dict = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    if (type == parquet::Type::DOUBLE) {
      return Rf_xlength(dict) * sizeof(double);
    } else if (type == parquet::Type::INT32) {
      return Rf_xlength(dict) * sizeof(int32_t);
    } else if (type == parquet::Type::INT64) {
      return Rf_xlength(dict) * sizeof(int64_t);
    } else if (type == parquet::Type::INT96) {
      return Rf_xlength(dict) * sizeof(Int96);
    } else if (type == parquet::Type::FLOAT) {
      return Rf_xlength(dict) * sizeof(float);
    } else if (type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      return Rf_xlength(dict) * sel.type_length;
    } else {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot convert a double vector to Parquet type %s.",
         parquet::_Type_VALUES_TO_NAMES.at(type)
        );
    }
    break;
  }
  case STRSXP: {
    // need to count the length of the stings that are indexed in dict
    create_dictionary(idx, from, until, sel);
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    R_xlen_t len = Rf_xlength(dictidx);
    bool is_uuid = sel.__isset.logicalType && sel.logicalType.__isset.UUID;

    if (is_uuid) {
      return len * 16;
    }

    uint32_t size = type == parquet::Type::BYTE_ARRAY ? len * 4 : 0;
    int *beg = INTEGER(dictidx);
    int *end = beg + len;
    for (; beg < end; beg++) {
      const char *c = CHAR(STRING_ELT(col, *beg + from));
      size += strlen(c);
    }
    return size;
    break;
  }
  case LGLSXP: {
    // this does not happen, no dictionaries for BOOLEAN, makes no sense
    create_dictionary(idx, from, until, sel);                // # nocov
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);    // # nocov
    R_xlen_t l = Rf_xlength(dictidx);                        // # nocov
    return l / 8 + (l % 8 > 0);                              // # nocov
    break;
  }
  default:
    throw runtime_error("Uninmplemented R type");  // # nocov
  }
}

void RParquetOutFile::write_dictionary(
    std::ostream &file,
    uint32_t idx,
    parquet::SchemaElement &sel,
    int64_t from,
    int64_t until) {

  parquet::Type::type type = sel.type;

  int32_t precision = 0, scale = 0;
  bool isdec = is_decimal(sel, precision, scale);
  SEXP col = VECTOR_ELT(df, idx);
  switch (TYPEOF(col)) {
  case INTSXP: {
    if (Rf_inherits(col, "factor")) {
      if (type != parquet::Type::BYTE_ARRAY) {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot convert a factor to Parquet type %s.",
         parquet:: _Type_VALUES_TO_NAMES.at(type)
        );
      }
      SEXP levels = PROTECT(Rf_getAttrib(col, R_LevelsSymbol));
      R_xlen_t len = XLENGTH(levels);
      for (R_xlen_t i = 0; i < len; i++) {
        const char *c = CHAR(STRING_ELT(levels, i));
        uint32_t len1 = strlen(c);
        file.write((const char *)&len1, 4);
        file.write(c, len1);
      }
      UNPROTECT(1);
    } else {
      SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
      R_xlen_t len = Rf_xlength(dictidx);
      int *icol = INTEGER(col) + from;
      int *iidx = INTEGER(dictidx);
      switch (type) {
      case parquet::Type::INT32: {
        if (isdec) {
          int32_t fact = pow(10, scale);
          int32_t max = ((int32_t)pow(10, precision)) / fact;
          int32_t min = -max;
          SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
          int32_t *idict = INTEGER(dict);
          for (auto i = 0; i < len; i++) {
            int32_t val = icol[iidx[i]];
            if (val <= min) {
              Rf_errorcall(
                nanoparquet_call,
                "Value too small for INT32 DECIMAL with precision %d, scale %d: %d",
                precision, scale, val
              );
            }
            if (val >= max) {
              Rf_errorcall(
                nanoparquet_call,
                "Value too large for INT32 DECIMAL with precision %d, scale %d: %d",
                precision, scale, val
              );
            }
            idict[i] = val * fact;
          }
          file.write((const char*) idict, sizeof(int32_t) * len);
          UNPROTECT(1);
        } else {
          SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
          int *idict = INTEGER(dict);
          for (auto i = 0; i < len; i++) {
            idict[i] = icol[iidx[i]];
          }
          file.write((const char*) idict, sizeof(int) * len);
          UNPROTECT(1);
        }
        break;
      }
      case parquet::Type::INT64: {
        if (isdec) {
          int64_t fact = pow(10, scale);
          int64_t max = ((int64_t)pow(10, precision)) / fact;
          int64_t min = -max;
          SEXP dict = PROTECT(Rf_allocVector(REALSXP, len));
          int64_t *idict = (int64_t*) REAL(dict);
          for (auto i = 0; i < len; i++) {
            int64_t val = icol[iidx[i]];
            if (val <= min) {
              Rf_errorcall(
                  nanoparquet_call,
                  "Value too small for INT64 DECIMAL with precision %d, scale "
                  "%d: %" PRId64,
                  precision, scale, val);
            }
            if (val >= max) {
              Rf_errorcall(
                  nanoparquet_call,
                  "Value too large for INT64 DECIMAL with precision %d, scale "
                  "%d: %" PRId64,
                  precision, scale, val);
            }
            idict[i] = val * fact;
          }
          file.write((const char*) idict, sizeof(int64_t) * len);
          UNPROTECT(1);
        } else {
          SEXP dict = PROTECT(Rf_allocVector(REALSXP, len));
          int64_t *idict = (int64_t*) REAL(dict);
          for (auto i = 0; i < len; i++) {
            idict[i] = icol[iidx[i]];
          }
          file.write((const char*) idict, sizeof(int64_t) * len);
          UNPROTECT(1);
        }
        break;
      }
      case parquet::Type::INT96: {
        SEXP dict = PROTECT(Rf_allocVector(INTSXP, len * 3));
        Int96 *idict = (Int96*) INTEGER(dict);
        for (auto i = 0; i < len; i++) {
          idict[i] = int32_to_int96(icol[iidx[i]]);
        }
        file.write((const char*) idict, sizeof(Int96) * len);
        UNPROTECT(1);
        break;
      }
      default:
        Rf_errorcall(                                              // # nocov
          nanoparquet_call,                                        // # nocov
          "Cannot convert an integer vector to Parquet type %s.",  // # nocov
         parquet::_Type_VALUES_TO_NAMES.at(type)                   // # nocov
        );
      }
    }
    break;
  }
  case REALSXP: {
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    R_xlen_t len = Rf_xlength(dictidx);
    double *icol = REAL(col) + from;
    int *iidx = INTEGER(dictidx);
    if (Rf_inherits(col, "POSIXct")) {
      if (type != parquet::Type::INT64) {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot convert a POSIXct vector to Parquet type %s.",
         parquet:: _Type_VALUES_TO_NAMES.at(type)
        );
      }
      int64_t fact = 1;
      if (sel.__isset.logicalType && sel.logicalType.__isset.TIMESTAMP) {
        auto &unit = sel.logicalType.TIMESTAMP.unit;
        if (unit.__isset.MILLIS) {
          fact = 1000;
        } else if (unit.__isset.MICROS) {
          fact = 1000 * 1000;
        } else if (unit.__isset.NANOS) {
          fact = 1000 * 1000 * 1000;
        }
      } else if (sel.__isset.converted_type) {
        if (sel.converted_type == parquet::ConvertedType::TIMESTAMP_MILLIS) {
          fact = 1000;
        } else if (sel.converted_type ==
                   parquet::ConvertedType::TIMESTAMP_MICROS) {
          fact = 1000 * 1000;
        }
      }
      for (auto i = 0; i < len; i++) {
        int64_t el = icol[iidx[i]] * fact;
        file.write((const char*) &el, sizeof(int64_t));
      }
    } else if (Rf_inherits(col, "hms")) {
      double fact;
      is_time(sel, fact);
      if (type == parquet::Type::INT32) {
        for (auto i = 0; i < len; i++) {
          int32_t el = icol[iidx[i]] * fact;
          file.write((const char*) &el, sizeof(int32_t));
        }
      } else if (type == parquet::Type::INT64) {
        for (auto i = 0; i < len; i++) {
          int64_t el = icol[iidx[i]] * fact;
          file.write((const char*) &el, sizeof(int64_t));
        }
      } else {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot convert an hms vector to Parquet type %s.",
         parquet:: _Type_VALUES_TO_NAMES.at(type)
        );
      }
    } else if (Rf_inherits(col, "difftime")) {
      if (type != parquet::Type::INT64) {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot convert a difftime vector to Parquet type %s.",
         parquet:: _Type_VALUES_TO_NAMES.at(type)
        );
      }
      for (auto i = 0; i < len; i++) {
        int64_t el = icol[iidx[i]] * 1000 * 1000 * 1000;
        file.write((const char*) &el, sizeof(int64_t));
      }
    } else {
      switch (type) {
      case parquet::Type::DOUBLE: {
        SEXP dict = PROTECT(Rf_allocVector(REALSXP, len));
        double *idict = REAL(dict);
        for (auto i = 0; i < len; i++) {
          idict[i] = icol[iidx[i]];
        }
        file.write((const char*) idict, sizeof(double) * len);
        UNPROTECT(1);
        break;
      }
      case parquet::Type::INT32: {
        if (isdec) {
          int32_t fact = pow(10, scale);
          double max = (pow(10, precision)) / fact;
          double min = -max;
          SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
          int32_t *idict = (int32_t*) INTEGER(dict);
          for (auto i = 0; i < len; i++) {
            double val = icol[iidx[i]];
            if (val <= min) {
              Rf_errorcall(nanoparquet_call,
                           "Value too small for INT32 DECIMAL with precision "
                           "%d, scale %d: %f",
                           precision, scale, round(val * fact) / fact);
            }
            if (val >= max) {
              Rf_errorcall(nanoparquet_call,
                           "Value too large for INT32 DECIMAL with precision "
                           "%d, scale %d: %f",
                           precision, scale, round(val * fact) / fact);
            }
            idict[i] = val * fact;
          }
          file.write((const char*) idict, sizeof(int32_t) * len);
          UNPROTECT(1);
        } else {
          bool is_signed = TRUE;
          int bit_width = 32;
          if (sel.__isset.logicalType && sel.logicalType.__isset.INTEGER) {
            is_signed = sel.logicalType.INTEGER.isSigned;
            bit_width = sel.logicalType.INTEGER.bitWidth;
          }
          if (is_signed) {
            int32_t min, max;
            switch (bit_width) {
            case 8:
              max = 0x7f;
              break;
            case 16:
              max = 0x7fff;
              break;
            case 32:
              max = 0x7fffffff;
              break;
            default:
              Rf_errorcall(nanoparquet_call, "Invalid bit width for INT32: %d",
                           bit_width);
            }
            min = -max - 1;
            SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
            int32_t *idict = (int32_t *)INTEGER(dict);
            for (auto i = 0; i < len; i++) {
              double val = icol[iidx[i]];
              const char *w = val < min ? "small" : (val > max ? "large" : "");
              if (w[0]) {
                Rf_errorcall(
                    nanoparquet_call,
                    "Integer value too %s for INT with bit width %d: %f"
                    " at column %u", w, bit_width, val, idx + 1);
              }
              idict[i] = val;
            }
            file.write((const char*) idict, sizeof(int32_t) * len);
            UNPROTECT(1);
          } else {
            uint32_t max;
            switch (bit_width) {
            case 8:
              max = 0xff;
              break;
            case 16:
              max = 0xffff;
              break;
            case 32:
              max = 0xffffffff;
              break;
            default:
              Rf_errorcall(nanoparquet_call, "Invalid bit width for INT32: %d",
                           bit_width);
            }
            SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
            int32_t *idict = (int32_t *)INTEGER(dict);
            for (auto i = 0; i < len; i++) {
              double val = icol[iidx[i]];
              if (val > max) {
                Rf_errorcall(
                    nanoparquet_call,
                    "Integer value too large for INT with bit width %d: %f"
                    " at column %u.", bit_width, val, idx + 1);
              }
              if (val < 0) {
                Rf_errorcall(
                    nanoparquet_call,
                    "Negative values are not allowed in unsigned INT column: "
                    "%f at column %u.", val, idx + 1);
              }
              idict[i] = val;
            }
            file.write((const char*) idict, sizeof(int32_t) * len);
            UNPROTECT(1);
          }
        }
        break;
      }
      case parquet::Type::INT64: {
        if (isdec) {
          int64_t fact = pow(10, scale);
          double max = (pow(10, precision)) / fact;
          double min = -max;
          SEXP dict = PROTECT(Rf_allocVector(REALSXP, len));
          int64_t *idict = (int64_t*) REAL(dict);
          for (auto i = 0; i < len; i++) {
            double val = icol[iidx[i]];
            if (val <= min) {
              Rf_errorcall(nanoparquet_call,
                           "Value too small for INT64 DECIMAL with precision "
                           "%d, scale %d: %f",
                           precision, scale, round(val * fact) / fact);
            }
            if (val >= max) {
              Rf_errorcall(nanoparquet_call,
                           "Value too large for INT64 DECIMAL with precision "
                           "%d, scale %d: %f",
                           precision, scale, round(val * fact) / fact);
            }
            idict[i] = val * fact;
          }
          file.write((const char*) idict, sizeof(int64_t) * len);
          UNPROTECT(1);
        } else {
          SEXP dict = PROTECT(Rf_allocVector(REALSXP, len));
          int64_t *idict = (int64_t*) REAL(dict);
          for (auto i = 0; i < len; i++) {
            idict[i] = icol[iidx[i]];
          }
          file.write((const char*) idict, sizeof(int64_t) * len);
          UNPROTECT(1);
        }
        break;
      }
      case parquet::Type::INT96: {
        SEXP dict = PROTECT(Rf_allocVector(INTSXP, len * 3));
        Int96 *idict = (Int96*) INTEGER(dict);
        for (auto i = 0; i < len; i++) {
          idict[i] = double_to_int96(icol[iidx[i]]);
        }
        file.write((const char*) idict, sizeof(Int96) * len);
        UNPROTECT(1);
        break;
      }
      case parquet::Type::FLOAT: {
        SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
        float *idict = (float*) INTEGER(dict);
        for (auto i = 0; i < len; i++) {
          idict[i] = icol[iidx[i]];
        }
        file.write((const char*) idict, sizeof(float) * len);
        UNPROTECT(1);
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        SEXP dict = PROTECT(Rf_allocVector(RAWSXP, len * sel.type_length));
        uint16_t *idict = (uint16_t*) RAW(dict);
        for (auto i = 0; i < len; i++) {
          idict[i] = double_to_float16(icol[iidx[i]]);
        }
        file.write((const char*) idict, sel.type_length * len);
        UNPROTECT(1);
        break;
      }
      default:
        Rf_errorcall(                                             // # nocov
          nanoparquet_call,                                       // # nocov
          "Cannot convert a double vector to Parquet type %s.",   // # nocov
          parquet:: _Type_VALUES_TO_NAMES.at(type)                // # nocov
        );
      }
    }
    break;
  }
  case STRSXP: {
    switch (type) {
    case parquet::Type::BYTE_ARRAY: {
      SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
      R_xlen_t len = Rf_xlength(dictidx);
      int *iidx = INTEGER(dictidx);
      for (uint64_t i = 0; i < len; i++) {
        const char *c = CHAR(STRING_ELT(col, from + iidx[i]));
        uint32_t len1 = strlen(c);
        file.write((const char *)&len1, 4);
        file.write(c, len1);
      }
      break;
    }
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
      bool is_uuid = sel.__isset.logicalType && sel.logicalType.__isset.UUID;
      if (is_uuid) {
        char u[16], tmp[18];  // need to be longer, for easier conversion
        SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
        R_xlen_t len = Rf_xlength(dictidx);
        int *iidx = INTEGER(dictidx);
        for (uint64_t i = 0; i < len; i++) {
          const char *c = CHAR(STRING_ELT(col, from + iidx[i]));
          if (!parse_uuid(c, u, tmp)) {
            Rf_errorcall(
              nanoparquet_call,
              "Invalid UUID value in column %d", idx + 1
            );
          }
          file.write(u, 16);
        }
      } else {
        SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
        R_xlen_t len = Rf_xlength(dictidx);
        int *iidx = INTEGER(dictidx);
        for (uint64_t i = 0; i < len; i++) {
          const char *c = CHAR(STRING_ELT(col, from + iidx[i]));
          uint32_t len1 = strlen(c);
          if (len1 != sel.type_length) {
            Rf_errorcall(
                nanoparquet_call,
                "Invalid string length in FIXED_LEN_BYTE_ARRAY column: %d, "
                "should be %d.",
                len1, sel.type_length);
          }
          file.write(c, len1);
        }
      }
      break;
    }
    default:
      Rf_errorcall(
        nanoparquet_call,
        "Cannot convert a character vector to Parquet type %s.",
       parquet:: _Type_VALUES_TO_NAMES.at(type)
      );
    }
    break;
  }
  case LGLSXP: {                                           // # nocov start
    // can Parquet have dicitonary encoded BOOLEANS? There isn't much point.
    if (type != parquet::Type::BOOLEAN) {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot convert a double vector to Parquet type %s.",
       parquet:: _Type_VALUES_TO_NAMES.at(type)
      );
    }
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    R_xlen_t len = Rf_xlength(dictidx);
    SEXP dict = PROTECT(Rf_allocVector(LGLSXP, len));
    int *icol = LOGICAL(col) + from;
    int *iidx = INTEGER(dictidx);
    int *idict = LOGICAL(dict);
    for (auto i = 0; i < len; i++) {
      idict[i] = icol[iidx[i]];
    }
    write_boolean_impl(file, dict, 0, len);
    UNPROTECT(1);
    break;
  }                                                        // # nocov end
  default:
    throw runtime_error("Uninmplemented R type");          // # nocov
  }
}

void RParquetOutFile::write_dictionary_indices(
  std::ostream &file,
  uint32_t idx,
  int64_t rg_from,
  int64_t rg_until,
  uint64_t page_from,
  uint64_t page_until) {

  // Both all of rg_* and page_* are in absolute coordinates

  SEXP col = VECTOR_ELT(df, idx);
  if (TYPEOF(col) == INTSXP && Rf_inherits(col, "factor")) {
    // there is a single dict for a factor, which is used in all row
    // groups, so we use absolute coordinates here
    for (uint64_t i = page_from; i < page_until; i++) {
      int el = INTEGER(col)[i];
      if (el != NA_INTEGER) {
        el--;
        file.write((const char *) &el, sizeof(int));
      }
    }
  } else {
    SEXP dictmap = VECTOR_ELT(VECTOR_ELT(dicts, idx), 1);
    // there is a separate dict for each row group, so we need to convert
    // the absolute page_* coordinates to relative coordinates, starting
    // at rg_from
    for (uint64_t i = page_from - rg_from; i < page_until - rg_from; i++) {
      int el = INTEGER(dictmap)[i];
      if (el != NA_INTEGER) {
        file.write((const char *) &el, sizeof(int));
      }
    }
  }
}

bool RParquetOutFile::get_group_minmax_values(uint32_t idx, uint32_t group,
                                              parquet::SchemaElement &sel,
                                              std::string &min_value,
                                              std::string &max_value) {

  if (!is_minmax_supported[idx]) {
    return false;
  } else if (!has_minmax_value[idx]) {
    // maybe all values are missing
    return false;
  } else {
    min_value = min_values[idx];
    max_value = max_values[idx];
    return true;
  }
}

void nanoparquet_map_to_parquet_type(
  SEXP x,
  SEXP options,
  parquet::SchemaElement &sel,
  std::string &rtype) {

  switch (TYPEOF(x)) {
  case INTSXP: {
    if (Rf_isFactor(x)) {
      rtype = "factor";
      parquet::StringType st;
      parquet::LogicalType lt;
      lt.__set_STRING(st);
      sel.__set_logicalType(lt);
      sel.__set_type(get_type_from_logical_type(lt));
    } else if (Rf_inherits(x, "Date")) {
      rtype = "integer";
      parquet::DateType dt;
      parquet::LogicalType lt;
      lt.__set_DATE(dt);
      sel.__set_logicalType(lt);
      sel.__set_type(get_type_from_logical_type(lt));
    } else if (Rf_inherits(x, "hms")) {
      rtype = "hms";
      parquet::TimeType tt;
      tt.__set_isAdjustedToUTC(true);
      parquet::TimeUnit tu;
      tu.__set_MILLIS(parquet::MilliSeconds());
      tt.__set_unit(tu);
      parquet::LogicalType lt;
      lt.__set_TIME(tt);
      sel.__set_logicalType(lt);
      sel.__set_type(get_type_from_logical_type(lt));
    } else {
      rtype = "integer";
      parquet::IntType it;
      it.__set_bitWidth(32);
      it.__set_isSigned(true);
      parquet::LogicalType lt;
      lt.__set_INTEGER(it);
      sel.__set_logicalType(lt);
      sel.__set_type(get_type_from_logical_type(lt));
    }
    break;
  }
  case REALSXP: {
    if (Rf_inherits(x, "POSIXct")) {
      rtype = "POSIXct";
      parquet::TimestampType tt;
      tt.__set_isAdjustedToUTC(true);
      parquet::TimeUnit tu;
      tu.__set_MICROS(parquet::MicroSeconds());
      tt.__set_unit(tu);
      parquet::LogicalType lt;
      lt.__set_TIMESTAMP(tt);
      sel.__set_logicalType(lt);
      sel.__set_type(get_type_from_logical_type(lt));

    } else if (Rf_inherits(x, "hms")) {
      rtype = "hms";
      parquet::TimeType tt;
      tt.__set_isAdjustedToUTC(true);
      parquet::TimeUnit tu;
      tu.__set_MILLIS(parquet::MilliSeconds());
      tt.__set_unit(tu);
      parquet::LogicalType lt;
      lt.__set_TIME(tt);
      sel.__set_logicalType(lt);
      sel.__set_type(get_type_from_logical_type(lt));
      sel.__set_type(parquet::Type::INT32);

    } else if (Rf_inherits(x, "difftime")) {
      rtype = "difftime";
      sel.__set_type(parquet::Type::INT64);

    } else {
      rtype = "double";
      sel.__set_type(parquet::Type::DOUBLE);
    }
    break;
  }
  case STRSXP: {
    rtype = "character";
    parquet::StringType st;
    parquet::LogicalType lt;
    lt.__set_STRING(st);
    sel.__set_logicalType(lt);
    sel.__set_type(get_type_from_logical_type(lt));
    break;
  }
  case LGLSXP: {
    rtype = "logical";
    sel.__set_type(parquet::Type::BOOLEAN);
    break;
  }
  case VECSXP: {
    // assume a list of RAW vectors, for now
    rtype = "raw";
    sel.__set_type(parquet::Type::BYTE_ARRAY);
    break;
  }
  default:
    Rf_errorcall(
      nanoparquet_call,
      "Cannot map %s to any Parquet type",
      type_names[TYPEOF(x)]
    );
  }

  fill_converted_type_for_logical_type(sel);
}

#define NUMERIC_SCALAR(x) \
  (TYPEOF(x) == INTSXP ? INTEGER(x)[0] : REAL(x)[0])

void r_to_logical_type(SEXP logical_type, parquet::SchemaElement &sel) {
  const char *ctype = CHAR(STRING_ELT(VECTOR_ELT(logical_type, 0), 0));
  parquet::LogicalType lt;
  if (!strcmp(ctype, "STRING")) {
    lt.__set_STRING(parquet::StringType());
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "ENUM")) {
    lt.__set_ENUM(parquet::EnumType());
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "DECIMAL")) {
    parquet::DecimalType dt;
    if (Rf_length(logical_type) != 3) {
      Rf_errorcall(
        nanoparquet_call,
        "Parquet decimal logical type needs scale and precision"
      );
    }
    if (!Rf_isNull(VECTOR_ELT(logical_type, 1))) {
      dt.__set_scale(NUMERIC_SCALAR(VECTOR_ELT(logical_type, 1)));
      sel.__set_scale(dt.scale);
    }
    dt.__set_precision(NUMERIC_SCALAR(VECTOR_ELT(logical_type, 2)));
    sel.__set_precision(dt.precision);
    lt.__set_DECIMAL(dt);
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "DATE")) {
    lt.__set_DATE(parquet::DateType());
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "TIME")) {
    parquet::TimeUnit tu;
    parquet::TimeType tt;
    tt.__set_isAdjustedToUTC(LOGICAL(VECTOR_ELT(logical_type, 1))[0]);
    const char *unit = CHAR(STRING_ELT(VECTOR_ELT(logical_type, 2), 0));
    if (!strcmp(unit, "MILLIS")) {
      tu.__set_MILLIS(parquet::MilliSeconds());
    } else if (!strcmp(unit, "MICROS")) {
      tu.__set_MICROS(parquet::MicroSeconds());
    } else if (!strcmp(unit, "NANOS")) {
      tu.__set_NANOS(parquet::NanoSeconds());
    } else {
      Rf_errorcall(nanoparquet_call, "Unknown TIME time unit: %s", unit);
    }
    tt.__set_unit(tu);
    lt.__set_TIME(tt);
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "TIMESTAMP")) {
    parquet::TimeUnit tu;
    parquet::TimestampType tt;
    tt.__set_isAdjustedToUTC(LOGICAL(VECTOR_ELT(logical_type, 1))[0]);
    const char *unit = CHAR(STRING_ELT(VECTOR_ELT(logical_type, 2), 0));
    if (!strcmp(unit, "MILLIS")) {
      tu.__set_MILLIS(parquet::MilliSeconds());
    } else if (!strcmp(unit, "MICROS")) {
      tu.__set_MICROS(parquet::MicroSeconds());
    } else if (!strcmp(unit, "NANOS")) {
      tu.__set_NANOS(parquet::NanoSeconds());
    } else {
      Rf_errorcall(nanoparquet_call, "Unknown TIMESTAMP time unit: %s", unit);
    }
    tt.__set_unit(tu);
    lt.__set_TIMESTAMP(tt);
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "INT") || !strcmp(ctype, "INTEGER")) {
    parquet::IntType it;
    if (Rf_xlength(logical_type) != 3) {
      Rf_errorcall(
        nanoparquet_call,
        "Parquet integer logical type needs bit width and signedness"
      );
    }
    it.__set_bitWidth(NUMERIC_SCALAR(VECTOR_ELT(logical_type, 1)));
    it.__set_isSigned(LOGICAL(VECTOR_ELT(logical_type, 2))[0]);
    lt.__set_INTEGER(it);
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "JSON")) {
    lt.__set_JSON(parquet::JsonType());
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "BSON")) {
    lt.__set_BSON(parquet::BsonType());
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "UUID")) {
    lt.__set_UUID(parquet::UUIDType());
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "FLOAT16")) {
    lt.__set_FLOAT16(parquet::Float16Type());
    sel.__set_logicalType(lt);

  } else {
    Rf_errorcall(
      nanoparquet_call,
      "Unknown Parquet logical type: %s", ctype
    );
  }
}

void RParquetOutFile::write(
  SEXP dfsxp,
  SEXP dim,
  SEXP metadata,
  SEXP rrequired,
  SEXP options,
  SEXP schema,
  SEXP encoding) {

  df = dfsxp;
  required = rrequired;
  dicts = PROTECT(Rf_allocVector(VECSXP, Rf_length(df)));
  dicts_from = PROTECT(Rf_allocVector(INTSXP, Rf_length(df)));
  SEXP nms = PROTECT(Rf_getAttrib(dfsxp, R_NamesSymbol));
  int *type = INTEGER(VECTOR_ELT(schema, 3));
  int *type_length = INTEGER(VECTOR_ELT(schema, 4));
  int *converted_type = INTEGER(VECTOR_ELT(schema, 6));
  SEXP logical_type = VECTOR_ELT(schema, 7);
  int *scale = INTEGER(VECTOR_ELT(schema, 9));
  int *precision = INTEGER(VECTOR_ELT(schema, 10));

  R_xlen_t nr = INTEGER(dim)[0];
  set_num_rows(nr);
  R_xlen_t nc = INTEGER(dim)[1];

  write_minmax_values = LOGICAL(get_list_element(options, "write_minmax_values"))[0];
  is_minmax_supported = std::vector<bool>(nc, false);
  has_minmax_value.resize(nc);
  min_values.resize(nc);
  max_values.resize(nc);

  for (R_xlen_t idx = 0; idx < nc; idx++) {
    SEXP col = VECTOR_ELT(dfsxp, idx);
    bool req = LOGICAL(required)[idx];
    std::string rtypename;
    parquet::SchemaElement sel;
    sel.__set_name(CHAR(STRING_ELT(nms, idx)));
    if (req) {
      sel.__set_repetition_type(parquet::FieldRepetitionType::REQUIRED);
    } else {
      sel.__set_repetition_type(parquet::FieldRepetitionType::OPTIONAL);
    }

    if (type[idx] == NA_INTEGER) {
      // default mapping
      nanoparquet_map_to_parquet_type(col, options, sel, rtypename);
      fill_converted_type_for_logical_type(sel);
    } else {
      // use the supplied schema
      sel.__set_type((parquet::Type::type) type[idx]);
      if (type_length[idx] != NA_INTEGER) {
        sel.__set_type_length(type_length[idx]);
      }
      if (converted_type[idx] != NA_INTEGER) {
        sel.__set_converted_type((parquet::ConvertedType::type) converted_type[idx]);
      }
      if (!Rf_isNull(VECTOR_ELT(logical_type, idx))) {
        r_to_logical_type(VECTOR_ELT(logical_type, idx), sel);
      }
      if (scale[idx] != NA_INTEGER) {
        sel.__set_scale(scale[idx]);
      }
      if (precision[idx] != NA_INTEGER) {
        sel.__set_precision(precision[idx]);
      }
    }

    if (!write_minmax_values) {
      // nothing to do
    } if (sel.__isset.logicalType) {
      parquet::LogicalType &lt = sel.logicalType;
      is_minmax_supported[idx] = lt.__isset.DATE || lt.__isset.INTEGER ||
        lt.__isset.TIME;
      // TODO: support the rest
      // is_minmax_supported[idx] =
      //   lt.__isset.STRING || lt.__isset.ENUM ||
      //   lt.__isset.TIMESTAMP ||
      //   lt.__isset.JSON || lt.__isset.BSON || lt.__isset.UUID ||
      //   lt.__isset.DECIMAL || lt.isset.FLOAT16;
    } else {
      switch(sel.type) {
      // case parquet::Type::BOOLEAN:
      case parquet::Type::INT32:
      // case parquet::Type::INT64:
      case parquet::Type::FLOAT:
      case parquet::Type::DOUBLE:
        is_minmax_supported[idx] = true;
        break;
      default:
        is_minmax_supported[idx] = false;
        break;
      }
    }

    int32_t ienc = INTEGER(encoding)[idx];
    parquet::Encoding::type enc = detect_encoding(idx, sel, ienc);
    schema_add_column(sel, enc);
  }

  if (!Rf_isNull(metadata)) {
    SEXP keys = VECTOR_ELT(metadata, 0);
    SEXP vals = VECTOR_ELT(metadata, 1);
    R_xlen_t len = Rf_xlength(keys);
    for (auto i = 0; i < len; i++) {
      add_key_value_metadata(
        CHAR(STRING_ELT(keys, i)),
        CHAR(STRING_ELT(vals, i))
      );
    }
  }

  ParquetOutFile::write();

  UNPROTECT(3);
}

extern "C" {

static SEXP get_list_element(SEXP list, const char *str) {
  SEXP elmt = R_NilValue;
  SEXP names = PROTECT(Rf_getAttrib(list, R_NamesSymbol));

  for (R_xlen_t i = 0; i < Rf_xlength(list); i++) {
    if (strcmp(CHAR(STRING_ELT(names, i)), str) == 0) {
       elmt = VECTOR_ELT(list, i);
       break;
    }
  }
  UNPROTECT(1);
  return elmt;
}

SEXP nanoparquet_write_(SEXP dfsxp, SEXP filesxp, SEXP dim, SEXP compression,
                        SEXP metadata, SEXP required, SEXP options,
                        SEXP schema, SEXP encoding,
                        SEXP row_group_starts) {

  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_errorcall(nanoparquet_call,
                 "nanoparquet_write: filename must be a string");
  }

  int c_compression = INTEGER(compression)[0];
  parquet::CompressionCodec::type codec;
  switch (c_compression) {
  case 0:
    codec = parquet::CompressionCodec::UNCOMPRESSED;
    break;
  case 1:
    codec = parquet::CompressionCodec::SNAPPY;
    break;
  case 2:
    codec = parquet::CompressionCodec::GZIP;
    break;
  case 6:
    codec = parquet::CompressionCodec::ZSTD;
    break;
  default:
    Rf_errorcall(nanoparquet_call, "Invalid compression type code: %d", // # nocov
                 c_compression);                                        // # nocov
    break;
  }

  int dp_ver = INTEGER(get_list_element(options, "write_data_page_version"))[0];
  int comp_level = INTEGER(get_list_element(options, "compression_level"))[0];

  R_xlen_t nrg = Rf_xlength(row_group_starts);
  std::vector<int64_t> row_groups(nrg);
  for (R_xlen_t i = 0; i < nrg; i++) {
    // convert to zero-based
    row_groups[i] = INTEGER(row_group_starts)[i] - 1;
  }

  std::string fname = (char *)CHAR(STRING_ELT(filesxp, 0));
  if (fname == ":raw:") {
    MemStream ms;
    std::ostream &os = ms.stream();
    RParquetOutFile of(os, codec, comp_level, row_groups);
    of.data_page_version = dp_ver;
    of.write(dfsxp, dim, metadata, required, options, schema, encoding);
    R_xlen_t bufsize = ms.size();
    SEXP res = Rf_allocVector(RAWSXP, bufsize);
    ms.copy(RAW(res), bufsize);
    return res;
  } else {
    RParquetOutFile of(fname, codec, comp_level, row_groups);
    of.data_page_version = dp_ver;
    of.write(dfsxp, dim, metadata, required, options, schema, encoding);
    return R_NilValue;
  }
}

struct nanoparquet_write_data {
  SEXP dfsxp;
  SEXP filesxp;
  SEXP dim;
  SEXP compression;
  SEXP metadata;
  SEXP required;
  SEXP options;
  SEXP schema;
  SEXP encoding;
  SEXP row_group_starts;
};

SEXP nanoparquet_write_wrapped(void *data) {

  nanoparquet_write_data *rdata = (struct nanoparquet_write_data*) data;
  SEXP dfsxp = rdata->dfsxp;
  SEXP filesxp = rdata->filesxp;
  SEXP dim = rdata->dim;
  SEXP compression = rdata->compression;
  SEXP metadata = rdata->metadata;
  SEXP required = rdata->required;
  SEXP options = rdata->options;
  SEXP schema = rdata->schema;
  SEXP encoding = rdata->encoding;
  SEXP row_group_starts = rdata->row_group_starts;

  return nanoparquet_write_(dfsxp, filesxp, dim, compression, metadata,
                            required, options, schema, encoding,
                            row_group_starts);
}

SEXP nanoparquet_write(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression,
  SEXP metadata,
  SEXP required,
  SEXP options,
  SEXP schema,
  SEXP encoding,
  SEXP row_group_starts,
  SEXP call) {

  struct nanoparquet_write_data data = {
    dfsxp, filesxp, dim, compression, metadata, required, options, schema,
    encoding, row_group_starts
  };

  SEXP uwt = PROTECT(R_MakeUnwindCont());
  R_API_START(call);

  SEXP ret = R_UnwindProtect(
    nanoparquet_write_wrapped,
    &data,
    throw_error,
    &uwt,
    uwt
  );

  UNPROTECT(1);
  return ret;

  R_API_END();                 // # nocov
}

extern SEXP convert_logical_type(parquet::LogicalType ltype, SEXP *uwt);

SEXP nanoparquet_map_to_parquet_types(SEXP df, SEXP options) {
  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START(R_NilValue);
  R_xlen_t nc = Rf_xlength(df);
  SEXP res = PROTECT(Rf_allocVector(VECSXP, nc));
  for (R_xlen_t cl = 0; cl < nc; cl++) {
    SEXP col = VECTOR_ELT(df, cl);
    parquet::SchemaElement sel;
    std::string rtype;
    nanoparquet_map_to_parquet_type(col, options, sel, rtype);
    SEXP typ = Rf_allocVector(VECSXP, 3);
    SET_VECTOR_ELT(res, cl, typ);
    SET_VECTOR_ELT(typ, 0, Rf_mkString(to_string(sel.type).c_str()));
    SET_VECTOR_ELT(typ, 1, Rf_mkString(rtype.c_str()));
    if (sel.__isset.logicalType) {
      SET_VECTOR_ELT(typ, 2, convert_logical_type(sel.logicalType, &uwtoken));
    } else {
      SET_VECTOR_ELT(typ, 2, R_NilValue);
    }
  }

  UNPROTECT(2);
  return res;
  R_API_END();             // # nocov
}

SEXP nanoparquet_logical_to_converted(SEXP logical_type) {
  const char *nms[] = { "converted_type", "scale", "precision", "" };
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, nms));
  SET_VECTOR_ELT(res, 0, Rf_ScalarInteger(NA_INTEGER));
  SET_VECTOR_ELT(res, 1, Rf_ScalarInteger(NA_INTEGER));
  SET_VECTOR_ELT(res, 2, Rf_ScalarInteger(NA_INTEGER));

  parquet::SchemaElement sel;
  r_to_logical_type(logical_type, sel);

  R_API_START(R_NilValue);
  fill_converted_type_for_logical_type(sel);

  if (sel.__isset.converted_type) {
    INTEGER(VECTOR_ELT(res, 0))[0] = sel.converted_type;
  }
  if (sel.__isset.scale) {
    INTEGER(VECTOR_ELT(res, 1))[0] = sel.scale;
  }
  if (sel.__isset.precision) {
    INTEGER(VECTOR_ELT(res, 2))[0] = sel.precision;
  }

  UNPROTECT(1);
  return res;
  R_API_END();           // # nocov
}

SEXP nanoparquet_any_null(SEXP x) {
  R_xlen_t l = Rf_xlength(x);
  for (R_xlen_t i = 0; i < l; i++) {
    if (Rf_isNull(VECTOR_ELT(x, i))) {
      return Rf_ScalarLogical(1);
    }
  }

  return Rf_ScalarLogical(0);
}

SEXP nanoparquet_any_na(SEXP x) {
  R_xlen_t l = Rf_xlength(x);
  double *ptr = REAL(x);
  double *end = ptr + l;
  for (; ptr < end; ptr++) {
    if (R_IsNA(*ptr)) {
      return Rf_ScalarLogical(1);
    }
  }

  return Rf_ScalarLogical(0);
}

} // extern "C"
