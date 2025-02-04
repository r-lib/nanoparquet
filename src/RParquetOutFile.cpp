#include <cinttypes>
#include <cmath>

#include "r-nanoparquet.h"
#include "RParquetOutFile.h"

#include "unwind.h"

extern SEXP nanoparquet_call;

// TODO
extern "C" {
SEXP nanoparquet_create_dict(SEXP x, SEXP rlen);
SEXP nanoparquet_create_dict_idx_(SEXP x, SEXP from, SEXP until);
SEXP nanoparquet_avg_run_length(SEXP x, SEXP rlen);
}

using namespace nanoparquet;

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
    r_call([&] {
      Rf_error("Unknown Praquet encoding code: %d", renc);
    });
  }

  // otherwise we need to check if the encoding is allowed and implemented
  switch (sel.type) {
  case parquet::Type::BOOLEAN:
    if (renc == parquet::Encoding::RLE_DICTIONARY ||
        renc == parquet::Encoding::BIT_PACKED) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for BOOLEAN column: %s", enc_[renc]
        );
      });
    }
    if (renc != parquet::Encoding::RLE && renc != parquet::Encoding::PLAIN) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for BOOLEAN column: %s", enc_[renc]
        );
      });
    }
    break;
  case parquet::Type::INT32:
    if (renc == parquet::Encoding::DELTA_BINARY_PACKED ||
        renc == parquet::Encoding::BYTE_STREAM_SPLIT) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for INT32 column: %s", enc_[renc]
        );
      });
    }
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for INT32 column: %s", enc_[renc]
        );
      });
    }
    break;
  case parquet::Type::INT64:
    if (renc == parquet::Encoding::DELTA_BINARY_PACKED ||
        renc == parquet::Encoding::BYTE_STREAM_SPLIT) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for INT64 column: %s", enc_[renc]
        );
      });
    }
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for INT64 column: %s", enc_[renc]
        );
      });
    }
    break;
  case parquet::Type::INT96:
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for INT96 column: %s", enc_[renc]
        );
      });
    }
    break;
  case parquet::Type::FLOAT:
    if (renc == parquet::Encoding::BYTE_STREAM_SPLIT) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for FLOAT column: %s", enc_[renc]
        );
      });
    }
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for FLOAT column: %s", enc_[renc]
        );
      });
    }
    break;
  case parquet::Type::DOUBLE:
    if (renc == parquet::Encoding::BYTE_STREAM_SPLIT) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unimplemented encoding for DOUBLE column: %s", enc_[renc]
        );
      });
    }
    if (renc != parquet::Encoding::RLE_DICTIONARY &&
        renc != parquet::Encoding::PLAIN_DICTIONARY &&
        renc != parquet::Encoding::PLAIN) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Unsupported encoding for DOUBLE column: %s", enc_[renc]
        );
      });
    }
    break;
  case parquet::Type::BYTE_ARRAY: {
    SEXP col = VECTOR_ELT(df, idx);
    if (TYPEOF(col) == VECSXP) {
      if (renc == parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY||
          renc == parquet::Encoding::DELTA_BYTE_ARRAY ||
          renc == parquet::Encoding::RLE_DICTIONARY ||
          renc == parquet::Encoding::PLAIN_DICTIONARY) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Unimplemented encoding for list(raw) BYTE_ARRAY column: %s",
            enc_[renc]
          );
        });
      }
      if (renc != parquet::Encoding::PLAIN) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Unsupported encoding for list(raw) BYTE_ARRAY column: %s", enc_[renc]
          );
        });
      }
    } else {
      if (renc == parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY||
          renc == parquet::Encoding::DELTA_BYTE_ARRAY) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Unimplemented encoding for BYTE_ARRAY column: %s",
            enc_[renc]
          );
        });
      }
      if (renc != parquet::Encoding::RLE_DICTIONARY &&
          renc != parquet::Encoding::PLAIN_DICTIONARY &&
          renc != parquet::Encoding::PLAIN) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Unsupported encoding for BYTE_ARRAY column: %s", enc_[renc]
          );
        });
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
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Unimplemented encoding for list(raw) FIXED_LEN_BYTE_ARRAY column: %s",
            enc_[renc]
          );
        });
      }
      if (renc != parquet::Encoding::PLAIN) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Unsupported encoding for list(raw) FIXED_LEN_BYTE_ARRAY column: %s",
            enc_[renc]
          );
        });
      }
    } else {
      if (renc == parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY||
          renc == parquet::Encoding::DELTA_BYTE_ARRAY) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Unimplemented encoding for FIXED_LEN_BYTE_ARRAY column: %s",
            enc_[renc]
          );
        });
      }
      if (renc != parquet::Encoding::RLE_DICTIONARY &&
          renc != parquet::Encoding::PLAIN_DICTIONARY &&
          renc != parquet::Encoding::PLAIN) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Unsupported encoding for FIXED_LEN_BYTE_ARRAY column: %s",
            enc_[renc]
          );
        });
      }
    }
    break;
  }
  default:
    r_call([&] {
      Rf_errorcall(nanoparquet_call, "Unsupported Parquet type");
    });
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
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Invalid Parquet file: precision is not set for DECIMAL converted type"
        );
      });
    }
    if (!sel.__isset.scale) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Invalid Parquet file: scale is not set for DECIMAL converted type"
        );
      });
    }
    precision = sel.precision;
    scale = sel. scale;
    return true;
  } else {
    return false;
  }
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
      if (sel.type == parquet::Type::INT32) {
        min_values[idx] = std::string((const char*) INTEGER(VECTOR_ELT(d, 2)), sizeof(int32_t));
        max_values[idx] = std::string((const char*) INTEGER(VECTOR_ELT(d, 3)), sizeof(int32_t));
      } else if (sel.type == parquet::Type::INT64) {
        int64_t min = INTEGER(VECTOR_ELT(d, 2))[0];
        int64_t max = INTEGER(VECTOR_ELT(d, 3))[0];
        min_values[idx] = std::string((const char*) &min, sizeof(int64_t));
        max_values[idx] = std::string((const char*) &max, sizeof(int64_t));
      } else {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot convert an integer vector to Parquet type %s.",
            parquet::_Type_VALUES_TO_NAMES.at(sel.type)
          );
        });
      }
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
      } else if (sel.type == parquet::Type::INT64) {
        int64_t min = REAL(VECTOR_ELT(d, 2))[0];
        int64_t max = REAL(VECTOR_ELT(d, 3))[0];
        min_values[idx] = std::string((const char*) &min, sizeof(int64_t));
        max_values[idx] = std::string((const char*) &max, sizeof(int64_t));
      } else {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot convert a double vector to Parquet type %s.",
            parquet::_Type_VALUES_TO_NAMES.at(sel.type)
          );
        });
      }
    } else if (TYPEOF(VECTOR_ELT(d, 2)) == CHARSXP) {
      const char *min = CHAR(VECTOR_ELT(d, 2));
      const char *max = CHAR(VECTOR_ELT(d, 3));
      min_values[idx] = std::string(min, strlen(min));
      max_values[idx] = std::string(max, strlen(max));
    } else {
      r_call([&] {
        Rf_error("Unknown R type when writing out min/max values, internal error");
      });
    }
  }
}


void write_integer_int32_dec(std::ostream & file, SEXP col, uint64_t from,
                             uint64_t until, int32_t precision,
                             int32_t scale) {

  if (precision > 9) {
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Internal nanoparquet error, precision to high for INT32 DECIMAL"
      );
    });
  }
  int32_t fact = pow(10, scale);
  int32_t max = ((int32_t)pow(10, precision)) / fact;
  int32_t min = -max;
  for (uint64_t i = from; i < until; i++) {
    int32_t val = INTEGER(col)[i];
    if (val == NA_INTEGER) continue;
    if (val <= min) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Value too small for INT32 DECIMAL with precision %d, scale %d: %d",
          precision, scale, val);
      });
    }
    if (val >= max) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Value too large for INT32 DECIMAL with precision %d, scale %d: %d",
          precision, scale, val);
      });
    }
    val *= fact;
    file.write((const char *)&val, sizeof(int32_t));
  }
}

#define GRAB_MIN2(tgt, idx) memcpy(&tgt, min_values[idx].data(), sizeof(tgt))
#define GRAB_MAX2(tgt, idx) memcpy(&tgt, max_values[idx].data(), sizeof(tgt))
#define SAVE_MIN2(tgt, idx, val) do {			          \
  min_values[idx] = std::string((const char*) &val, sizeof(tgt)); \
  tgt = val; } while (0)
#define SAVE_MAX2(tgt, idx, val) do {				  \
  max_values[idx] = std::string((const char*) &val, sizeof(tgt)); \
  tgt = val; } while (0)

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
  bool has_min = false, has_max = false;
  int32_t min_value = 0, max_value = 0;
  if (minmax && has_minmax_value[idx]) {
    GRAB_MIN2(min_value, idx);
    GRAB_MAX2(max_value, idx);
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
        if (minmax && (!has_min || val < min_value)) {
	  has_min = true;
          SAVE_MIN2(min_value, idx, val);
        }
        if (minmax && (!has_max || val > max_value)) {
	  has_max = true;
          SAVE_MAX2(max_value, idx, val);
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
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Invalid bit width for INT32: %d", bit_width
        );
      });
    }
    min = is_signed ? -max-1 : 0;
    for (uint64_t i = from; i < until; i++) {
      int32_t val = INTEGER(col)[i];
      if (val == NA_INTEGER) continue;
      const char *w = val < min ? "small" : (val > max ? "large" : "");
      if (w[0]) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Integer value too %s for %sINT with bit width %d: %d"
            " at column %u, row %" PRIu64 ":",
            w, (is_signed ? "" : "U"), bit_width, val, idx + 1, i + 1
          );
        });
      }
      if (minmax && (!has_min || val < min_value)) {
	has_min = true;
        SAVE_MIN2(min_value, idx, val);
      }
      if (minmax && (!has_max || val > max_value)) {
	has_max = true;
        SAVE_MAX2(max_value, idx, val);
      }
      file.write((const char *) &val, sizeof(int32_t));
    }
  }
  has_minmax_value[idx] = has_minmax_value[idx] || has_min;
}

void write_double_int32_dec(std::ostream &file, SEXP col, uint64_t from,
                            uint64_t until, int32_t precision,
                            int32_t scale) {

  if (precision > 9) {
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Internal nanoparquet error, precision to high for INT32 DECIMAL"
      );
    });
  }
  int32_t fact = pow(10, scale);
  double max = (pow(10, precision)) / fact;
  double min = -max;
  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    if (val <= min) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Value too small for INT32 DECIMAL with precision %d, scale %d: %f",
          precision, scale, round(val * fact) / fact);
      });
    }
    if (val >= max) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Value too large for INT32 DECIMAL with precision %d, scale %d: %f",
          precision, scale, round(val * fact)/ fact);
      });
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
  bool has_min = false, has_max = false;
  int32_t min_value = 0, max_value = 0;
  bool minmax = write_minmax_values && is_minmax_supported[idx];
  if (minmax && has_minmax_value[idx]) {
    GRAB_MIN2(min_value, idx);
    GRAB_MAX2(max_value, idx);
  }

  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    int32_t ival = val * factor;
    if (minmax && (!has_min || ival < min_value)) {
      has_min = true;
      SAVE_MIN2(min_value, idx, ival);
    }
    if (minmax && (!has_max || ival > max_value)) {
      has_max = true;
      SAVE_MAX2(max_value, idx, ival);
    }
    file.write((const char *)&ival, sizeof(int32_t));
  }
  has_minmax_value[idx] = has_minmax_value[idx] || has_min;
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
    int32_t min_value = 0, max_value = 0;
    bool has_min = false, has_max = false;
    bool minmax = write_minmax_values && is_minmax_supported[idx];
    if (minmax && has_minmax_value[idx]) {
      GRAB_MIN2(min_value, idx);
      GRAB_MAX2(max_value, idx);
    }

    int32_t min, max = 0;
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
      r_call([&] {
        Rf_errorcall(nanoparquet_call, "Invalid bit width for INT32: %d", bit_width);
      });
    }
    min = -max - 1;
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      const char *w = val < min ? "small" : (val > max ? "large" : "");
      if (w[0]) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Integer value too %s for INT with bit width %d: %f"
            " at column %u, row %" PRIu64 ":",
            w, bit_width, val, idx + 1, i + 1
          );
        });
      }
      int32_t ival = val;
      if (minmax && (!has_min || ival < min_value)) {
	has_min = true;
        SAVE_MIN2(min_value, idx, ival);
      }
      if (minmax && (!has_max || ival > max_value)) {
	has_max = true;
        SAVE_MAX2(max_value, idx, ival);
      }
      file.write((const char *)&ival, sizeof(int32_t));
    }
    has_minmax_value[idx] = has_minmax_value[idx] || has_min;
  } else {
    uint32_t min_value = 0, max_value = 0;
    bool has_min = false, has_max = false;
    bool minmax = write_minmax_values && is_minmax_supported[idx];
    if (minmax && has_minmax_value[idx]) {
      GRAB_MIN2(min_value, idx);
      GRAB_MAX2(max_value, idx);
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
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Invalid bit width for INT32: %d", bit_width
        );
      });
    }
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      if (val > max) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Integer value too large for INT with bit width %d: %f"
            " at column %u, row %" PRIu64 ".",
            bit_width, val, idx + 1, i + 1
          );
        });
      }
      if (val < 0 ) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Negative values are not allowed in unsigned INT column:"
            "%f at column %u, row %"  PRIu64 ".",
            val, idx + 1, i + 1
          );
        });
      }
      uint32_t uival = val;
      if (minmax && (!has_min || uival < min_value)) {
	has_min = true;
        SAVE_MIN2(min_value, idx, uival);
      }
      if (minmax && (!has_max || uival > max_value)) {
	has_max = true;
        SAVE_MAX2(max_value, idx, uival);
      }
      file.write((const char *)&uival, sizeof(uint32_t));
    }
    has_minmax_value[idx] = has_minmax_value[idx] || has_min;
  }
}

void RParquetOutFile::write_int32(std::ostream &file, uint32_t idx,
                                  uint32_t group, uint32_t page,
                                  uint64_t from, uint64_t until,
                                  parquet::SchemaElement &sel) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                          // # nocov
        nanoparquet_call,                                    // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
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
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet INT32 type.",
        type_names[TYPEOF(col)]
      );
    });
  }
}

void write_integer_int64_dec(std::ostream &file, SEXP col, uint64_t from,
                             uint64_t until, int32_t precision,
                             int32_t scale) {
  if (precision > 18) {
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Internal nanoparquet error, precision to high for INT64 DECIMAL"
      );
    });
  }
  int64_t fact = pow(10, scale);
  int64_t max = ((int64_t)pow(10, precision)) / fact;
  int64_t min = -max;
  for (uint64_t i = from; i < until; i++) {
    int32_t val = INTEGER(col)[i];
    if (val == NA_INTEGER) continue;
    int64_t ival = val;
    if (ival <= min) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Value too small for INT64 DECIMAL with precision %d, scale "
          "%d: %" PRId64,
          precision, scale, ival
        );
      });
    }
    if (ival >= max) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Value too large for INT64 DECIMAL with precision %d, scale "
          "%d: %" PRId64,
          precision, scale, ival
        );
      });
    }
    ival *= fact;
    file.write((const char *)&ival, sizeof(int64_t));
  }
}

void RParquetOutFile::write_integer_int64(std::ostream &file, SEXP col,
                                          uint32_t idx,
                                          uint64_t from, uint64_t until) {

  int64_t min_value = 0, max_value = 0;
  bool has_min = false, has_max = false;
  bool minmax = write_minmax_values && is_minmax_supported[idx];
  if (minmax && has_minmax_value[idx]) {
    GRAB_MIN2(min_value, idx);
    GRAB_MAX2(max_value, idx);
  }

  for (uint64_t i = from; i < until; i++) {
    int32_t val = INTEGER(col)[i];
    if (val == NA_INTEGER) continue;
    int64_t el = val;
    if (minmax && (!has_min || el < min_value)) {
      has_min = true;
      SAVE_MIN2(min_value, idx, el);
    }
    if (minmax && (!has_max || el > max_value)) {
      has_max = true;
      SAVE_MAX2(max_value, idx, el);
    }
    file.write((const char*) &el, sizeof(int64_t));
   }
  has_minmax_value[idx] = has_minmax_value[idx] || has_min;
}

 void write_double_int64_dec(std::ostream &file, SEXP col, uint64_t from,
                             uint64_t until, int32_t precision,
                             int32_t scale) {
  if (precision > 18) {
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Internal nanoparquet error, precision to high for INT64 DECIMAL"
      );
    });
  }
  int64_t fact = pow(10, scale);
  double max = (pow(10, precision)) / fact;
  double min = -max;
  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    if (val <= min) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Value too small for INT64 DECIMAL with precision %d, scale %d: %f",
          precision, scale, round(val * fact) / fact);
      });
    }
    if (val >= max) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Value too large for INT64 DECIMAL with precision %d, scale %d: %f",
          precision, scale, round(val * fact)/ fact);
      });
    }
    int64_t ival = val * fact;
    file.write((const char *)&ival, sizeof(int64_t));
  }
}

void RParquetOutFile::write_double_int64_time(std::ostream &file, SEXP col,
                                              uint32_t idx, uint64_t from,
                                              uint64_t until,
                                              parquet::SchemaElement &sel,
                                              double factor) {
  int64_t min_value = 0, max_value = 0;
  bool has_min = false, has_max = false;
  bool minmax = write_minmax_values && is_minmax_supported[idx];
  if (minmax && has_minmax_value[idx]) {
    GRAB_MIN2(min_value, idx);
    GRAB_MAX2(max_value, idx);
  }

  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    int64_t ival = val * factor;
    if (minmax && (!has_min || ival < min_value)) {
      has_min = true;
      SAVE_MIN2(min_value, idx, ival);
    }
    if (minmax && (!has_max || ival > max_value)) {
      has_max = true;
      SAVE_MAX2(max_value, idx, ival);
    }
    file.write((const char *)&ival, sizeof(int64_t));
  }
  has_minmax_value[idx] = has_minmax_value[idx] || has_min;
}

void RParquetOutFile::write_double_int64(std::ostream &file, SEXP col,
                                         uint32_t idx, uint64_t from,
                                         uint64_t until,
                                         parquet::SchemaElement &sel) {
  int64_t min_value = 0, max_value = 0;
  bool has_min = false, has_max = false;
  bool minmax = write_minmax_values && is_minmax_supported[idx];
  if (minmax && has_minmax_value[idx]) {
    GRAB_MIN2(min_value, idx);
    GRAB_MAX2(max_value, idx);
  }

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
      if (minmax && (!has_min || el < min_value)) {
	has_min = true;
        SAVE_MIN2(min_value, idx, el);
      }
      if (minmax && (!has_max || el > max_value)) {
	has_max = true;
        SAVE_MAX2(max_value, idx, el);
      }
      file.write((const char *)&el, sizeof(int64_t));
    }
    has_minmax_value[idx] = has_minmax_value[idx] || has_min;
  } else if (Rf_inherits(col, "difftime")) {
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      int64_t el = val * 1000 * 1000 * 1000;
      if (minmax && (!has_min || el < min_value)) {
	has_min = true;
        SAVE_MIN2(min_value, idx, el);
      }
      if (minmax && (!has_max || el > max_value)) {
	has_max = true;
        SAVE_MAX2(max_value, idx, el);
      }
      file.write((const char *)&el, sizeof(int64_t));
    }
    has_minmax_value[idx] = has_minmax_value[idx] || has_min;
  } else {
    bool is_signed = TRUE;
    int bit_width = 64;
    if (sel.__isset.logicalType && sel.logicalType.__isset.INTEGER) {
      is_signed = sel.logicalType.INTEGER.isSigned;
      bit_width = sel.logicalType.INTEGER.bitWidth;
    }
    if (bit_width != 64) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Invalid bit width for INT64 INT type: %d", bit_width
        );
      });
    }
    if (is_signed) {
      // smallest & largest double that can be put into an int64_t
      double min = -9223372036854774899.0, max = 9223372036854774899.0;
      for (uint64_t i = from; i < until; i++) {
        double val = REAL(col)[i];
        if (R_IsNA(val)) continue;
        const char *w = val < min ? "small" : (val > max ? "large" : "");
        if (w[0]) {
          r_call([&] {
            Rf_errorcall(
              nanoparquet_call,
              "Integer value too %s for %sINT with bit width %d: %f"
              " at column %u, row %" PRIu64 ".",
              w, (is_signed ? "" : "U"), bit_width, val, idx + 1, i + 1
            );
          });
        }
        int64_t el = val;
        if (minmax && (!has_min || el < min_value)) {
	  has_min = true;
          SAVE_MIN2(min_value, idx, el);
        }
        if (minmax && (!has_max || el > max_value)) {
	  has_max = true;
          SAVE_MAX2(max_value, idx, el);
        }
        file.write((const char *)&el, sizeof(int64_t));
      }
      has_minmax_value[idx] = has_minmax_value[idx] || has_min;
    } else {
      // largest double that can be put into an uint64_t
      double max = 18446744073709549999.0;
      uint64_t min_value = 0, max_value = 0;
      bool has_min = false, has_max = false;
      bool minmax = write_minmax_values && is_minmax_supported[idx];
      if (minmax && has_minmax_value[idx]) {
	GRAB_MIN2(min_value, idx);
	GRAB_MAX2(max_value, idx);
      }
      for (uint64_t i = from; i < until; i++) {
        double val = REAL(col)[i];
        if (R_IsNA(val)) continue;
        if (val > max) {
          r_call([&] {
            Rf_errorcall(
              nanoparquet_call,
              "Integer value too large for unsigned INT with bit width %d: %f"
              " at column %u, row %" PRIu64 ".",
              bit_width, val, idx + 1, i + 1
            );
          });
        }
        if (val < 0) {
          r_call([&] {
            Rf_errorcall(
              nanoparquet_call,
              "Negative values are not allowed in unsigned INT column:"
              "%f at column %u, row %"  PRIu64 ".",
              val, idx + 1, i + 1
            );
          });
        }
        uint64_t el = val;
        if (minmax && (!has_min || el < min_value)) {
	  has_min = true;
          SAVE_MIN2(min_value, idx, el);
        }
        if (minmax && (!has_max || el > max_value)) {
	  has_max = true;
          SAVE_MAX2(max_value, idx, el);
        }
        file.write((const char *)&el, sizeof(uint64_t));
      }
      has_minmax_value[idx] = has_minmax_value[idx] || has_min;
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
    r_call([&] {
      Rf_errorcall(                                          // # nocov
        nanoparquet_call,                                    // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
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
      write_integer_int64(file, col, idx, from, until);
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
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet INT64 type.",
        type_names[TYPEOF(col)]
      );
    });
  }
}

void RParquetOutFile::write_int96(std::ostream &file, uint32_t idx,
                                  uint32_t group, uint32_t page,
                                  uint64_t from, uint64_t until,
                                  parquet::SchemaElement &sel) {
  // This is double in R, so we need to convert
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                         // # nocov
        nanoparquet_call,                                   // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
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
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet INT96 type.",
        type_names[TYPEOF(col)]
      );
    });
  }
}

void RParquetOutFile::write_float(std::ostream &file, uint32_t idx,
                                  uint32_t group, uint32_t page,
                                  uint64_t from, uint64_t until,
                                  parquet::SchemaElement &sel) {
  SEXP col = VECTOR_ELT(df, idx);
  if (TYPEOF(col) != REALSXP) {
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet FLOAT type.",
        type_names[TYPEOF(col)]
      );
    });
  }
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                         // # nocov
        nanoparquet_call,                                   // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
  }

  bool minmax = write_minmax_values && is_minmax_supported[idx];
  float min_value = 0, max_value = 0;
  bool has_min = false, has_max = false;
  if (minmax && has_minmax_value[idx]) {
    GRAB_MIN2(min_value, idx);
    GRAB_MAX2(max_value, idx);
  }

  for (uint64_t i = from; i < until; i++) {
    double val = REAL(col)[i];
    if (R_IsNA(val)) continue;
    float el = val;
    if (minmax && (!has_min || el< min_value)) {
      has_min = true;
      SAVE_MIN2(min_value, idx, el);
    }
    if (minmax && (!has_max || el > max_value)) {
      has_max = true;
      SAVE_MAX2(max_value, idx, el);
    }
    file.write((const char*) &el, sizeof(float));
  }
  has_minmax_value[idx] = has_minmax_value[idx] || has_min;
}

void RParquetOutFile::write_double(std::ostream &file, uint32_t idx,
                                   uint32_t group, uint32_t page,
                                   uint64_t from, uint64_t until,
                                   parquet::SchemaElement &sel) {
  SEXP col = VECTOR_ELT(df, idx);
  if (TYPEOF(col) != REALSXP) {
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet DOUBLE type.",
        type_names[TYPEOF(col)]
      );
    });
  }
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                         // # nocov
        nanoparquet_call,                                   // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
  }

  bool minmax = write_minmax_values && is_minmax_supported[idx];
  double min_value = 0, max_value = 0;
  bool has_min = false, has_max = false;
  if (minmax && has_minmax_value[idx]) {
    GRAB_MIN2(min_value, idx);
    GRAB_MAX2(max_value, idx);
  }

  if (!minmax &&
      sel.repetition_type == parquet::FieldRepetitionType::REQUIRED) {
    uint64_t len = until - from;
    file.write((const char *) (REAL(col) + from), sizeof(double) * len);
  } else {
    for (uint64_t i = from; i < until; i++) {
      double val = REAL(col)[i];
      if (R_IsNA(val)) continue;
      if (minmax && (!has_min || val < min_value)) {
	has_min = true;
        SAVE_MIN2(min_value, idx, val);
      }
      if (minmax && (!has_max || val > max_value)) {
	has_max = true;
        SAVE_MAX2(max_value, idx, val);
      }
      file.write((const char*) &val, sizeof(double));
    }
    has_minmax_value[idx] = has_minmax_value[idx] || has_min;
  }
}

static inline bool STR_LESS(const char *c, size_t l, std::string &etalon) {
  size_t el = etalon.size();
  // "" is less than anything but ""
  if (l == 0) return el > 0;
  // otherwise anything is more than ""
  if (el == 0) return false;
  int res = memcmp(c, etalon.data(), l < el ? l : el);
  return res < 0 || (res == 0 && l < el);
}

static inline bool STR_MORE(const char *c, size_t l, std::string &etalon) {
  size_t el = etalon.size();
  // "" is not more than anything
  if (l == 0) return false;
  // othwrwise anything is more than ""
  if (el == 0) return true;
  int res = memcmp(c, etalon.data(), l < el ? l : el);
  return res > 0 || (res == 0 && l > el);
}

#define SAVE_MIN_STR(idx, c, l) do {          \
  min_values[idx] = std::string((c), (l));    \
  min_value = &min_values[idx]; } while (0)
#define SAVE_MAX_STR(idx, c, l) do {          \
  max_values[idx] = std::string((c), (l));    \
  max_value = &max_values[idx]; } while (0)

void RParquetOutFile::write_byte_array(std::ostream &file, uint32_t idx,
                                       uint32_t group, uint32_t page,
                                       uint64_t from, uint64_t until,
                                       parquet::SchemaElement &sel) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                         // # nocov
        nanoparquet_call,                                   // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
  }

  switch (TYPEOF(col)) {
  case STRSXP: {
    bool minmax = write_minmax_values && is_minmax_supported[idx];
    std::string *min_value = nullptr, *max_value = nullptr;
    if (minmax && has_minmax_value[idx]) {
      min_value = &min_values[idx];
      max_value = &max_values[idx];
    }

    for (uint64_t i = from; i < until; i++) {
      SEXP el = STRING_ELT(col, i);
      if (el == NA_STRING) {
        continue;
      }
      const char *c = CHAR(el);
      uint32_t len1 = strlen(c);
      if (minmax && (min_value == nullptr || STR_LESS(c, len1, *min_value))) {
        SAVE_MIN_STR(idx, c, len1);
      }
      if (minmax && (max_value == nullptr || STR_MORE(c, len1, *max_value))) {
        SAVE_MAX_STR(idx, c, len1);
      }
      file.write((const char *)&len1, 4);
      file.write(c, len1);
    }
    has_minmax_value[idx] = has_minmax_value[idx] || min_value != nullptr;
    break;
  }
  case VECSXP: {
    for (uint64_t i = from; i < until; i++) {
      SEXP el = VECTOR_ELT(col, i);
      if (Rf_isNull(el)) {
        continue;
      }
      if (TYPEOF(el) != RAWSXP) {
        r_call([&] {
          Rf_errorcall(                                                       // # nocov
            nanoparquet_call,                                                 // # nocov
            "Cannot write %s as a Parquet BYTE_ARRAY element when writing a " // # nocov
            "list column of RAW vectors.", type_names[TYPEOF(el)]             // # nocov
          );
        });
      }
      uint32_t len1 = Rf_xlength(el);
      file.write((const char*) &len1, sizeof(uint32_t));
      file.write((const char*) RAW(el), len1);
    }
    break;
  }
  default:
    r_call([&] {
      Rf_errorcall(                                       // # nocov
        nanoparquet_call,                                 // # nocov
        "Cannot write %s as a Parquet BYTE_ARRAY type.",  // # nocov
        type_names[TYPEOF(col)]                           // # nocov
      );
    });
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
    r_call([&] {
      Rf_errorcall(                                          // # nocov
        nanoparquet_call,                                    // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
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
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot write %s as a Parquet BYTE_ARRAY element when writing a "
            "list column of RAW vectors.", type_names[TYPEOF(el)]
          );
        });
      }
      size += Rf_xlength(el) + 4;
    }
    break;
  }
  default:
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet BYTE_ARRAY type.",
        type_names[TYPEOF(col)]
      );
    });
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
    r_call([&] {
      Rf_errorcall(                                          // # nocov
        nanoparquet_call,                                    // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
  }
  bool is_uuid = sel.__isset.logicalType && sel.logicalType.__isset.UUID;
  bool is_f16 = sel.__isset.logicalType && sel.logicalType.__isset.FLOAT16;
  if (is_uuid) {
    if (TYPEOF(col) != STRSXP) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot write %s as a Parquet UUID (FIXED_LEN_BYTE_ARRAY) type.",
          type_names[TYPEOF(col)]
        );
      });
    }
    char u[16], tmp[18];  // need to be longer, for easier conversion
    for (uint64_t i = from; i < until; i++) {
      SEXP s = STRING_ELT(col, i);
      if (s == NA_STRING) continue;
      const char *c = CHAR(s);
      if (!parse_uuid(c, u, tmp)) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Invalid UUID value in column %d, row %" PRIu64,
            idx + 1, i + 1
          );
        });
      }
      file.write(u, 16);
    }

  } else if (is_f16) {
    if (TYPEOF(col) != REALSXP) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot write %s as a Parquet FLOAT16 type.",
          type_names[TYPEOF(col)]
        );
      });
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
          r_call([&] {
            Rf_errorcall(
              nanoparquet_call,
              "Invalid string length: %d, expenting %d for "
              "FIXED_LEN_TYPE_ARRAY", len1, type_length
            );
          });
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
          r_call([&] {
            Rf_errorcall(
              nanoparquet_call,
              "Cannot write %s as a Parquet BYTE_ARRAY element when writing a "
              "list column of RAW vectors.", type_names[TYPEOF(el)]
            );
          });
        }
        uint32_t len1 = Rf_xlength(el);
        if (len1 != type_length) {
          r_call([&] {
            Rf_errorcall(
              nanoparquet_call,
              "Invalid string length: %d, expenting %d for "
              "FIXED_LEN_TYPE_ARRAY", len1, type_length
            );
          });
        }
        file.write((const char *)RAW(el), len1);
      }
      break;
    }
    default:
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot write %s as a Parquet FIXED_LEN_BYTE_ARRAY type.",
          type_names[TYPEOF(col)]
        );
      });
    }
  }
}

void write_boolean_impl(std::ostream &file, SEXP col,
                        uint64_t from, uint64_t until) {
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                          // # nocov
        nanoparquet_call,                                    // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
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
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet BOOLEAN type.",
        type_names[TYPEOF(col)]
      );
    });
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
    r_call([&] {
      Rf_errorcall(                                          // # nocov
        nanoparquet_call,                                    // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
  }
  uint64_t len = until - from;
  file.write((const char *) (LOGICAL(col) + from), sizeof(int) * len);
}

uint32_t RParquetOutFile:: write_present(std::ostream &file, uint32_t idx,
                                         uint64_t from, uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                         // # nocov
        nanoparquet_call,                                   // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
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
    r_call([&] {
      Rf_errorcall(nanoparquet_call, "Uninmplemented R type");   // # nocov
    });
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
    r_call([&] {
      Rf_errorcall(                                         // # nocov
        nanoparquet_call,                                   // # nocov
        "Cannot write %s as a Parquet BOOLEAN type.",       // # nocov
        type_names[TYPEOF(col)]                             // # nocov
      );
    });
  }
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                         // # nocov
        nanoparquet_call,                                   // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
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
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot write %s as a Parquet BOOLEAN type.", type_names[TYPEOF(col)]
      );
    });
  }
  SEXP col2 = PROTECT(Rf_allocVector(LGLSXP, num_present));
  uint64_t i, o;
  if (until > Rf_xlength(col)) {
    r_call([&] {
      Rf_errorcall(                                         // # nocov
        nanoparquet_call,                                   // # nocov
        "Internal nanoparquet error, row index too large"
      );
    });
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
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot convert an integer vector to Parquet type %s.",
          parquet::_Type_VALUES_TO_NAMES.at(type)
          );
        });
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
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot convert a double vector to Parquet type %s.",
          parquet::_Type_VALUES_TO_NAMES.at(type)
          );
        });
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
    throw std::runtime_error("Uninmplemented R type");  // # nocov
  }
  // never reached
  return 0;
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
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot convert a factor to Parquet type %s.",
          parquet:: _Type_VALUES_TO_NAMES.at(type)
          );
        });
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
              r_call([&] {
                Rf_errorcall(
                  nanoparquet_call,
                  "Value too small for INT32 DECIMAL with precision %d, scale %d: %d",
                  precision, scale, val
                );
              });
            }
            if (val >= max) {
              r_call([&] {
                Rf_errorcall(
                  nanoparquet_call,
                  "Value too large for INT32 DECIMAL with precision %d, scale %d: %d",
                  precision, scale, val
                );
              });
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
              r_call([&] {
                Rf_errorcall(
                    nanoparquet_call,
                    "Value too small for INT64 DECIMAL with precision %d, scale "
                    "%d: %" PRId64,
                    precision, scale, val);
              });
            }
            if (val >= max) {
              r_call([&] {
                Rf_errorcall(
                    nanoparquet_call,
                    "Value too large for INT64 DECIMAL with precision %d, scale "
                    "%d: %" PRId64,
                    precision, scale, val);
              });
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
        r_call([&] {
          Rf_errorcall(                                              // # nocov
            nanoparquet_call,                                        // # nocov
            "Cannot convert an integer vector to Parquet type %s.",  // # nocov
            parquet::_Type_VALUES_TO_NAMES.at(type)                   // # nocov
          );
        });
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
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot convert a POSIXct vector to Parquet type %s.",
          parquet:: _Type_VALUES_TO_NAMES.at(type)
          );
        });
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
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot convert an hms vector to Parquet type %s.",
          parquet:: _Type_VALUES_TO_NAMES.at(type)
          );
        });
      }
    } else if (Rf_inherits(col, "difftime")) {
      if (type != parquet::Type::INT64) {
        r_call([&] {
          Rf_errorcall(
            nanoparquet_call,
            "Cannot convert a difftime vector to Parquet type %s.",
          parquet:: _Type_VALUES_TO_NAMES.at(type)
          );
        });
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
              r_call([&] {
                Rf_errorcall(nanoparquet_call,
                            "Value too small for INT32 DECIMAL with precision "
                            "%d, scale %d: %f",
                            precision, scale, round(val * fact) / fact);
              });
            }
            if (val >= max) {
              r_call([&] {
                Rf_errorcall(nanoparquet_call,
                            "Value too large for INT32 DECIMAL with precision "
                            "%d, scale %d: %f",
                            precision, scale, round(val * fact) / fact);
              });
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
            int32_t min, max = 0;
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
              r_call([&] {
                Rf_errorcall(nanoparquet_call, "Invalid bit width for INT32: %d",
                            bit_width);
              });
            }
            min = -max - 1;
            SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
            int32_t *idict = (int32_t *)INTEGER(dict);
            for (auto i = 0; i < len; i++) {
              double val = icol[iidx[i]];
              const char *w = val < min ? "small" : (val > max ? "large" : "");
              if (w[0]) {
                r_call([&] {
                  Rf_errorcall(
                      nanoparquet_call,
                      "Integer value too %s for INT with bit width %d: %f"
                      " at column %u", w, bit_width, val, idx + 1);
                });
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
              r_call([&] {
                Rf_errorcall(nanoparquet_call, "Invalid bit width for INT32: %d",
                            bit_width);
              });
            }
            SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
            int32_t *idict = (int32_t *)INTEGER(dict);
            for (auto i = 0; i < len; i++) {
              double val = icol[iidx[i]];
              if (val > max) {
                r_call([&] {
                  Rf_errorcall(
                      nanoparquet_call,
                      "Integer value too large for INT with bit width %d: %f"
                      " at column %u.", bit_width, val, idx + 1);
                });
              }
              if (val < 0) {
                r_call([&] {
                  Rf_errorcall(
                      nanoparquet_call,
                      "Negative values are not allowed in unsigned INT column: "
                      "%f at column %u.", val, idx + 1);
                });
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
              r_call([&] {
                Rf_errorcall(nanoparquet_call,
                            "Value too small for INT64 DECIMAL with precision "
                            "%d, scale %d: %f",
                            precision, scale, round(val * fact) / fact);
              });
            }
            if (val >= max) {
              r_call([&] {
                Rf_errorcall(nanoparquet_call,
                            "Value too large for INT64 DECIMAL with precision "
                            "%d, scale %d: %f",
                            precision, scale, round(val * fact) / fact);
              });
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
        r_call([&] {
          Rf_errorcall(                                             // # nocov
            nanoparquet_call,                                       // # nocov
            "Cannot convert a double vector to Parquet type %s.",   // # nocov
            parquet:: _Type_VALUES_TO_NAMES.at(type)                // # nocov
          );
        });
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
            r_call([&] {
              Rf_errorcall(
                nanoparquet_call,
                "Invalid UUID value in column %d", idx + 1
              );
            });
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
            r_call([&] {
              Rf_errorcall(
                  nanoparquet_call,
                  "Invalid string length in FIXED_LEN_BYTE_ARRAY column: %d, "
                  "should be %d.",
                  len1, sel.type_length);
            });
          }
          file.write(c, len1);
        }
      }
      break;
    }
    default:
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot convert a character vector to Parquet type %s.",
        parquet:: _Type_VALUES_TO_NAMES.at(type)
        );
      });
    }
    break;
  }
  case LGLSXP: {                                           // # nocov start
    // can Parquet have dicitonary encoded BOOLEANS? There isn't much point.
    if (type != parquet::Type::BOOLEAN) {
      r_call([&] {
        Rf_errorcall(
          nanoparquet_call,
          "Cannot convert a double vector to Parquet type %s.",
        parquet:: _Type_VALUES_TO_NAMES.at(type)
        );
      });
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
    throw std::runtime_error("Uninmplemented R type");          // # nocov
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

void RParquetOutFile::init_metadata(
  SEXP dfsxp,
  SEXP dim,
  SEXP metadata,
  SEXP rrequired,
  SEXP options,
  SEXP schema,
  SEXP encoding) {

  df = dfsxp;
  required = rrequired;
  R_xlen_t nr = INTEGER(dim)[0];
  set_num_rows(nr);

  dicts = Rf_allocVector(VECSXP, Rf_length(df));
  R_PreserveObject(dicts);
  dicts_from = Rf_allocVector(INTSXP, Rf_length(df));
  R_PreserveObject(dicts_from);
  SEXP nms = PROTECT(Rf_getAttrib(dfsxp, R_NamesSymbol));
  int *type = INTEGER(VECTOR_ELT(schema, 3));
  int *type_length = INTEGER(VECTOR_ELT(schema, 4));
  int *converted_type = INTEGER(VECTOR_ELT(schema, 6));
  SEXP logical_type = VECTOR_ELT(schema, 7);
  int *scale = INTEGER(VECTOR_ELT(schema, 9));
  int *precision = INTEGER(VECTOR_ELT(schema, 10));

  R_xlen_t nc = INTEGER(dim)[1];
  write_minmax_values = LOGICAL(rf_get_list_element(options, "write_minmax_values"))[0];
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
        lt.__isset.TIME || lt.__isset.STRING || lt.__isset.ENUM ||
        lt.__isset.JSON || lt.__isset.BSON || lt.__isset.TIMESTAMP;
      // TODO: support the rest
      // is_minmax_supported[idx] = lt.__isset.UUID ||
      //   lt.__isset.DECIMAL || lt.isset.FLOAT16;
    } else {
      switch(sel.type) {
      // case parquet::Type::BOOLEAN:
      case parquet::Type::INT32:
      case parquet::Type::INT64:
      case parquet::Type::FLOAT:
      case parquet::Type::DOUBLE:
      // case parquet::Type::BYTE_ARRAY;
      // case parquet::Type::FIXED_LEN_BYTE_ARRAY;
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
  UNPROTECT(1);
};

void RParquetOutFile::init_append_metadata(
  SEXP dfsxp,
  SEXP dim,
  SEXP rrequired,
  SEXP options,
  std::vector<parquet::SchemaElement> &schema,
  SEXP encoding,
  std::vector<parquet::RowGroup> &row_groups,
  std::vector<parquet::KeyValue> &key_value_metadata) {

  set_row_groups(row_groups);
  set_key_value_metadata(key_value_metadata);

  df = dfsxp;
  required = rrequired;
  R_xlen_t nr = INTEGER(dim)[0];
  R_xlen_t ntotal = INTEGER(dim)[2];
  set_num_rows(nr, ntotal);

  dicts = Rf_allocVector(VECSXP, Rf_length(df));
  R_PreserveObject(dicts);
  dicts_from = Rf_allocVector(INTSXP, Rf_length(df));
  R_PreserveObject(dicts_from);

  R_xlen_t nc = INTEGER(dim)[1];
  write_minmax_values = LOGICAL(rf_get_list_element(options, "write_minmax_values"))[0];
  is_minmax_supported = std::vector<bool>(nc, false);
  has_minmax_value.resize(nc);
  min_values.resize(nc);
  max_values.resize(nc);

  // root schema element is already there
  for (R_xlen_t idx = 0; idx < nc; idx++) {
    parquet::SchemaElement &sel = schema[idx+1];

    // TODO: DRY
    if (!write_minmax_values) {
      // nothing to do
    } if (sel.__isset.logicalType) {
      parquet::LogicalType &lt = sel.logicalType;
      is_minmax_supported[idx] = lt.__isset.DATE || lt.__isset.INTEGER ||
        lt.__isset.TIME || lt.__isset.STRING || lt.__isset.ENUM ||
        lt.__isset.JSON || lt.__isset.BSON || lt.__isset.TIMESTAMP;
      // TODO: support the rest
      // is_minmax_supported[idx] = lt.__isset.UUID ||
      //   lt.__isset.DECIMAL || lt.isset.FLOAT16;
    } else {
      switch(sel.type) {
      // case parquet::Type::BOOLEAN:
      case parquet::Type::INT32:
      case parquet::Type::INT64:
      case parquet::Type::FLOAT:
      case parquet::Type::DOUBLE:
      // case parquet::Type::BYTE_ARRAY;
      // case parquet::Type::FIXED_LEN_BYTE_ARRAY;
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
}

void RParquetOutFile::write() {
  ParquetOutFile::write();
}

void RParquetOutFile::append() {
  ParquetOutFile::append();
}
