#include "lib/nanoparquet.h"
#include "r-nanoparquet.h"
#include "unwind.h"

using namespace nanoparquet;

#define NUMERIC_SCALAR(x) \
  (TYPEOF(x) == INTSXP ? INTEGER(x)[0] : REAL(x)[0])

extern SEXP nanoparquet_call;

void r_to_logical_type(SEXP logical_type, parquet::SchemaElement &sel) {
  // first get everything from the R API
  const char *ctype;
  R_xlen_t lt_len;
  bool is_lt1_null = false;
  bool is_decimal = false;
  bool is_time = false;
  bool is_timestamp = false;
  bool is_int = false;
  double scale, precision;
  int is_utc;
  const char *unit;
  double bit_width;
  int is_signed;
  bool is_unit_millis = false;
  bool is_unit_micros = false;
  bool is_unit_nanos = false;
  r_call([&] {
    ctype = CHAR(STRING_ELT(VECTOR_ELT(logical_type, 0), 0));
    lt_len = Rf_length(logical_type);
    is_lt1_null = lt_len > 1 ? Rf_isNull(VECTOR_ELT(logical_type, 1)) : 1;
    is_decimal = !strcmp(ctype, "DECIMAL");
    if (is_decimal) {
      if (lt_len != 3) {
        Rf_errorcall(
          nanoparquet_call,
          "Parquet decimal logical type needs scale and precision"
        );
      }
      if (!is_lt1_null) {
        scale = NUMERIC_SCALAR(VECTOR_ELT(logical_type, 1));
      }
      precision = NUMERIC_SCALAR(VECTOR_ELT(logical_type, 2));
    }
    is_time = !strcmp(ctype, "TIME");
    is_timestamp = !strcmp(ctype, "TIMESTAMP");
    if (is_time || is_timestamp) {
      if (lt_len != 3) {
        Rf_errorcall(
          nanoparquet_call,
          "%s logical type needs is_adjusted_utc and unit.", ctype
        );
      }
      is_utc = LOGICAL(VECTOR_ELT(logical_type, 1))[0];
      unit = CHAR(STRING_ELT(VECTOR_ELT(logical_type, 2), 0));
      if (!strcmp(unit, "MILLIS")) {
        is_unit_millis = true;
      } else if (!strcmp(unit, "MICROS")) {
        is_unit_micros = true;
      } else if (!strcmp(unit, "NANOS")) {
        is_unit_nanos = true;
      } else {
        Rf_errorcall(
          nanoparquet_call,
          "Unknown %s time unit: %s", ctype, unit
        );
      }
    }
    is_int = !strcmp(ctype, "INT") || !strcmp(ctype, "INTEGER");
    if (is_int) {
      if (lt_len != 3) {
        Rf_errorcall(
          nanoparquet_call,
          "Parquet integer logical type needs bit width and signedness"
        );
      }
      bit_width = NUMERIC_SCALAR(VECTOR_ELT(logical_type, 1));
      is_signed = LOGICAL(VECTOR_ELT(logical_type, 2))[0];
    }
  });

  // extracted everything, no more R API calls now
  parquet::LogicalType lt;
  if (!strcmp(ctype, "STRING")) {
    lt.__set_STRING(parquet::StringType());
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "ENUM")) {
    lt.__set_ENUM(parquet::EnumType());
    sel.__set_logicalType(lt);

  } else if (is_decimal) {
    parquet::DecimalType dt;
    if (!is_lt1_null) {
      dt.__set_scale(scale);
      sel.__set_scale(dt.scale);
    }
    dt.__set_precision(precision);
    sel.__set_precision(dt.precision);
    lt.__set_DECIMAL(dt);
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "DATE")) {
    lt.__set_DATE(parquet::DateType());
    sel.__set_logicalType(lt);

  } else if (is_time) {
    parquet::TimeUnit tu;
    parquet::TimeType tt;
    tt.__set_isAdjustedToUTC(is_utc);
    if (is_unit_millis) {
      tu.__set_MILLIS(parquet::MilliSeconds());
    } else if (is_unit_micros) {
      tu.__set_MICROS(parquet::MicroSeconds());
    } else {
      tu.__set_NANOS(parquet::NanoSeconds());
    }
    tt.__set_unit(tu);
    lt.__set_TIME(tt);
    sel.__set_logicalType(lt);

  } else if (is_timestamp) {
    parquet::TimeUnit tu;
    parquet::TimestampType tt;
    tt.__set_isAdjustedToUTC(is_utc);
    if (is_unit_millis) {
      tu.__set_MILLIS(parquet::MilliSeconds());
    } else if (is_unit_micros) {
      tu.__set_MICROS(parquet::MicroSeconds());
    } else {
      tu.__set_NANOS(parquet::NanoSeconds());
    }
    tt.__set_unit(tu);
    lt.__set_TIMESTAMP(tt);
    sel.__set_logicalType(lt);

  } else if (is_int) {
    parquet::IntType it;
    it.__set_bitWidth(bit_width);
    it.__set_isSigned(is_signed);
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
    // this is ok, it is rare, and only happens on error
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Unknown Parquet logical type: %s", ctype
      );
    });
  }
}

void nanoparquet_map_to_parquet_type(
  SEXP x,
  SEXP options,
  parquet::SchemaElement &sel,
  std::string &rtype) {

  // to minimize R API calls, we do all this up front
  int rtypeof;
  bool is_factor = false;
  bool is_date = false;
  bool is_hms = false;
  bool is_posixct = false;
  bool is_difftime = false;
  r_call([&] {
    rtypeof = TYPEOF(x);
    switch (rtypeof) {
    case INTSXP:
      if (Rf_isFactor(x)) {
        is_factor = true;
      } else if (Rf_inherits(x, "Date")) {
        is_date = true;
      } else if (Rf_inherits(x, "hms")) {
        is_hms = true;
      }
      break;
    case REALSXP:
      if (Rf_inherits(x, "POSIXct")) {
        is_posixct = true;
      } else if (Rf_inherits(x, "hms")) {
        is_hms = true;
      } else if (Rf_inherits(x, "difftime")) {
        is_difftime = true;
      }
      break;
    default:
      break;
    }
  });

  switch (rtypeof) {
  case INTSXP: {
    if (is_factor) {
      rtype = "factor";
      parquet::StringType st;
      parquet::LogicalType lt;
      lt.__set_STRING(st);
      sel.__set_logicalType(lt);
      sel.__set_type(get_type_from_logical_type(lt));
    } else if (is_date) {
      rtype = "integer";
      parquet::DateType dt;
      parquet::LogicalType lt;
      lt.__set_DATE(dt);
      sel.__set_logicalType(lt);
      sel.__set_type(get_type_from_logical_type(lt));
    } else if (is_hms) {
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
    if (is_posixct) {
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

    } else if (is_hms) {
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

    } else if (is_difftime) {
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
    r_call([&] {
      Rf_errorcall(
        nanoparquet_call,
        "Cannot map %s to any Parquet type",
        type_names[rtypeof]
      );
    });
  }

  fill_converted_type_for_logical_type(sel);
}
