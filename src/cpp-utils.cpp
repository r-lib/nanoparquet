#include <Rdefines.h>
#include "r-nanoparquet.h"
#include "lib/nanoparquet.h"

using namespace nanoparquet;

#define NUMERIC_SCALAR(x) \
  (TYPEOF(x) == INTSXP ? INTEGER(x)[0] : REAL(x)[0])

extern SEXP nanoparquet_call;

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
