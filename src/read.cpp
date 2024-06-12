#include <cmath>
#include <iostream>

#include "lib/nanoparquet.h"
#undef ERROR
#include <Rdefines.h>
#include "protect.h"

using namespace nanoparquet;
using namespace std;

// surely they are joking
constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;
constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

static int64_t impala_timestamp_to_nanoseconds(const Int96 &impala_timestamp) {
  int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;

  int64_t nanoseconds;
  memcpy(&nanoseconds, impala_timestamp.value, sizeof(nanoseconds));
  return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

extern "C" {

SEXP nanoparquet_read(SEXP filesxp) {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("nanoparquet_read: Need single filename parameter");
  }

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();

  // parse the query and transform it into a set of statements
  char *fname = (char *)CHAR(STRING_ELT(filesxp, 0));

  ParquetFile f(fname);

  // check if we are able to read this file
  f.read_checks();

  // allocate vectors

  auto ncols = f.columns.size();
  auto nrows = f.nrow;

  SEXP retlist = PROTECT(safe_allocvector_vec(ncols, &uwtoken));
  SEXP names = PROTECT(safe_allocvector_str(ncols, &uwtoken));
  SET_NAMES(retlist, names);
  UNPROTECT(1); // names

  SEXP dicts = PROTECT(safe_allocvector_vec(ncols, &uwtoken));
  SEXP types = PROTECT(safe_allocvector_int(ncols, &uwtoken));

  // we sometimes need to multiply TIME and TIMESTAMP data to convert
  // to seconds from NANOS or MILLIS
  unique_ptr<int[]> time_factors(new int[ncols]);
  for (auto i = 0; i < ncols; i++) {
    time_factors[i] = 1;
  }

  for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
    SEXP varname =
        PROTECT(safe_mkchar_utf8(f.columns[col_idx]->name.c_str(), &uwtoken));
    SET_STRING_ELT(names, col_idx, varname);
    UNPROTECT(1); // varname

    INTEGER(types)[col_idx] = f.columns[col_idx]->type;

    SEXP varvalue = NULL;
    switch (f.columns[col_idx]->type) {
    case parquet::format::Type::BOOLEAN:
      varvalue = PROTECT(safe_allocvector_lgl(nrows, &uwtoken));
      break;
    case parquet::format::Type::INT32: {
      varvalue = PROTECT(safe_allocvector_int(nrows, &uwtoken));
      auto &s_ele = f.columns[col_idx]->schema_element;
      if ((s_ele->__isset.logicalType &&
           s_ele->logicalType.__isset.DATE) ||
          (s_ele->__isset.converted_type &&
           s_ele->converted_type == parquet::format::ConvertedType::DATE)) {
        SEXP cl = PROTECT(safe_mkstring("Date", &uwtoken));
        SET_CLASS(varvalue, cl);
        UNPROTECT(1);
      } else if ((s_ele->__isset.logicalType &&
                  s_ele->logicalType.__isset.TIME &&
                  s_ele->logicalType.TIME.unit.__isset.MILLIS) ||
                 (s_ele->__isset.converted_type &&
                  s_ele->converted_type == parquet::format::ConvertedType::TIME_MILLIS)) {
        // note: if not MILLIS and INT32, we'll read it as plain INT32
        SEXP cl = PROTECT(safe_allocvector_str(2, &uwtoken));
        SET_STRING_ELT(cl, 0, safe_mkchar("hms", &uwtoken));
        SET_STRING_ELT(cl, 1, safe_mkchar("difftime", &uwtoken));
        SET_CLASS(varvalue, cl);
        safe_setattrib(varvalue, Rf_install("units"), safe_mkstring("secs", &uwtoken), &uwtoken);
        UNPROTECT(1);
      }
      break;
    }
    case parquet::format::Type::INT64:
    case parquet::format::Type::DOUBLE:
    case parquet::format::Type::FLOAT: {
      varvalue = PROTECT(safe_allocvector_real(nrows, &uwtoken));
      auto &s_ele = f.columns[col_idx]->schema_element;
      if ((s_ele->__isset.logicalType &&
           s_ele->logicalType.__isset.TIMESTAMP &&
           (s_ele->logicalType.TIMESTAMP.unit.__isset.MILLIS ||
            s_ele->logicalType.TIMESTAMP.unit.__isset.MICROS ||
            s_ele->logicalType.TIMESTAMP.unit.__isset.NANOS)) ||
          (s_ele->__isset.converted_type &&
           (s_ele->converted_type == parquet::format::ConvertedType::TIMESTAMP_MILLIS ||
            s_ele->converted_type == parquet::format::ConvertedType::TIMESTAMP_MICROS))) {
        if (s_ele->__isset.logicalType &&
            s_ele->logicalType.__isset.TIMESTAMP) {
          if (s_ele->logicalType.TIMESTAMP.unit.__isset.MILLIS) {
            time_factors[col_idx] = 1;
          } else if (s_ele->logicalType.TIMESTAMP.unit.__isset.MICROS) {
            time_factors[col_idx] = 1000;
          } else if (s_ele->logicalType.TIMESTAMP.unit.__isset.NANOS) {
            time_factors[col_idx] = 1000 * 1000;
          }
        } else if (s_ele->__isset.converted_type) {
          if (s_ele->converted_type == parquet::format::ConvertedType::TIMESTAMP_MILLIS) {
            time_factors[col_idx] = 1;
          } else if (s_ele->converted_type == parquet::format::ConvertedType::TIMESTAMP_MICROS) {
            time_factors[col_idx] = 1000;
          }
        }
        SEXP cl = PROTECT(safe_allocvector_str(2, &uwtoken));
        SET_STRING_ELT(cl, 0, safe_mkchar("POSIXct", &uwtoken));
        SET_STRING_ELT(cl, 1, safe_mkchar("POSIXt", &uwtoken));
        SET_CLASS(varvalue, cl);
        // only set UTC if no logical type, or logical type is UTC
        if (!s_ele->__isset.logicalType ||
            (s_ele->logicalType.__isset.TIMESTAMP &&
             s_ele->logicalType.TIMESTAMP.isAdjustedToUTC)) {
          safe_setattrib(varvalue, Rf_install("tzone"), safe_mkstring("UTC", &uwtoken), &uwtoken);
        }
        UNPROTECT(1);
      } else if ((s_ele->__isset.logicalType &&
                  s_ele->logicalType.__isset.TIME &&
                  (s_ele->logicalType.TIME.unit.__isset.MICROS ||
                     s_ele->logicalType.TIME.unit.__isset.NANOS)) ||
                   (s_ele->__isset.converted_type &&
                    s_ele->converted_type == parquet::format::ConvertedType::TIME_MICROS)) {
        // can be MICROS or NANOS currently, other values read as INT64
        if (s_ele->__isset.logicalType &&
            s_ele->logicalType.__isset.TIME) {
          if (s_ele->logicalType.TIME.unit.__isset.MICROS) {
            time_factors[col_idx] = 1000;
          } else if (s_ele->logicalType.TIME.unit.__isset.NANOS) {
            time_factors[col_idx] = 1000 * 1000;
          }
        } else if (s_ele->converted_type == parquet::format::ConvertedType::TIME_MICROS) {
          time_factors[col_idx] = 1000;
        }
        SEXP cl = PROTECT(safe_allocvector_str(2, &uwtoken));
        SET_STRING_ELT(cl, 0, safe_mkchar("hms", &uwtoken));
        SET_STRING_ELT(cl, 1, safe_mkchar("difftime", &uwtoken));
        SET_CLASS(varvalue, cl);
        safe_setattrib(varvalue, Rf_install("units"), safe_mkstring("secs", &uwtoken), &uwtoken);
        UNPROTECT(1);
      }
      break;
    }
    case parquet::format::Type::INT96: {
      varvalue = PROTECT(safe_allocvector_real(nrows, &uwtoken));
      SEXP cl = PROTECT(safe_allocvector_str(2, &uwtoken));
      SET_STRING_ELT(cl, 0, PROTECT(safe_mkchar("POSIXct", &uwtoken)));
      SET_STRING_ELT(cl, 1, PROTECT(safe_mkchar("POSIXt", &uwtoken)));
      SET_CLASS(varvalue, cl);
      safe_setattrib(varvalue, Rf_install("tzone"),
                   PROTECT(safe_mkstring("UTC", &uwtoken)), &uwtoken);
      UNPROTECT(4);
      break;
    }
    case parquet::format::Type::BYTE_ARRAY:
    case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: { // oof
      auto &s_ele = f.columns[col_idx]->schema_element;
      // STRIGN, ENUM, UUID, UTF8 are read as strings
      if ((s_ele->__isset.logicalType &&
           (s_ele->logicalType.__isset.STRING ||
            s_ele->logicalType.__isset.ENUM ||
            s_ele->logicalType.__isset.UUID)) ||
          (s_ele->__isset.converted_type &&
           s_ele->converted_type == parquet::format::ConvertedType::UTF8)) {
        varvalue = PROTECT(safe_allocvector_str(nrows, &uwtoken));
      } else if (s_ele->__isset.converted_type &&
                 s_ele->converted_type == parquet::format::ConvertedType::DECIMAL) {
        // DECIMAL converted type as REAL, for now
        varvalue = PROTECT(safe_allocvector_real(nrows, &uwtoken));
      } else {
        // list of RAW vectors
        varvalue = PROTECT(safe_allocvector_vec(nrows, &uwtoken));
      }
      break;
    }
    default:
      auto it = parquet::format::_Type_VALUES_TO_NAMES.find(
          f.columns[col_idx]->type);
      string msg = string("nanoparquet_read: Unknown column type ") +
        it->second + " @ " __FILE__ ":" STR(__LINE__) " (" + __func__ + ")";
      throw msg;
    }
    SET_VECTOR_ELT(retlist, col_idx, varvalue);
    UNPROTECT(1); /* varvalue */
  }

  // at this point retlist, dictm uwtoken, are the only protected SEXPs

  ResultChunk rc;
  ScanState s;

  f.initialize_result(rc);
  uint64_t dest_offset = 0;

  while (f.scan(s, rc)) {
    for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
      int time_factor = time_factors[col_idx];
      auto &col = rc.cols[col_idx];
      SEXP dest = VECTOR_ELT(retlist, col_idx);
      // if it is a string with a dictionary, then store the dictionary
      // so we can recover missing factor levels.
      if (col.dict && TYPEOF(dest) == STRSXP) {
        auto strings = col.dict->dict;
        SEXP rd = PROTECT(safe_allocvector_str(strings.size(), &uwtoken));
        for (auto i = 0; i < strings.size(); i++) {
          SET_STRING_ELT(rd, i, safe_mkchar_utf8(strings[i].second, &uwtoken));
        }
        SET_VECTOR_ELT(dicts, col_idx, rd);
        UNPROTECT(1);
        col.dict.reset();
      }

      for (uint64_t row_idx = 0; row_idx < rc.nrows; row_idx++) {
        if (!col.defined.ptr[row_idx]) {

          // NULLs
          switch (f.columns[col_idx]->type) {
          case parquet::format::Type::BOOLEAN:
            LOGICAL_POINTER(dest)[row_idx + dest_offset] = NA_LOGICAL;
            break;
          case parquet::format::Type::INT32:
            INTEGER_POINTER(dest)[row_idx + dest_offset] = NA_INTEGER;
            break;
          case parquet::format::Type::INT64:
          case parquet::format::Type::DOUBLE:
          case parquet::format::Type::FLOAT:
          case parquet::format::Type::INT96:
            NUMERIC_POINTER(dest)[row_idx + dest_offset] = NA_REAL;
            break;
          case parquet::format::Type::FIXED_LEN_BYTE_ARRAY:
          case parquet::format::Type::BYTE_ARRAY: {
            switch(TYPEOF(dest)) {
            case REALSXP:
              NUMERIC_POINTER(dest)[row_idx + dest_offset] = NA_REAL;
              break;
            case STRSXP:
              SET_STRING_ELT(dest, row_idx + dest_offset, NA_STRING);
              break;
            case VECSXP:
              // NULL already, nothing to do?
              SET_VECTOR_ELT(dest, row_idx + dest_offset, R_NilValue);
              break;
            default:
              string msg = string("nanoparquet_read: internal error, unexpected R type") +
                " @ " __FILE__ ":" STR(__LINE__) " (" + __func__ + ")";
              throw msg;
            }
            break;
          }
          default: {
            auto it = parquet::format::_Type_VALUES_TO_NAMES.find(
                f.columns[col_idx]->type);
            string msg = string("nanoparquet_read: Unknown column type ") +
              it->second + " @ " __FILE__ ":" STR(__LINE__) " (" +
              __func__ + ")";
            throw msg;
            }
          }
          continue;
        }

        switch (f.columns[col_idx]->type) {
        case parquet::format::Type::BOOLEAN:
          LOGICAL_POINTER(dest)
          [row_idx + dest_offset] = ((bool *)col.data.ptr)[row_idx];
          break;
        case parquet::format::Type::INT32:
          INTEGER_POINTER(dest)
          [row_idx + dest_offset] = ((int32_t *)col.data.ptr)[row_idx];
          break;
        case parquet::format::Type::INT64:
          NUMERIC_POINTER(dest)
          [row_idx + dest_offset] =
              (double)((int64_t *)col.data.ptr)[row_idx] / time_factor;
        break;
        case parquet::format::Type::DOUBLE:
          NUMERIC_POINTER(dest)
          [row_idx + dest_offset] = ((double *)col.data.ptr)[row_idx];
        break;
        case parquet::format::Type::FLOAT:
          NUMERIC_POINTER(dest)
          [row_idx + dest_offset] = (double)((float *)col.data.ptr)[row_idx];
          break;
        case parquet::format::Type::INT96:
          // it is further adjusted in the R code, so that we can have
          // the same adjustments as for TIMESTAMPs
          NUMERIC_POINTER(dest)
          [row_idx + dest_offset] = impala_timestamp_to_nanoseconds(
                                        ((Int96 *)col.data.ptr)[row_idx]) /
                                    1000000;
          break;

        case parquet::format::Type::FIXED_LEN_BYTE_ARRAY:
        case parquet::format::Type::BYTE_ARRAY: {
          auto &s_ele = f.columns[col_idx]->schema_element;
          switch(TYPEOF(dest)) {
          case REALSXP: {
            // this is a giant mess
            auto type_len = s_ele->type_length;
            auto bytes = ((char **)col.data.ptr)[row_idx];
            int64_t val = 0;
            for (auto i = 0; i < type_len; i++) {
              val = val << ((type_len - i) * 8) | (uint8_t)bytes[i];
            }

            NUMERIC_POINTER(dest)
              [row_idx + dest_offset] = val / pow(10.0, s_ele->scale);
            break;
          }
          case STRSXP:
            SET_STRING_ELT(
              dest, row_idx + dest_offset,
              safe_mkchar_utf8(((pair<uint32_t, char *>*)col.data.ptr)[row_idx].second, &uwtoken));
            break;
          case VECSXP: {
            uint32_t len;
            if (f.columns[col_idx]->type == parquet::format::Type::FIXED_LEN_BYTE_ARRAY) {
              len = f.columns[col_idx]->schema_element->type_length;
            } else {
              len = ((pair<uint32_t, char*>*) col.data.ptr)[row_idx].first;
            }
            SEXP bts = PROTECT(safe_allocvector_raw(len, &uwtoken));
            memcpy(RAW(bts), ((pair<uint32_t, char *>*)col.data.ptr)[row_idx].second, len);
            SET_VECTOR_ELT(dest, row_idx + dest_offset, bts);
            UNPROTECT(1);
            break;
          }
          default:
            string msg = string("nanoparquet_read: internal error, unexpected R type") +
              " @ " __FILE__ ":" STR(__LINE__) " (" + __func__ + ")";
            throw msg;
          break;
          }
        break;
        }
        default: {
          auto it = parquet::format::_Type_VALUES_TO_NAMES.find(
              f.columns[col_idx]->type);
          string msg = string("nanoparquet_read: Unknown column type ") +
            it->second + " @ " __FILE__ ":" STR(__LINE__) " (" + __func__ + ")";
          throw msg;
        }
        }
      }
    }
    dest_offset += rc.nrows;
  }
  assert(dest_offset == nrows);

  SEXP res = PROTECT(safe_allocvector_vec(3, &uwtoken));
  SET_VECTOR_ELT(res, 0, retlist);
  SET_VECTOR_ELT(res, 1, dicts);
  SET_VECTOR_ELT(res, 2, types);

  UNPROTECT(5); // + retlist, dicts, types, uwtoken
  return res;
  R_API_END();
}

} // extern "C"
