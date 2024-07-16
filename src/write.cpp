#include "lib/nanoparquet.h"
#include "lib/bitpacker.h"

#include <Rdefines.h>

#include "lib/memstream.h"

#include "protect.h"

using namespace nanoparquet;
using namespace std;

extern "C" {
SEXP nanoparquet_create_dict(SEXP x, SEXP rlen);
SEXP nanoparquet_create_dict_idx(SEXP x);
SEXP nanoparquet_avg_run_length(SEXP x, SEXP rlen);
}

class RParquetOutFile : public ParquetOutFile {
public:
  RParquetOutFile(
    std::string filename,
    parquet::CompressionCodec::type codec
  );
  RParquetOutFile(
    std::ostream &stream,
    parquet::CompressionCodec::type codec
  );
  void write_int32(std::ostream &file, uint32_t idx, uint64_t from,
                   uint64_t until);
  void write_int64(std::ostream &file, uint32_t idx, uint64_t from,
                   uint64_t until);
  void write_double(std::ostream &file, uint32_t idx, uint64_t from,
                    uint64_t until);
  void write_byte_array(std::ostream &file, uint32_t id, uint64_t from,
                        uint64_t until);
  uint32_t get_size_byte_array(uint32_t idx, uint32_t num_present,
                               uint64_t from, uint64_t until);
  void write_boolean(std::ostream &file, uint32_t idx, uint64_t from,
                     uint64_t until);
  void write_boolean_as_int(std::ostream &file, uint32_t idx,
                            uint64_t from, uint64_t until);

  uint32_t write_present(std::ostream &file, uint32_t idx, uint64_t from,
                         uint64_t until);
  void write_present_int32(std::ostream &file, uint32_t idx,
                           uint32_t num_present, uint64_t from, uint64_t until);
  void write_present_int64(std::ostream &file, uint32_t idx,
                           uint32_t num_present, uint64_t from, uint64_t until);
  void write_present_double(std::ostream &file, uint32_t idx,
                            uint32_t num_present, uint64_t from,
                            uint64_t until);
  void write_present_byte_array(std::ostream &file, uint32_t idx,
                                uint32_t num_present, uint64_t from,
                                uint64_t until);
  void write_present_boolean(std::ostream &file, uint32_t idx,
                             uint32_t num_present, uint64_t from,
                             uint64_t until);
  void write_present_boolean_as_int(std::ostream &file, uint32_t idx,
                                    uint32_t num_present, uint64_t from,
                                    uint64_t until);

  // for dictionaries
  uint32_t get_num_values_dictionary(uint32_t idx);
  uint32_t get_size_dictionary(uint32_t idx);
  void write_dictionary(std::ostream &file, uint32_t idx);
  void write_dictionary_indices(std::ostream &file, uint32_t idx,
                                uint64_t from, uint64_t until);

  void write(
    SEXP dfsxp,
    SEXP dim,
    SEXP metadata,
    SEXP rrequired,
    SEXP options,
    SEXP schema
  );

private:
  SEXP df = R_NilValue;
  SEXP required = R_NilValue;
  SEXP dicts = R_NilValue;
  ByteBuffer present;

  void create_dictionary(uint32_t idx);
  // for LGLSXP this mean RLE encoding
  bool should_use_dict_encoding(uint32_t idx);
};

RParquetOutFile::RParquetOutFile(
  std::string filename,
  parquet::CompressionCodec::type codec) :
    ParquetOutFile(filename, codec) {
}

RParquetOutFile::RParquetOutFile(
  std::ostream &stream,
  parquet::CompressionCodec::type codec
) : ParquetOutFile(stream, codec) {
}

void RParquetOutFile::create_dictionary(uint32_t idx) {
  // olny do it once
  if (!Rf_isNull(VECTOR_ELT(dicts, idx))) {
    return;
  }

  SEXP col = VECTOR_ELT(df, idx);
  SEXP d = PROTECT(nanoparquet_create_dict_idx(col));
  SET_VECTOR_ELT(dicts, idx, d);
  UNPROTECT(1);
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

void RParquetOutFile::write_int32(std::ostream &file, uint32_t idx,
                                  uint64_t from, uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  uint64_t len = until - from;
  file.write((const char *) (INTEGER(col) + from), sizeof(int) * len);
}

void RParquetOutFile::write_int64(std::ostream &file, uint32_t idx,
                                  uint64_t from, uint64_t until) {
  // This is double in R, so we need to convert
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  if (Rf_inherits(col, "POSIXct")) {
    // need to convert seconds to microseconds
    for (uint64_t i = from; i < until; i++) {
      int64_t el = REAL(col)[i] * 1000 * 1000;
      file.write((const char*) &el, sizeof(int64_t));
    }

  } else if (Rf_inherits(col, "difftime")) {
    // need to convert seconds to nanoseconds
    for (uint64_t i = from; i < until; i++) {
      int64_t el = REAL(col)[i] * 1000 * 1000 * 1000;
      file.write((const char*) &el, sizeof(int64_t));
    }

  } else {
    for (uint64_t i = from; i < until; i++) {
      int64_t el = REAL(col)[i];
      file.write((const char*) &el, sizeof(int64_t));
    }
  }
}

void RParquetOutFile::write_double(std::ostream &file, uint32_t idx,
                                   uint64_t from, uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  uint64_t len = until - from;
  file.write((const char *) (REAL(col) + from), sizeof(double) * len);
}

void RParquetOutFile::write_byte_array(std::ostream &file, uint32_t idx,
                                       uint64_t from, uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  for (uint64_t i = from; i < until; i++) {
    const char *c = CHAR(STRING_ELT(col, i));
    uint32_t len1 = strlen(c);
    file.write((const char *)&len1, 4);
    file.write(c, len1);
  }
}

uint32_t RParquetOutFile::get_size_byte_array(
  uint32_t idx,
  uint32_t num_present,
  uint64_t from,
  uint64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  uint32_t size = 0;
  for (uint64_t i = from; i < until; i++) {
    SEXP csxp = STRING_ELT(col, i);
    if (csxp != NA_STRING) {
      const char *c = CHAR(csxp);
      size += strlen(c) + 4;
    }
  }
  return size;
}

void write_boolean_impl(std::ostream &file, SEXP col,
                        uint64_t from, uint64_t until) {
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
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
                                    uint64_t from, uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  write_boolean_impl(file, col, from, until);
}

void RParquetOutFile::write_boolean_as_int(std::ostream &file,
                                           uint32_t idx,
                                           uint64_t from,
                                           uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  uint64_t len = until - from;
  file.write((const char *) (LOGICAL(col) + from), sizeof(int) * len);
}

uint32_t RParquetOutFile:: write_present(std::ostream &file, uint32_t idx,
                                         uint64_t from, uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
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
  default:
    throw runtime_error("Uninmplemented R type");
  }

  file.write(present.ptr, len * sizeof(int));

  return num_pres;
}

void RParquetOutFile::write_present_int32(
  std::ostream &file,
  uint32_t idx,
  uint32_t num_present,
  uint64_t from,
  uint64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  for (uint64_t i = from; i < until; i++) {
    int el = INTEGER(col)[i];
    if (el != NA_INTEGER) {
      file.write((const char*) &el, sizeof(int));
    }
  }
}

void RParquetOutFile::write_present_int64(
  std::ostream &file,
  uint32_t idx,
  uint32_t num_present,
  uint64_t from,
  uint64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  if (Rf_inherits(col, "POSIXct")) {
    for (uint64_t i = from; i < until; i++) {
      double el = REAL(col)[i];
      if (R_IsNA(el)) continue;
      int64_t el2 = el * 1000 * 1000;
      file.write((const char*) &el2, sizeof(int64_t));
    }
  } else if (Rf_inherits(col, "difftime")) {
    // need to convert seconds to nanoseconds
    for (uint64_t i = from; i < until; i++) {
      double el = REAL(col)[i];
      if (R_IsNA(el)) continue;
      int64_t el2 = el * 1000 * 1000 * 1000;
      file.write((const char*) &el2, sizeof(int64_t));
    }

  } else {
    for (uint64_t i = from; i < until; i++) {
      double el = REAL(col)[i];
      if (R_IsNA(el)) continue;
      int64_t el2 = el;
      file.write((const char*) &el2, sizeof(int64_t));
    }
  }
}

void RParquetOutFile::write_present_double(
  std::ostream &file,
  uint32_t idx,
  uint32_t num_present,
  uint64_t from,
  uint64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  for (uint64_t i = from; i < until; i++) {
    double el = REAL(col)[i];
    if (!R_IsNA(el)) {
      file.write((const char*) &el, sizeof(double));
    }
  }
}

void RParquetOutFile::write_present_boolean_as_int(std::ostream &file,
                                                   uint32_t idx,
                                                   uint32_t num_present,
                                                   uint64_t from,
                                                   uint64_t until) {
  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  for (uint64_t i = from; i < until; i++) {
    int el = LOGICAL(col)[i];
    if (el != NA_LOGICAL) {
      file.write((const char*) &el, sizeof(int));
    }
  }
}


void RParquetOutFile::write_present_byte_array(
  std::ostream &file,
  uint32_t idx,
  uint32_t num_present,
  uint64_t from,
  uint64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  for (uint64_t i = from; i < until; i++) {
    SEXP csxp = STRING_ELT(col, i);
    if (csxp != R_NaString) {
      const char *c = CHAR(csxp);
      uint32_t len1 = strlen(c);
      file.write((const char *)&len1, 4);
      file.write(c, len1);
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
  SEXP col2 = PROTECT(Rf_allocVector(LGLSXP, num_present));
  uint64_t i, o;
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
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
    uint32_t idx) {
  SEXP col = VECTOR_ELT(df, idx);
  if (Rf_inherits(col, "factor")) {
    return Rf_nlevels(col);
  } else {
    create_dictionary(idx);
    return Rf_length(VECTOR_ELT(VECTOR_ELT(dicts, idx), 0));
  }
}

uint32_t RParquetOutFile::get_size_dictionary(uint32_t idx) {
  SEXP col = VECTOR_ELT(df, idx);
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
      create_dictionary(idx);
      SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
      return Rf_xlength(dictidx) * sizeof(int);
    }
    break;
  }
  case REALSXP: {
    create_dictionary(idx);
    SEXP dict = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    return Rf_xlength(dict) * sizeof(double);
    break;
  }
  case STRSXP: {
    // need to count the length of the stings that are indexed in dict
    create_dictionary(idx);
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    R_xlen_t len = Rf_xlength(dictidx);
    uint32_t size = len * 4;
    int *beg = INTEGER(dictidx);
    int *end = beg + len;
    for (; beg < end; beg++) {
      const char *c = CHAR(STRING_ELT(col, *beg));
      size += strlen(c);
    }
    return size;
    break;
  }
  case LGLSXP: {
    create_dictionary(idx);
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    R_xlen_t l = Rf_xlength(dictidx);
    return l / 8 + (l % 8 > 0);
    break;
  }
  default:
    throw runtime_error("Uninmplemented R type");  // # nocov
  }
}

void RParquetOutFile::write_dictionary(
    std::ostream &file,
    uint32_t idx) {
  SEXP col = VECTOR_ELT(df, idx);
  switch (TYPEOF(col)) {
  case INTSXP: {
    if (Rf_inherits(col, "factor")) {
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
      SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
      int *icol = INTEGER(col);
      int *iidx = INTEGER(dictidx);
      int *idict = INTEGER(dict);
      for (auto i = 0; i < len; i++) {
        idict[i] = icol[iidx[i]];
      }
      file.write((const char*) idict, sizeof(int) * len);
      UNPROTECT(1);
    }
    break;
  }
  case REALSXP: {
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    R_xlen_t len = Rf_xlength(dictidx);
    double *icol = REAL(col);
    int *iidx = INTEGER(dictidx);
    if (Rf_inherits(col, "POSIXct")) {
      for (auto i = 0; i < len; i++) {
        int64_t el = icol[iidx[i]] * 1000 * 1000;
        file.write((const char*) &el, sizeof(int64_t));
      }
    } else if (Rf_inherits(col, "difftime")) {
      for (auto i = 0; i < len; i++) {
        int64_t el = icol[iidx[i]] * 1000 * 1000 * 1000;
        file.write((const char*) &el, sizeof(int64_t));
      }
    } else {
      SEXP dict = PROTECT(Rf_allocVector(REALSXP, len));
      double *idict = REAL(dict);
      for (auto i = 0; i < len; i++) {
        idict[i] = icol[iidx[i]];
      }
      file.write((const char*) idict, sizeof(double) * len);
      UNPROTECT(1);
    }
    break;
  }
  case STRSXP: {
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    R_xlen_t len = Rf_xlength(dictidx);
    int *iidx = INTEGER(dictidx);
    for (uint64_t i = 0; i < len; i++) {
      const char *c = CHAR(STRING_ELT(col, iidx[i]));
      uint32_t len1 = strlen(c);
      file.write((const char *)&len1, 4);
      file.write(c, len1);
    }
    break;
  }
  case LGLSXP: {
    SEXP dictidx = VECTOR_ELT(VECTOR_ELT(dicts, idx), 0);
    R_xlen_t len = Rf_xlength(dictidx);
    SEXP dict = PROTECT(Rf_allocVector(LGLSXP, len));
    int *icol = LOGICAL(col);
    int *iidx = INTEGER(dictidx);
    int *idict = LOGICAL(dict);
    for (auto i = 0; i < len; i++) {
      idict[i] = icol[iidx[i]];
    }
    write_boolean_impl(file, dict, 0, len);
    UNPROTECT(1);
    break;
  }
  default:
    throw runtime_error("Uninmplemented R type");  // # nocov
  }
}

void RParquetOutFile::write_dictionary_indices(
  std::ostream &file,
  uint32_t idx,
  uint64_t from,
  uint64_t until) {

  SEXP col = VECTOR_ELT(df, idx);
  if (until > Rf_xlength(col)) {
    Rf_error("Internal nanoparquet error, row index too large");
  }
  if (TYPEOF(col) == INTSXP && Rf_inherits(col, "factor")) {
    for (uint64_t i = from; i < until; i++) {
      int el = INTEGER(col)[i];
      if (el != NA_INTEGER) {
        el--;
        file.write((const char *) &el, sizeof(int));
      }
    }
  } else {
    SEXP dictmap = VECTOR_ELT(VECTOR_ELT(dicts, idx), 1);
    for (uint64_t i = from; i < until; i++) {
      int el = INTEGER(dictmap)[i];
      if (el != NA_INTEGER) {
        file.write((const char *) &el, sizeof(int));
      }
    }
  }
}

bool nanoparquet_map_to_parquet_type(
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
  default:
    return false;
  }

  fill_converted_type_for_logical_type(sel);
  return true;
}

#define NUMERIC_SCALAR(x) \
  (TYPEOF(x) == INTSXP ? INTEGER(x)[0] : REAL(x)[0])

bool r_to_logical_type(SEXP logical_type, parquet::SchemaElement &sel) {
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
      Rf_error("Parquet decimal logical type needs scale and precision");
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
      Rf_error("Unknown TIME time unit: %s", unit);
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
      Rf_error("Unknown TIME time unit: %s", unit);
    }
    tt.__set_unit(tu);
    lt.__set_TIMESTAMP(tt);
    sel.__set_logicalType(lt);

  } else if (!strcmp(ctype, "INT") || !strcmp(ctype, "INTEGER")) {
    parquet::IntType it;
    if (Rf_xlength(logical_type) != 3) {
      Rf_error("Parquet integer logical type needs bit width and signedness");
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
    Rf_error("Unknown Parquet logical type: %s", ctype);
  }

  return true;
}

void RParquetOutFile::write(
  SEXP dfsxp,
  SEXP dim,
  SEXP metadata,
  SEXP rrequired,
  SEXP options,
  SEXP schema) {

  df = dfsxp;
  required = rrequired;
  dicts = PROTECT(Rf_allocVector(VECSXP, Rf_length(df)));
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
      bool ok = nanoparquet_map_to_parquet_type(col, options, sel, rtypename);
      if (!ok) {
        throw runtime_error("Cannot write R column to Parquet");
      }
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
        bool ok = r_to_logical_type(VECTOR_ELT(logical_type, idx), sel);
        if (!ok) {
          throw runtime_error("Unknown logical type");
        }
      }
      if (scale[idx] != NA_INTEGER) {
        sel.__set_scale(scale[idx]);
      }
      if (precision[idx] != NA_INTEGER) {
        sel.__set_precision(precision[idx]);
      }
    }

    bool dict = should_use_dict_encoding(idx);
    schema_add_column(sel, dict);
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

  UNPROTECT(2);
}

extern "C" {

SEXP nanoparquet_write(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression,
  SEXP metadata,
  SEXP required,
  SEXP options,
  SEXP schema) {

  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("nanoparquet_write: filename must be a string"); // # nocov
  }

  int c_compression = INTEGER(compression)[0];
  parquet::CompressionCodec::type codec;
  switch(c_compression) {
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
      Rf_error("Invalid compression type code: %d", c_compression); // # nocov
      break;
  }

  char error_buffer[8192];
  error_buffer[0] = '\0';

  try {
    std::string fname = (char *) CHAR(STRING_ELT(filesxp, 0));
    if (fname == ":raw:") {
      MemStream ms;
      std::ostream &os = ms.stream();
      RParquetOutFile of(os, codec);
      of.write(dfsxp, dim, metadata, required, options, schema);
      R_xlen_t bufsize = ms.size();
      SEXP res = Rf_allocVector(RAWSXP, bufsize);
      ms.copy(RAW(res), bufsize);
      return res;
    } else {
      RParquetOutFile of(fname, codec);
      of.write(dfsxp, dim, metadata, required, options, schema);
      return R_NilValue;
    }
  } catch (std::exception &ex) {
    strncpy(error_buffer, ex.what(), sizeof(error_buffer) - 1); // # nocov
  }

  if (error_buffer[0] != '\0') {         // # nocov
    Rf_error("%s", error_buffer);        // # nocov
  }                                      // # nocov

  // never reached
  return R_NilValue; // # nocov
}

extern SEXP convert_logical_type(parquet::LogicalType ltype, SEXP *uwt);

SEXP nanoparquet_map_to_parquet_types(SEXP df, SEXP options) {
  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();
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
  R_API_END();
}

SEXP nanoparquet_logical_to_converted(SEXP logical_type) {
  const char *nms[] = { "converted_type", "scale", "precision", "" };
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, nms));
  SET_VECTOR_ELT(res, 0, Rf_ScalarInteger(NA_INTEGER));
  SET_VECTOR_ELT(res, 1, Rf_ScalarInteger(NA_INTEGER));
  SET_VECTOR_ELT(res, 2, Rf_ScalarInteger(NA_INTEGER));

  parquet::SchemaElement sel;
  if (!r_to_logical_type(logical_type, sel)) {
    // Unknown logucal type, do nothing
    UNPROTECT(1);
    return res;
  }

  bool err_ = false;
  char error_buffer_[8192];                                              \
  error_buffer_[0] = '\0';                                               \
  try {
    fill_converted_type_for_logical_type(sel);
  } catch(std::exception& ex) {                                          \
    strncpy(error_buffer_, ex.what(), sizeof(error_buffer_) - 1);        \
  } catch(std::string& ex) {                                             \
    strncpy(error_buffer_, ex.c_str(), sizeof(error_buffer_) - 1);       \
  } catch(...) {                                                         \
    Rf_error("nanoparquet error @ " __FILE__ ":" STR(__LINE__));         \
  }                                                                      \
  if (err_) {
    Rf_error("%s", error_buffer_);
  }

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
}

} // extern "C"
