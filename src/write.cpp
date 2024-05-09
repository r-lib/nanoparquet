#include "lib/miniparquet.h"

#include <Rdefines.h>

using namespace miniparquet;
using namespace std;

class RParquetOutFile : public ParquetOutFile {
public:
  RParquetOutFile(
    std::string filename,
    parquet::format::CompressionCodec::type codec
  );
  void write_int32(std::ostream &file, uint32_t idx);
  void write_double(std::ostream &file, uint32_t idx);
  void write_byte_array(std::ostream &file, uint32_t idx);
  uint32_t get_size_byte_array(uint32_t idx);
  void write_boolean(std::ostream &file, uint32_t idx);

  void write(SEXP dfsxp, SEXP dim);

private:
  SEXP df = R_NilValue;
};

RParquetOutFile::RParquetOutFile(
  std::string filename,
  parquet::format::CompressionCodec::type codec
) : ParquetOutFile(filename, codec) {
  // nothing to do here
}

void RParquetOutFile::write_int32(std::ostream &file, uint32_t idx) {
  SEXP col = VECTOR_ELT(df, idx);
  R_xlen_t len = XLENGTH(col);
  file.write((const char *)INTEGER(col), sizeof(int) * len);
}

void RParquetOutFile::write_double(std::ostream &file, uint32_t idx) {
  SEXP col = VECTOR_ELT(df, idx);
  R_xlen_t len = XLENGTH(col);
  file.write((const char *)REAL(col), sizeof(double) * len);
}

void RParquetOutFile::write_byte_array(std::ostream &file, uint32_t idx) {
  SEXP col = VECTOR_ELT(df, idx);
  R_xlen_t len = XLENGTH(col);
  for (R_xlen_t i = 0; i < len; i++) {
    const char *c = CHAR(STRING_ELT(col, i));
    uint32_t len1 = strlen(c);
    file.write((const char *)&len1, 4);
    file.write(c, len1);
  }
}

uint32_t RParquetOutFile::get_size_byte_array(uint32_t idx) {
  SEXP col = VECTOR_ELT(df, idx);
  R_xlen_t len = XLENGTH(col);
  uint32_t size = len * 4; // 4 bytes of length for each CHARSXP
  for (R_xlen_t i = 0; i < len; i++) {
    const char *c = CHAR(STRING_ELT(col, i));
    size += strlen(c);
  }
  return size;
}

void RParquetOutFile::write_boolean(std::ostream &file, uint32_t idx) {
  // TODO: this is a very inefficient implementation
  SEXP col = VECTOR_ELT(df, idx);
  R_xlen_t len = XLENGTH(col);
  int *p = LOGICAL(col);
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

void RParquetOutFile::write(SEXP dfsxp, SEXP dim) {
  df = dfsxp;
  SEXP nms = Rf_getAttrib(dfsxp, R_NamesSymbol);
  R_xlen_t nr = INTEGER(dim)[0];
  set_num_rows(nr);
  R_xlen_t nc = INTEGER(dim)[1];
  for (R_xlen_t idx = 0; idx < nc; idx++) {
    SEXP col = VECTOR_ELT(dfsxp, idx);
    int rtype = TYPEOF(col);
    switch (rtype) {
    case INTSXP: {
      parquet::format::IntType it;
      it.__set_isSigned(true);
      it.__set_bitWidth(32);
      parquet::format::LogicalType logical_type;
      logical_type.__set_INTEGER(it);
      schema_add_column(CHAR(STRING_ELT(nms, idx)), logical_type);
      break;
    }
    case REALSXP: {
      parquet::format::Type::type type = parquet::format::Type::DOUBLE;
      schema_add_column(CHAR(STRING_ELT(nms, idx)), type);
      break;
    }
    case STRSXP: {
      parquet::format::StringType st;
      parquet::format::LogicalType logical_type;
      logical_type.__set_STRING(st);
      schema_add_column(CHAR(STRING_ELT(nms, idx)), logical_type);
      break;
    }
    case LGLSXP: {
      parquet::format::Type::type type = parquet::format::Type::BOOLEAN;
      schema_add_column(CHAR(STRING_ELT(nms, idx)), type);
      break;
    }
    default:
      throw runtime_error("Uninmplemented R type");
    }
  }

  ParquetOutFile::write();
}

extern "C" {

SEXP miniparquet_write(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression) {

  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("miniparquet_write: filename must be a string");
  }

  int c_compression = INTEGER(compression)[0];
  parquet::format::CompressionCodec::type codec;
  switch(c_compression) {
    case 0:
      codec = parquet::format::CompressionCodec::UNCOMPRESSED;
      break;
    case 1:
      codec = parquet::format::CompressionCodec::SNAPPY;
      break;
    default:
      Rf_error("Invalid compression type code: %d", c_compression);
      break;
  }

  char error_buffer[8192];
  error_buffer[0] = '\0';

  try {
    char *fname = (char *)CHAR(STRING_ELT(filesxp, 0));
    RParquetOutFile of(fname, codec);
    of.write(dfsxp, dim);
    return R_NilValue;

  } catch (std::exception &ex) {
    strncpy(error_buffer, ex.what(), sizeof(error_buffer) - 1);
  }

  if (error_buffer[0] != '\0') {
    Rf_error("%s", error_buffer);
  }

  // never reached
  return R_NilValue;
}

} // extern "C"