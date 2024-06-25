#include <Rdefines.h>
#include "RParquetReader.h"

extern "C" {

SEXP nanoparquet_read2(SEXP filesxp) {
  std::string fname((char *)CHAR(STRING_ELT(filesxp, 0)));
  RParquetReader reader(fname);

  reader.read_all_columns();

  SEXP res = Rf_allocVector(VECSXP, 2);
  SET_VECTOR_ELT(res, 0, reader.metadata);
  SET_VECTOR_ELT(res, 1, reader.columns);
  return res;
}

} // extern "C"
