#include <Rdefines.h>
#include "RParquetReader.h"

extern "C" {

SEXP nanoparquet_read2(SEXP filesxp) {
  std::string fname((char *)CHAR(STRING_ELT(filesxp, 0)));
  RParquetReader reader(fname);

  reader.read_all_columns();
  reader.convert_columns_to_r();
  // reader.decode_dicts();

  SEXP res = PROTECT(Rf_allocVector(VECSXP, 3));
  SET_VECTOR_ELT(res, 0, reader.columns);
  SET_VECTOR_ELT(res, 1, reader.tmpdata);
  SET_VECTOR_ELT(res, 2, reader.present);
  UNPROTECT(1);
  return res;
}

} // extern "C"
