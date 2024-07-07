#include <Rdefines.h>
#include "RParquetReader.h"

extern "C" {

SEXP nanoparquet_read2(SEXP filesxp) {
  std::string fname((char *)CHAR(STRING_ELT(filesxp, 0)));
  RParquetReader reader(fname);

  reader.read_all_columns();
  reader.convert_columns_to_r();

  return reader.columns;
}

} // extern "C"
