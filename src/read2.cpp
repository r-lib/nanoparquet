#include <Rdefines.h>
#include "RParquetReader.h"

extern "C" {

SEXP nanoparquet_read2(SEXP filesxp) {
  std::string fname((char *)CHAR(STRING_ELT(filesxp, 0)));
  RParquetReader reader(fname);

  reader.read_all_columns();
  reader.convert_columns_to_r();
  reader.handle_missing();
  reader.decode_dicts();
  reader.rbind_row_groups();

  return reader.columns;
}

} // extern "C"
