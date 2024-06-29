#pragma once
#include <Rdefines.h>

#include "parquet/parquet_types.h"
#include "lib/ParquetReader.h"

using namespace nanoparquet;

class RParquetReader : public ParquetReader {
public:
  RParquetReader(std::string filename);
  ~RParquetReader();

  void convert_columns_to_r();

  void add_dict_page(DictPage &dict);
  void add_dict_index_page(DictIndexPage &idx);
  void add_data_page(DataPage &data);
  void add_dict_page_byte_array(BADictPage &dict);
  void add_data_page_byte_array(BADataPage &data);

  SEXP columns = R_NilValue;
  SEXP metadata = R_NilValue;

protected:
  void convert_int64_to_double(SEXP x);
  void convert_int96_to_double(SEXP x, uint32_t idx);
  void convert_float_to_double(SEXP x);
};
