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

  void alloc_dict_page(DictPage &dict);
  void alloc_dict_index_page(DataPage &idx);
  void alloc_dict_page_byte_array(BADictPage &dict);
  void alloc_data_page(DataPage &data);

  SEXP columns = R_NilValue;
  SEXP metadata = R_NilValue;

protected:
  void alloc_data_page_byte_array(DataPage &data);
  void convert_int64_to_double(SEXP x);
  void convert_int96_to_double(SEXP x, uint32_t idx);
  void convert_float_to_double(SEXP x);
};
