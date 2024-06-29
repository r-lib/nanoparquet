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

  void add_data_page_int32(DataPage<int32_t> &data);
  void add_data_page_int64(DataPage<int64_t> &data);
  void add_data_page_int96(DataPage<int96_t> &data);
  void add_data_page_float(DataPage<float> &data);
  void add_data_page_double(DataPage<double> &data);
  void add_dict_page_byte_array(BADictPage &dict);
  void add_data_page_byte_array(BADataPage &dict);

  SEXP columns = R_NilValue;
  SEXP metadata = R_NilValue;

protected:
  void convert_int64_to_double(SEXP x);
  void convert_int96_to_double(SEXP x, uint32_t idx);
  void convert_float_to_double(SEXP x);
};
