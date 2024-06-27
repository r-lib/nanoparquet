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

  void add_dict_page_int32(
    uint32_t column,
    uint32_t row_group,
    int32_t **dict,
    uint32_t dict_len
  );

  void add_data_page_int32(
    uint32_t columns,
    uint32_t row_group,
    uint32_t page,
    int32_t **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  );

  void add_dict_page_int64(
    uint32_t column,
    uint32_t row_group,
    int64_t **dict,
    uint32_t dict_len
  );

  void add_data_page_int64(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    int64_t **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  );

  void add_dict_page_int96(
    uint32_t column,
    uint32_t row_group,
    int96_t **dict,
    uint32_t dict_len
  );

  void add_data_page_int96(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    int96_t **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  );

  void add_dict_page_double(
    uint32_t column,
    uint32_t row_group,
    double **dict,
    uint32_t dict_len
  );

  void add_data_page_double(
    uint32_t columns,
    uint32_t row_group,
    uint32_t page,
    double **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  );

  void add_dict_indices(
    uint32_t idx,
    uint32_t row_group,
    uint32_t page,
    uint32_t **dict_idx,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  );

  SEXP columns = R_NilValue;
  SEXP metadata = R_NilValue;

protected:
  void convert_int64_to_double(SEXP x);
  void convert_int96_to_double(SEXP x, uint32_t idx);
};
