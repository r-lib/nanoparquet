#pragma once
#include <Rdefines.h>

#include "lib/ParquetReader.h"

using namespace nanoparquet;

class RParquetReader : public ParquetReader {
public:
  virtual int32_t *allocate_int32(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    uint32_t size
  ) ;

  virtual void add_dictionary_int32(
    uint32_t idx,
    uint32_t row_group,
    int32_t *dict,
    uint32_t dict_len
  );

  virtual void add_int32(
    uint32_t idx,
    uint32_t row_group,
    int32_t *data,
    int32_t *present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  );

  virtual void add_dict_indices(
    uint32_t idx,
    uint32_t row_group,
    uint32_t *dict_idx,
    int32_t *present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  );
};
