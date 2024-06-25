#include "RParquetReader.h"

int32_t *RParquetReader::allocate_int32(
  uint32_t column,
  uint32_t row_group,
  uint32_t page,      // 0 is the dictionary page
  uint32_t size)  {

}

 void RParquetReader::add_dictionary_int32(
  uint32_t column,
  uint32_t row_group,
  int32_t *dict,
  uint32_t dict_len) {

}

void RParquetReader::add_int32(
  uint32_t column,
  uint32_t row_group,
  int32_t *data,
  int32_t *present,
  uint64_t len,
  uint64_t from,
  uint64_t to) {

}

void RParquetReader::add_dict_indices(
  uint32_t column,
  uint32_t row_group,
  uint32_t *dict_idx,
  int32_t *present,
  uint64_t len,
  uint64_t from,
  uint64_t to) {

}
