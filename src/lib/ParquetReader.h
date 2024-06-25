#include <fstream>
#include <vector>

#include "parquet/parquet_types.h"
#include "bytebuffer.h"

namespace nanoparquet {

enum parquet_input_type {
  FILE_ON_DISK,
  MEMORY_BUFFER
};

class ParquetReader {
public:
  ParquetReader(std::string filename);

  // meta data
  const parquet::FileMetaData &get_file_meta_data() {
    if (has_file_meta_data_) {
      return file_meta_data_;
    } else {
      throw std::runtime_error("Parquet metadata has not been parsed yet");
    }
  }
  bool has_file_meta_data() {
    return has_file_meta_data_;
  }

  // read data
  void read_all_columns();
  void read_selected_columns(std::vector<uint32_t> sel);
  void read_column(uint32_t idx);

  // API to be implemented by the embedding software

  virtual int32_t *allocate_int32(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    uint32_t size
  ) = 0;

  virtual void add_dictionary_int32(
    uint32_t column,
    uint32_t row_group,
    int32_t *dict,
    uint32_t dict_len
  ) = 0;

  virtual void add_int32(
    uint32_t column,
    uint32_t row_group,
    int32_t *data,
    int32_t *present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  ) = 0;

  virtual void add_dict_indices(
    uint32_t column,
    uint32_t row_group,
    uint32_t *dict_idx,
    int32_t *present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  ) = 0;

private:
  enum parquet_input_type file_type_;
  std::string filename_;
  std::ifstream pfile;

  parquet::FileMetaData file_meta_data_;
  bool has_file_meta_data_;
  std::vector<int> leaf_cols; // map schema columns to leaf columns in chunks

  ByteBuffer tmp_buf;

  void init_file_on_disk();
  void check_meta_data();

  void read_column_int32(uint32_t column);
  void read_column_chunk_int32(
    uint32_t column,
    uint32_t row_group,
    parquet::ColumnChunk &cc,
    uint64_t from
  );
  void read_dict_page_int32(uint32_t column, uint32_t row_group, parquet::PageHeader &ph, const char *buf, int32_t len);
  void read_data_page_int32(uint32_t column, uint32_t row_group, uint32_t page, uint64_t from, parquet::PageHeader &ph, const char *buf, int32_t len);
};

} // namespace nanoparquet
