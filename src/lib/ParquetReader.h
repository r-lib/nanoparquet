#include <fstream>
#include <vector>

#include "parquet/parquet_types.h"
#include "bytebuffer.h"

namespace nanoparquet {

struct int96_t {
  int32_t value[3];
};

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

  virtual void add_dict_page_int32(
    uint32_t column,
    uint32_t row_group,
    int32_t **dict,
    uint32_t dict_len
  ) = 0;

  virtual void add_data_page_int32(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    int32_t **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  ) = 0;

  virtual void add_dict_page_int64(
    uint32_t column,
    uint32_t row_group,
    int64_t **dict,
    uint32_t dict_len
  ) = 0;

  virtual void add_data_page_int64(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    int64_t **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  ) = 0;

  virtual void add_dict_page_int96(
    uint32_t column,
    uint32_t row_group,
    int96_t **dict,
    uint32_t dict_len
  ) = 0;

  virtual void add_data_page_int96(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    int96_t **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  ) = 0;

  virtual void add_dict_page_float(
    uint32_t column,
    uint32_t row_group,
    float **dict,
    uint32_t dict_len
  ) = 0;

  virtual void add_data_page_float(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    float **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  ) = 0;

  virtual void add_dict_page_double(
    uint32_t column,
    uint32_t row_group,
    double **dict,
    uint32_t dict_len
  ) = 0;

  virtual void add_data_page_double(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    double **data,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  ) = 0;

  virtual void add_dict_indices(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    uint32_t **dict_idx,
    int32_t **present,
    uint64_t len,
    uint64_t from,
    uint64_t to
  ) = 0;

protected:
  enum parquet_input_type file_type_;
  std::string filename_;
  std::ifstream pfile;

  parquet::FileMetaData file_meta_data_;
  bool has_file_meta_data_;
  std::vector<int> leaf_cols; // map schema columns to leaf columns in chunks

  ByteBuffer tmp_buf;

  void init_file_on_disk();
  void check_meta_data();

  void read_column_chunk(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    parquet::ColumnChunk &cc
  );

  void read_dict_page(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len
  );

  uint32_t read_data_page(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len
  );

  void read_data_page_rle(
    uint32_t column,
    uint32_t row_group,
    uint32_t page,
    uint64_t from,
    const char *buf,
    int32_t buflen,
    uint32_t num_values
  );

  void read_data_page_int32(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len,
    parquet::Encoding::type encoding,
    uint32_t num_values
  );

  void read_data_page_int64(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len,
    parquet::Encoding::type encoding,
    uint32_t num_values
  );

  void read_data_page_int96(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len,
    parquet::Encoding::type encoding,
    uint32_t num_values
  );

  void read_data_page_float(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len,
    parquet::Encoding::type encoding,
    uint32_t num_values
  );

  void read_data_page_double(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len,
    parquet::Encoding::type encoding,
    uint32_t num_values
  );

};

} // namespace nanoparquet
