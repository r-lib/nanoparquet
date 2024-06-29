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

struct ColumnChunk {
public:
  parquet::ColumnChunk &cc;
  parquet::SchemaElement &sel;
  uint32_t column;
  uint32_t row_group;
};

struct DictPage {
public:
  DictPage(ColumnChunk &cc_, uint32_t dict_len_)
    : cc(cc_), dict_len(dict_len_) {
  }
  ColumnChunk& cc;
  uint8_t *dict = nullptr;
  uint32_t dict_len;
};

struct StringSet {
  StringSet(uint32_t len_, uint32_t total_len_)
    : buf(nullptr), len(len_), total_len(total_len_) {
  }
  char *buf;
  uint32_t len;
  uint32_t total_len;
  uint32_t *offsets = nullptr;
  uint32_t *lengths = nullptr;
};

struct BADictPage : public DictPage {
  BADictPage(ColumnChunk &cc_, uint32_t dict_len_, uint32_t total_len_)
    : DictPage(cc_, dict_len_), strs(dict_len_, total_len_) {
  }
  StringSet strs;
};

struct DataPage {
public:
  DataPage(ColumnChunk &cc_, parquet::PageHeader &ph_, uint32_t page_,
           uint32_t len_, uint32_t from_, bool optional_)
    : cc(cc_), ph(ph_), page(page_), data(nullptr), present(nullptr),
      len(len_), from(from_), optional(optional_), strs(len_, ph.compressed_page_size) {
    if (ph.__isset.data_page_header) {
      encoding = ph.data_page_header.encoding;
    } else {
      encoding = ph.data_page_header_v2.encoding;
    }
  }
  ColumnChunk &cc;
  parquet::PageHeader &ph;
  uint32_t page;
  uint8_t *data;
  int32_t *present;
  uint32_t len;
  uint64_t from;
  bool optional;
  parquet::Encoding::type encoding;
  StringSet strs;
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

  virtual void alloc_dict_page(DictPage &dict) = 0;
  virtual void alloc_dict_index_page(DataPage &idx) = 0;
  virtual void alloc_dict_page_byte_array(BADictPage &dict) = 0;
  virtual void alloc_data_page(DataPage &data) = 0;

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

  void read_column_chunk(ColumnChunk &cc);

  void read_dict_page(
    ColumnChunk &cc,
    parquet::PageHeader &ph,
    uint8_t *buf,
    int32_t len
  );

  uint32_t read_data_page(
    ColumnChunk &cc,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    uint8_t *buf,
    int32_t len
  );

  void read_data_page_rle(DataPage &dp, uint8_t *buf);

  void read_data_page_boolean(DataPage &dp, uint8_t *buf, int32_t len);
  void read_data_page_int32(DataPage &dp, uint8_t *buf, int32_t len);
  void read_data_page_int64(DataPage &dp, uint8_t *buf, int32_t len);
  void read_data_page_int96(DataPage &dp, uint8_t *buf, int32_t len);
  void read_data_page_float(DataPage &dp, uint8_t *buf, int32_t len);
  void read_data_page_double(DataPage &dp, uint8_t *buf, int32_t len);
  void read_data_page_byte_array(DataPage &dp, uint8_t *buf, int32_t len);
  void read_data_page_fixed_len_byte_array(DataPage &dp, uint8_t *buf, int32_t len);

  void unpack_plain_boolean(uint32_t *res, uint8_t *buf, uint32_t num_values);
  void scan_byte_array_plain(StringSet &strs, uint8_t *buf);
  void scan_fixed_len_byte_array_plain(StringSet &strs, uint8_t *buf, uint32_t len);
};

} // namespace nanoparquet
