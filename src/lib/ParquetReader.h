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
    offsets.resize(len);
    lengths.resize(len);
  }
  const char *buf;
  uint32_t len;
  uint32_t total_len;
  std::vector<uint32_t> offsets;
  std::vector<uint32_t> lengths;
};

struct BADictPage : public DictPage {
  BADictPage(ColumnChunk &cc_, uint32_t dict_len_, uint32_t total_len_)
    : DictPage(cc_, dict_len_), strs(dict_len_, total_len_) {
  }
  StringSet strs;
};

template <typename T>
struct DataPage {
public:
  uint32_t column;
  uint32_t row_group;
  uint32_t page;
  T *data;
  int32_t *present;
  uint32_t len;
  uint64_t from;
  uint64_t to;
  bool optional;
};

struct BADataPage: public DataPage<char*> {
  BADataPage(uint32_t column_, uint32_t row_group_, uint32_t page_,
             uint64_t len_, uint64_t from_, uint32_t total_len_,
             bool optional_)
    : strs(len_, total_len_) {
    column = column_;
    row_group = row_group_;
    page = page_;
    data = nullptr;
    present = nullptr;
    len = len_;
    from = from_;
    optional = optional_;
  }
  StringSet strs;
};

struct DictIndexPage {
  uint32_t column;
  uint32_t row_group;
  uint32_t page;
  uint32_t *dict_idx;
  int32_t *present;
  uint64_t len;
  uint64_t from;
  uint64_t to;
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

  virtual void add_dict_page(DictPage &dict) = 0;
  virtual void add_dict_index_page(DictIndexPage &idx) = 0;

  virtual void add_data_page_int32(DataPage<int32_t> &data) = 0;
  virtual void add_data_page_int64(DataPage<int64_t> &data) = 0;
  virtual void add_data_page_int96(DataPage<int96_t> &data) = 0;
  virtual void add_data_page_float(DataPage<float> &data) = 0;
  virtual void add_data_page_double(DataPage<double> &data) = 0;
  virtual void add_dict_page_byte_array(BADictPage &dict) = 0;
  virtual void add_data_page_byte_array(BADataPage &dict) = 0;

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
    uint32_t num_values,
    bool optional
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
    uint32_t num_values,
    bool optional
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
    uint32_t num_values,
    bool optional
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
    uint32_t num_values,
    bool optional
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
    uint32_t num_values,
    bool optional
  );

  void read_data_page_byte_array(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len,
    parquet::Encoding::type encoding,
    uint32_t num_values,
    bool optional
  );

  void read_data_page_fixed_len_byte_array(
    uint32_t column,
    uint32_t row_group,
    parquet::SchemaElement &sel,
    uint32_t page,
    uint64_t from,
    parquet::PageHeader &ph,
    const char *buf,
    int32_t len,
    parquet::Encoding::type encoding,
    uint32_t num_values,
    bool optional
  );

  void scan_byte_array_plain(StringSet &strs, const char *buf);
  void scan_fixed_len_byte_array_plain(StringSet &strs, const char *buf, uint32_t len);
};

} // namespace nanoparquet
