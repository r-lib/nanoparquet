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
  ColumnChunk(parquet::ColumnChunk &cc, parquet::SchemaElement &sel,
              uint32_t column, uint32_t row_group, int64_t num_rows)
    : cc(cc), sel(sel), column(column), row_group(row_group),
      num_rows(num_rows) {
    parquet::ColumnMetaData cmd = cc.meta_data;
    has_dictionary = cmd.__isset.dictionary_page_offset;
    optional = sel.repetition_type !=
      parquet::FieldRepetitionType::REQUIRED;
  }
  parquet::ColumnChunk &cc;
  parquet::SchemaElement &sel;
  uint32_t column;
  uint32_t row_group;
  int64_t num_rows;
  bool has_dictionary;
  bool optional;
};

struct StringSet {
  StringSet()
    : buf(nullptr), len(0), total_len(0) {
  }
  StringSet(uint32_t len, uint32_t total_len)
    : buf(nullptr), len(len), total_len(total_len) {
  }
  char *buf;
  uint32_t len;
  uint32_t total_len;
  uint32_t *offsets = nullptr;
  uint32_t *lengths = nullptr;
};

struct DictPage {
public:
  DictPage(ColumnChunk &cc, parquet::PageHeader &ph, uint32_t dict_len)
    : cc(cc), ph(ph), dict_len(dict_len) {
  }
  DictPage(ColumnChunk &cc, parquet::PageHeader &ph, uint32_t dict_len,
           uint32_t total_len)
    : cc(cc), ph(ph), dict_len(dict_len), strs(dict_len, total_len) {
  }
  ColumnChunk& cc;
  parquet::PageHeader &ph;
  uint8_t *dict = nullptr;
  uint32_t dict_len;
  StringSet strs;
};

struct DataPage {
public:
  DataPage(ColumnChunk &cc, parquet::PageHeader &ph, uint32_t page,
           uint32_t from)
    : cc(cc), ph(ph), page(page), data(nullptr), present(nullptr),
      num_values(0), num_present(0), from(from) {
    if (ph.__isset.data_page_header) {
      encoding = ph.data_page_header.encoding;
    } else {
      encoding = ph.data_page_header_v2.encoding;
    }
    if (cc.sel.type == parquet::Type::BYTE_ARRAY ||
        cc.sel.type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      strs.len = num_present;
      strs.total_len = ph.uncompressed_page_size;
    }
  }
  void set_num_values(uint32_t num_values_) {
    num_values = num_values_;
    num_present = num_values_;
    strs.len = num_values_;
  }
  void set_num_present(uint32_t num_present_) {
    num_present = num_present_;
    strs.len = num_present_;
  }
  ColumnChunk &cc;
  parquet::PageHeader &ph;
  uint32_t page;
  uint8_t *data;
  uint8_t *present;
  uint32_t num_values;
  uint32_t num_present;
  uint64_t from;
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

  virtual void alloc_column_chunk(ColumnChunk &cc) = 0;
  virtual void alloc_dict_page(DictPage &dict) = 0;
  virtual void alloc_data_page(DataPage &data) = 0;

protected:
  enum parquet_input_type file_type_;
  std::string filename_;
  std::ifstream pfile;

  parquet::FileMetaData file_meta_data_;
  bool has_file_meta_data_;
  std::vector<int> leaf_cols; // map schema columns to leaf columns in chunks
  uint32_t num_leaf_cols;

  ByteBuffer tmp_buf;
  ByteBuffer def_levels;

  void init_file_on_disk();
  void check_meta_data();

  void read_column_chunk(ColumnChunk &cc);

  void read_dict_page(
    ColumnChunk &cc,
    parquet::PageHeader &ph,
    uint8_t *buf,
    int32_t len
  );

  uint32_t read_data_page(DataPage &dp, uint8_t *buf, int32_t len);
  uint32_t read_data_page_v1(DataPage &dp, uint8_t *buf, int32_t len);
  uint32_t read_data_page_v2(DataPage &dp, uint8_t *buf, int32_t len);
  uint32_t read_data_page_internal(DataPage &dp, uint8_t *buf, int32_t len);

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
