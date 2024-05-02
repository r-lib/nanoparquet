#include <bitset>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "parquet/parquet_types.h"

namespace miniparquet {

class ParquetColumn {
public:
  uint64_t id;
  parquet::format::Type::type type;
  std::string name;
  parquet::format::SchemaElement *schema_element;
};

struct Int96 {
  uint32_t value[3];
};

template <class T> class Dictionary {
public:
  std::vector<T> dict;
  Dictionary(uint64_t n_values) { dict.resize(n_values); }
  T &get(uint64_t offset) {
    if (offset >= dict.size()) {
      throw std::runtime_error("Dictionary offset out of bounds"); // # nocov
    } else
      return dict.at(offset);
  }
};

class ScanState {
public:
  uint64_t row_group_idx = 0;
  uint64_t row_group_offset = 0;
};

struct ResultColumn {
  uint64_t id;
  ByteBuffer data;
  ParquetColumn *col;
  ByteBuffer defined;
  std::vector<std::unique_ptr<char[]>> string_heap_chunks;
};

struct ResultChunk {
  std::vector<ResultColumn> cols;
  uint64_t nrows;
};

class ParquetFile {
public:
  ParquetFile(std::string filename);
  void initialize_result(ResultChunk &result);
  bool scan(ScanState &s, ResultChunk &result);
  uint64_t nrow;
  std::vector<std::unique_ptr<ParquetColumn>> columns;

private:
  void initialize(std::string filename);
  void initialize_column(ResultColumn &col, uint64_t num_rows);
  void scan_column(ScanState &state, ResultColumn &result_col);
  parquet::format::FileMetaData file_meta_data;
  std::ifstream pfile;
};

} // namespace miniparquet
