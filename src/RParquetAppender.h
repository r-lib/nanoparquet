#pragma once
#include "lib/ParquetOutFile.h"
#include "RParquetOutFile.h"
#include "RParquetReader.h"

using namespace nanoparquet;

class ParquetReaderAppender : public ParquetReader {
public:
  ParquetReaderAppender(std::string filename, bool readwrite = false)
  : ParquetReader(filename, readwrite) {};
  void alloc_column_chunk(ColumnChunk &cc) {};
  void alloc_dict_page(DictPage &dict) {};
  void alloc_data_page(DataPage &data) {};
};

class RParquetAppender {
public:
  RParquetAppender(
    std::string filename,
    parquet::CompressionCodec::type codec,
    int compression_level,
    std::vector<int64_t> &row_groups,
    int data_page_version,
    bool overwrite_last_row_group
  );
  void init_metadata(
    SEXP dfsxp,
    SEXP dim,
    SEXP required,
    SEXP options,
    SEXP schema,
    SEXP encoding
  );
  void append();
private:
  ParquetReaderAppender reader;
  RParquetOutFile outfile;
  int data_page_version;
  bool overwrite_last_row_group;
};
