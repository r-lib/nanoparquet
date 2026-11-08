#pragma once

#include <string>

#include "lib/ParquetReader.h"

// A reader that only parses the file metadata (the footer). Unlike
// RParquetReader it does not allocate memory for the column data, so it
// is cheap to create even for files with many rows. Use it when only
// `file_meta_data_` or the low level page reading functions are needed.

class MetadataReader : public nanoparquet::ParquetReader {
public:
  MetadataReader(std::string filename) : ParquetReader(filename) { }
  // These are never called, because MetadataReader does not read data
  void alloc_column_chunk(nanoparquet::ColumnChunk &cc) { } // # nocov
  void alloc_dict_page(nanoparquet::DictPage &dict) { }     // # nocov
  void alloc_data_page(nanoparquet::DataPage &data) { }     // # nocov
};
