#pragma once
#include <Rdefines.h>

#include "parquet/parquet_types.h"
#include "lib/ParquetReader.h"

using namespace nanoparquet;

enum r_type_conversion {
  NONE = 0,
  INT64_DOUBLE = 1,
  INT96_DOUBLE = 2,
  FLOAT_DOUBLE = 3,
  BA_STRING = 4,
  BA_DECIMAL = 5,
  BA_RAW = 6,
  BA_UUID = 7,
  BA_FLOAT16 = 8,
  INT32_DECIMAL = 9,
  INT64_DECIMAL = 10
};

class rtype {
public:
  rtype() { }
  rtype(parquet::SchemaElement &sel);
  // final type
  int type;
  r_type_conversion type_conversion = NONE;
  // type we use temporarily, defaults to type
  // if NILSXP, then allocate nothing
  int tmptype = 0;
  // size of tmptype or type in bytes
  int elsize;
  // number of R tmptype elements for 1 Parquet element
  int rsize = 1;
  std::vector<std::string> classes;
  std::vector<std::string> units;
  std::string tzone = "";
  double time_fct = 1.0;
  bool byte_array = false;
  // for DECIMAL
  int32_t scale;
};

struct rmetadata {
public:
  int64_t num_rows;
  size_t num_cols;
  size_t num_leaf_cols;
  size_t num_cols_to_read;
  size_t num_row_groups;
  std::vector<int64_t> row_group_num_rows;
  std::vector<int64_t> row_group_offsets;
  std::vector<rtype> r_types;
  std::vector<uint8_t*> dataptr;
};

struct tmpbytes {
public:
  // we need to know where this page goes to be able to process row groups
  // concurrently
  int64_t from;
  std::vector<uint8_t> buffer;
  std::vector<uint32_t> offsets;
  std::vector<uint32_t> lengths;
};

struct tmpdict {
public:
  uint32_t dict_len;
  std::vector<uint8_t> buffer;
  tmpbytes bytes;
  std::vector<uint32_t> indices;
};

struct presentmap {
public:
  uint32_t num_present;
  std::vector<uint8_t> map;
};

class RParquetFilter {
public:
  RParquetFilter() : filter_row_groups(false), filter_columns(false) { };
  bool filter_row_groups;
  std::vector<uint32_t> row_groups;
  bool filter_columns;
  std::vector<uint32_t> columns;
};

struct dict_step {
  int64_t start;
  int64_t num_values;
  int64_t num_present;
  bool dict;
};

class RParquetReader : public ParquetReader {
public:
  RParquetReader(std::string filename, bool readwrite = false);
  RParquetReader(std::string filename, RParquetFilter &filter);
  ~RParquetReader();

  void create_metadata(RParquetFilter &filter);
  void read_arrow_metadata();
  void read_columns();
  void convert_columns_to_r();
  void create_df();

  void decode_dicts();
  void handle_missing();
  void rbind_row_groups();

  void alloc_column_chunk(ColumnChunk &cc) ;
  void alloc_dict_page(DictPage &dict);
  void alloc_data_page(DataPage &data);

  SEXP columns = R_NilValue;
  SEXP facdicts = R_NilValue;
  SEXP types = R_NilValue;
  SEXP arrow_metadata = R_NilValue;

  std::vector<std::vector<uint8_t>> tmpdata;
  std::vector<std::vector<tmpdict>> dicts;
  std::vector<std::vector<std::vector<dict_step>>> dict_steps;
  std::vector<std::vector<std::vector<tmpbytes>>> byte_arrays;
  std::vector<std::vector<presentmap>> present;
  rmetadata metadata;

protected:
  RParquetFilter filter;
  void init(RParquetFilter &filter);
  std::vector<uint32_t> colmap;
  void calculate_dict_steps();
  void calculate_dict_steps_simple();
  void calculate_dict_steps_bad();
};
