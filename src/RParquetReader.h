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
  BA_RAW = 6
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
};

struct rmetadata {
public:
  int64_t num_rows;
  size_t num_cols;
  size_t num_row_groups;
  std::vector<int64_t> row_group_num_rows;
  std::vector<int64_t> row_group_offsets;
  std::vector<rtype> r_types;
};

class RParquetReader : public ParquetReader {
public:
  RParquetReader(std::string filename);
  ~RParquetReader();

  void create_metadata();
  void convert_columns_to_r();
  void decode_dicts();
  void handle_missing();
  void rbind_row_groups();

  void alloc_column_chunk(ColumnChunk &cc) ;
  void alloc_dict_page(DictPage &dict);
  void alloc_data_page(DataPage &data);
  void add_data_page(DataPage &data);

  SEXP columns = R_NilValue;
  SEXP tmpdata = R_NilValue;
  rmetadata metadata;

protected:
  void convert_column_to_r_dicts(uint32_t cn);
  void convert_column_to_r_int64(uint32_t cn);
  void convert_column_to_r_int96(uint32_t cn);
  void convert_column_to_r_float(uint32_t cn);
  void convert_column_to_r_ba_string(uint32_t cn);
  void convert_column_to_r_ba_decimal(uint32_t cn);
  void convert_column_to_r_ba_raw(uint32_t cn);

  void convert_buffer_to_string(SEXP x);
  void convert_buffer_to_string1(SEXP x, SEXP nv, R_xlen_t &idx);
  SEXP subset_vector(SEXP x, SEXP idx);
};
