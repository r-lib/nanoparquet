#pragma once
#include <Rdefines.h>

#include "parquet/parquet_types.h"
#include "lib/ParquetReader.h"

using namespace nanoparquet;

class rtype {
public:
  rtype() { }
  rtype(parquet::SchemaElement &sel);
  int type;
  int elsize;
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
  std::vector<rtype> r_types;
};

class RParquetReader : public ParquetReader {
public:
  RParquetReader(std::string filename);
  ~RParquetReader();

  rtype get_r_type(parquet::SchemaElement &sel);

  void create_metadata();
  void convert_columns_to_r();
  void decode_dicts();
  void handle_missing();
  void rbind_row_groups();

  void alloc_column_chunk(ColumnChunk &cc) ;
  void alloc_dict_page(DictPage &dict);
  void alloc_data_page(DataPage &data);

  SEXP columns = R_NilValue;
  rmetadata metadata;

protected:
  void convert_int64_to_double(SEXP x);
  void convert_int96_to_double(SEXP x);
  void convert_float_to_double(SEXP x);
  void convert_buffer_to_string(SEXP x);
  void convert_buffer_to_string1(SEXP x, SEXP nv, R_xlen_t &idx);
  SEXP subset_vector(SEXP x, SEXP idx);
};
