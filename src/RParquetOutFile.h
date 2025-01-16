#pragma once
#include <Rdefines.h>
#include "lib/nanoparquet.h"

namespace nanoparquet {

class RParquetOutFile : public ParquetOutFile {
public:
  RParquetOutFile(
    std::string filename,
    parquet::CompressionCodec::type codec,
    int compression_level,
    std::vector<int64_t> &row_groups
  );
  RParquetOutFile(
    std::ostream &stream,
    parquet::CompressionCodec::type codec,
    int compsession_level,
    std::vector<int64_t> &row_groups
  );
  void write_row_group(uint32_t group);
  void write_int32(std::ostream &file, uint32_t idx, uint32_t group,
                   uint32_t page, uint64_t from, uint64_t until,
                   parquet::SchemaElement &sel);
  void write_int64(std::ostream &file, uint32_t idx, uint32_t group,
                   uint32_t page, uint64_t from, uint64_t until,
                   parquet::SchemaElement &sel);
  void write_int96(std::ostream &file, uint32_t idx, uint32_t group,
                   uint32_t page, uint64_t from, uint64_t until,
                   parquet::SchemaElement &sel);
  void write_float(std::ostream &file, uint32_t idx, uint32_t group,
                   uint32_t page, uint64_t from, uint64_t until,
                   parquet::SchemaElement &sel);
  void write_double(std::ostream &file, uint32_t idx, uint32_t group,
                    uint32_t page, uint64_t from, uint64_t until,
                    parquet::SchemaElement &sel);
  void write_byte_array(std::ostream &file, uint32_t idx, uint32_t group,
                        uint32_t page, uint64_t from, uint64_t until,
                        parquet::SchemaElement &sel);
  void write_fixed_len_byte_array(std::ostream &file, uint32_t id,
                                  uint32_t group, uint32_t page, uint64_t from,
                                  uint64_t until, parquet::SchemaElement &sel);
  uint32_t get_size_byte_array(uint32_t idx, uint32_t num_present,
                               uint64_t from, uint64_t until);
  void write_boolean(std::ostream &file, uint32_t idx, uint32_t group,
                     uint32_t page, uint64_t from, uint64_t until);
  void write_boolean_as_int(std::ostream &file, uint32_t idx, uint32_t group,
                            uint32_t page, uint64_t from, uint64_t until);

  uint32_t write_present(std::ostream &file, uint32_t idx, uint64_t from,
                         uint64_t until);
  void write_present_boolean(std::ostream &file, uint32_t idx,
                             uint32_t num_present, uint64_t from,
                             uint64_t until);
  void write_present_boolean_as_int(std::ostream &file, uint32_t idx,
                                    uint32_t num_present, uint64_t from,
                                    uint64_t until);

  // for dictionaries
  uint32_t get_num_values_dictionary(uint32_t idx,
                                     parquet::SchemaElement &sel,
                                     int64_t form, int64_t until);
  uint32_t get_size_dictionary(uint32_t idx, parquet::SchemaElement &type,
                               int64_t from, int64_t until);
  void write_dictionary(std::ostream &file, uint32_t idx,
                        parquet::SchemaElement &sel, int64_t from,
                        int64_t until);
  void write_dictionary_indices(std::ostream &file, uint32_t idx,
                                int64_t rg_from, int64_t rg_until,
                                uint64_t page_from, uint64_t page_until);

  // statistics
  bool get_group_minmax_values(uint32_t idx, uint32_t group,
                               parquet::SchemaElement &sel,
                               std::string &min_value,
                               std::string &max_value);

  void init_metadata(
    SEXP dfsxp,
    SEXP dim,
    SEXP metadata,
    SEXP rrequired,
    SEXP options,
    SEXP schema,
    SEXP encoding
  );

  void init_append_metadata(
    SEXP dfsxp,
    SEXP dim,
    SEXP rrequired,
    SEXP options,
    std::vector<parquet::SchemaElement> &schema,
    SEXP encoding,
    std::vector<parquet::RowGroup> &row_groups,
    std::vector<parquet::KeyValue> &key_value_metadata
  );

  void write();
  void append();

private:
  SEXP df = R_NilValue;
  SEXP required = R_NilValue;
  SEXP dicts = R_NilValue;
  SEXP dicts_from = R_NilValue;
  ByteBuffer present;

  bool write_minmax_values;
  std::vector<bool> is_minmax_supported;
  std::vector<std::string> min_values;
  std::vector<std::string> max_values;
  std::vector<bool> has_minmax_value;

  void create_dictionary(uint32_t idx, int64_t from, int64_t until,
                         parquet::SchemaElement &sel);
  // for LGLSXP this mean RLE encoding
  bool should_use_dict_encoding(uint32_t idx);
  parquet::Encoding::type
  detect_encoding(uint32_t idx, parquet::SchemaElement &sel, int32_t renc);

  void write_integer_int32(std::ostream &file, SEXP col, uint32_t idx,
                           uint64_t from, uint64_t until,
                           parquet::SchemaElement &sel);
  void write_double_int32_time(std::ostream &file, SEXP col, uint32_t idx,
                               uint64_t from, uint64_t until,
                               parquet::SchemaElement &sel, double factor);
  void write_double_int32(std::ostream &file, SEXP col, uint32_t idx,
                          uint64_t from, uint64_t until,
                          parquet::SchemaElement &sel);
  void write_integer_int64(std::ostream &file, SEXP col, uint32_t idx,
                           uint64_t from, uint64_t until);
  void write_double_int64(std::ostream &file, SEXP col, uint32_t idx,
                          uint64_t from, uint64_t until,
                          parquet::SchemaElement &sel);
  void write_double_int64_time(std::ostream &file, SEXP col, uint32_t idx,
                               uint64_t from, uint64_t until,
                               parquet::SchemaElement &sel, double factor);
};

} // namespace nanoparquet
