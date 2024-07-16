#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "parquet/parquet_types.h"

namespace nanoparquet {


class ParquetOutFile {
public:
  ParquetOutFile(
    std::string filename,
    parquet::CompressionCodec::type codec
  );
  ParquetOutFile(
    std::ostream &stream,
    parquet::CompressionCodec::type codec
  );
  void set_num_rows(uint32_t nr);
  void schema_add_column(parquet::SchemaElement &sel, bool dict = false);
  void schema_add_column(std::string name,
                       parquet::Type::type type,
                       bool required, bool dict);
  void schema_add_column(
    std::string name,
    parquet::LogicalType logical_type,
    bool required,
    bool dict);
  void add_key_value_metadata(std::string key, std::string value);
  void write();

  // write out various parquet types, these must be implemented in
  // the subclass
  virtual void write_int32(std::ostream &file, uint32_t idx, uint64_t from,
                           uint64_t until) = 0;
  virtual void write_int64(std::ostream &file, uint32_t idx, uint64_t from,
                           uint64_t until) = 0;
  virtual void write_double(std::ostream &file, uint32_t idx, uint64_t from,
                            uint64_t until) = 0;
  virtual void write_byte_array(std::ostream &file, uint32_t idx,
                                uint64_t from, uint64_t until) = 0;
  virtual void write_boolean(std::ostream &file, uint32_t idx,
                             uint64_t from, uint64_t until) = 0;
  virtual void write_boolean_as_int(std::ostream &file, uint32_t idx,
                                    uint64_t from, uint64_t until) = 0;

  // callbacks for missing values
  virtual uint32_t write_present(std::ostream &file, uint32_t idx,
                                 uint64_t from, uint64_t until) = 0;
  virtual void write_present_int32(std::ostream &file, uint32_t idx,
                                   uint32_t num_present, uint64_t from,
                                   uint64_t until) = 0;
  virtual void write_present_int64(std::ostream &file, uint32_t idx,
                                   uint32_t num_present, uint64_t from,
                                   uint64_t until) = 0;
  virtual void write_present_double(std::ostream &file, uint32_t idx,
                                    uint32_t num_present, uint64_t from,
                                    uint64_t until) = 0;
  virtual void write_present_byte_array(std::ostream &file, uint32_t idx,
                                        uint32_t num_present,
                                        uint64_t from, uint64_t until) = 0;
  virtual void write_present_boolean(std::ostream &file, uint32_t idx,
                                     uint32_t num_present, uint64_t from,
                                     uint64_t until) = 0;
  virtual void write_present_boolean_as_int(std::ostream &file,
                                            uint32_t idx,
                                            uint32_t num_present,
                                            uint64_t from,
                                            uint64_t until) = 0;

  // callbacks to write a  dictionary
  virtual uint32_t get_size_byte_array(uint32_t idx,
                                       uint32_t num_present,
                                       uint64_t from, uint64_t until) = 0;
  virtual uint32_t get_num_values_dictionary(uint32_t idx) = 0;
  virtual uint32_t get_size_dictionary(uint32_t idx) = 0;
  virtual void write_dictionary(std::ostream &file,
                                uint32_t idx,
                                parquet::Type::type type) = 0;
  // Needs to write indices as int32_t
  virtual void write_dictionary_indices(std::ostream &file, uint32_t idx,
                                        uint64_t from, uint64_t until) = 0;

private:
  std::ofstream pfile_;
  std::ostream &pfile;
  uint32_t num_rows, num_cols;
  bool num_rows_set;
  uint32_t total_size; // for the single row group we have for now
  parquet::CompressionCodec::type codec;

  std::vector<parquet::Encoding::type> encodings;
  std::vector<parquet::SchemaElement> schemas;
  std::vector<parquet::ColumnMetaData> column_meta_data;
  std::vector<parquet::KeyValue> kv;

  std::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem_buffer;
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer>
      tproto_factory;
  std::shared_ptr<apache::thrift::protocol::TProtocol> tproto;

  void write_columns();
  void write_column(uint32_t idx);
  void write_dictionary_page(uint32_t idx);
  void write_data_pages(uint32_t idx);
  void write_data_page(uint32_t idx, uint64_t from, uint64_t until);
  void write_page_header(uint32_t idx, parquet::PageHeader &ph);
  void write_footer();

  void write_data_(std::ostream &file, uint32_t idx, uint32_t size,
                   uint64_t from, uint64_t until);
  void write_present_data_(std::ostream &file, uint32_t idx,
                           uint32_t size, uint32_t num_present,
                           uint64_t from, uint64_t until);
  void write_dictionary_(std::ostream &file, uint32_t idx, uint32_t size,
                         parquet::Type::type type);
  void write_dictionary_indices_(std::ostream &file, uint32_t idx,
                                uint32_t size, uint64_t from,
                                uint64_t until);

  size_t compress(parquet::CompressionCodec::type codec,
                  ByteBuffer &src, uint32_t src_size, ByteBuffer &tgt);
  uint32_t rle_encode(ByteBuffer &src, uint32_t src_size, ByteBuffer &tgt,
                      uint8_t bit_width, bool add_bit_width = false,
                      bool add_size = false, uint32_t skip = 0);

  ByteBuffer buf_unc;
  ByteBuffer buf_com;

  uint64_t calculate_column_data_size(uint32_t idx, uint32_t num_present,
                                      uint64_t from, uint64_t until);
};

void fill_converted_type_for_logical_type(parquet::SchemaElement &sel);
parquet::Type::type get_type_from_logical_type(
  parquet::LogicalType &logical_type);

} // namespace nanoparquet
