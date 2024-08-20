#include <fstream>
#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "parquet/parquet_types.h"

static uint8_t *out_buffer;
static uint32_t out_length;
static apache::thrift::protocol::TCompactProtocolFactoryT<
    apache::thrift::transport::TMemoryBuffer>
    tproto_factory;

int main(int argc, char *argv[]) {

  std::shared_ptr<apache::thrift::transport::TMemoryBuffer>
    mem_buffer(new apache::thrift::transport::TMemoryBuffer(1024 * 1024));
  std::shared_ptr<apache::thrift::protocol::TProtocol>
    tproto(tproto_factory.getProtocol(mem_buffer));

  uint32_t num_cols = 0;
  const uint32_t num_rows = 5;
  std::vector<parquet::SchemaElement> schemas;
  parquet::SchemaElement root;
  root.__set_name("schema");
  root.__set_num_children(0);
  schemas.push_back(root);
  std::vector<parquet::ColumnMetaData> column_meta_data;
  std::vector<parquet::KeyValue> kv;

  std::ofstream pfile;
  pfile.open("../tests/testthat/data/broken/decimal-1.parquet");
  pfile.write("PAR1", 4);

  int64_t total_start = pfile.tellp();

  // ----------------------------------------------------------------------
  // column metadata
  // ----------------------------------------------------------------------

  int64_t col_start = pfile.tellp();

  parquet::SchemaElement sel;
  sel.__set_type(parquet::Type::INT32);
  sel.__set_repetition_type(parquet::FieldRepetitionType::REQUIRED);
  sel.__set_name("col1");
  parquet::ConvertedType::type ct = parquet::ConvertedType::DECIMAL;
  sel.__set_converted_type(ct);
  sel.__set_precision(5);
  sel.__set_scale(2);
  schemas.push_back(sel);

  parquet::ColumnMetaData cmd;
  cmd.__set_type(sel.type);
  std::vector<parquet::Encoding::type> encs;
  encs.push_back(parquet::Encoding::PLAIN);
  cmd.__set_encodings(encs);
  std::vector<std::string> paths;
  paths.push_back(sel.name);
  cmd.__set_path_in_schema(paths);
  cmd.__set_codec(parquet::CompressionCodec::UNCOMPRESSED);
  cmd.__set_num_values(num_rows);

  schemas[0].__set_num_children(schemas[0].num_children + 1);
  num_cols++;

  // ----------------------------------------------------------------------
  // page header
  // ----------------------------------------------------------------------

  int64_t data_offset = pfile.tellp();

  parquet::PageHeader ph;
  ph.__set_type(parquet::PageType::DATA_PAGE);
  parquet::DataPageHeader dph;
  dph.__set_num_values(5);
  dph.__set_encoding(parquet::Encoding::PLAIN);
  ph.__set_data_page_header(dph);
  ph.__set_uncompressed_page_size(num_rows * sizeof(int32_t));
  ph.__set_compressed_page_size(num_rows * sizeof(int32_t));
  ph.write(tproto.get());
  mem_buffer->getBuffer(&out_buffer, &out_length);
  pfile.write((char*) out_buffer, out_length);
  mem_buffer->resetBuffer();

  // ----------------------------------------------------------------------
  // page data
  // ----------------------------------------------------------------------

  std::vector<int32_t> data(num_rows);
  pfile.write((const char*) data.data(), sizeof(int32_t) * 5);

  // ----------------------------------------------------------------------
  // complete column metadata
  // ----------------------------------------------------------------------

  int64_t column_bytes = pfile.tellp() - col_start;
  cmd.__set_total_compressed_size(column_bytes);
  cmd.__set_data_page_offset(data_offset);
  column_meta_data.push_back(cmd);

  // ----------------------------------------------------------------------
  // footer
  // ----------------------------------------------------------------------

  int64_t total_end = pfile.tellp();
  int64_t total_size = total_end - total_start;

  std::vector<parquet::ColumnChunk> ccs;
  for (uint32_t idx = 0; idx < num_cols; idx++) {
    parquet::ColumnChunk cc;
    cc.__set_file_offset(column_meta_data[idx].data_page_offset);
    cc.__set_meta_data(column_meta_data[idx]);
    ccs.push_back(cc);
  }

  std::vector<parquet::RowGroup> rgs;
  parquet::RowGroup rg;
  rg.__set_num_rows(num_rows);
  rg.__set_total_byte_size(total_size);
  rg.__set_columns(ccs);
  rgs.push_back(rg);

  parquet::FileMetaData fmd;
  fmd.__set_version(1);
  fmd.__set_schema(schemas);
  fmd.__set_num_rows(num_rows);
  fmd.__set_row_groups(rgs);
  fmd.__set_key_value_metadata(kv);
  fmd.__set_created_by("https://github.com/gaborcsardi/nanoparquet");
  fmd.write(tproto.get());
  uint8_t *out_buffer;
  uint32_t out_length;
  mem_buffer->getBuffer(&out_buffer, &out_length);
  pfile.write((char *)out_buffer, out_length);

  // size of the footer (without the magic bytes)
  pfile.write((const char *)&out_length, 4);

  pfile.write("PAR1", 4);
  pfile.close();
  return 0;
}
