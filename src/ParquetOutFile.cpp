#include <fstream>
#include <iostream>
#include <sstream>

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "snappy/snappy.h"
#include "miniparquet.h"

using namespace std;

using namespace parquet;
using namespace parquet::format;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace miniparquet;

// # nocov start
static string type_to_string(Type::type t) {
  std::ostringstream ss;
  ss << t;
  return ss.str();
}
// # nocov end

ParquetOutFile::ParquetOutFile(
  std::string filename,
  parquet::format::CompressionCodec::type codec) :
  num_rows(0), num_cols(0), num_rows_set(false),
  codec(codec),
  mem_buffer(new TMemoryBuffer(1024 * 1024)), // 1MB, what if not enough?
  tproto(tproto_factory.getProtocol(mem_buffer)) {

  // open file
  pfile.open(filename, std::ios::binary);

  // root schema element
  SchemaElement sch;
  sch.__set_name("schema");
  sch.__set_num_children(0);
  schemas.push_back(sch);
}

void ParquetOutFile::set_num_rows(uint32_t nr) {
  num_rows = nr;
  num_rows_set = true;
}

void ParquetOutFile::schema_add_column(std::string name,
                                       parquet::format::Type::type type) {

  SchemaElement sch;
  sch.__set_name(name);
  sch.__set_type(type);
  sch.__set_repetition_type(FieldRepetitionType::REQUIRED);
  schemas.push_back(sch);
  schemas[0].__set_num_children(schemas[0].num_children + 1);

  ColumnMetaData cmd;
  cmd.__set_type(type);
  vector<Encoding::type> encs;
  encs.push_back(Encoding::PLAIN);
  cmd.__set_encodings(encs);
  vector<string> paths;
  paths.push_back(name);
  cmd.__set_path_in_schema(paths);
  cmd.__set_codec(codec);
  // num_values set later
  // total_uncompressed_size set later
  // total_compressed_size set later
  // data_page_offset  set later
  // dictionary_page_offset set later when we have dictionaries
  column_meta_data.push_back(cmd);

  num_cols++;
}

void ParquetOutFile::schema_add_column(
    std::string name, parquet::format::LogicalType logical_type) {

  SchemaElement sch;
  sch.__set_name(name);
  Type::type type = get_type_from_logical_type(logical_type);
  sch.__set_type(type);
  sch.__set_logicalType(logical_type);
  sch.__set_repetition_type(FieldRepetitionType::REQUIRED);
  ConvertedType::type converted_type =
      get_converted_type_from_logical_type(logical_type);
  sch.__set_converted_type(converted_type);
  schemas.push_back(sch);
  schemas[0].__set_num_children(schemas[0].num_children + 1);

  ColumnMetaData cmd;
  cmd.__set_type(type);
  vector<Encoding::type> encs;
  encs.push_back(Encoding::PLAIN);
  cmd.__set_encodings(encs);
  vector<string> paths;
  paths.push_back(name);
  cmd.__set_path_in_schema(paths);
  cmd.__set_codec(codec);
  // num_values set later
  // total_uncompressed_size set later
  // total_compressed_size set later
  // data_page_offset  set later
  // dictionary_page_offset set later when we have dictionaries
  column_meta_data.push_back(cmd);

  num_cols++;
}

parquet::format::Type::type ParquetOutFile::get_type_from_logical_type(
    parquet::format::LogicalType logical_type) {

  if (logical_type.__isset.STRING) {
    return Type::BYTE_ARRAY;

  } else if (logical_type.__isset.INTEGER) {
    IntType it = logical_type.INTEGER;
    if (!it.isSigned) {
      throw runtime_error("Unsigned integers are not implemented"); // # nocov
    }
    if (it.bitWidth != 32) {
      throw runtime_error("Only 32 bit integers are implemented");  // # nocov
    }
    return Type::INT32;

  } else {
    throw runtime_error("Unimplemented logical type");             // # nocov
  }
}

parquet::format::ConvertedType::type
ParquetOutFile::get_converted_type_from_logical_type(
    parquet::format::LogicalType logical_type) {

  if (logical_type.__isset.STRING) {
    return ConvertedType::UTF8;

  } else if (logical_type.__isset.INTEGER) {
    IntType it = logical_type.INTEGER;
    if (!it.isSigned) {
      throw runtime_error("Unsigned integers are not implemented"); // # nocov
    }
    if (it.bitWidth != 32) {
      throw runtime_error("Only 32 bit integers are implemented");  // # nocov
    }
    return ConvertedType::INT_32;

  } else {
    throw runtime_error("Unimplemented logical type");              // # nocov
  }
}

void ParquetOutFile::write() {
  if (!num_rows_set) {
    throw runtime_error("Need to set the number of rows before writing"); // # nocov
  }
  pfile.write("PAR1", 4);
  write_columns();
  write_footer();
  pfile.write("PAR1", 4);
  pfile.close();
}

void ParquetOutFile::write_columns() {
  uint32_t start = pfile.tellp();
  for (uint32_t idx = 0; idx < num_cols; idx++) {
    write_column(idx);
  }
  uint32_t end = pfile.tellp();
  total_size = end - start;
}

void ParquetOutFile::write_column(uint32_t idx) {
  // Do we need compression? If yes, then we first write into a buffer
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  if (cmd->codec == CompressionCodec::UNCOMPRESSED) {
    write_column_uncompressed(idx);
  } else {
    write_column_compressed(idx);
  }
}

void ParquetOutFile::write_column_uncompressed(uint32_t idx) {
  uint32_t col_start = pfile.tellp();
  // TODO: dictionary here
  // data page header
  uint32_t data_offset = pfile.tellp();
  uint32_t data_size = calculate_column_data_size(idx);
  PageHeader ph;
  ph.__set_type(PageType::DATA_PAGE);
  ph.__set_uncompressed_page_size(data_size);
  ph.__set_compressed_page_size(data_size);
  DataPageHeader dph;
  dph.__set_num_values(num_rows);
  dph.__set_encoding(Encoding::PLAIN);
  ph.__set_data_page_header(dph);
  ph.write(tproto.get());
  uint8_t *out_buffer;
  uint32_t out_length;
  mem_buffer->getBuffer(&out_buffer, &out_length);
  pfile.write((char *)out_buffer, out_length);
  mem_buffer->resetBuffer();


  // data via callback
  uint32_t cb_start = pfile.tellp();
  parquet::format::Type::type type = schemas[idx + 1].type;
  switch (type) {
  case Type::INT32:
    write_int32(pfile, idx);
    break;
  case Type::DOUBLE:
    write_double(pfile, idx);
    break;
  case Type::BYTE_ARRAY:
    write_byte_array(pfile, idx);
    break;
  case Type::BOOLEAN:
    write_boolean(pfile, idx);
    break;
  default:
    throw runtime_error("Cannot write unknown column type");   // # nocov
  }
  uint32_t cb_end = pfile.tellp();

  if (cb_end - cb_start != data_size) {
    throw runtime_error("Wrong number of bytes written for parquet column"); // # nocov
  }

  ColumnMetaData *cmd = &(column_meta_data[idx]);
  int32_t column_bytes = ((int32_t)pfile.tellp()) - col_start;
  cmd->__set_num_values(num_rows);
  cmd->__set_total_uncompressed_size(column_bytes);
  cmd->__set_total_compressed_size(column_bytes);
  cmd->__set_data_page_offset(data_offset);
}

void ParquetOutFile::write_column_compressed(uint32_t idx) {
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  uint32_t data_size = calculate_column_data_size(idx);
  std::unique_ptr<std::ostream> os;
  if (cmd->codec == CompressionCodec::SNAPPY) {
      buf_unc.resize(data_size);
      buf_unc.reset();
      os = std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
  } else {
    throw runtime_error("Only SNAPPY compression is supported at this time"); // # nocov
  }

  // data via callback
  parquet::format::Type::type type = schemas[idx + 1].type;
  switch (type) {
  case Type::INT32:
    write_int32(*os, idx);
    break;
  case Type::DOUBLE:
    write_double(*os, idx);
    break;
  case Type::BYTE_ARRAY:
    write_byte_array(*os, idx);
    break;
  case Type::BOOLEAN:
    write_boolean(*os, idx);
    break;
  default:
    throw runtime_error("Cannot write unknown column type");  // # nocov
  }

  if (buf_unc.tellp != data_size) {
    throw runtime_error("Wrong number of bytes written for parquet column"); // # nocov
  }

  size_t cl = 0;
  if (cmd->codec == CompressionCodec::SNAPPY) {
    size_t ms = snappy::MaxCompressedLength(data_size);
    buf_com.resize(ms);
    snappy::RawCompress(buf_unc.ptr, data_size, buf_com.ptr, &cl);
  }

  uint32_t col_start = pfile.tellp();
  // TODO: dictionary here
  // data page header
  uint32_t data_offset = pfile.tellp();
  PageHeader ph;
  ph.__set_type(PageType::DATA_PAGE);
  ph.__set_uncompressed_page_size(data_size);
  ph.__set_compressed_page_size(cl);
  DataPageHeader dph;
  dph.__set_num_values(num_rows);
  dph.__set_encoding(Encoding::PLAIN);
  ph.__set_data_page_header(dph);
  ph.write(tproto.get());
  uint8_t *out_buffer;
  uint32_t out_length;
  mem_buffer->getBuffer(&out_buffer, &out_length);
  pfile.write((char *)out_buffer, out_length);
  mem_buffer->resetBuffer();

  pfile.write(buf_com.ptr, cl);

  int32_t column_bytes = ((int32_t)pfile.tellp()) - col_start;
  cmd->__set_num_values(num_rows);
  cmd->__set_total_uncompressed_size(column_bytes);
  cmd->__set_total_compressed_size(column_bytes);
  cmd->__set_data_page_offset(data_offset);
}

uint32_t ParquetOutFile::calculate_column_data_size(uint32_t idx) {
  // +1 is to skip the root schema
  parquet::format::Type::type type = schemas[idx + 1].type;
  switch (type) {
  case Type::BOOLEAN: {
    return num_rows / 8 + (num_rows % 8 > 0);
  }
  case Type::INT32: {
    return num_rows * sizeof(int32_t);
  }
  case Type::DOUBLE: {
    return num_rows * sizeof(double);
  }
  case Type::BYTE_ARRAY: {
    // not known yet
    return get_size_byte_array(idx);
  }
  default: {
    throw runtime_error("Unknown type encountered: " + type_to_string(type)); // # nocov
  }
  }
}

void ParquetOutFile::write_footer() {
  vector<ColumnChunk> ccs;
  for (uint32_t idx = 0; idx < num_cols; idx++) {
    ColumnChunk cc;
    cc.__set_file_offset(column_meta_data[idx].data_page_offset);
    cc.__set_meta_data(column_meta_data[idx]);
    ccs.push_back(cc);
  }

  vector<RowGroup> rgs;
  RowGroup rg;
  rg.__set_num_rows(num_rows);
  rg.__set_total_byte_size(total_size);
  rg.__set_columns(ccs);
  rgs.push_back(rg);

  FileMetaData fmd;
  fmd.__set_version(1);
  fmd.__set_schema(schemas);
  fmd.__set_num_rows(num_rows);
  fmd.__set_row_groups(rgs);
  fmd.__set_created_by("https://github.com/gaborcsardi/miniparquet");
  fmd.write(tproto.get());
  uint8_t *out_buffer;
  uint32_t out_length;
  mem_buffer->getBuffer(&out_buffer, &out_length);
  pfile.write((char *)out_buffer, out_length);

  // size of the footer (without the magic bytes)
  pfile.write((const char *)&out_length, 4);

  // Magic bytes
  pfile.write("PAR1", 4);
  pfile.close();
}
