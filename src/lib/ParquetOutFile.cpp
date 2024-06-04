#include <fstream>
#include <iostream>
#include <sstream>
#include <cmath>

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "snappy/snappy.h"
#include "nanoparquet.h"
#include "RleBpEncoder.h"
#include "memstream.h"

using namespace std;

using namespace parquet;
using namespace parquet::format;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace nanoparquet;

#define STR(x) STR2(x)
#define STR2(x) #x

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
                                       parquet::format::Type::type type,
                                       bool required, bool dict) {

  SchemaElement sch;
  sch.__set_name(name);
  sch.__set_type(type);
  if (required) {
    sch.__set_repetition_type(FieldRepetitionType::REQUIRED);
  } else {
    sch.__set_repetition_type(FieldRepetitionType::OPTIONAL);
  }
  schemas.push_back(sch);
  schemas[0].__set_num_children(schemas[0].num_children + 1);

  ColumnMetaData cmd;
  cmd.__set_type(type);
  vector<Encoding::type> encs;
  if (dict) {
    if (type == Type::BOOLEAN) {
      encs.push_back(Encoding::RLE);              // def levels + BOOLEAN
      encodings.push_back(Encoding::RLE);
    } else {
      encs.push_back(Encoding::PLAIN);            // the dict itself
      encs.push_back(Encoding::RLE);              // definition levels
      encs.push_back(Encoding::RLE_DICTIONARY);   // dictionary page
      encodings.push_back(Encoding::RLE_DICTIONARY);
    }
  } else {
    encs.push_back(Encoding::PLAIN);
    encodings.push_back(Encoding::PLAIN);
  }
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
    std::string name,
    parquet::format::LogicalType logical_type,
    bool required,
    bool dict) {

  SchemaElement sch;
  sch.__set_name(name);
  Type::type type = get_type_from_logical_type(logical_type);
  sch.__set_type(type);
  sch.__set_logicalType(logical_type);
  if (required) {
    sch.__set_repetition_type(FieldRepetitionType::REQUIRED);
  } else {
    sch.__set_repetition_type(FieldRepetitionType::OPTIONAL);
  }
  ConvertedType::type converted_type =
      get_converted_type_from_logical_type(logical_type);
  sch.__set_converted_type(converted_type);
  schemas.push_back(sch);
  schemas[0].__set_num_children(schemas[0].num_children + 1);

  ColumnMetaData cmd;
  cmd.__set_type(type);
  vector<Encoding::type> encs;
  if (dict) {
    if (type == Type::BOOLEAN) {
      encs.push_back(Encoding::RLE);              // def levels + BOOLEAN
      encodings.push_back(Encoding::RLE);
    } else {
      encs.push_back(Encoding::PLAIN);            // the dict itself
      encs.push_back(Encoding::RLE);              // definition levels
      encs.push_back(Encoding::RLE_DICTIONARY);   // dictionary page
      encodings.push_back(Encoding::RLE_DICTIONARY);
    }
  } else {
    encs.push_back(Encoding::PLAIN);
    encodings.push_back(Encoding::PLAIN);
  }
  cmd.__set_encodings(encs);
  vector<string> paths;
  paths.push_back(name);
  cmd.__set_path_in_schema(paths);
  cmd.__set_codec(codec);
  // num_values set later
  // total_uncompressed_size set later
  // total_compressed_size set later
  // data_page_offset  set later
  // dictionary_page_offset set later
  column_meta_data.push_back(cmd);

  num_cols++;
}

void ParquetOutFile::add_key_value_metadata(
    std::string key, std::string value) {
  KeyValue kv0;
  kv0.__set_key(key);
  kv0.__set_value(value);
  kv.push_back(kv0);
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

  } else if (logical_type.__isset.DATE) {
    return Type::INT32;

  } else if (logical_type.__isset.TIME &&
             logical_type.TIME.isAdjustedToUTC &&
             logical_type.TIME.unit.__isset.MILLIS) {
    return Type::INT32;

  } else if (logical_type.__isset.TIMESTAMP &&
             logical_type.TIMESTAMP.isAdjustedToUTC &&
             logical_type.TIMESTAMP.unit.__isset.MICROS) {
    return Type::INT64;

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

  } else if (logical_type.__isset.DATE) {
    return ConvertedType::DATE;

  } else if (logical_type.__isset.TIME &&
             logical_type.TIME.unit.__isset.MILLIS) {
    // we do this even if not adjusted to UTC, according to the spec
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-time-convertedtype
    // (Although we only create UTC TIME right now)
    return ConvertedType::TIME_MILLIS;

  } else if (logical_type.__isset.TIMESTAMP &&
             logical_type.TIMESTAMP.unit.__isset.MICROS) {
    // we do this even if not adjusted to UTC, according to the spec
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-timestamp-convertedtype
    // (Although we only create UTC TIMESTAMP right now)
    return ConvertedType::TIMESTAMP_MICROS;

  } else {
    throw runtime_error("Unimplemented logical type");              // # nocov
  }
}

void ParquetOutFile::write_data_(
  std::ostream &file,
  uint32_t idx,
  uint32_t size,
  uint64_t from,
  uint64_t until) {

  streampos cb_start = file.tellp();
  parquet::format::Type::type type = schemas[idx + 1].type;
  switch (type) {
  case Type::INT32:
    write_int32(file, idx, from, until);
    break;
  case Type::INT64:
    write_int64(file, idx, from, until);
    break;
  case Type::DOUBLE:
    write_double(file, idx, from, until);
    break;
  case Type::BYTE_ARRAY:
    write_byte_array(file, idx, from, until);
    break;
  case Type::BOOLEAN:
    write_boolean(file, idx, from, until);
    break;
  default:
    throw runtime_error("Cannot write unknown column type");   // # nocov
  }
  streampos cb_end = file.tellp();

  if (cb_end - cb_start != size) {
    throw runtime_error(
      string("Wrong number of bytes written for parquet column @ ") +
      __FILE__ + ":" + STR(__LINE__)
    );
  }

  ColumnMetaData *cmd = &(column_meta_data[idx]);
  cmd->__set_total_uncompressed_size(
    cmd->total_uncompressed_size + size
  );
}

void ParquetOutFile::write_present_data_(
  std::ostream &file,
  uint32_t idx,
  uint32_t size,
  uint32_t num_present,
  uint64_t from,
  uint64_t until) {

  streampos cb_start = file.tellp();
  parquet::format::Type::type type = schemas[idx + 1].type;
  switch (type) {
  case Type::INT32:
    write_present_int32(file, idx, num_present, from, until);
    break;
  case Type::DOUBLE:
    write_present_double(file, idx, num_present, from, until);
    break;
  case Type::BYTE_ARRAY:
    write_present_byte_array(file, idx, num_present, from, until);
    break;
  case Type::BOOLEAN:
    write_present_boolean(file, idx, num_present, from, until);
    break;
  default:
    throw runtime_error("Cannot write unknown column type");   // # nocov
  }
  streampos cb_end = file.tellp();

  if (cb_end - cb_start != size) {
    throw runtime_error(
      string("Wrong number of bytes written for parquet column @") +
      __FILE__ + ":" + STR(__LINE__)
    );
  }

  ColumnMetaData *cmd = &(column_meta_data[idx]);
  cmd->__set_total_uncompressed_size(
    cmd->total_uncompressed_size + size
  );
}

void ParquetOutFile::write_dictionary_(
  std::ostream &file,
  uint32_t idx,
  uint32_t size) {

  ColumnMetaData *cmd = &(column_meta_data[idx]);
  uint32_t start = file.tellp();
  write_dictionary(file, idx);
  uint32_t end = file.tellp();
  if (end - start != size) {
    throw runtime_error(
      string("Wrong number of bytes written for parquet dictionary @") +
      __FILE__ + ":" + STR(__LINE__)
    );
  }
  cmd->__set_total_uncompressed_size(
    cmd->total_uncompressed_size + size
  );
}

void ParquetOutFile::write_dictionary_indices_(
  std::ostream &file,
  uint32_t idx,
  uint32_t size,
  uint64_t from,
  uint64_t until) {

  streampos start = file.tellp();
  write_dictionary_indices(file, idx, from, until);
  streampos end = file.tellp();
  if (end - start != size) {
    throw runtime_error(
      string("Wrong number of bytes written for parquet dictionary @ ") +
      __FILE__ + ":" + STR(__LINE__)
    );
  }
  // these are still RLE encoded, so they do not increase the
  // total uncompressed size
}

size_t ParquetOutFile::compress(
  parquet::format::CompressionCodec::type codec,
  ByteBuffer &src,
  uint32_t src_size,
  ByteBuffer &tgt) {

  if (codec == CompressionCodec::SNAPPY) {
    size_t tgt_size_est = snappy::MaxCompressedLength(src_size);
    tgt.reset(tgt_size_est);
    size_t tgt_size;
    snappy::RawCompress(src.ptr, src_size, tgt.ptr, &tgt_size);
    return tgt_size;
  } else {
    std::stringstream ss;
    ss << "Unsupported Parquet compression codec: " << codec;
    throw runtime_error(ss.str());
  }
}

uint32_t ParquetOutFile::rle_encode(
  ByteBuffer &src,
  uint32_t src_size,
  ByteBuffer &tgt,
  uint8_t bit_width,
  bool add_bit_width,
  bool add_size,
  uint32_t skip) {

  size_t tgt_size_est = MaxRleBpSize((int*) src.ptr, src_size, bit_width);
  tgt.reset(
    skip + tgt_size_est + (add_bit_width ? 1 : 0) + (add_size ? 4 : 0),
    true
  );
  if (add_bit_width) {
    tgt.ptr[skip] = bit_width;
  }
  size_t tgt_size = RleBpEncode(
    (int*) src.ptr,
    src_size,
    bit_width,
    (uint8_t *) tgt.ptr + skip + (add_bit_width ? 1 : 0) + (add_size ? 4 : 0),
    tgt_size_est
  );
  if (add_size) {
    ((uint32_t*) (tgt.ptr + skip +  (add_bit_width ? 1 : 0)))[0] = tgt_size;
    tgt_size += 4;
  }
  if (add_bit_width) {
    tgt_size++;
  }

  return tgt_size + skip;
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
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  SchemaElement se = schemas[idx + 1];
  uint32_t col_start = pfile.tellp();
  // we increase this as needed
  cmd->__set_total_uncompressed_size(0);
  if (encodings[idx] == Encoding::RLE_DICTIONARY) {
    uint32_t dictionary_page_offset = pfile.tellp();
    write_dictionary_page(idx);
    cmd->__set_dictionary_page_offset(dictionary_page_offset);
  }
  uint32_t data_offset = pfile.tellp();
  write_data_pages(idx);
  int32_t column_bytes = ((int32_t) pfile.tellp()) - col_start;
  cmd->__set_num_values(num_rows);
  cmd->__set_total_compressed_size(column_bytes);
  cmd->__set_data_page_offset(data_offset);
}

void ParquetOutFile::write_page_header(uint32_t idx, PageHeader &ph) {
  uint8_t *out_buffer;
  uint32_t out_length;
  ph.write(tproto.get());
  mem_buffer->getBuffer(&out_buffer, &out_length);
  pfile.write((char*) out_buffer, out_length);
  mem_buffer->resetBuffer();
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  cmd->__set_total_uncompressed_size(
    cmd->total_uncompressed_size + out_length
  );
}

// Currently only for byte arrays, more later

void ParquetOutFile::write_dictionary_page(uint32_t idx) {
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  SchemaElement se = schemas[idx + 1];
  // Uncompresed size of the dictionary in bytes
  uint32_t dict_size = get_size_dictionary(idx);
  // Number of entries in the dicitonary
  uint32_t num_dict_values = get_num_values_dictionary(idx);

  // Init page header
  PageHeader ph;
  ph.__set_type(PageType::DICTIONARY_PAGE);
  ph.__set_uncompressed_page_size(dict_size);
  DictionaryPageHeader diph;
  diph.__set_num_values(num_dict_values);
  diph.__set_encoding(Encoding::PLAIN);
  ph.__set_dictionary_page_header(diph);

  // If uncompressed, then write it out directly, otherwise to a buffer
  if (cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // we knpw the compressed size, so we can write out the header first,
    // then the data directly to the file
    ph.__set_compressed_page_size(dict_size);
    write_page_header(idx, ph);
    write_dictionary_(pfile, idx, dict_size);

  } else {
    // With compression we need two temporary buffers
    // 1. write data to buf_unc
    buf_unc.reset(dict_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_(*os0, idx, dict_size);

    // 2. compress buf_unc to buf_com
    size_t cdict_size = compress(cmd->codec, buf_unc, dict_size, buf_com);

    // 3. write buf_com to file
    ph.__set_compressed_page_size(cdict_size);
    write_page_header(idx, ph);
    pfile.write((const char *) buf_com.ptr, cdict_size);
  }
}

void ParquetOutFile::write_data_pages(uint32_t idx) {
  SchemaElement se = schemas[idx + 1];

  // guess total size and decide on number of pages
  uint64_t total_size;
  if (encodings[idx] == Encoding::PLAIN) {
    total_size = calculate_column_data_size(idx, num_rows, 0, num_rows);
  } else {
    // estimate the max RLE length
    uint32_t num_values = get_num_values_dictionary(idx);
    uint8_t bit_width = ceil(log2((double) num_values));
    total_size = MaxRleBpSizeSimple(num_rows, bit_width);
  }

  uint32_t page_size = 1024 * 1024;
  const char *ev = std::getenv("NANOPARQUEST_PAGE_SIZE");
  if (ev && strlen(ev) > 0) {
    try {
      page_size = std::stoi(ev);
    } catch (...) {
      // fall back to default
    }
  }
  // at least 1k
  if (page_size < 1024) {
    page_size = 1024;
  }

  uint32_t num_pages = total_size / page_size + (total_size % page_size ? 1 : 0);
  if (num_pages == 0) {
    num_pages = 1;
  }

  uint32_t rows_per_page = num_rows / num_pages + (num_rows % num_pages ? 1 : 0);
  if (rows_per_page == 0)  {
    rows_per_page = 1;
  }

  for (auto i = 0; i < num_pages; i++) {
    uint64_t from = i * rows_per_page;
    uint64_t until = (i + 1) * rows_per_page;
    if (until > num_rows) {
      until = num_rows;
    }
    write_data_page(idx, from, until);
  }
}

void ParquetOutFile::write_data_page(uint32_t idx, uint64_t from,
                                     uint64_t until) {
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  SchemaElement se = schemas[idx + 1];
  PageHeader ph;
  ph.__set_type(PageType::DATA_PAGE);
  DataPageHeader dph;
  dph.__set_num_values(until - from);
  dph.__set_encoding(encodings[idx]);
  if (se.repetition_type == FieldRepetitionType::OPTIONAL) {
    dph.__set_definition_level_encoding(Encoding::RLE);
  }
  ph.__set_data_page_header(dph);

  if (se.repetition_type == FieldRepetitionType::REQUIRED &&
      encodings[idx] == Encoding::PLAIN &&
      cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 1: REQ, PLAIN, UNC
    // 1. write directly to file
    uint32_t data_size = calculate_column_data_size(
      idx, until - from, from, until
    );
    ph.__set_uncompressed_page_size(data_size);
    ph.__set_compressed_page_size(data_size);
    write_page_header(idx, ph);
    write_data_(pfile, idx, data_size, from, until);

  } else if (se.repetition_type == FieldRepetitionType::REQUIRED &&
             encodings[idx] == Encoding::PLAIN &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 2: REQ, PLAIN, COMP
    // 1. write data to buf_unc
    uint32_t data_size = calculate_column_data_size(
      idx, until - from, from, until
    );
    ph.__set_uncompressed_page_size(data_size);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_data_(*os0, idx, data_size, from, until);

    // 2. compress buf_unc to buf_com
    size_t cdata_size = compress(cmd->codec, buf_unc, data_size, buf_com);

    // 3. write buf_com to file
    ph.__set_compressed_page_size(cdata_size);
    write_page_header(idx, ph);
    pfile.write((const char *) buf_com.ptr, cdata_size);

  } else if (se.repetition_type == FieldRepetitionType::REQUIRED &&
             encodings[idx] == Encoding::RLE_DICTIONARY &&
             cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 3: REQ, RLE_DICT, UNC
    // 1. write dictionary indices to buf_unc
    uint32_t data_size = (until - from) * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_indices_(*os0, idx, data_size, from, until);

    // 2. RLE encode buf_unc to buf_com
    uint32_t num_dict_values = get_num_values_dictionary(idx);
    uint8_t bit_width = ceil(log2((double) num_dict_values));
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      bit_width,
      true
    );

    // 3. write buf_com to file
    ph.__set_uncompressed_page_size(rle_size);
    ph.__set_compressed_page_size(rle_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_com.ptr, rle_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle_size
    );

  } else if (se.repetition_type == FieldRepetitionType::REQUIRED &&
             encodings[idx] == Encoding::RLE_DICTIONARY &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 4: REQ, RLE_DICT, COMP
    // 1. write dictionary indices to buf_unc
    uint32_t data_size = (until - from) * sizeof(int);
    ph.__set_uncompressed_page_size(data_size);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_indices_(*os0, idx, data_size, from, until);

    // 2. RLE encode buf_unc to buf_com
    uint32_t num_dict_values = get_num_values_dictionary(idx);
    uint8_t bit_width = ceil(log2((double) num_dict_values));
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      bit_width,
      true,       // add_bit_width
      false       // add_size
    );

    // 3. compress buf_com to buf_unc
    size_t crle_size = compress(cmd->codec, buf_com, rle_size, buf_unc);

    // 4. write buf_unc to file
    ph.__set_uncompressed_page_size(rle_size);
    ph.__set_compressed_page_size(crle_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_unc.ptr, crle_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle_size
    );

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::PLAIN &&
             cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 5: OPT, PLAIN, UNC
    // 1. write definition levels to buf_unc
    uint32_t miss_size = (until - from) * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os0, idx, from, until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(buf_unc, until - from, buf_com, 1, false);

    // 3. Write buf_unc to file
    uint32_t data_size = calculate_column_data_size(
      idx, num_present, from, until
    );
    ph.__set_uncompressed_page_size(data_size + rle_size + 4);
    ph.__set_compressed_page_size(data_size + rle_size + 4);
    write_page_header(idx, ph);
    pfile.write((const char*) &rle_size, 4);
    pfile.write((const char*) buf_com.ptr, rle_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle_size + 4
    );

    // 4. write data to file
    write_present_data_(pfile, idx, data_size, num_present, from, until);

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::PLAIN &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 6: OPT, PLAIN, COMP
    // 1. write definition levels to buf_unc
    uint32_t miss_size = (until - from) * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os0, idx, from, until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      1,         // bit_width
      false,     // add_bit_width
      true       // add_size
    );

    // 3. Append data to buf_com
    uint32_t data_size = calculate_column_data_size(
      idx, num_present, from, until
    );
    buf_com.resize(rle_size + data_size, true);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_com));
    buf_com.skip(rle_size);
    write_present_data_(*os1, idx, data_size, num_present, from, until);

    // 4. compress buf_com to buf_unc
    size_t comp_size = compress(cmd->codec, buf_com, rle_size + data_size, buf_unc);

    // 5. write buf_unc to file
    ph.__set_uncompressed_page_size(rle_size + data_size);
    ph.__set_compressed_page_size(comp_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_unc.ptr, comp_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle_size
    );

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::RLE_DICTIONARY &&
             cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 7: OPT RLE_DICT UNC
    // 1. write definition levels to buf_unc
    uint32_t miss_size = (until - from) * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os1, idx, from, until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      1,         // bit_width
      false,     // add_bit_width
      true       // add_size
    );

    // 3. write dictionaery indices to buf_unc
    uint32_t data_size = num_present * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_indices_(*os0, idx, data_size, from, until);

    // 4. append RLE buf_unc to buf_com
    uint32_t num_dict_values = get_num_values_dictionary(idx);
    uint8_t bit_width = ceil(log2((double) num_dict_values));
    uint32_t rle2_size = rle_encode(
      buf_unc,
      num_present,
      buf_com,
      bit_width,
      true,        // add_bit_width
      false,       // add_size
      rle_size     // skip
    );

    // 5. write buf_com to file
    ph.__set_uncompressed_page_size(rle2_size);
    ph.__set_compressed_page_size(rle2_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_com.ptr, rle2_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle2_size
    );

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::RLE_DICTIONARY &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 8: OPT RLE_DICT COM
    // 1. write definition levels to buf_unc
    uint32_t miss_size = (until - from) * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os1, idx, from, until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      1,         // bit_width
      false,     // add_bit_width
      true       // add_size
    );

    // 3. write dictionaery indices to buf_unc
    uint32_t data_size = num_present * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_indices_(*os0, idx, data_size, from, until);

    // 4. append RLE buf_unc to buf_com
    uint32_t num_dict_values = get_num_values_dictionary(idx);
    uint8_t bit_width = ceil(log2((double) num_dict_values));
    uint32_t rle2_size = rle_encode(
      buf_unc,
      num_present,
      buf_com,
      bit_width,
      true,        // add_bit_width
      false,       // add_size
      rle_size     // skip
    );

    // 5. compress buf_com to buf_unc
    size_t crle2_size = compress(cmd->codec, buf_com, rle2_size, buf_unc);

    // 6. write buf_unc to file
    ph.__set_uncompressed_page_size(rle2_size);
    ph.__set_compressed_page_size(crle2_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_unc.ptr, crle2_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle2_size
    );

  } else if (se.repetition_type == FieldRepetitionType::REQUIRED &&
             encodings[idx] == Encoding::RLE &&
             cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 9: REQ, RLE, UNCOMP
    // 1. write logicals into buf_unc
    uint32_t data_size = (until - from) * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_boolean_as_int(*os0, idx, from, until);

    // 2. RLE encode buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      1,             // bit_width
      false,         // add_bit_width
      true           // add_size
    );

    // 3. write buf_com to file
    ph.__set_uncompressed_page_size(rle_size);
    ph.__set_compressed_page_size(rle_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_com.ptr, rle_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle_size
    );

  } else if (se.repetition_type == FieldRepetitionType::REQUIRED &&
             encodings[idx] == Encoding::RLE &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 10: REQ, RLE, COMP
    // 1. write logicals into buf_unc
    uint32_t data_size = (until - from) * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_boolean_as_int(*os0, idx, from, until);

    // 2. RLE encode buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      1,             // bit_width
      false,         // add_bit_width
      true           // add_size
    );

    // 3. compress buf_com to buf_unc
    size_t crle_size = compress(cmd->codec, buf_com, rle_size, buf_unc);

    // 4. write buf_unc to file
    ph.__set_uncompressed_page_size(rle_size);
    ph.__set_compressed_page_size(crle_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_unc.ptr, crle_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle_size
    );

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::RLE &&
             cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 11: OPT RLE UNC
    // 1. write definition levels to buf_unc
    uint32_t miss_size = (until - from) * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os1, idx, from, until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      1,         // bit_width
      false,     // add_bit_width
      true       // add_size
    );

    // 3. write logicals into buf_unc
    uint32_t data_size = num_present * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_present_boolean_as_int(*os0, idx, num_present, from, until);

    // 4. append RLE buf_unc to buf_com
    uint32_t rle2_size = rle_encode(
      buf_unc,
      num_present,
      buf_com,
      1,           // bit_width
      false,       // add_bit_width
      true,        // add_size
      rle_size     // skip
    );

    // 5. write buf_com to file
    ph.__set_uncompressed_page_size(rle2_size);
    ph.__set_compressed_page_size(rle2_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_com.ptr, rle2_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle2_size
    );

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::RLE &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 12: OPT RLE COMP
    // 1. write definition levels to buf_unc
    uint32_t miss_size = (until - from) * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os1, idx, from, until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      until - from,
      buf_com,
      1,         // bit_width
      false,     // add_bit_width
      true       // add_size
    );

    // 3. write logicals into buf_unc
    uint32_t data_size = num_present * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_present_boolean_as_int(*os0, idx, num_present, from, until);

    // 4. append RLE buf_unc to buf_com
    uint32_t rle2_size = rle_encode(
      buf_unc,
      num_present,
      buf_com,
      1,           // bit_width
      false,       // add_bit_width
      true,        // add_size
      rle_size     // skip
    );

    // 5. compress buf_com to buf_unc
    size_t crle2_size = compress(cmd->codec, buf_com, rle2_size, buf_unc);

    // 6. write buf_unc to file
    ph.__set_uncompressed_page_size(rle2_size);
    ph.__set_compressed_page_size(crle2_size);
    write_page_header(idx, ph);
    pfile.write((const char*) buf_unc.ptr, crle2_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle2_size
    );
  }
}

uint64_t ParquetOutFile::calculate_column_data_size(uint32_t idx,
                                                    uint32_t num_present,
                                                    uint64_t from,
                                                    uint64_t until) {
  // +1 is to skip the root schema
  parquet::format::Type::type type = schemas[idx + 1].type;
  switch (type) {
  case Type::BOOLEAN: {
    return num_present / 8 + (num_present % 8 > 0);
  }
  case Type::INT32: {
    return num_present * sizeof(int32_t);
  }
  case Type::INT64: {
    return num_present * sizeof(int64_t);
  }
  case Type::DOUBLE: {
    return num_present * sizeof(double);
  }
  case Type::BYTE_ARRAY: {
    // not known yet
    return get_size_byte_array(idx, num_present, from, until);
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
  fmd.__set_key_value_metadata(kv);
  fmd.__set_created_by("https://github.com/gaborcsardi/nanoparquet");
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
