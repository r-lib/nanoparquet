#include <fstream>
#include <iostream>
#include <sstream>
#include <cmath>

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "snappy/snappy.h"
#include "miniz/miniz_wrapper.hpp"
#include "zstd.h"
#include "nanoparquet.h"
#include "RleBpEncoder.h"

using namespace std;

using namespace parquet;
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
  parquet::CompressionCodec::type codec,
  int compression_level,
  vector<int64_t> &row_group_starts)
  : pfile(pfile_), num_rows(0), num_cols(0), num_rows_set(false),
    num_total_rows_set(false), codec(codec),
    compression_level(compression_level),
    row_group_starts(row_group_starts),
    mem_buffer(new TMemoryBuffer(1024 * 1024)), // 1MB, what if not enough?
    tproto(tproto_factory.getProtocol(mem_buffer)) {
}

ParquetOutFile::ParquetOutFile(
  std::string filename,
  parquet::CompressionCodec::type codec,
  int compression_level,
  vector<int64_t> &row_group_starts) :
    pfile(pfile_), num_rows(0), num_cols(0), num_rows_set(false),
    num_total_rows_set(false), codec(codec),
    compression_level(compression_level),
    row_group_starts(row_group_starts),
    mem_buffer(new TMemoryBuffer(1024 * 1024)), // 1MB, what if not enough?
    tproto(tproto_factory.getProtocol(mem_buffer)) {

  // open file
  pfile_.open(filename, std::ios::binary);

  // root schema element
  SchemaElement sch;
  sch.__set_name("schema");
  sch.__set_num_children(0);
  schemas.push_back(sch);
}

ParquetOutFile::ParquetOutFile(
  std::ostream &stream,
  parquet::CompressionCodec::type codec,
  int compression_level,
  vector<int64_t> &row_group_starts) :
    pfile(stream), num_rows(0), num_cols(0), num_rows_set(false),
    num_total_rows_set(false), codec(codec),
    compression_level(compression_level),
    row_group_starts(row_group_starts),
    mem_buffer(new TMemoryBuffer(1024 * 1024)), // 1MB, what if not enough?
    tproto(tproto_factory.getProtocol(mem_buffer)) {

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

void ParquetOutFile::set_num_rows(uint32_t nr, uint32_t ntotal) {
  num_rows = nr;
  num_total_rows = ntotal;
  num_rows_set = true;
  num_total_rows_set = true;
}

void ParquetOutFile::schema_add_column(parquet::SchemaElement &sel,
                                       parquet::Encoding::type encoding) {
  schemas.push_back(sel);
  schemas[0].__set_num_children(schemas[0].num_children + 1);
  encodings.push_back(encoding);
  num_cols++;
}

void ParquetOutFile::init_column_meta_data() {
  column_meta_data.clear();
  for (uint32_t cl = 0; cl < schemas.size() - 1; cl++) {
    parquet::SchemaElement &sel = schemas[cl + 1];
    parquet::Encoding::type encoding = encodings[cl];
    ColumnMetaData cmd;
    cmd.__set_type(sel.type);
    vector<Encoding::type> encs;
    if (sel.repetition_type != parquet::FieldRepetitionType::REQUIRED &&
        encoding != Encoding::RLE) {
      // def levels, but do not duplicate
      encs.push_back(Encoding::RLE);
    }
    if (encoding == Encoding::RLE_DICTIONARY ||
        encoding == Encoding::PLAIN_DICTIONARY) {
      // dictionary values
      encs.push_back(Encoding::PLAIN);
    }
    encs.push_back(encoding);

    cmd.__set_encodings(encs);
    vector<string> paths;
    paths.push_back(sel.name);
    cmd.__set_path_in_schema(paths);
    cmd.__set_codec(codec);
    // num_values set later
    // total_uncompressed_size set later
    // total_compressed_size set later
    // data_page_offset  set later
    // dictionary_page_offset set later when we have dictionaries
    column_meta_data.push_back(cmd);
  }
}

void ParquetOutFile::add_key_value_metadata(
    std::string key, std::string value) {
  KeyValue kv0;
  kv0.__set_key(key);
  kv0.__set_value(value);
  kv.push_back(kv0);
}

void ParquetOutFile::set_key_value_metadata(
  std::vector<parquet::KeyValue> &kv2) {

  kv = kv2;
}

void ParquetOutFile::set_row_groups(std::vector<RowGroup> &row_groups_) {
  row_groups = row_groups_;
}

namespace nanoparquet {

parquet::Type::type get_type_from_logical_type(
  parquet::LogicalType &logical_type) {

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

void fill_converted_type_for_logical_type(
    parquet::SchemaElement &sel) {

  if (!sel.__isset.logicalType) {
    return;
  }
  parquet::LogicalType &logical_type = sel.logicalType;

  if (logical_type.__isset.STRING) {
    sel.__set_converted_type(ConvertedType::UTF8);

  } else if (logical_type.__isset.ENUM) {
    sel.__set_converted_type(ConvertedType::ENUM);

  } else if (logical_type.__isset.DECIMAL) {
    sel.__set_converted_type(ConvertedType::DECIMAL);
    parquet::DecimalType &dt = logical_type.DECIMAL;
    sel.__set_scale(dt.scale);
    sel.__set_precision(dt.precision);

  } else if (logical_type.__isset.DATE) {
    sel.__set_converted_type(ConvertedType::DATE);

  } else if (logical_type.__isset.TIME) {
    if (logical_type.TIME.unit.__isset.MILLIS) {
      // we do this even if not adjusted to UTC, according to the spec
      // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-time-convertedtype
      sel.__set_converted_type(ConvertedType::TIME_MILLIS);
    } else if (logical_type.TIME.unit.__isset.MICROS) {
      sel.__set_converted_type(ConvertedType::TIME_MICROS);
    }

  } else if (logical_type.__isset.TIMESTAMP) {
    if (logical_type.TIMESTAMP.unit.__isset.MILLIS) {
      // we do this even if not adjusted to UTC, according to the spec
      // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-timestamp-convertedtype
      sel.__set_converted_type(ConvertedType::TIMESTAMP_MILLIS);
    } else if (logical_type.TIMESTAMP.unit.__isset.MICROS) {
      sel.__set_converted_type(ConvertedType::TIMESTAMP_MICROS);
    }

  } else if (logical_type.__isset.INTEGER) {
    IntType it = logical_type.INTEGER;
    if (it.isSigned) {
      if (it.bitWidth == 8) {
        sel.__set_converted_type(ConvertedType::INT_8);
      } else if (it.bitWidth == 16) {
        sel.__set_converted_type(ConvertedType::INT_16);
      } else if (it.bitWidth == 32) {
        sel.__set_converted_type(ConvertedType::INT_32);
      } else if (it.bitWidth == 64) {
        sel.__set_converted_type(ConvertedType::INT_64);
      }
    } else {
      if (it.bitWidth == 8) {
        sel.__set_converted_type(ConvertedType::UINT_8);
      } else if (it.bitWidth == 16) {
        sel.__set_converted_type(ConvertedType::UINT_16);
      } else if (it.bitWidth == 32) {
        sel.__set_converted_type(ConvertedType::UINT_32);
      } else if (it.bitWidth == 64) {
        sel.__set_converted_type(ConvertedType::UINT_64);
      }
    }

  } else if (logical_type.__isset.JSON) {
    sel.__set_converted_type(ConvertedType::JSON);

  } else if (logical_type.__isset.BSON) {
    sel.__set_converted_type(ConvertedType::BSON);
  }

  // There is no INTERVAL logical type? What's going on here?
  // For the other logical types (e.g. UNKNOWN, FLOAT16) we don't fill in
  // a converted type because there isn't one.
  // We also skip types we don't know about.
}

}

void ParquetOutFile::write_data_(
  std::ostream &file,
  uint32_t idx,
  uint32_t size,
  uint32_t group,
  uint32_t page,
  uint64_t from,
  uint64_t until) {

  streampos cb_start = file.tellp();
  parquet::SchemaElement &se = schemas[idx + 1];
  parquet::Type::type type = se.type;
  switch (type) {
  case Type::INT32:
    write_int32(file, idx, group, page, from, until, se);
    break;
  case Type::INT64:
    write_int64(file, idx, group, page, from, until, se);
    break;
  case Type::INT96:
    write_int96(file, idx, group, page, from, until, se);
    break;
  case Type::FLOAT:
    write_float(file, idx, group, page, from, until, se);
    break;
  case Type::DOUBLE:
    write_double(file, idx, group, page, from, until, se);
    break;
  case Type::BYTE_ARRAY:
    write_byte_array(file, idx, group, page, from, until, se);
    break;
  case Type::FIXED_LEN_BYTE_ARRAY:
    write_fixed_len_byte_array(file, idx, group, page, from, until, se);
    break;
  case Type::BOOLEAN:
    write_boolean(file, idx, group, page, from, until);
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
  uint32_t group,
  uint32_t page,
  uint64_t from,
  uint64_t until) {

  streampos cb_start = file.tellp();
  parquet::SchemaElement &se = schemas[idx + 1];
  parquet::Type::type type = se.type;
  switch (type) {
  case Type::INT32:
    write_int32(file, idx, group, page, from, until, se);
    break;
  case Type::INT64:
    write_int64(file, idx, group, page, from, until, se);
    break;
  case Type::INT96:
    write_int96(file, idx, group, page, from, until, se);
    break;
  case Type::FLOAT:
    write_float(file, idx, group, page, from, until, se);
    break;
  case Type::DOUBLE:
    write_double(file, idx, group, page, from, until, se);
    break;
  case Type::BYTE_ARRAY:
    write_byte_array(file, idx, group, page, from, until, se);
    break;
  case Type::FIXED_LEN_BYTE_ARRAY:
    write_fixed_len_byte_array(file, idx, group, page, from, until, se);
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
  uint32_t size,
  parquet::SchemaElement &sel,
  int64_t from,
  int64_t until) {

  ColumnMetaData *cmd = &(column_meta_data[idx]);
  uint32_t start = file.tellp();
  write_dictionary(file, idx, sel, from, until);
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
  int64_t rg_from,
  int64_t rg_until,
  uint64_t page_from,
  uint64_t page_until) {

  streampos start = file.tellp();
  write_dictionary_indices(file, idx, rg_from, rg_until,
                           page_from, page_until);
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
  parquet::CompressionCodec::type codec,
  ByteBuffer &src,
  uint32_t src_size,
  ByteBuffer &tgt,
  uint32_t skip) {

  if (codec == CompressionCodec::SNAPPY) {
    size_t tgt_size_est = snappy::MaxCompressedLength(src_size - skip);
    tgt.reset(tgt_size_est + skip);
    if (skip > 0) memcpy(tgt.ptr, src.ptr, skip);
    size_t tgt_size;
    snappy::RawCompress(src.ptr + skip, src_size - skip, tgt.ptr + skip, &tgt_size);
    return tgt_size + skip;

  } else if (codec == CompressionCodec::GZIP) {
    miniz::MiniZStream mzs;
    if (compression_level < 0) {
      mzs.compression_level = miniz::MZ_DEFAULT_LEVEL;
    } else if (compression_level >= 9) {
      mzs.compression_level = 9;
    } else {
      mzs.compression_level = compression_level;
    }
    size_t tgt_size_est = mzs.MaxCompressedLength(src_size - skip);
    tgt.reset(tgt_size_est + skip);
    if (skip > 0) memcpy(tgt.ptr, src.ptr, skip);
    size_t tgt_size = tgt_size_est;
    // throws on error
    mzs.Compress(src.ptr + skip, src_size - skip, tgt.ptr + skip, &tgt_size);
    return tgt_size + skip;

  } else if (codec == CompressionCodec::ZSTD) {
    size_t tgt_size_est = zstd::ZSTD_compressBound(src_size - skip);
    tgt.reset(tgt_size_est);
    if (skip > 0) memcpy(tgt.ptr, src.ptr, skip);
    int level;
    int minlevel = zstd::ZSTD_minCLevel();
    int maxlevel = zstd::ZSTD_maxCLevel();
    if (compression_level < minlevel) {
      level = minlevel;
    } else if (compression_level > maxlevel) {
      level = maxlevel;
    } else {
      level = compression_level;
    }
    size_t tgt_size = zstd::ZSTD_compress(
      tgt.ptr + skip,
      tgt_size_est,
      src.ptr + skip,
      src_size - skip,
      level
    );
    if (zstd::ZSTD_isError(tgt_size)) {
        std::stringstream ss;
        ss << "Zstd compression failure" << "' @ " << __FILE__ << ":"
           << __LINE__;
        throw runtime_error(ss.str());
    }
    return tgt_size + skip;

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

  size_t tgt_size_est = MaxRleBpSize((uint32_t*) src.ptr, src_size, bit_width);
  // std::cerr << "Estimated size: " << tgt_size_est << std::endl;
  tgt.reset(
    skip + tgt_size_est + (add_bit_width ? 1 : 0) + (add_size ? 4 : 0),
    true
  );
  if (add_bit_width) {
    tgt.ptr[skip] = bit_width;
  }
  size_t tgt_size = RleBpEncode(
    (uint32_t*) src.ptr,
    src_size,
    bit_width,
    (uint8_t *) tgt.ptr + skip + (add_bit_width ? 1 : 0) + (add_size ? 4 : 0),
    tgt_size_est
  );
  if (add_size) {
    // need memcpy for alignment
    memcpy(tgt.ptr + skip +  (add_bit_width ? 1 : 0), &tgt_size, 4);
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
  append();
}

void ParquetOutFile::append() {
  for (int idx = 0; idx < row_group_starts.size(); idx++) {
    // init for row group
    init_column_meta_data();

    // write
    int64_t from = row_group_starts[idx];
    int64_t until = idx < row_group_starts.size() - 1 ? row_group_starts[idx + 1] : num_rows;
    write_row_group(idx);
    int64_t total_size = write_columns(idx, from, until);

    // row group metadata
    vector<ColumnChunk> ccs;
    for (uint32_t col = 0; col < num_cols; col++) {
      ColumnChunk cc;
      cc.__set_file_offset(column_meta_data[col].data_page_offset);
      cc.__set_meta_data(column_meta_data[col]);
      ccs.push_back(cc);
    }
    RowGroup rg;
    rg.__set_num_rows(until - from);
    rg.__set_total_byte_size(total_size);
    rg.__set_columns(ccs);
    row_groups.push_back(rg);
  }
  write_footer();
  pfile.write("PAR1", 4);
  pfile_.close();
}

int64_t ParquetOutFile::write_columns(uint32_t group, int64_t from,
                                      int64_t until) {
  uint32_t start = pfile.tellp();
  for (uint32_t idx = 0; idx < num_cols; idx++) {
    write_column(idx, group, from, until);
  }
  uint32_t end = pfile.tellp();
  // return total size
  return end - start;
}

void ParquetOutFile::write_column(uint32_t idx, uint32_t group,
                                  int64_t from, int64_t until) {
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  SchemaElement se = schemas[idx + 1];
  uint32_t col_start = pfile.tellp();
  // we increase this as needed
  cmd->__set_total_uncompressed_size(0);
  Statistics stat;
  // we increase this as we write
  stat.__set_null_count(0);
  cmd->__set_statistics(stat);
  if (encodings[idx] == Encoding::RLE_DICTIONARY) {
    uint32_t dictionary_page_offset = pfile.tellp();
    write_dictionary_page(idx, from, until);
    cmd->__set_dictionary_page_offset(dictionary_page_offset);
  }
  uint32_t data_offset = pfile.tellp();
  write_data_pages(idx, group, from, until);
  int32_t column_bytes = ((int32_t) pfile.tellp()) - col_start;
  cmd->__set_num_values(until - from);
  cmd->__set_total_compressed_size(column_bytes);
  cmd->__set_data_page_offset(data_offset);
  // min-max values
  std::string min_value, max_value;
  if (get_group_minmax_values(idx, group, se, min_value, max_value)) {
    Statistics *stat = &cmd->statistics;
    stat->__set_min_value(min_value);
    stat->__set_max_value(max_value);
    stat->__set_is_min_value_exact(true);
    stat->__set_is_max_value_exact(true);
  }
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

void ParquetOutFile::write_dictionary_page(uint32_t idx, int64_t from,
                                           int64_t until) {
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  SchemaElement se = schemas[idx + 1];
  // Uncompresed size of the dictionary in bytes
  uint32_t dict_size = get_size_dictionary(idx, se, from, until);
  // Number of entries in the dicitonary
  uint32_t num_dict_values = get_num_values_dictionary(idx, se, from, until);

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
    write_dictionary_(pfile, idx, dict_size, se, from, until);

  } else {
    // With compression we need two temporary buffers
    // 1. write data to buf_unc
    buf_unc.reset(dict_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_(*os0, idx, dict_size, se, from, until);

    // 2. compress buf_unc to buf_com
    size_t cdict_size = compress(cmd->codec, buf_unc, dict_size, buf_com);

    // 3. write buf_com to file
    ph.__set_compressed_page_size(cdict_size);
    write_page_header(idx, ph);
    pfile.write((const char *) buf_com.ptr, cdict_size);
  }
}

void ParquetOutFile::write_data_pages(uint32_t idx, uint32_t group,
                                      int64_t from, int64_t until) {
  SchemaElement se = schemas[idx + 1];
  int64_t rg_num_rows = until - from;

  // guess total size and decide on number of pages
  uint64_t total_size;
  if (encodings[idx] == Encoding::PLAIN) {
    total_size = calculate_column_data_size(idx, rg_num_rows, from, until);
  } else {
    // estimate the max RLE length
    uint32_t num_values = get_num_values_dictionary(idx, se, from, until);
    uint8_t bit_width = num_values > 0 ? ceil(log2((double) num_values)) : 1;
    total_size = MaxRleBpSize(rg_num_rows, bit_width);
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

  uint32_t rows_per_page = rg_num_rows / num_pages + (rg_num_rows % num_pages ? 1 : 0);
  if (rows_per_page == 0)  {
    rows_per_page = 1;
  }

  for (auto i = 0; i < num_pages; i++) {
    uint64_t page_from = from + i * rows_per_page;
    uint64_t page_until = from + (i + 1) * rows_per_page;
    if (page_until > until) {
      page_until = until;
    }
    write_data_page(idx, group, i, from, until, page_from, page_until);
  }
}

void ParquetOutFile::write_data_page(uint32_t idx, uint32_t group,
                                     uint32_t page, int64_t rg_from,
                                     int64_t rg_until, uint64_t page_from,
                                     uint64_t page_until) {
  ColumnMetaData *cmd = &(column_meta_data[idx]);
  Statistics *stat = &(cmd->statistics);
  SchemaElement se = schemas[idx + 1];
  PageHeader ph;
  DataPageHeaderV2 dph2;
  uint32_t page_num_values = page_until - page_from;
  if (data_page_version == 1) {
    ph.__set_type(PageType::DATA_PAGE);
    DataPageHeader dph;
    dph.__set_num_values(page_num_values);
    dph.__set_encoding(encodings[idx]);
    if (se.repetition_type == FieldRepetitionType::OPTIONAL) {
      dph.__set_definition_level_encoding(Encoding::RLE);
    }
    // for version 1 we can set it here
    ph.__set_data_page_header(dph);
  } else if (data_page_version == 2) {
    ph.__set_type(PageType::DATA_PAGE_V2);
    dph2.__set_num_values(page_num_values);
    dph2.__set_num_rows(page_num_values);
    dph2.__set_encoding(encodings[idx]);
    // these might be overwritten later if there are NAs
    dph2.__set_num_nulls(0);
    dph2.__set_definition_levels_byte_length(0);
    dph2.__set_repetition_levels_byte_length(0);
    // for version 2 we need to set the header after we
    // set the remaining fields
  } else {
    throw runtime_error("Invalid data page version");
  }

  if (se.repetition_type == FieldRepetitionType::REQUIRED &&
      encodings[idx] == Encoding::PLAIN &&
      cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 1: REQ, PLAIN, UNC
    // 1. write directly to file
    uint32_t data_size = calculate_column_data_size(
      idx, page_num_values, page_from, page_until
    );
    ph.__set_uncompressed_page_size(data_size);
    ph.__set_compressed_page_size(data_size);
    if (data_page_version == 2) {
      ph.__set_data_page_header_v2(dph2);
    }
    write_page_header(idx, ph);
    write_data_(pfile, idx, data_size, group, page, page_from, page_until);

  } else if (se.repetition_type == FieldRepetitionType::REQUIRED &&
             encodings[idx] == Encoding::PLAIN &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 2: REQ, PLAIN, COMP
    // 1. write data to buf_unc
    uint32_t data_size = calculate_column_data_size(
      idx, page_num_values, page_from, page_until
    );
    ph.__set_uncompressed_page_size(data_size);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_data_(*os0, idx, data_size, group, page, page_from, page_until);

    // 2. compress buf_unc to buf_com
    size_t cdata_size = compress(cmd->codec, buf_unc, data_size, buf_com);

    // 3. write buf_com to file
    ph.__set_compressed_page_size(cdata_size);
    if (data_page_version == 2) {
      ph.__set_data_page_header_v2(dph2);
    }
    write_page_header(idx, ph);
    pfile.write((const char *) buf_com.ptr, cdata_size);

  } else if (se.repetition_type == FieldRepetitionType::REQUIRED &&
             encodings[idx] == Encoding::RLE_DICTIONARY &&
             cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 3: REQ, RLE_DICT, UNC
    // 1. write dictionary indices to buf_unc
    uint32_t data_size = (page_num_values) * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_indices_(*os0, idx, data_size, rg_from, rg_until,
                              page_from, page_until);

    // 2. RLE encode buf_unc to buf_com
    uint32_t num_dict_values = get_num_values_dictionary(idx, se, rg_from, rg_until);
    uint8_t bit_width = ceil(log2((double) num_dict_values));
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
      buf_com,
      bit_width,
      true
    );

    // 3. write buf_com to file
    ph.__set_uncompressed_page_size(rle_size);
    ph.__set_compressed_page_size(rle_size);
    if (data_page_version == 2) {
      ph.__set_data_page_header_v2(dph2);
    }
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
    uint32_t data_size = page_num_values * sizeof(int);
    ph.__set_uncompressed_page_size(data_size);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_indices_(*os0, idx, data_size, rg_from, rg_until,
                              page_from, page_until);

    // 2. RLE encode buf_unc to buf_com
    uint32_t num_dict_values = get_num_values_dictionary(idx, se, rg_from, rg_until);
    uint8_t bit_width = ceil(log2((double) num_dict_values));
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
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
    if (data_page_version == 2) {
      ph.__set_data_page_header_v2(dph2);
    }
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
    uint32_t miss_size = page_num_values * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os0, idx, page_from, page_until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(buf_unc, page_num_values, buf_com, 1, false);

    // 3. Write buf_unc to file
    uint32_t data_size = calculate_column_data_size(
      idx, num_present, page_from, page_until
    );
    int prep_length = data_page_version == 1 ? 4 : 0;
    ph.__set_uncompressed_page_size(data_size + rle_size + prep_length);
    ph.__set_compressed_page_size(data_size + rle_size + prep_length);
    if (data_page_version == 2) {
      dph2.__set_num_nulls(page_num_values - num_present);
      dph2.__set_definition_levels_byte_length(rle_size);
      ph.__set_data_page_header_v2(dph2);
    }
    write_page_header(idx, ph);
    if (data_page_version == 1) {
      pfile.write((const char*) &rle_size, 4);
    }
    pfile.write((const char*) buf_com.ptr, rle_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle_size + 4
    );
    stat->__set_null_count(stat->null_count + page_num_values - num_present);

    // 4. write data to file
    write_present_data_(pfile, idx, data_size, num_present, group, page,
                        page_from, page_until);

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::PLAIN &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 6: OPT, PLAIN, COMP
    // 1. write definition levels to buf_unc
    uint32_t miss_size = page_num_values * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os0, idx, page_from, page_until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
      buf_com,
      1,                      // bit_width
      false,                  // add_bit_width
      data_page_version == 1  // add_size
    );

    // 3. Append data to buf_com
    uint32_t data_size = calculate_column_data_size(
      idx, num_present, page_from, page_until
    );
    buf_com.resize(rle_size + data_size, true);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_com));
    buf_com.skip(rle_size);
    write_present_data_(*os1, idx, data_size, num_present, group, page,
                        page_from, page_until);

    // 4. compress buf_com to buf_unc
    // for data page v2, the def levels are not compressed!
    uint32_t skip = data_page_version == 1 ? 0 : rle_size;
    size_t comp_size = compress(cmd->codec, buf_com, rle_size + data_size, buf_unc, skip);

    // 5. write buf_unc to file
    ph.__set_uncompressed_page_size(rle_size + data_size);
    ph.__set_compressed_page_size(comp_size);
    if (data_page_version == 2) {
      dph2.__set_num_nulls(page_num_values - num_present);
      dph2.__set_definition_levels_byte_length(rle_size);
      ph.__set_data_page_header_v2(dph2);
    }
    write_page_header(idx, ph);
    pfile.write((const char*) buf_unc.ptr, comp_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle_size
    );
    stat->__set_null_count(stat->null_count + page_num_values - num_present);

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::RLE_DICTIONARY &&
             cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 7: OPT RLE_DICT UNC
    // 1. write definition levels to buf_unc
    uint32_t miss_size = page_num_values * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os1, idx, page_from, page_until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
      buf_com,
      1,                       // bit_width
      false,                   // add_bit_width
      data_page_version == 1   // add_size
    );

    // 3. write dictionaery indices to buf_unc
    uint32_t data_size = num_present * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_indices_(*os0, idx, data_size, rg_from, rg_until,
                              page_from, page_until);

    // 4. append RLE buf_unc to buf_com
    uint32_t num_dict_values = get_num_values_dictionary(idx, se, rg_from, rg_until);
    uint8_t bit_width = num_dict_values > 0 ? ceil(log2((double) num_dict_values)) : 1;
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
    if (data_page_version == 2) {
      dph2.__set_num_nulls(page_num_values - num_present);
      dph2.__set_definition_levels_byte_length(rle_size);
      ph.__set_data_page_header_v2(dph2);
    }
    write_page_header(idx, ph);
    pfile.write((const char*) buf_com.ptr, rle2_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle2_size
    );
    stat->__set_null_count(stat->null_count + page_num_values - num_present);

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::RLE_DICTIONARY &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 8: OPT RLE_DICT COM
    // 1. write definition levels to buf_unc
    uint32_t miss_size = page_num_values * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os1, idx, page_from, page_until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
      buf_com,
      1,                      // bit_width
      false,                  // add_bit_width
      data_page_version == 1  // add_size
    );

    // 3. write dictionaery indices to buf_unc
    uint32_t data_size = num_present * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_dictionary_indices_(*os0, idx, data_size, rg_from, rg_until,
                              page_from, page_until);

    // 4. append RLE buf_unc to buf_com
    uint32_t num_dict_values = get_num_values_dictionary(idx, se, rg_from, rg_until);
    uint8_t bit_width = num_dict_values > 0 ? ceil(log2((double) num_dict_values)) : 0;
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
    uint32_t skip = data_page_version == 1 ? 0 : rle_size;
    size_t crle2_size = compress(cmd->codec, buf_com, rle2_size, buf_unc, skip);

    // 6. write buf_unc to file
    ph.__set_uncompressed_page_size(rle2_size);
    ph.__set_compressed_page_size(crle2_size);
    if (data_page_version == 2) {
      dph2.__set_num_nulls(page_num_values - num_present);
      dph2.__set_definition_levels_byte_length(rle_size);
      ph.__set_data_page_header_v2(dph2);
    }
    write_page_header(idx, ph);
    pfile.write((const char*) buf_unc.ptr, crle2_size);
    cmd->__set_total_uncompressed_size(
      cmd->total_uncompressed_size + rle2_size
    );
    stat->__set_null_count(stat->null_count + page_num_values - num_present);

  } else if (se.repetition_type == FieldRepetitionType::REQUIRED &&
             encodings[idx] == Encoding::RLE &&
             cmd->codec == CompressionCodec::UNCOMPRESSED) {
    // CASE 9: REQ, RLE, UNCOMP
    // 1. write logicals into buf_unc
    uint32_t data_size = page_num_values * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_boolean_as_int(*os0, idx, group, page, page_from, page_until);

    // 2. RLE encode buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
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
    uint32_t data_size = page_num_values * sizeof(int);
    buf_unc.reset(data_size);
    std::unique_ptr<std::ostream> os0 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    write_boolean_as_int(*os0, idx, group, page, page_from, page_until);

    // 2. RLE encode buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
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
    uint32_t miss_size = page_num_values * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os1, idx, page_from, page_until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
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
    write_present_boolean_as_int(*os0, idx, num_present, page_from, page_until);

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
    stat->__set_null_count(stat->null_count + page_num_values - num_present);

  } else if (se.repetition_type == FieldRepetitionType::OPTIONAL &&
             encodings[idx] == Encoding::RLE &&
             cmd->codec != CompressionCodec::UNCOMPRESSED) {
    // CASE 12: OPT RLE COMP
    // 1. write definition levels to buf_unc
    uint32_t miss_size = page_num_values * sizeof(int);
    buf_unc.reset(miss_size);
    std::unique_ptr<std::ostream> os1 =
      std::unique_ptr<std::ostream>(new std::ostream(&buf_unc));
    uint32_t num_present = write_present(*os1, idx, page_from, page_until);

    // 2. RLE buf_unc to buf_com
    uint32_t rle_size = rle_encode(
      buf_unc,
      page_num_values,
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
    write_present_boolean_as_int(*os0, idx, num_present, page_from, page_until);

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
    stat->__set_null_count(stat->null_count + page_num_values - num_present);
  }
}

uint64_t ParquetOutFile::calculate_column_data_size(uint32_t idx,
                                                    uint32_t num_present,
                                                    uint64_t from,
                                                    uint64_t until) {
  // +1 is to skip the root schema
  parquet::SchemaElement &se = schemas[idx + 1];
  parquet::Type::type type = se.type;
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
  case Type::INT96: {
    return num_present * sizeof(Int96);
  }
  case Type::FLOAT: {
    return num_present * sizeof(float);
  }
  case Type::DOUBLE: {
    return num_present * sizeof(double);
  }
  case Type::FIXED_LEN_BYTE_ARRAY: {
    return num_present * se.type_length;
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
  FileMetaData fmd;
  fmd.__set_version(1);
  fmd.__set_schema(schemas);
  fmd.__set_num_rows(num_total_rows_set ? num_total_rows : num_rows);
  fmd.__set_row_groups(row_groups);
  fmd.__set_key_value_metadata(kv);
  fmd.__set_created_by("https://github.com/gaborcsardi/nanoparquet");
  fmd.write(tproto.get());
  uint8_t *out_buffer;
  uint32_t out_length;
  mem_buffer->getBuffer(&out_buffer, &out_length);
  pfile.write((char *)out_buffer, out_length);

  // size of the footer (without the magic bytes)
  pfile.write((const char *)&out_length, 4);
}
