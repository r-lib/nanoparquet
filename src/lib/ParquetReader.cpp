#include <sstream>

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "ParquetReader.h"
#include "bytebuffer.h"
#include "RleBpDecoder.h"

using namespace std;
using namespace parquet;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace nanoparquet;

static TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;

template <class T>
static void thrift_unpack(const uint8_t *buf, uint32_t *len,
                          T *deserialized_msg, string &filename) {
  shared_ptr<TMemoryBuffer> tmem_transport(
      new TMemoryBuffer(const_cast<uint8_t *>(buf), *len));
  shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
  try {
    deserialized_msg->read(tproto.get());
  } catch (std::exception &e) {
    std::stringstream ss;
    ss << "Invalid Parquet file '" << filename
       << "'. Couldn't deserialize thrift: " << e.what() << "\n";
    throw std::runtime_error(ss.str());
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
}

ParquetReader::ParquetReader(std::string filename)
  : file_type_(FILE_ON_DISK), filename_(filename) {
  init_file_on_disk();
}

void ParquetReader::init_file_on_disk() {
  ByteBuffer buf;
  pfile.open(filename_, std::ios::binary);
  if (pfile.fail()) {
    std::stringstream ss;
    ss << "Can't open Parquet file at '" << filename_ << "' @ "
       << __FILE__ << ":" << __LINE__ + 1;
    throw std::runtime_error(ss.str());
  }

  buf.resize(4);
  memset(buf.ptr, '\0', 4);
  // check for magic bytes at start of file
  pfile.read(buf.ptr, 4);
  if (strncmp(buf.ptr, "PAR1", 4) != 0) {
    std::stringstream ss;
    ss << "No leading magic bytes, invalid Parquet file at '" << filename_
       << "' @ " << __FILE__ << ":" << __LINE__ + 1;
    throw runtime_error(ss.str());
  }

  // check for magic bytes at end of file
  pfile.seekg(-4, ios_base::end);
  pfile.read(buf.ptr, 4);
  if (strncmp(buf.ptr, "PAR1", 4) != 0) {
    std::stringstream ss;
    ss << "No trailing magic bytes, invalid Parquet file at '" << filename_
       << "' @ " << __FILE__ << ":" << __LINE__ + 1;
    throw runtime_error(ss.str());
  }

  // read four-byte footer length from just before the end magic bytes
  pfile.seekg(-8, ios_base::end);
  pfile.read(buf.ptr, 4);
  int32_t footer_len = *(uint32_t *)buf.ptr;
  if (footer_len == 0) {
    std::stringstream ss;
    ss << "Footer length is zero, invalid Parquet file at '" << filename_
       << "' @ " << __FILE__ << ":" << __LINE__ + 1;
    throw runtime_error(ss.str());
  }

  // read footer into buffer and de-thrift
  buf.resize(footer_len);
  pfile.seekg(-(footer_len + 8), ios_base::end);
  pfile.read(buf.ptr, footer_len);
  if (!pfile) {
    std::stringstream ss;
    ss << "Could not read footer, invalid Parquet file at '" << filename_
       << "' @ " << __FILE__ << ":" << __LINE__ + 1;
    throw runtime_error(ss.str());
  }

  thrift_unpack((const uint8_t *)buf.ptr, (uint32_t *)&footer_len,
                &file_meta_data_, filename_);
  has_file_meta_data_ = true;

  leaf_cols.resize(file_meta_data_.schema.size());
  for (int i = 0, idx = 0; i < file_meta_data_.schema.size(); i++) {
    parquet::SchemaElement sel = file_meta_data_.schema[i];
    if (sel.__isset.num_children && sel.num_children > 0) {
      leaf_cols[i] = -1;
    } else {
      leaf_cols[i] = idx++;
    }
  }
}

// This is if we want to actually read the data. Not needed for metadata
// queries.
void ParquetReader::check_meta_data() {
  if (!has_file_meta_data_) {
    throw runtime_error("Parquet metadata is not known yet");
  }

  if (file_meta_data_.__isset.encryption_algorithm) {
    std::stringstream ss;
    ss << "Encrypted Parquet file are not supported, could not read file at '"
       << filename_ << "' @ " << __FILE__ << ":" << __LINE__ + 1;
    throw runtime_error(ss.str());
  }

  // check if we like this schema
  if (file_meta_data_.schema.size() < 2) {
    std::stringstream ss;
    ss << "Need at least one column, could not read Parquet file at '"
       << filename_ << "' @ " << __FILE__ << ":" << __LINE__ + 1;
    throw runtime_error(ss.str());
  }
  if (file_meta_data_.schema[0].num_children !=
      file_meta_data_.schema.size() - 1) {
    std::stringstream ss;
    ss << "Only flat tables (no nesting) are supported, could not read Parquet file at '"
       << filename_ << "' @ " << __FILE__ << ":" << __LINE__ + 1;
    throw runtime_error(ss.str());
  }

  // TODO assert that the first col is root

  for (uint64_t col_idx = 1; col_idx < file_meta_data_.schema.size();
       col_idx++) {
    auto &s_ele = file_meta_data_.schema[col_idx];

    if (!s_ele.__isset.type || s_ele.num_children > 0) {
      std::stringstream ss;
      ss << "Only flat tables (no nesting) are supported, could not read Parquet file at '"
         << filename_ << "' @ " << __FILE__ << ":" << __LINE__ + 1;
      throw runtime_error(ss.str());
    }
  }
}

void ParquetReader::read_all_columns() {
  for (uint32_t i = 1; i < file_meta_data_.schema.size(); i++) {
    read_column(i);
  }
}

void ParquetReader::read_selected_columns(std::vector<uint32_t> sel) {
  for (uint32_t i = 0; i < sel.size(); i++) {
    read_column(sel[i]);
  }
}

void ParquetReader::read_column(uint32_t column) {
  if (!has_file_meta_data_) {
    throw runtime_error("Cannot read column, metadata is not known");
  }
  SchemaElement &sel = file_meta_data_.schema[column];
  if (!sel.__isset.type) {
    throw runtime_error("Invalid Parquet file, column type is not set");
  }
  vector<parquet::RowGroup> &rgs = file_meta_data_.row_groups;

  for (auto rgi = 0; rgi < rgs.size(); rgi++) {
    parquet::ColumnChunk cc = rgs[rgi].columns[leaf_cols[column]];
    parquet::ColumnMetaData cmd = cc.meta_data;
    read_column_chunk(column, rgi, sel, cc);
  }
}

void ParquetReader::read_column_chunk(
  uint32_t column,
  uint32_t row_group,
  parquet::SchemaElement &sel,
  parquet::ColumnChunk &cc) {

  parquet::ColumnMetaData cmd = cc.meta_data;
  bool has_dictionary = cmd.__isset.dictionary_page_offset;
  int64_t dictionary_page_offset =
    has_dictionary ? cmd.dictionary_page_offset : -1;
  int64_t data_page_offset = cmd.data_page_offset;
  int64_t chunk_start =
    has_dictionary ? dictionary_page_offset : data_page_offset;
  int64_t chunk_end = chunk_start + cmd.total_compressed_size;

  // read in the whole chunk
  tmp_buf.resize(cmd.total_compressed_size, false);
  pfile.seekg(chunk_start, ios_base::beg);
  pfile.read(tmp_buf.ptr, cmd.total_compressed_size);
  char *ptr = tmp_buf.ptr;
  char *end = ptr + cmd.total_compressed_size;

  // dictionary page, if any
  if (has_dictionary) {
    PageHeader dph;
    uint32_t ph_size = cmd.total_compressed_size;
    thrift_unpack((const uint8_t *) ptr, &ph_size, &dph, filename_);
    ptr += ph_size;
    read_dict_page(column, row_group, sel, dph, ptr, dph.compressed_page_size);
    ptr += dph.compressed_page_size;
  }

  // data pages
  uint64_t from = 0;
  uint32_t page = 1;
  while (ptr < end) {
    PageHeader ph;
    uint32_t ph_size = end - ptr;
    thrift_unpack((const uint8_t *) ptr, &ph_size, &ph, filename_);
    ptr += ph_size;
    switch (ph.type) {
    case PageType::DATA_PAGE:
    case PageType::DATA_PAGE_V2: {
      uint32_t num_values = read_data_page(column, row_group, sel, page, from, ph, ptr, ph.compressed_page_size);
      from += num_values;
      page++;
      break;
    }
    case PageType::DICTIONARY_PAGE:
      throw runtime_error("Found dictionary page instead of data page");
      break;
    default:
      // INDEX_PAGE? skip silently
      break;
    }
    ptr += ph.compressed_page_size;
  }
}

void ParquetReader::read_dict_page(
  uint32_t column,
  uint32_t row_group,
  parquet::SchemaElement &sel,
  parquet::PageHeader &ph,
  const char *buf,
  int32_t len) {

  if (!ph.__isset.dictionary_page_header) {
    throw runtime_error("Invalid dictionary page");
  }
  if (ph.dictionary_page_header.encoding != Encoding::PLAIN &&
      ph.dictionary_page_header.encoding != Encoding::PLAIN_DICTIONARY) {
    throw runtime_error("Unknown encoding for dictionary page");
  }

  int32_t num_values = ph.dictionary_page_header.num_values;
  switch (sel.type) {
  case Type::INT32: {
    int32_t *res;
    add_dict_page_int32(column, row_group, &res, num_values);
    memcpy(res, buf, num_values * sizeof(int32_t));
    break;
  }
  case Type::INT64: {
    int64_t *res;
    add_dict_page_int64(column, row_group, &res, num_values);
    memcpy(res, buf, num_values * sizeof(int64_t));
    break;
  }
  case Type::INT96: {
    int96_t *res;
    add_dict_page_int96(column, row_group, &res, num_values);
    memcpy(res, buf, num_values * sizeof(int96_t));
    break;
  }
  case Type::FLOAT: {
    float *res;
    add_dict_page_float(column, row_group, &res, num_values);
    memcpy(res, buf, num_values * sizeof(float));
    break;
  }
  case Type::DOUBLE: {
    double *res;
    add_dict_page_double(column, row_group, &res, num_values);
    memcpy(res, buf, num_values * sizeof(double));
    break;
  }
  default:
    throw runtime_error("Not implemented yet 1");
  }
}

uint32_t ParquetReader::read_data_page(
  uint32_t column,
  uint32_t row_group,
  parquet::SchemaElement &sel,
  uint32_t page,
  uint64_t from,
  parquet::PageHeader &ph,
  const char *buf,
  int32_t len) {

  // TODO: definition levels
  // TODO: compression

  int32_t num_values;
  Encoding::type encoding;

  switch (ph.type) {
  case PageType::DATA_PAGE:
    if (!ph.__isset.data_page_header) {
      throw runtime_error("Invalid page, data page header not set");
    }
    num_values = ph.data_page_header.num_values;
    encoding = ph.data_page_header.encoding;
    break;
  case PageType::DATA_PAGE_V2:
    if (!ph.__isset.data_page_header_v2) {
      throw runtime_error("Invalid page, data page header not set");
    }
    num_values = ph.data_page_header_v2.num_values;
    encoding = ph.data_page_header_v2.encoding;
    break;
  default:
    throw runtime_error("Invalid page type, expected data page");
  }

  // TODO: parse/skip repetition levels
  // TODO: parse/skip definition levels
  // TODO: uncompress

  switch (sel.type) {
  case Type::INT32:
    read_data_page_int32(column, row_group, sel, page, from, ph, buf, len, encoding, num_values);
    break;
  case Type::INT64:
    read_data_page_int64(column, row_group, sel, page, from, ph, buf, len, encoding, num_values);
    break;
  case Type::INT96:
    read_data_page_int96(column, row_group, sel, page, from, ph, buf, len, encoding, num_values);
    break;
  case Type::FLOAT:
    read_data_page_float(column, row_group, sel, page, from, ph, buf, len, encoding, num_values);
    break;
  case Type::DOUBLE:
    read_data_page_double(column, row_group, sel, page, from, ph, buf, len, encoding, num_values);
    break;
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
  return num_values;
}

void ParquetReader::read_data_page_rle(
  uint32_t column,
  uint32_t row_group,
  uint32_t page,
  uint64_t from,
  const char *buf,
  int32_t buflen,
  uint32_t num_values) {

  uint32_t *res;
  add_dict_indices(column, row_group, page, &res, nullptr, num_values, from, from + num_values);
  int bw = *((uint8_t *) buf);
  buf += 1; buflen -= 1;
  if (bw == 0) {
    memset(res, 0, num_values * sizeof(uint32_t));
  } else {
    RleBpDecoder dec((const uint8_t*) buf, buflen, bw);
    // TODO: missing values
    dec.GetBatch<uint32_t>(res, num_values);
  }
}

void ParquetReader::read_data_page_int32(
  uint32_t column,
  uint32_t row_group,
  parquet::SchemaElement &sel,
  uint32_t page,
  uint64_t from,
  parquet::PageHeader &ph,
  const char *buf,
  int32_t len,
  parquet::Encoding::type encoding,
  uint32_t num_values) {

  switch (encoding) {
  case Encoding::PLAIN: {
    int32_t *res;
    add_data_page_int32(column, row_group, page, &res, nullptr, num_values, from, from + num_values);
    memcpy(res, buf, num_values * sizeof(int32_t));
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY: {
    read_data_page_rle(column, row_group, page, from, buf, ph.compressed_page_size, num_values);
    break;
  }
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_int64(
  uint32_t column,
  uint32_t row_group,
  parquet::SchemaElement &sel,
  uint32_t page,
  uint64_t from,
  parquet::PageHeader &ph,
  const char *buf,
  int32_t len,
  parquet::Encoding::type encoding,
  uint32_t num_values) {

  switch (encoding) {
  case Encoding::PLAIN: {
    int64_t *res;
    add_data_page_int64(column, row_group, page, &res, nullptr, num_values, from, from + num_values);
    memcpy(res, buf, num_values * sizeof(int64_t));
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY: {
    read_data_page_rle(column, row_group, page, from, buf, ph.compressed_page_size, num_values);
    break;
  }
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_int96(
  uint32_t column,
  uint32_t row_group,
  parquet::SchemaElement &sel,
  uint32_t page,
  uint64_t from,
  parquet::PageHeader &ph,
  const char *buf,
  int32_t len,
  parquet::Encoding::type encoding,
  uint32_t num_values) {

  switch (encoding) {
  case Encoding::PLAIN: {
    int96_t *res;
    add_data_page_int96(column, row_group, page, &res, nullptr, num_values, from, from + num_values);
    memcpy(res, buf, num_values * sizeof(int96_t));
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY: {
    read_data_page_rle(column, row_group, page, from, buf, ph.compressed_page_size, num_values);
    break;
  }
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_double(
  uint32_t column,
  uint32_t row_group,
  parquet::SchemaElement &sel,
  uint32_t page,
  uint64_t from,
  parquet::PageHeader &ph,
  const char *buf,
  int32_t len,
  parquet::Encoding::type encoding,
  uint32_t num_values) {

  double *res;
  switch (encoding) {
  case Encoding::PLAIN:
    add_data_page_double(column, row_group, page, &res, nullptr, num_values, from, from + num_values);
    memcpy(res, buf, num_values * sizeof(double));
    break;
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY:
    read_data_page_rle(column, row_group, page, from, buf, ph.compressed_page_size, num_values);
    break;
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_float(
  uint32_t column,
  uint32_t row_group,
  parquet::SchemaElement &sel,
  uint32_t page,
  uint64_t from,
  parquet::PageHeader &ph,
  const char *buf,
  int32_t len,
  parquet::Encoding::type encoding,
  uint32_t num_values) {

  float *res;
  switch (encoding) {
  case Encoding::PLAIN:
    add_data_page_float(column, row_group, page, &res, nullptr, num_values, from, from + num_values);
    memcpy(res, buf, num_values * sizeof(float));
    break;
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY:
    read_data_page_rle(column, row_group, page, from, buf, ph.compressed_page_size, num_values);
    break;
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}
