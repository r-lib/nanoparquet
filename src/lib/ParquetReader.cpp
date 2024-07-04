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

  for (uint32_t rgi = 0; rgi < rgs.size(); rgi++) {
    parquet::ColumnChunk pcc = rgs[rgi].columns[leaf_cols[column]];
    ColumnChunk cc(pcc, sel, column, rgi, rgs[rgi].num_rows);
    read_column_chunk(cc);
  }
}

void ParquetReader::read_column_chunk(ColumnChunk &cc) {
  parquet::ColumnMetaData cmd = cc.cc.meta_data;
  int64_t dictionary_page_offset =
    cc.has_dictionary ? cmd.dictionary_page_offset : -1;
  int64_t data_page_offset = cmd.data_page_offset;
  int64_t chunk_start =
    cc.has_dictionary ? dictionary_page_offset : data_page_offset;
  int64_t chunk_end = chunk_start + cmd.total_compressed_size;

  // Give a chance to R to allocate memory for the column chunk
  alloc_column_chunk(cc);

  // read in the whole chunk
  tmp_buf.resize(cmd.total_compressed_size, false);
  pfile.seekg(chunk_start, ios_base::beg);
  pfile.read(tmp_buf.ptr, cmd.total_compressed_size);
  uint8_t *ptr = (uint8_t*) tmp_buf.ptr;
  uint8_t *end = ptr + cmd.total_compressed_size;

  // dictionary page, if any
  if (cc.has_dictionary) {
    PageHeader dph;
    uint32_t ph_size = cmd.total_compressed_size;
    thrift_unpack(ptr, &ph_size, &dph, filename_);
    ptr += ph_size;
    read_dict_page(cc, dph, ptr, dph.compressed_page_size);
    ptr += dph.compressed_page_size;
  }

  // data pages
  uint64_t from = 0;
  uint32_t page = 0;
  while (ptr < end) {
    PageHeader ph;
    uint32_t ph_size = end - ptr;
    thrift_unpack((const uint8_t *) ptr, &ph_size, &ph, filename_);
    ptr += ph_size;
    switch (ph.type) {
    case PageType::DATA_PAGE:
    case PageType::DATA_PAGE_V2: {
      DataPage dp(cc, ph, page, from);
      uint32_t num_values = read_data_page(dp, ptr, ph.compressed_page_size);
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
  ColumnChunk &cc,
  parquet::PageHeader &ph,
  uint8_t *buf,
  int32_t len) {

  if (!ph.__isset.dictionary_page_header) {
    throw runtime_error("Invalid dictionary page");
  }
  if (ph.dictionary_page_header.encoding != Encoding::PLAIN &&
      ph.dictionary_page_header.encoding != Encoding::PLAIN_DICTIONARY) {
    throw runtime_error("Unknown encoding for dictionary page");
  }

  uint32_t num_values = ph.dictionary_page_header.num_values;

  switch (cc.sel.type) {
  case Type::INT32: {
    DictPage dict(cc, ph, num_values);
    alloc_dict_page(dict);
    memcpy(dict.dict, buf, num_values * sizeof(int32_t));
    break;
  }
  case Type::INT64: {
    DictPage dict(cc, ph, num_values);
    alloc_dict_page(dict);
    memcpy(dict.dict, buf, num_values * sizeof(int64_t));
    break;
  }
  case Type::INT96: {
    DictPage dict(cc, ph, num_values);
    alloc_dict_page(dict);
    memcpy(dict.dict, buf, num_values * sizeof(int96_t));
    break;
  }
  case Type::FLOAT: {
    DictPage dict(cc, ph, num_values);
    alloc_dict_page(dict);
    memcpy(dict.dict, buf, num_values * sizeof(float));
    break;
  }
  case Type::DOUBLE: {
    DictPage dict(cc, ph, num_values);
    alloc_dict_page(dict);
    memcpy(dict.dict, buf, num_values * sizeof(double));
    break;
  }
  case Type::BYTE_ARRAY: {
    DictPage dict(cc, ph, num_values, ph.uncompressed_page_size);
    alloc_dict_page(dict);
    scan_byte_array_plain(dict.strs, buf);
    break;
  }
  case Type::FIXED_LEN_BYTE_ARRAY: {
    DictPage dict(cc, ph, num_values, ph.uncompressed_page_size);
    alloc_dict_page(dict);
    scan_fixed_len_byte_array_plain(dict.strs, buf, cc.sel.type_length);
    break;
  }
  default:
    throw runtime_error("Not implemented yet 1");
  }
}

uint32_t ParquetReader::read_data_page(DataPage &dp, uint8_t *buf, int32_t len) {
  if (dp.ph.type == PageType::DATA_PAGE) {
    return read_data_page_v1(dp, buf, len);
  } else if (dp.ph.type == PageType::DATA_PAGE_V2) {
    return read_data_page_v2(dp, buf, len);
  } else {
    throw std::runtime_error("Invalid data page");
  }
}

uint32_t ParquetReader::read_data_page_v1(DataPage &dp, uint8_t *buf, int32_t len) {
  if (!dp.ph.__isset.data_page_header) {
    throw runtime_error("Invalid page, data page header not set");
  }
  dp.set_num_values(dp.ph.data_page_header.num_values);
  dp.encoding = dp.ph.data_page_header.encoding;

  uint8_t *def_buf = nullptr;
  uint32_t def_len = 0;

  if (dp.cc.optional) {
    if (dp.ph.data_page_header.definition_level_encoding != Encoding::RLE) {
      throw runtime_error("Unknown definition level encoding");
    }
    def_buf = buf;
    memcpy(&def_len, def_buf, sizeof(uint32_t));
    def_buf += sizeof(uint32_t);
    buf += sizeof(uint32_t);
    len -= sizeof(uint32_t);
    buf += def_len;
    len -= def_len;
    def_levels.resize(dp.num_values);
    RleBpDecoder dec((const uint8_t *)def_buf, def_len, 1);
    uint32_t num_present = dec.GetBatchCount<uint8_t>(
      (uint8_t*) def_levels.ptr,
      dp.num_values
    );
    dp.set_num_present(num_present);
  }
  alloc_data_page(dp);
  if (dp.cc.optional && dp.present != nullptr) {
    memcpy(dp.present, def_levels.ptr, dp.num_values);
  }

  uint32_t ret = read_data_page_internal(dp, buf, len);
  if (dp.cc.optional) {
    dp.present = (uint8_t*) def_levels.ptr;
  }
  add_data_page(dp);

  return ret;
}

uint32_t ParquetReader::read_data_page_v2(DataPage &dp, uint8_t *buf, int32_t len) {
  if (!dp.ph.__isset.data_page_header_v2) {
    throw runtime_error("Invalid page, data page v2 header not set");
  }
  dp.set_num_values(dp.ph.data_page_header_v2.num_values);
  dp.encoding = dp.ph.data_page_header_v2.encoding;

  uint8_t *def_buf = nullptr;
  uint32_t def_len = 0;

  if (dp.cc.optional) {
    def_buf = buf;
    def_len = dp.ph.data_page_header_v2.definition_levels_byte_length;
    buf += def_len;
    len -= def_len;
    uint32_t num_present = dp.num_values - dp.ph.data_page_header_v2.num_nulls;
    dp.set_num_present(num_present);
    alloc_data_page(dp);
    RleBpDecoder dec((const uint8_t *)def_buf, def_len, 1);
    if (dp.present) {
      dec.GetBatch<uint8_t>(dp.present, dp.num_values);
    } else {
      def_levels.resize(dp.num_values);
      dec.GetBatch<uint8_t>((uint8_t*) def_levels.ptr, dp.num_values);
    }
  } else {
    alloc_data_page(dp);
  }

  uint32_t ret = read_data_page_internal(dp, buf, len);
  if (!dp.present) {
    dp.present = (uint8_t*) def_levels.ptr;
  }
  add_data_page(dp);

  return ret;
}

uint32_t ParquetReader::read_data_page_internal(DataPage &dp, uint8_t *buf, int32_t len) {
  switch (dp.cc.sel.type) {
  case Type::BOOLEAN:
    read_data_page_boolean(dp, buf, len);
    break;
  case Type::INT32:
    read_data_page_int32(dp, buf, len);
    break;
  case Type::INT64:
    read_data_page_int64(dp, buf, len);
    break;
  case Type::INT96:
    read_data_page_int96(dp, buf, len);
    break;
  case Type::FLOAT:
    read_data_page_float(dp, buf, len);
    break;
  case Type::DOUBLE:
    read_data_page_double(dp, buf, len);
    break;
  case Type::BYTE_ARRAY:
    read_data_page_byte_array(dp, buf, len);
    break;
  case Type::FIXED_LEN_BYTE_ARRAY:
    read_data_page_fixed_len_byte_array(dp, buf, len);
    break;
  default:
    throw runtime_error("Not implemented yet");
    break;
  }

  return dp.num_present;
}

void ParquetReader::read_data_page_rle(DataPage &dp, uint8_t *buf) {
  uint32_t buflen = dp.ph.compressed_page_size;
  int bw = *((uint8_t *) buf);
  buf += 1; buflen -= 1;
  if (bw == 0) {
    memset(dp.data, 0, dp.num_present * sizeof(uint32_t));
  } else {
    RleBpDecoder dec((const uint8_t*) buf, buflen, bw);
    // TODO: missing values
    dec.GetBatch<uint32_t>((uint32_t*) dp.data, dp.num_present);
  }
}

void ParquetReader::read_data_page_boolean(DataPage &dp, uint8_t *buf, int32_t len) {
  switch (dp.encoding) {
  case Encoding::PLAIN: {
    unpack_plain_boolean((uint32_t*) dp.data, (uint8_t*) buf, dp.num_present);
    break;
  }
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_int32(
  DataPage &dp,
  uint8_t *buf,
  int32_t len) {

  switch (dp.encoding) {
  case Encoding::PLAIN: {
    memcpy(dp.data, buf, dp.num_present * sizeof(int32_t));
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY: {
    read_data_page_rle(dp, buf);
    break;
  }
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_int64(
  DataPage &dp,
  uint8_t *buf,
  int32_t len) {

  switch (dp.encoding) {
  case Encoding::PLAIN: {
    memcpy(dp.data, buf, dp.num_present * sizeof(int64_t));
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY: {
    read_data_page_rle(dp, buf);
    break;
  }
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_int96(
  DataPage &dp,
  uint8_t *buf,
  int32_t len) {

  switch (dp.encoding) {
  case Encoding::PLAIN: {
    memcpy(dp.data, buf, dp.num_present * sizeof(int96_t));
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY: {
    read_data_page_rle(dp, buf);
    break;
  }
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_float(
  DataPage &dp,
  uint8_t *buf,
  int32_t len) {

  switch (dp.encoding) {
  case Encoding::PLAIN: {
    memcpy(dp.data, buf, dp.num_present * sizeof(float));
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY:
    read_data_page_rle(dp, buf);
    break;
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_double(
  DataPage &dp,
  uint8_t *buf,
  int32_t len) {

  switch (dp.encoding) {
  case Encoding::PLAIN: {
    memcpy(dp.data, buf, dp.num_present * sizeof(double));
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY:
    read_data_page_rle(dp, buf);
    break;
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_byte_array(
  DataPage &dp,
  uint8_t *buf,
  int32_t len) {

  switch (dp.encoding) {
  case Encoding::PLAIN: {
    scan_byte_array_plain(dp.strs, buf);
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY:
    read_data_page_rle(dp, buf);
    break;
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::read_data_page_fixed_len_byte_array(
  DataPage &dp,
  uint8_t *buf,
  int32_t len) {

  switch (dp.encoding) {
  case Encoding::PLAIN: {
    scan_fixed_len_byte_array_plain(dp.strs, buf, dp.cc.sel.type_length);
    break;
  }
  case Encoding::RLE_DICTIONARY:
  case Encoding::PLAIN_DICTIONARY:
    read_data_page_rle(dp, buf);
    break;
  // TODO: rest
  default:
    throw runtime_error("Not implemented yet");
    break;
  }
}

void ParquetReader::unpack_plain_boolean(uint32_t *res, uint8_t *buf, uint32_t num_values) {
  // TODO: fast unpacking
  int byte_pos = 0;
  for (int32_t i = 0; i < num_values; i++) {
    res[i] = ((*buf) >> byte_pos) & 1;
    byte_pos++;
    if (byte_pos == 8) {
      byte_pos = 0;
      buf++;
    }
  }
}

void ParquetReader::scan_byte_array_plain(StringSet &strs, uint8_t *buf) {
  uint8_t *start = buf;
  uint8_t *end = buf + strs.total_len;
  memcpy((void*) strs.buf, buf, strs.total_len);
  // TODO: check for overflow
  for (uint32_t i = 0; i < strs.len; i++) {
    strs.lengths[i] = *((uint32_t*) buf);
    buf += 4;
    strs.offsets[i] = buf - start;
    buf += strs.lengths[i];
  }
}

void ParquetReader::scan_fixed_len_byte_array_plain(
  StringSet &strs, uint8_t *buf, uint32_t len) {
  memcpy((void*) strs.buf, buf, strs.total_len);
  for (uint32_t i = 0; i < strs.len; i++) {
    strs.lengths[i] = len;
    strs.offsets[i] = i * len;
  }
}
