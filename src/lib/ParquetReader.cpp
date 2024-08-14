#include <sstream>

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "ParquetReader.h"
#include "bytebuffer.h"
#include "RleBpDecoder.h"
#include "DbpDecoder.h"

#include "snappy/snappy.h"
#include "miniz/miniz_wrapper.hpp"
#include "zstd.h"

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
  // set nuber of threads here, assuming each thread needs k buffers
  bufman_cc = std::unique_ptr<BufferManager>(new BufferManager(1));
  bufman_na = std::unique_ptr<BufferManager>(new BufferManager(1));
  bufman_pg = std::unique_ptr<BufferManager>(new BufferManager(1));
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

  num_leaf_cols = 0;
  leaf_cols.resize(file_meta_data_.schema.size());
  for (int i = 0; i < file_meta_data_.schema.size(); i++) {
    parquet::SchemaElement sel = file_meta_data_.schema[i];
    if (sel.__isset.num_children && sel.num_children > 0) {
      leaf_cols[i] = -1;
    } else {
      leaf_cols[i] = num_leaf_cols++;
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
  BufferGuard tmp_buf_g = bufman_cc->claim();
  ByteBuffer &tmp_buf = tmp_buf_g.buf;
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

static CompressionCodec::type get_compression(nanoparquet::ColumnChunk &cc,
                                              parquet::PageHeader &ph) {
  CompressionCodec::type codec = cc.cc.meta_data.codec;
  if (ph.__isset.data_page_header_v2 &&
      ph.data_page_header_v2.__isset.is_compressed &&
      !ph.data_page_header_v2.is_compressed) {
    codec = CompressionCodec::UNCOMPRESSED;
  }
  return codec;
}

void extract_snappy(uint8_t* buf, int32_t len, ByteBuffer &bb,
                    int32_t unc_len, int32_t skip) {

  do {
    size_t dec_size;
    bool ok = snappy::GetUncompressedLength(
      (const char *) buf + skip,
      len - skip,
      &dec_size
    );
    if (dec_size + skip != unc_len) break;
    if (!ok) break;

    bb.resize(dec_size + skip);
    if (skip > 0) {
      memcpy(bb.ptr, buf, skip);
    }
    ok = snappy::RawUncompress(
      (const char*) buf + skip,
      len - skip,
      bb.ptr + skip
    );
    if (!ok) break;

    return;

  } while (false);

  std::stringstream ss;
  ss << "Decompression failure, possibly corrupt Parquet file '"
     << "' @ " << __FILE__ << ":" << __LINE__;
  throw runtime_error(ss.str());
}

void extract_gzip(uint8_t* buf, int32_t len, ByteBuffer &bb,
                  int32_t unc_len, int32_t skip) {
  miniz::MiniZStream gzst;
  bb.resize(unc_len);
  memcpy(bb.ptr, buf, skip);

  // throws on error
  gzst.Decompress(
    (const char*) buf + skip,
    len - skip,
    bb.ptr + skip,
    unc_len - skip
  );
}

void extract_zstd(uint8_t* buf, int32_t len, ByteBuffer &bb,
                  int32_t unc_len, int32_t skip) {
  bb.resize(unc_len);
  memcpy(bb.ptr, buf, skip);

  size_t res = zstd::ZSTD_decompress(
    bb.ptr + skip,
    unc_len - skip,
    buf + skip,
    len - skip);

  if (zstd::ZSTD_isError(res) ||
      res != (size_t) unc_len - skip) {
    std::stringstream ss;
    ss << "Zstd decompression failure, possibly corrupt Parquet file '"
       << "' @ " << __FILE__ << ":" << __LINE__;
      throw runtime_error(ss.str());
  }
}

// Not a very clean API, admittedly.

std::tuple<uint8_t *, int32_t>
ParquetReader::extract_page(ColumnChunk &cc,
                            parquet::PageHeader &ph,
                            uint8_t *buf,
                            int32_t len,
                            ByteBuffer &outbuf,
                            int32_t skip) {

  CompressionCodec::type codec = get_compression(cc, ph);
  if (codec == CompressionCodec::UNCOMPRESSED) {
    return std::make_tuple(buf, len);
  }

  switch(codec) {
  case CompressionCodec::SNAPPY:
    extract_snappy(buf, len, outbuf, ph.uncompressed_page_size, skip);
    break;

  case CompressionCodec::GZIP:
    extract_gzip(buf, len, outbuf, ph.uncompressed_page_size, skip);
    break;

  case CompressionCodec::ZSTD:
    extract_zstd(buf, len, outbuf, ph.uncompressed_page_size, skip);
    break;

  default:
    throw runtime_error("Compression is not supported yet");
  }

  return std::make_tuple((uint8_t*) outbuf.ptr, ph.uncompressed_page_size);
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

  // TODO: do not claim if not compressed
  BufferGuard bg = bufman_pg->claim();
  tie(buf, len) = extract_page(cc, ph, buf, len, bg.buf, 0);

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
  // TODO: do not claim if not compressed
  BufferGuard bg = bufman_pg->claim();

  if (dp.ph.type == PageType::DATA_PAGE) {
    tie(buf, len) = extract_page(dp.cc, dp.ph, buf, len, bg.buf, 0);
    return read_data_page_v1(dp, buf, len);
  } else if (dp.ph.type == PageType::DATA_PAGE_V2) {
    // Data page v2 does not compress the repetition and definition levels
    int32_t skip =
      dp.ph.data_page_header_v2.definition_levels_byte_length +
      dp.ph.data_page_header_v2.repetition_levels_byte_length;
    tie(buf, len) = extract_page(dp.cc, dp.ph, buf, len, bg.buf, skip);
    return read_data_page_v2(dp, buf, len);
  } else {
    throw std::runtime_error("Invalid data page");
  }
}

void ParquetReader::update_data_page_size(DataPage &dp, uint8_t *buf, int32_t len) {
  if (dp.encoding != Encoding::DELTA_BYTE_ARRAY) {
    return;
  }
  dp.prelen.resize(dp.num_present);
  dp.suflen.resize(dp.num_present);
  struct buffer prebuf = { buf, (uint32_t) len };
  DbpDecoder<int32_t, uint32_t> predec(&prebuf);
  uint8_t *sufpos = predec.decode(dp.prelen.data());
  struct buffer sufbuf = { sufpos, (uint32_t) (len - (sufpos - buf)) };
  DbpDecoder<int32_t, uint32_t> sufdec(&sufbuf);
  uint8_t *str = sufdec.decode(dp.suflen.data());
  dp.stroffset = str - buf;
  uint32_t totlen = 0;
  for (auto i = 0; i < dp.prelen.size(); i++) {
    totlen += dp.prelen[i] + dp.suflen[i];
  }
  dp.strs.total_len = totlen;
}

uint32_t ParquetReader::read_data_page_v1(DataPage &dp, uint8_t *buf, int32_t len) {
  if (!dp.ph.__isset.data_page_header) {
    throw runtime_error("Invalid page, data page header not set");
  }
  dp.set_num_values(dp.ph.data_page_header.num_values);
  dp.encoding = dp.ph.data_page_header.encoding;

  uint8_t *def_buf = nullptr;
  uint32_t def_len = 0;

  BufferGuard def_levels_g = bufman_na->claim();
  ByteBuffer &def_levels = def_levels_g.buf;

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
  update_data_page_size(dp, buf, len);
  alloc_data_page(dp);
  if (dp.cc.optional && dp.present != nullptr) {
    memcpy(dp.present, def_levels.ptr, dp.num_values);
  }

  return read_data_page_internal(dp, buf, len);
}

uint32_t ParquetReader::read_data_page_v2(DataPage &dp, uint8_t *buf, int32_t len) {
  if (!dp.ph.__isset.data_page_header_v2) {
    throw runtime_error("Invalid page, data page v2 header not set");
  }
  dp.set_num_values(dp.ph.data_page_header_v2.num_values);
  dp.encoding = dp.ph.data_page_header_v2.encoding;

  // skip junk repetition and definition levels, if any
  int32_t skip = dp.ph.data_page_header_v2.repetition_levels_byte_length;
  buf += skip;
  len -= skip;

  uint8_t *def_buf = nullptr;
  uint32_t def_len = 0;

  BufferGuard def_levels_g = bufman_na->claim();
  ByteBuffer &def_levels = def_levels_g.buf;

  if (dp.cc.optional) {
    def_buf = buf;
    def_len = dp.ph.data_page_header_v2.definition_levels_byte_length;
    buf += def_len;
    len -= def_len;
    uint32_t num_present = dp.num_values - dp.ph.data_page_header_v2.num_nulls;
    dp.set_num_present(num_present);
    update_data_page_size(dp, buf, len);
    alloc_data_page(dp);
    RleBpDecoder dec((const uint8_t *)def_buf, def_len, 1);
    if (dp.present) {
      dec.GetBatch<uint8_t>(dp.present, dp.num_values);
    } else {
      def_levels.resize(dp.num_values);
      dec.GetBatch<uint8_t>((uint8_t*) def_levels.ptr, dp.num_values);
    }
  } else {
    update_data_page_size(dp, buf, len);
    alloc_data_page(dp);
  }

  return read_data_page_internal(dp, buf, len);
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

template <class T> void ParquetReader::read_data_page_bss(
  DataPage &dp,
  uint8_t *buf,
  int32_t len,
  uint32_t elt_size) {

  uint8_t *end = dp.data + dp.num_present * elt_size;
  for (uint32_t b = 0; b < elt_size; b++) {
    for (uint8_t *out = dp.data + b; out < end; out += elt_size) {
      *out = *buf++;
    }
  }
}

void ParquetReader::read_data_page_boolean(DataPage &dp, uint8_t *buf, int32_t len) {
  switch (dp.encoding) {
  case Encoding::PLAIN: {
    unpack_plain_boolean((uint32_t*) dp.data, (uint8_t*) buf, dp.num_present);
    break;
  }
  case Encoding::RLE: {
    // skip length, we know how many values we want. bit width is 1
    RleBpDecoder dec(buf + 4, len - 4, 1);
    dec.GetBatch<uint32_t>((uint32_t*) dp.data, dp.num_present);
    break;
  }
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
  case Encoding::DELTA_BINARY_PACKED: {
    struct buffer dbpbuf = { (uint8_t*) buf, (uint32_t) len };
    DbpDecoder<int32_t, uint32_t> dec(&dbpbuf);
    dec.decode((int32_t*) dp.data);
    break;
  }
  case Encoding::BYTE_STREAM_SPLIT: {
    read_data_page_bss<int32_t>(dp, buf, len);
    break;
  }
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
  case Encoding::DELTA_BINARY_PACKED: {
    struct buffer dbpbuf = { (uint8_t*) buf, (uint32_t) len };
    DbpDecoder<int64_t, uint64_t> dec(&dbpbuf);
    dec.decode((int64_t*) dp.data);
    break;
  }
  case Encoding::BYTE_STREAM_SPLIT: {
    read_data_page_bss<int64_t>(dp, buf, len);
    break;
  }
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
  case Encoding::BYTE_STREAM_SPLIT: {
    read_data_page_bss<float>(dp, buf, len);
    break;
  }
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
  case Encoding::BYTE_STREAM_SPLIT: {
    read_data_page_bss<double>(dp, buf, len);
    break;
  }
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
  case Encoding::PLAIN_DICTIONARY: {
    read_data_page_rle(dp, buf);
    break;
  }
  case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
    scan_byte_array_delta_length(dp.strs, buf);
    break;
  }
  case Encoding::DELTA_BYTE_ARRAY: {
    scan_byte_array_delta(dp, buf, len);
    break;
  }
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
  case Encoding::PLAIN_DICTIONARY: {
    read_data_page_rle(dp, buf);
    break;
  }
  case Encoding::DELTA_BYTE_ARRAY: {
    scan_byte_array_delta(dp, buf, len);
    break;
  }
  case Encoding::BYTE_STREAM_SPLIT: {
    for (uint32_t i = 0; i < dp.strs.len; i++) {
      dp.strs.lengths[i] = dp.cc.sel.type_length;
      dp.strs.offsets[i] = i * dp.cc.sel.type_length;
    }
    dp.data = (uint8_t*) dp.strs.buf;
    read_data_page_bss<uint8_t>(dp, buf, len, dp.cc.sel.type_length);
    dp.data = nullptr;
    break;
  }
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
  memcpy((void*) strs.buf, buf, strs.total_len);
  // TODO: check for overflow
  for (uint32_t i = 0; i < strs.len; i++) {
    strs.lengths[i] = *((uint32_t*) buf);
    buf += 4;
    strs.offsets[i] = buf - start;
    buf += strs.lengths[i];
  }
}

void ParquetReader::scan_byte_array_delta_length(StringSet &strs, uint8_t *buf) {
  struct buffer dlbbuf = { buf, (uint32_t) strs.total_len };
  DbpDecoder<int32_t, uint32_t> dec(&dlbbuf);
  uint32_t num_present = dec.size();
  uint8_t *str = dec.decode((int32_t*) strs.lengths);
  uint32_t strlen = strs.total_len - (str - buf);
  memcpy(strs.buf, str, strlen);
  if (num_present > 0) strs.offsets[0] = 0;
  for (uint32_t i = 1; i < num_present; i++) {
    strs.offsets[i] = strs.offsets[i - 1] + strs.lengths[i - 1];
  }
}

void ParquetReader::scan_byte_array_delta(DataPage &dp, uint8_t *buf, int32_t len) {
  uint8_t *bse = (uint8_t*) dp.strs.buf, *out = bse;
  uint8_t *inp = buf + dp.stroffset;
  uint32_t offs = 0;
  for (uint32_t i = 0; i < dp.strs.len; i++) {
    dp.strs.offsets[i] = offs;
    dp.strs.lengths[i] = dp.prelen[i] + dp.suflen[i];
    if (i > 0 && dp.prelen[i] > 0) {
      memcpy(out, bse + dp.strs.offsets[i-1], dp.prelen[i]);
      out += dp.prelen[i];
      offs += dp.prelen[i];
    }
    if (dp.suflen[i]) {
      memcpy(out, inp, dp.suflen[i]);
      out += dp.suflen[i];
      offs += dp.suflen[i];
      inp += dp.suflen[i];
    }
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
