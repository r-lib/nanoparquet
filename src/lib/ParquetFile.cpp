#include <fstream>
#include <iostream>
#include <math.h>
#include <sstream>
#include <string>

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

#include "snappy/snappy.h"
#include "miniparquet.h"
#include "RleBpDecoder.h"

using namespace std;

using namespace parquet;
using namespace parquet::format;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace miniparquet;

static TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;

template <class T>
static void thrift_unpack(const uint8_t *buf, uint32_t *len,
                          T *deserialized_msg) {
  shared_ptr<TMemoryBuffer> tmem_transport(
      new TMemoryBuffer(const_cast<uint8_t *>(buf), *len));
  shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
  try {
    deserialized_msg->read(tproto.get());
  } catch (std::exception &e) {
    std::stringstream ss;
    ss << "Couldn't deserialize thrift: " << e.what() << "\n";
    throw std::runtime_error(ss.str());
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
}

ParquetFile::ParquetFile(std::string filename) {
  initialize(filename);
}

void ParquetFile::initialize(string filename) {
  ByteBuffer buf;
  pfile.open(filename, std::ios::binary);

  buf.resize(4);
  memset(buf.ptr, '\0', 4);
  // check for magic bytes at start of file
  pfile.read(buf.ptr, 4);
  if (strncmp(buf.ptr, "PAR1", 4) != 0) {
    throw runtime_error("File not found or missing magic bytes");
  }

  // check for magic bytes at end of file
  pfile.seekg( 0, ios_base::end );
  file_size = pfile.tellg();
  pfile.seekg(-4, ios_base::end);
  pfile.read(buf.ptr, 4);
  if (strncmp(buf.ptr, "PAR1", 4) != 0) {
    throw runtime_error("No magic bytes found at end of file");
  }

  // read four-byte footer length from just before the end magic bytes
  pfile.seekg(-8, ios_base::end);
  pfile.read(buf.ptr, 4);
  int32_t footer_len = *(uint32_t *)buf.ptr;
  if (footer_len == 0) {
    throw runtime_error("Footer length can't be 0");
  }

  // read footer into buffer and de-thrift
  buf.resize(footer_len);
  pfile.seekg(-(footer_len + 8), ios_base::end);
  pfile.read(buf.ptr, footer_len);
  if (!pfile) {
    throw runtime_error("Could not read footer");
  }

  thrift_unpack((const uint8_t *)buf.ptr, (uint32_t *)&footer_len,
                &file_meta_data);

  //	file_meta_data.printTo(cerr);
  //	cerr << "\n";

  // skip the first column its the root and otherwise useless
  for (uint64_t col_idx = 1; col_idx < file_meta_data.schema.size();
       col_idx++) {
    auto &s_ele = file_meta_data.schema[col_idx];

    // TODO scale? precision? complain if set
    auto col = unique_ptr<ParquetColumn>(new ParquetColumn());
    col->id = col_idx - 1;
    col->name = s_ele.name;
    col->schema_element = &s_ele;
    col->type = s_ele.type;
    columns.push_back(std::move(col));
  }
  this->nrow = file_meta_data.num_rows;
}

void ParquetFile::read_checks() {
  if (file_meta_data.__isset.encryption_algorithm) {
    throw runtime_error("Encrypted Parquet files are not supported");
  }

  // check if we like this schema
  if (file_meta_data.schema.size() < 2) {
    throw runtime_error("Need at least one column in the file");
  }
  if (file_meta_data.schema[0].num_children !=
      file_meta_data.schema.size() - 1) {
    throw runtime_error("Only flat tables are supported (no nesting)");
  }

  // TODO assert that the first col is root

  for (uint64_t col_idx = 1; col_idx < file_meta_data.schema.size();
       col_idx++) {
    auto &s_ele = file_meta_data.schema[col_idx];

    if (!s_ele.__isset.type || s_ele.num_children > 0) {
      throw runtime_error("Only flat tables are supported (no nesting)");
    }
  }
}

static string type_to_string(Type::type t) {
  std::ostringstream ss;
  ss << t;
  return ss.str();
}

class ColumnScan {
public:
  PageHeader page_header;
  bool seen_dict = false;
  const char *page_buf_ptr = nullptr;
  const char *page_buf_end_ptr = nullptr;
  void *dict = nullptr;
  uint64_t dict_size;

  uint64_t page_buf_len = 0;
  uint64_t page_start_row = 0;

  uint8_t *defined_ptr;

  // for FIXED_LEN_BYTE_ARRAY
  int32_t type_len;

  template <class T> void fill_dict() {
    auto dict_size = page_header.dictionary_page_header.num_values;
    dict = new Dictionary<T>(dict_size);
    for (int32_t dict_index = 0; dict_index < dict_size; dict_index++) {
      T val;
      memcpy(&val, page_buf_ptr, sizeof(val));
      page_buf_ptr += sizeof(T);

      ((Dictionary<T> *)dict)->dict[dict_index] = val;
    }
  }

  void scan_dict_page(ResultColumn &result_col) {
    if (page_header.__isset.data_page_header ||
        !page_header.__isset.dictionary_page_header) {
      throw runtime_error("Dictionary page header mismatch");
    }

    // make sure we like the encoding
    switch (page_header.dictionary_page_header.encoding) {
    case Encoding::PLAIN:
    case Encoding::PLAIN_DICTIONARY: // deprecated
      break;

    default:
      throw runtime_error("Dictionary page has unsupported/invalid encoding");
    }

    if (seen_dict) {
      throw runtime_error("Multiple dictionary pages for column chunk");
    }
    seen_dict = true;
    dict_size = page_header.dictionary_page_header.num_values;

    // initialize dictionaries per type
    switch (result_col.col->type) {
    case Type::BOOLEAN:
      fill_dict<bool>();
      break;
    case Type::INT32:
      fill_dict<int32_t>();
      break;
    case Type::INT64:
      fill_dict<int64_t>();
      break;
    case Type::INT96:
      fill_dict<Int96>();
      break;
    case Type::FLOAT:
      fill_dict<float>();
      break;
    case Type::DOUBLE:
      fill_dict<double>();
      break;
    case Type::BYTE_ARRAY:
      // no dict here we use the result set string heap directly
      {
        // never going to have more string data than this uncompressed_page_size
        // (lengths use bytes)
        auto string_heap_chunk = std::unique_ptr<char[]>(
            new char[page_header.uncompressed_page_size]);
        result_col.string_heap_chunks.push_back(std::move(string_heap_chunk));
        auto str_ptr =
            result_col
                .string_heap_chunks[result_col.string_heap_chunks.size() - 1]
                .get();
        dict = new Dictionary<char *>(dict_size);

        for (int32_t dict_index = 0; dict_index < dict_size; dict_index++) {
          uint32_t str_len;
          memcpy(&str_len, page_buf_ptr, sizeof(str_len));
          page_buf_ptr += sizeof(str_len);

          if (page_buf_ptr + str_len > page_buf_end_ptr) {
            throw runtime_error("Declared string length exceeds payload size");
          }

          ((Dictionary<char *> *)dict)->dict[dict_index] = str_ptr;
          // TODO make sure we dont run out of str_ptr
          memcpy(str_ptr, page_buf_ptr, str_len);
          str_ptr[str_len] = '\0'; // terminate
          str_ptr += str_len + 1;
          page_buf_ptr += str_len;
        }

        break;
      }
    default:
      throw runtime_error("Unsupported type for dictionary: " +
                          type_to_string(result_col.col->type));
    }
  }

  void scan_data_page(ResultColumn &result_col, bool has_def_levels) {
    if (!page_header.__isset.data_page_header ||
        page_header.__isset.dictionary_page_header) {
      throw runtime_error("Data page header mismatch");
    }

    if (page_header.__isset.data_page_header_v2) {
      throw runtime_error("Data page v2 unsupported");
    }

    auto num_values = page_header.data_page_header.num_values;

    // we have to first decode the define levels, if we have them
    if (has_def_levels) {
      switch (page_header.data_page_header.definition_level_encoding) {
      case Encoding::RLE: {
        // read length of define payload, always
        uint32_t def_length;
        memcpy(&def_length, page_buf_ptr, sizeof(def_length));
        page_buf_ptr += sizeof(def_length);

        RleBpDecoder dec((const uint8_t *)page_buf_ptr, def_length, 1);
        dec.GetBatch<uint8_t>(defined_ptr, num_values);

        page_buf_ptr += def_length;
      } break;
      default:
        throw runtime_error(
            "Definition levels have unsupported/invalid encoding");
      }
    } else {
      std::fill(defined_ptr, defined_ptr + num_values, static_cast<uint8_t>(1));
    }

    switch (page_header.data_page_header.encoding) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY: // deprecated
      scan_data_page_dict(result_col);
      break;

    case Encoding::PLAIN:
      scan_data_page_plain(result_col);
      break;

    default:
      throw runtime_error("Data page has unsupported/invalid encoding");
    }

    defined_ptr += num_values;
    page_start_row += num_values;
  }

  template <class T> void fill_values_plain(ResultColumn &result_col) {
    T *result_arr = (T *)result_col.data.ptr;
    for (int32_t val_offset = 0;
         val_offset < page_header.data_page_header.num_values; val_offset++) {

      if (!defined_ptr[val_offset]) {
        continue;
      }

      auto row_idx = page_start_row + val_offset;
      T val;
      memcpy(&val, page_buf_ptr, sizeof(val));
      page_buf_ptr += sizeof(T);
      result_arr[row_idx] = val;
    }
  }

  void scan_data_page_plain(ResultColumn &result_col) {
    // TODO compute null count while getting the def levels already?
    // uint32_t null_count = 0;
    // for (uint32_t i = 0; i < page_header.data_page_header.num_values; i++) {
    // 	 if (!defined_ptr[i]) {
    //	 null_count++;
    //   }
    // }

    switch (result_col.col->type) {
    case Type::BOOLEAN: {
      bool *result_arr = (bool *)result_col.data.ptr;
      int32_t nv = page_header.data_page_header.num_values;
      // current byte position
      int byte_pos = 0;
      for (int32_t idx = 0; idx < nv; idx++) {
        // missing?
        if (!defined_ptr[idx]) {
          continue;
        }
        result_arr[page_start_row + idx] =
          ((*page_buf_ptr) >> byte_pos) & 1;
        byte_pos++;
        if (byte_pos == 8) {
          byte_pos = 0;
          page_buf_ptr++;
        }
      }

    } break;
    case Type::INT32:
      fill_values_plain<int32_t>(result_col);
      break;
    case Type::INT64:
      fill_values_plain<int64_t>(result_col);
      break;
    case Type::INT96:
      fill_values_plain<Int96>(result_col);
      break;
    case Type::FLOAT:
      fill_values_plain<float>(result_col);
      break;
    case Type::DOUBLE:
      fill_values_plain<double>(result_col);
      break;

    case Type::FIXED_LEN_BYTE_ARRAY:
    case Type::BYTE_ARRAY: {
      uint32_t str_len = type_len; // in case of FIXED_LEN_BYTE_ARRAY

      uint64_t shc_len = page_header.uncompressed_page_size;
      if (result_col.col->type == Type::FIXED_LEN_BYTE_ARRAY) {
        shc_len += page_header.data_page_header
                       .num_values; // make space for terminators
      }
      auto string_heap_chunk = std::unique_ptr<char[]>(new char[shc_len]);
      result_col.string_heap_chunks.push_back(std::move(string_heap_chunk));
      auto str_ptr =
          result_col
              .string_heap_chunks[result_col.string_heap_chunks.size() - 1]
              .get();

      for (int32_t val_offset = 0;
           val_offset < page_header.data_page_header.num_values; val_offset++) {

        if (!defined_ptr[val_offset]) {
          continue;
        }

        auto row_idx = page_start_row + val_offset;

        if (result_col.col->type == Type::BYTE_ARRAY) {
          memcpy(&str_len, page_buf_ptr, sizeof(str_len));
          page_buf_ptr += sizeof(str_len);
        }

        if (page_buf_ptr + str_len > page_buf_end_ptr) {
          throw runtime_error("Declared string length exceeds payload size");
        }

        ((char **)result_col.data.ptr)[row_idx] = str_ptr;
        // TODO make sure we dont run out of str_ptr too
        memcpy(str_ptr, page_buf_ptr, str_len);
        str_ptr[str_len] = '\0';
        str_ptr += str_len + 1;

        page_buf_ptr += str_len;
      }
    } break;

    default:
      throw runtime_error("Unsupported type page_plain " +
                          type_to_string(result_col.col->type));
    }
  }

  template <class T>
  void fill_values_dict(ResultColumn &result_col, uint32_t *offsets) {
    auto result_arr = (T *)result_col.data.ptr;
    for (int32_t val_offset = 0;
         val_offset < page_header.data_page_header.num_values; val_offset++) {
      // always unpack because NULLs area also encoded (?)
      auto row_idx = page_start_row + val_offset;

      if (defined_ptr[val_offset]) {
        auto offset = offsets[val_offset];
        result_arr[row_idx] = ((Dictionary<T> *)dict)->get(offset);
      }
    }
  }

  // here we look back into the dicts and emit the values we find if the value
  // is defined, otherwise NULL
  void scan_data_page_dict(ResultColumn &result_col) {
    if (!seen_dict) {
      throw runtime_error("Missing dictionary page");
    }

    auto num_values = page_header.data_page_header.num_values;

    // num_values is int32, hence all dict offsets have to fit in 32 bit
    auto offsets = unique_ptr<uint32_t[]>(new uint32_t[num_values]);

    // the array offset width is a single byte
    auto enc_length = *((uint8_t *)page_buf_ptr);
    page_buf_ptr += sizeof(uint8_t);

    if (enc_length > 0) {
      RleBpDecoder dec((const uint8_t *)page_buf_ptr, page_buf_len, enc_length);

      uint32_t null_count = 0;
      for (uint32_t i = 0; i < num_values; i++) {
        if (!defined_ptr[i]) {
          null_count++;
        }
      }
      if (null_count > 0) {
        dec.GetBatchSpaced<uint32_t>(num_values, null_count, defined_ptr,
                                     offsets.get());
      } else {
        dec.GetBatch<uint32_t>(offsets.get(), num_values);
      }

    } else {
      memset(offsets.get(), 0, num_values * sizeof(uint32_t));
    }

    switch (result_col.col->type) {
      // TODO no bools here? I guess makes no sense to use dict...

    case Type::INT32:
      fill_values_dict<int32_t>(result_col, offsets.get());

      break;

    case Type::INT64:
      fill_values_dict<int64_t>(result_col, offsets.get());

      break;
    case Type::INT96:
      fill_values_dict<Int96>(result_col, offsets.get());

      break;

    case Type::FLOAT:
      fill_values_dict<float>(result_col, offsets.get());

      break;

    case Type::DOUBLE:
      fill_values_dict<double>(result_col, offsets.get());

      break;

    case Type::BYTE_ARRAY: {
      auto result_arr = (char **)result_col.data.ptr;
      for (int32_t val_offset = 0;
           val_offset < page_header.data_page_header.num_values; val_offset++) {
        if (defined_ptr[val_offset]) {
          result_arr[page_start_row + val_offset] =
              ((Dictionary<char *> *)dict)->get(offsets[val_offset]);
        } else {
          result_arr[page_start_row + val_offset] = nullptr;
        }
      }
      break;
    }
    default:
      throw runtime_error("Unsupported type page_dict " +
                          type_to_string(result_col.col->type));
    }
  }

  // ugly but well
  void cleanup(ResultColumn &result_col) {
    switch (result_col.col->type) {
    case Type::BOOLEAN:
      delete (Dictionary<bool> *)dict;
      break;
    case Type::INT32:
      delete (Dictionary<int32_t> *)dict;
      break;
    case Type::INT64:
      delete (Dictionary<int64_t> *)dict;
      break;
    case Type::INT96:
      delete (Dictionary<Int96> *)dict;
      break;
    case Type::FLOAT:
      delete (Dictionary<float> *)dict;
      break;
    case Type::DOUBLE:
      delete (Dictionary<double> *)dict;
      break;
    case Type::BYTE_ARRAY:
    case Type::FIXED_LEN_BYTE_ARRAY:
      delete (Dictionary<char *> *)dict;
      break;
    default:
      throw runtime_error("Unsupported type for dictionary: " +
                          type_to_string(result_col.col->type));
    }
  }
};

void ParquetFile::scan_column(ScanState &state, ResultColumn &result_col) {
  // we now expect a sequence of data pages in the buffer

  auto &row_group = file_meta_data.row_groups[state.row_group_idx];
  auto &chunk = row_group.columns[result_col.id];

  //	chunk.printTo(cerr);
  //	cerr << "\n";

  if (chunk.__isset.file_path) {
    throw runtime_error(
        "Only inlined data files are supported (no references)");
  }

  if (chunk.meta_data.path_in_schema.size() != 1) {
    throw runtime_error("Only flat tables are supported (no nesting)");
  }

  // ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
  auto chunk_start = chunk.meta_data.data_page_offset;
  if (chunk.meta_data.__isset.dictionary_page_offset &&
      chunk.meta_data.dictionary_page_offset >= 4) {
    // this assumes the data pages follow the dict pages directly.
    chunk_start = chunk.meta_data.dictionary_page_offset;
  }
  auto chunk_len = chunk.meta_data.total_compressed_size;

  // read entire chunk into RAM
  pfile.seekg(chunk_start);
  ByteBuffer chunk_buf;
  chunk_buf.resize(chunk_len);

  pfile.read(chunk_buf.ptr, chunk_len);
  if (!pfile) {
    throw runtime_error("Could not read chunk. File corrupt?");
  }

  // now we have whole chunk in buffer, proceed to read pages
  ColumnScan cs;
  auto bytes_to_read = chunk_len;

  // handle fixed len byte arrays, their length lives in schema
  if (result_col.col->type == Type::FIXED_LEN_BYTE_ARRAY) {
    cs.type_len = result_col.col->schema_element->type_length;
  }

  cs.page_start_row = 0;
  cs.defined_ptr = (uint8_t *)result_col.defined.ptr;
  SchemaElement sch = file_meta_data.schema[result_col.id + 1]; // skip root
  bool has_def_levels = sch.repetition_type != FieldRepetitionType::REQUIRED;

  while (bytes_to_read > 0) {
    auto page_header_len = bytes_to_read; // the header is clearly not that long
                                          // but we have no idea

    // this is the only other place where we actually unpack a thrift object
    cs.page_header = PageHeader();
    thrift_unpack((const uint8_t *)chunk_buf.ptr, (uint32_t *)&page_header_len,
                  &cs.page_header);
    //
    //		cs.page_header.printTo(cerr);
    //		cerr << "\n";

    // compressed_page_size does not include the header size
    chunk_buf.ptr += page_header_len;
    bytes_to_read -= page_header_len;

    auto payload_end_ptr = chunk_buf.ptr + cs.page_header.compressed_page_size;

    ByteBuffer decompressed_buf;

    switch (chunk.meta_data.codec) {
    case CompressionCodec::UNCOMPRESSED:
      cs.page_buf_ptr = chunk_buf.ptr;
      cs.page_buf_len = cs.page_header.compressed_page_size;

      break;
    case CompressionCodec::SNAPPY: {
      size_t decompressed_size;
      snappy::GetUncompressedLength(chunk_buf.ptr,
                                    cs.page_header.compressed_page_size,
                                    &decompressed_size);
      decompressed_buf.resize(decompressed_size + 1);

      auto res = snappy::RawUncompress(chunk_buf.ptr,
                                       cs.page_header.compressed_page_size,
                                       decompressed_buf.ptr);
      if (!res) {
        throw runtime_error("Decompression failure");
      }

      cs.page_buf_ptr = (char *)decompressed_buf.ptr;
      cs.page_buf_len = cs.page_header.uncompressed_page_size;

      break;
    }
    default:
      throw runtime_error(
          "Unsupported compression codec. Try uncompressed or snappy");
    }

    cs.page_buf_end_ptr = cs.page_buf_ptr + cs.page_buf_len;

    switch (cs.page_header.type) {
    case PageType::DICTIONARY_PAGE:
      cs.scan_dict_page(result_col);
      break;

    case PageType::DATA_PAGE: {
      cs.scan_data_page(result_col, has_def_levels);
      break;
    }
    case PageType::DATA_PAGE_V2:
      throw runtime_error("v2 data page format is not supported");

    default:
      break; // ignore INDEX page type and any other custom extensions
    }

    chunk_buf.ptr = payload_end_ptr;
    bytes_to_read -= cs.page_header.compressed_page_size;
  }
  cs.cleanup(result_col);
}

void ParquetFile::initialize_column(ResultColumn &col, uint64_t num_rows) {
  col.defined.resize(num_rows, false);
  memset(col.defined.ptr, 0, num_rows);
  col.string_heap_chunks.clear();

  // TODO do some logical type checking here, we dont like map, list, enum,
  // json, bson etc

  switch (col.col->type) {
  case Type::BOOLEAN:
    col.data.resize(sizeof(bool) * num_rows, false);
    break;
  case Type::INT32:
    col.data.resize(sizeof(int32_t) * num_rows, false);
    break;
  case Type::INT64:
    col.data.resize(sizeof(int64_t) * num_rows, false);
    break;
  case Type::INT96:
    col.data.resize(sizeof(Int96) * num_rows, false);
    break;
  case Type::FLOAT:
    col.data.resize(sizeof(float) * num_rows, false);
    break;
  case Type::DOUBLE:
    col.data.resize(sizeof(double) * num_rows, false);
    break;
  case Type::BYTE_ARRAY:
    col.data.resize(sizeof(char *) * num_rows, false);
    break;

  case Type::FIXED_LEN_BYTE_ARRAY: {
    auto s_ele = columns[col.id]->schema_element;

    if (!s_ele->__isset.type_length) {
      throw runtime_error("need a type length for fixed byte array");
    }
    col.data.resize(num_rows * sizeof(char *), false);
    break;
  }

  default:
    throw runtime_error("Unsupported type " + type_to_string(col.col->type));
  }
}

bool ParquetFile::scan(ScanState &s, ResultChunk &result) {
  if (s.row_group_idx >= file_meta_data.row_groups.size()) {
    result.nrows = 0;
    return false;
  }

  auto &row_group = file_meta_data.row_groups[s.row_group_idx];
  result.nrows = row_group.num_rows;

  for (auto &result_col : result.cols) {
    initialize_column(result_col, row_group.num_rows);
    scan_column(s, result_col);
  }

  s.row_group_idx++;
  return true;
}

void ParquetFile::initialize_result(ResultChunk &result) {
  result.nrows = 0;
  result.cols.resize(columns.size());
  for (size_t col_idx = 0; col_idx < columns.size(); col_idx++) {
    // result.cols[col_idx].type = columns[col_idx]->type;
    result.cols[col_idx].col = columns[col_idx].get();

    result.cols[col_idx].id = col_idx;
  }
}

pair<parquet::format::PageHeader, int64_t>
ParquetFile::read_page_header(int64_t pos) {
  uint32_t len = 2048;  // guessing, but this must be enough
  // Avoid going EOF, file_size is set when we open the file
  if (len > file_size - pos) {
    len = file_size - pos - 4;
  }
  tmp_buf.resize(len);
  pfile.seekg(pos, ios_base::beg);
  pfile.read(tmp_buf.ptr, len);
  if (pfile.eof()) {
    throw runtime_error(
      "End of file while reading Parquet file. This "
      "should not happen, please report it"
    );
  }
  PageHeader ph;
  uint32_t ph_size = len;
  thrift_unpack((const uint8_t *) tmp_buf.ptr, &ph_size, &ph);
  return std::make_pair(ph, ph_size);
}

void ParquetFile::read_chunk(int64_t offset, int64_t size, int8_t *buffer) {
  if (size > file_size - offset) {
    throw runtime_error("Cannot read past the end of a Parquet file");
  }
  pfile.seekg(offset, ios_base::beg);
  pfile.read((char*) buffer, size);
}
