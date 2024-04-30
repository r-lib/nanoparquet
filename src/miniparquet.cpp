#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <math.h>

#include "snappy/snappy.h"

#include "miniparquet.h"

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

using namespace std;

using namespace parquet;
using namespace parquet::format;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace miniparquet;

static TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;

template<class T>
static void thrift_unpack(const uint8_t *buf, uint32_t *len,
		T *deserialized_msg) {
	shared_ptr<TMemoryBuffer> tmem_transport(
			new TMemoryBuffer(const_cast<uint8_t*>(buf), *len));
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
	pfile.seekg(-4, ios_base::end);
	pfile.read(buf.ptr, 4);
	if (strncmp(buf.ptr, "PAR1", 4) != 0) {
		throw runtime_error("No magic bytes found at end of file");
	}

	// read four-byte footer length from just before the end magic bytes
	pfile.seekg(-8, ios_base::end);
	pfile.read(buf.ptr, 4);
	int32_t footer_len = *(uint32_t*) buf.ptr;
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

	thrift_unpack((const uint8_t*) buf.ptr, (uint32_t*) &footer_len,
			&file_meta_data);

//	file_meta_data.printTo(cerr);
//	cerr << "\n";

	if (file_meta_data.__isset.encryption_algorithm) {
		throw runtime_error("Encrypted Parquet files are not supported");
	}

	// check if we like this schema
	if (file_meta_data.schema.size() < 2) {
		throw runtime_error("Need at least one column in the file");
	}
	if (file_meta_data.schema[0].num_children
			!= file_meta_data.schema.size() - 1) {
		throw runtime_error("Only flat tables are supported (no nesting)");
	}

	// TODO assert that the first col is root

	// skip the first column its the root and otherwise useless
	for (uint64_t col_idx = 1; col_idx < file_meta_data.schema.size();
			col_idx++) {
		auto &s_ele = file_meta_data.schema[col_idx];

		if (!s_ele.__isset.type || s_ele.num_children > 0) {
			throw runtime_error("Only flat tables are supported (no nesting)");
		}
		// TODO if this is REQUIRED, there are no defined levels in file, support this
		// if field is REPEATED, no bueno
		if (s_ele.repetition_type != FieldRepetitionType::OPTIONAL) {
			throw runtime_error("Only OPTIONAL fields support for now");
		}
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

static string type_to_string(Type::type t) {
	std::ostringstream ss;
	ss << t;
	return ss.str();
}

// adapted from arrow parquet reader
class RleBpDecoder {

public:
	/// Create a decoder object. buffer/buffer_len is the decoded data.
	/// bit_width is the width of each value (before encoding).
	RleBpDecoder(const uint8_t *buffer, uint32_t buffer_len, uint32_t bit_width) :
			buffer(buffer), bit_width_(bit_width), current_value_(0), repeat_count_(
					0), literal_count_(0) {

		if (bit_width >= 64) {
			throw runtime_error("Decode bit width too large");
		}
		byte_encoded_len = ((bit_width_ + 7) / 8);
		max_val = (1 << bit_width_) - 1;

	}

	/// Gets a batch of values.  Returns the number of decoded elements.
	template<typename T>
	inline int GetBatch(T *values, int batch_size) {
		uint32_t values_read = 0;

		while (values_read < batch_size) {
			if (repeat_count_ > 0) {
				int repeat_batch = std::min(batch_size - values_read,
						static_cast<uint32_t>(repeat_count_));
				std::fill(values + values_read,
						values + values_read + repeat_batch,
						static_cast<T>(current_value_));
				repeat_count_ -= repeat_batch;
				values_read += repeat_batch;
			} else if (literal_count_ > 0) {
				uint32_t literal_batch = std::min(batch_size - values_read,
						static_cast<uint32_t>(literal_count_));
				uint32_t actual_read = BitUnpack<T>(values + values_read,
						literal_batch);
				if (literal_batch != actual_read) {
					throw runtime_error("Did not find enough values");
				}
				literal_count_ -= literal_batch;
				values_read += literal_batch;
			} else {
				if (!NextCounts<T>())
					return values_read;
			}
		}
		return values_read;
	}

	template<typename T>
	inline int GetBatchSpaced(uint32_t batch_size, uint32_t null_count,
			const uint8_t *defined, T *out) {
		//  DCHECK_GE(bit_width_, 0);
		uint32_t values_read = 0;
		uint32_t remaining_nulls = null_count;

		uint32_t d_off = 0; // defined_offset

		while (values_read < batch_size) {
			bool is_valid = defined[d_off++];

			if (is_valid) {
				if ((repeat_count_ == 0) && (literal_count_ == 0)) {
					if (!NextCounts<T>())
						return values_read;
				}
				if (repeat_count_ > 0) {
					// The current index is already valid, we don't need to check that again
					uint32_t repeat_batch = 1;
					repeat_count_--;

					while (repeat_count_ > 0
							&& (values_read + repeat_batch) < batch_size) {
						if (defined[d_off]) {
							repeat_count_--;
						} else {
							remaining_nulls--;
						}
						repeat_batch++;

						d_off++;
					}
					std::fill(out, out + repeat_batch,
							static_cast<T>(current_value_));
					out += repeat_batch;
					values_read += repeat_batch;
				} else if (literal_count_ > 0) {
					int literal_batch = std::min(
							batch_size - values_read - remaining_nulls,
							static_cast<uint32_t>(literal_count_));

					// Decode the literals
					constexpr int kBufferSize = 1024;
					T indices[kBufferSize];
					literal_batch = std::min(literal_batch, kBufferSize);
					auto actual_read = BitUnpack<T>(indices, literal_batch);

					if (actual_read != literal_batch) {
						throw runtime_error("Did not find enough values");

					}

					uint32_t skipped = 0;
					uint32_t literals_read = 1;
					*out++ = indices[0];

					// Read the first bitset to the end
					while (literals_read < literal_batch) {
						if (defined[d_off]) {
							*out = indices[literals_read];
							literals_read++;
						} else {
							skipped++;
						}
						++out;
						d_off++;
					}
					literal_count_ -= literal_batch;
					values_read += literal_batch + skipped;
					remaining_nulls -= skipped;
				}
			} else {
				++out;
				values_read++;
				remaining_nulls--;
			}
		}

		return values_read;
	}

private:
	const uint8_t *buffer;

	ByteBuffer unpack_buf;

	/// Number of bits needed to encode the value. Must be between 0 and 64.
	int bit_width_;
	uint64_t current_value_;
	uint32_t repeat_count_;
	uint32_t literal_count_;
	uint8_t byte_encoded_len;
	uint32_t max_val;

	// this is slow but whatever, calls are rare
	static uint8_t VarintDecode(const uint8_t *source, uint32_t *result_out) {
		uint32_t result = 0;
		uint8_t shift = 0;
		uint8_t len = 0;
		while (true) {
			auto byte = *source++;
			len++;
			result |= (byte & 127) << shift;
			if ((byte & 128) == 0)
				break;
			shift += 7;
			if (shift > 32) {
				throw runtime_error("Varint-decoding found too large number");
			}
		}
		*result_out = result;
		return len;
	}

	/// Fills literal_count_ and repeat_count_ with next values. Returns false if there
	/// are no more.
	template<typename T>
	bool NextCounts() {
		// Read the next run's indicator int, it could be a literal or repeated run.
		// The int is encoded as a vlq-encoded value.
		uint32_t indicator_value;

		// TODO check in varint decode if we have enough buffer left
		buffer += VarintDecode(buffer, &indicator_value);

		// TODO check a bunch of lengths here against the standard

		// lsb indicates if it is a literal run or repeated run
		bool is_literal = indicator_value & 1;
		if (is_literal) {
			literal_count_ = (indicator_value >> 1) * 8;
		} else {
			repeat_count_ = indicator_value >> 1;
			// (ARROW-4018) this is not big-endian compatible, lol
			current_value_ = 0;
			for (auto i = 0; i < byte_encoded_len; i++) {
				current_value_ |= ((uint8_t) *buffer++) << (i * 8);
			}
			// sanity check
			if (current_value_ > max_val) {
				throw runtime_error(
						"Payload value bigger than allowed. Corrupted file?");
			}
		}
		// TODO complain if we run out of buffer
		return true;
	}

	// somewhat optimized implementation that avoids non-alignment

	static const uint32_t BITPACK_MASKS[];
	static const uint8_t BITPACK_DLEN;

	template<typename T>
	uint32_t BitUnpack(T *dest, uint32_t count) {
		assert(bit_width_ < 32);

		int8_t bitpack_pos = 0;
		auto source = buffer;
		auto mask = BITPACK_MASKS[bit_width_];

		for (uint32_t i = 0; i < count; i++) {
			T val = (*source >> bitpack_pos) & mask;
			bitpack_pos += bit_width_;
			while (bitpack_pos > BITPACK_DLEN) {
				val |= (*++source << (BITPACK_DLEN - (bitpack_pos - bit_width_)))
						& mask;
				bitpack_pos -= BITPACK_DLEN;
			}
			dest[i] = val;
		}

		buffer += bit_width_ * count / 8;
		return count;
	}

};

const uint32_t RleBpDecoder::BITPACK_MASKS[] = { 0, 1, 3, 7, 15, 31, 63, 127,
		255, 511, 1023, 2047, 4095, 8191, 16383, 32767, 65535, 131071, 262143,
		524287, 1048575, 2097151, 4194303, 8388607, 16777215, 33554431,
		67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647 };

const uint8_t RleBpDecoder::BITPACK_DLEN = 8;

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

	template<class T>
	void fill_dict() {
		auto dict_size = page_header.dictionary_page_header.num_values;
		dict = new Dictionary<T>(dict_size);
		for (int32_t dict_index = 0; dict_index < dict_size; dict_index++) {
			T val;
			memcpy(&val, page_buf_ptr, sizeof(val));
			page_buf_ptr += sizeof(T);

			((Dictionary<T>*) dict)->dict[dict_index] = val;
		}
	}

	void scan_dict_page(ResultColumn &result_col) {
		if (page_header.__isset.data_page_header
				|| !page_header.__isset.dictionary_page_header) {
			throw runtime_error("Dictionary page header mismatch");
		}

		// make sure we like the encoding
		switch (page_header.dictionary_page_header.encoding) {
		case Encoding::PLAIN:
		case Encoding::PLAIN_DICTIONARY: // deprecated
			break;

		default:
			throw runtime_error(
					"Dictionary page has unsupported/invalid encoding");
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
			// never going to have more string data than this uncompressed_page_size (lengths use bytes)
			auto string_heap_chunk = std::unique_ptr<char[]>(
					new char[page_header.uncompressed_page_size]);
			result_col.string_heap_chunks.push_back(std::move(string_heap_chunk));
			auto str_ptr =
					result_col.string_heap_chunks[result_col.string_heap_chunks.size()
							- 1].get();
			dict = new Dictionary<char*>(dict_size);

			for (int32_t dict_index = 0; dict_index < dict_size; dict_index++) {
				uint32_t str_len;
				memcpy(&str_len, page_buf_ptr, sizeof(str_len));
				page_buf_ptr += sizeof(str_len);

				if (page_buf_ptr + str_len > page_buf_end_ptr) {
					throw runtime_error(
							"Declared string length exceeds payload size");
				}

				((Dictionary<char*>*) dict)->dict[dict_index] = str_ptr;
				// TODO make sure we dont run out of str_ptr
				memcpy(str_ptr, page_buf_ptr, str_len);
				str_ptr[str_len] = '\0'; // terminate
				str_ptr += str_len + 1;
				page_buf_ptr += str_len;
			}

			break;
		}
		default:
			throw runtime_error(
					"Unsupported type for dictionary: "
							+ type_to_string(result_col.col->type));
		}
	}

	void scan_data_page(ResultColumn &result_col) {
		if (!page_header.__isset.data_page_header
				|| page_header.__isset.dictionary_page_header) {
			throw runtime_error("Data page header mismatch");
		}

		if (page_header.__isset.data_page_header_v2) {
			throw runtime_error("Data page v2 unsupported");
		}

		auto num_values = page_header.data_page_header.num_values;

		// we have to first decode the define levels
		switch (page_header.data_page_header.definition_level_encoding) {
		case Encoding::RLE: {
			// read length of define payload, always
			uint32_t def_length;
			memcpy(&def_length, page_buf_ptr, sizeof(def_length));
			page_buf_ptr += sizeof(def_length);

			RleBpDecoder dec((const uint8_t*) page_buf_ptr, def_length, 1);
			dec.GetBatch<uint8_t>(defined_ptr, num_values);

			page_buf_ptr += def_length;
		}
			break;
		default:
			throw runtime_error(
					"Definition levels have unsupported/invalid encoding");
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

	template<class T> void fill_values_plain(ResultColumn &result_col) {
		T *result_arr = (T*) result_col.data.ptr;
		for (int32_t val_offset = 0;
				val_offset < page_header.data_page_header.num_values;
				val_offset++) {

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
			// uargh, but unfortunately neccessary because sometimes bool values are > 1
			bool *result_arr = (bool*) result_col.data.ptr;
			for (int32_t val_offset = 0;
					val_offset < page_header.data_page_header.num_values;
					val_offset++) {

				if (!defined_ptr[val_offset]) {
					continue;
				}

				auto row_idx = page_start_row + val_offset;
				result_arr[row_idx] = ((bool*) page_buf_ptr != 0);
				page_buf_ptr += sizeof(bool);
			}

		}
			break;
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
				shc_len += page_header.data_page_header.num_values; // make space for terminators
			}
			auto string_heap_chunk = std::unique_ptr<char[]>(new char[shc_len]);
			result_col.string_heap_chunks.push_back(std::move(string_heap_chunk));
			auto str_ptr =
					result_col.string_heap_chunks[result_col.string_heap_chunks.size()
							- 1].get();

			for (int32_t val_offset = 0;
					val_offset < page_header.data_page_header.num_values;
					val_offset++) {

				if (!defined_ptr[val_offset]) {
					continue;
				}

				auto row_idx = page_start_row + val_offset;

				if (result_col.col->type == Type::BYTE_ARRAY) {
					memcpy(&str_len, page_buf_ptr, sizeof(str_len));
					page_buf_ptr += sizeof(str_len);
				}

				if (page_buf_ptr + str_len > page_buf_end_ptr) {
					throw runtime_error(
							"Declared string length exceeds payload size");
				}

				((char**) result_col.data.ptr)[row_idx] = str_ptr;
				// TODO make sure we dont run out of str_ptr too
				memcpy(str_ptr, page_buf_ptr, str_len);
				str_ptr[str_len] = '\0';
				str_ptr += str_len + 1;

				page_buf_ptr += str_len;

			}
		}
			break;

		default:
			throw runtime_error(
					"Unsupported type page_plain "
							+ type_to_string(result_col.col->type));
		}

	}

	template<class T> void fill_values_dict(ResultColumn &result_col,
			uint32_t *offsets) {
		auto result_arr = (T*) result_col.data.ptr;
		for (int32_t val_offset = 0;
				val_offset < page_header.data_page_header.num_values;
				val_offset++) {
			// always unpack because NULLs area also encoded (?)
			auto row_idx = page_start_row + val_offset;

			if (defined_ptr[val_offset]) {
				auto offset = offsets[val_offset];
				result_arr[row_idx] = ((Dictionary<T>*) dict)->get(offset);
			}
		}
	}

	// here we look back into the dicts and emit the values we find if the value is defined, otherwise NULL
	void scan_data_page_dict(ResultColumn &result_col) {
		if (!seen_dict) {
			throw runtime_error("Missing dictionary page");
		}

		auto num_values = page_header.data_page_header.num_values;

		// num_values is int32, hence all dict offsets have to fit in 32 bit
		auto offsets = unique_ptr<uint32_t[]>(new uint32_t[num_values]);

		// the array offset width is a single byte
		auto enc_length = *((uint8_t*) page_buf_ptr);
		page_buf_ptr += sizeof(uint8_t);

		if (enc_length > 0) {
			RleBpDecoder dec((const uint8_t*) page_buf_ptr, page_buf_len,
					enc_length);

			uint32_t null_count = 0;
			for (uint32_t i = 0; i < num_values; i++) {
				if (!defined_ptr[i]) {
					null_count++;
				}
			}
			if (null_count > 0) {
				dec.GetBatchSpaced<uint32_t>(num_values, null_count,
						defined_ptr, offsets.get());
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
			auto result_arr = (char**) result_col.data.ptr;
			for (int32_t val_offset = 0;
					val_offset < page_header.data_page_header.num_values;
					val_offset++) {
				if (defined_ptr[val_offset]) {
					result_arr[page_start_row + val_offset] =
							((Dictionary<char*>*) dict)->get(
									offsets[val_offset]);
				} else {
					result_arr[page_start_row + val_offset] = nullptr;
				}
			}
			break;
		}
		default:
			throw runtime_error(
					"Unsupported type page_dict "
							+ type_to_string(result_col.col->type));
		}
	}

	// ugly but well
	void cleanup(ResultColumn &result_col) {
		switch (result_col.col->type) {
		case Type::BOOLEAN:
			delete (Dictionary<bool>*) dict;
			break;
		case Type::INT32:
			delete (Dictionary<int32_t>*) dict;
			break;
		case Type::INT64:
			delete (Dictionary<int64_t>*) dict;
			break;
		case Type::INT96:
			delete (Dictionary<Int96>*) dict;
			break;
		case Type::FLOAT:
			delete (Dictionary<float>*) dict;
			break;
		case Type::DOUBLE:
			delete (Dictionary<double>*) dict;
			break;
		case Type::BYTE_ARRAY:
		case Type::FIXED_LEN_BYTE_ARRAY:
			delete (Dictionary<char*>*) dict;
			break;
		default:
			throw runtime_error(
					"Unsupported type for dictionary: "
							+ type_to_string(result_col.col->type));
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
	if (chunk.meta_data.__isset.dictionary_page_offset
			&& chunk.meta_data.dictionary_page_offset >= 4) {
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
	cs.defined_ptr = (uint8_t*) result_col.defined.ptr;

	while (bytes_to_read > 0) {
		auto page_header_len = bytes_to_read; // the header is clearly not that long but we have no idea

		// this is the only other place where we actually unpack a thrift object
		cs.page_header = PageHeader();
		thrift_unpack((const uint8_t*) chunk_buf.ptr,
				(uint32_t*) &page_header_len, &cs.page_header);
//
//		cs.page_header.printTo(cerr);
//		cerr << "\n";

		// compressed_page_size does not include the header size
		chunk_buf.ptr += page_header_len;
		bytes_to_read -= page_header_len;

		auto payload_end_ptr = chunk_buf.ptr
				+ cs.page_header.compressed_page_size;

		ByteBuffer decompressed_buf;

		switch (chunk.meta_data.codec) {
		case CompressionCodec::UNCOMPRESSED:
			cs.page_buf_ptr = chunk_buf.ptr;
			cs.page_buf_len = cs.page_header.compressed_page_size;

			break;
		case CompressionCodec::SNAPPY: {
			size_t decompressed_size;
			snappy::GetUncompressedLength(chunk_buf.ptr,
					cs.page_header.compressed_page_size, &decompressed_size);
			decompressed_buf.resize(decompressed_size + 1);

			auto res = snappy::RawUncompress(chunk_buf.ptr,
					cs.page_header.compressed_page_size, decompressed_buf.ptr);
			if (!res) {
				throw runtime_error("Decompression failure");
			}

			cs.page_buf_ptr = (char*) decompressed_buf.ptr;
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
			cs.scan_data_page(result_col);
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

	// TODO do some logical type checking here, we dont like map, list, enum, json, bson etc

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
		col.data.resize(sizeof(char*) * num_rows, false);
		break;

	case Type::FIXED_LEN_BYTE_ARRAY: {
		auto s_ele = columns[col.id]->schema_element;

		if (!s_ele->__isset.type_length) {
			throw runtime_error("need a type length for fixed byte array");
		}
		col.data.resize(num_rows * sizeof(char*), false);
		break;
	}

	default:
		throw runtime_error(
				"Unsupported type " + type_to_string(col.col->type));
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
		//result.cols[col_idx].type = columns[col_idx]->type;
		result.cols[col_idx].col = columns[col_idx].get();

		result.cols[col_idx].id = col_idx;

	}
}

// ========================================================================

ParquetOutFile::ParquetOutFile(std::string filename) :
	num_rows(0),
	num_cols(0),
	num_rows_set(false),
	mem_buffer(new TMemoryBuffer(1024 * 1024)), // 1MB, what if not enough?
	tproto(tproto_factory.getProtocol(mem_buffer)) {

	// open file
	pfile.open(filename, std::ios::binary);

	// root schema element
	SchemaElement sch;
	sch.__set_num_children(0);
	sch.__set_repetition_type(FieldRepetitionType::REQUIRED);
	schemas.push_back(sch);
}

void ParquetOutFile::set_num_rows(uint32_t nr) {
	num_rows = nr;
	num_rows_set = true;
}

void ParquetOutFile::schema_add_column(
	std::string name,
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
	cmd.__set_codec(CompressionCodec::UNCOMPRESSED);
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
	parquet::format::LogicalType logical_type) {

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
	cmd.__set_codec(CompressionCodec::UNCOMPRESSED);
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
			throw runtime_error("Unsigned integers are not implemented");
		}
		if (it.bitWidth != 32) {
			throw runtime_error("Only 32 bit integers are implemented");
		}
		return Type::INT32;

	} else {
		throw runtime_error("Unimplemented logical type");
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
			throw runtime_error("Unsigned integers are not implemented");
		}
		if (it.bitWidth != 32) {
			throw runtime_error("Only 32 bit integers are implemented");
		}
		return ConvertedType::INT_32;

	} else {
		throw runtime_error("Unimplemented logical type");
	}
}

void ParquetOutFile::write() {
	if (!num_rows_set) {
		throw runtime_error("Need to set the number of rows before writing");
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
	pfile.write((char*) out_buffer, out_length);
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
		default:
			throw runtime_error("Cannot write unknown column type");
	}
	uint32_t cb_end = pfile.tellp();

	if (cb_end - cb_start != data_size) {
		throw runtime_error("Wrong number of bytes written for parquet column");
	}
	ColumnMetaData *cmd = &(column_meta_data[idx]);
	int32_t column_bytes = ((int32_t) pfile.tellp()) - col_start;
	cmd->__set_num_values(num_rows);
	cmd->__set_total_uncompressed_size(column_bytes);
	cmd->__set_total_compressed_size(column_bytes);
	cmd->__set_data_page_offset(data_offset);
}

uint32_t ParquetOutFile::calculate_column_data_size(uint32_t idx) {
	// +1 is to skip the root schema
	parquet::format::Type::type type = schemas[idx + 1].type;
	switch (type) {
		case Type::INT32: {
			return num_rows * sizeof(int32_t);
		}
		case Type::DOUBLE: {
			return num_rows * sizeof(double);
		}
		default: {
			throw runtime_error(
				"Unknown type encountered: " + type_to_string(type)
			);
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
	pfile.write((char*) out_buffer, out_length);

  // size of the footer (without the magic bytes)
	pfile.write((const char*) &out_length, 4);

	// Magic bytes
	pfile.write("PAR1", 4);
	pfile.close();
}

void experiment() {
	// This is the content, a single integer column (yes, I know)
	vector<int32_t> mydata(10);
	for (int i = 0; i < 10; i++) mydata[i] = i;
	int32_t data_size = mydata.size() * sizeof(int32_t);

	shared_ptr<TMemoryBuffer> mem_buffer(new TMemoryBuffer(1024 * 1024));
  TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;
  shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(mem_buffer);
	uint8_t *out_buffer;
	uint32_t out_length;

	// Magic bytes
	std::ofstream f;
	f.open("/tmp/out.parquet", std::ios::binary);
	f.write("PAR1", 4);

	// row group 0 ==========================================================
	// column a -------------------------------------------------------------
	uint32_t col_start = f.tellp();

	// page 0 ------------------
	// page header

  // here starts the dictionary of the first column
	// uint32_t dictionary_offset = f.tellp();
	// PageHeader ph0;
	// ph0.__set_type(PageType::DICTIONARY_PAGE);
	// DictionaryPageHeader phd;
	// ph0.__set_uncompressed_page_size(data_size);
	// ph0.__set_compressed_page_size(data_size);
	// phd.__set_num_values(mydata.size());
	// phd.__set_encoding(Encoding::PLAIN);
	// phd.__set_is_sorted(true);
	// ph0.dictionary_page_header = phd;
	// ph0.write(tproto.get());
	// mem_buffer->getBuffer(&out_buffer, &out_length);
	// f.write((char*) out_buffer, out_length);
	// mem_buffer->resetBuffer();

	// Repetition headers
	// Definition levels
	// data

	//	f.write((const char*) mydata.data(), data_size);

	// end of page 0 -----------
	// page 1 ------------------

  // here starts the data of the first column
	uint32_t data_offset = f.tellp();
	PageHeader ph;
	ph.__set_type(PageType::DATA_PAGE);
	ph.__set_uncompressed_page_size(data_size);
	ph.__set_compressed_page_size(data_size);
	DataPageHeader dph;
	dph.__set_num_values(mydata.size());
	dph.__set_encoding(Encoding::PLAIN);
	ph.__set_data_page_header(dph);
	ph.write(tproto.get());
	mem_buffer->getBuffer(&out_buffer, &out_length);
	f.write((char*) out_buffer, out_length);
	mem_buffer->resetBuffer();

	// Repetition headers
	// Definition levels
	// data

	f.write((const char*) mydata.data(), data_size);
	// end of page 1 -----------

	// total bytes in columns 1
	int32_t column_bytes = ((int32_t) f.tellp()) - col_start;

	// end of column a ------------------------------------------------------
	// end if row group 0 ===================================================

	// footer ===============================================================
	vector<SchemaElement> schs;
	SchemaElement sch1, sch2;
	//	sch1.name = "r";
	sch1.__set_num_children(1);
	sch1.__set_repetition_type(FieldRepetitionType::REQUIRED);
	schs.push_back(sch1);
	sch2.__set_type(Type::INT32);
	// sch2.logicalType; // TODO?
	sch2.__set_repetition_type(FieldRepetitionType::REQUIRED);
	sch2.__set_name("b");
	schs.push_back(sch2);

	ColumnMetaData cmd;
	cmd.__set_type(Type::INT32);
	vector<Encoding::type> encs;
	encs.push_back(Encoding::PLAIN);
	cmd.__set_encodings(encs);
	vector<string> paths;
	paths.push_back("b");
	cmd.__set_path_in_schema(paths);
	cmd.__set_codec(CompressionCodec::UNCOMPRESSED);
	cmd.__set_num_values(mydata.size());
	cmd.__set_total_uncompressed_size(column_bytes);
	cmd.__set_total_compressed_size(column_bytes);
	cmd.__set_data_page_offset(data_offset);
	// cmd.__set_dictionary_page_offset(dictionary_offset);

	vector<ColumnChunk> ccs;
	ColumnChunk cc;
	cc.__set_file_offset(data_offset);
	cc.__set_meta_data(cmd);
	ccs.push_back(cc);

	vector<RowGroup> rgs;
	RowGroup rg;
	rg.__set_num_rows(mydata.size());
	rg.__set_total_byte_size(column_bytes);
	rg.__set_columns(ccs);
	rgs.push_back(rg);

	FileMetaData fmd;
	fmd.__set_version(1);
	fmd.__set_schema(schs);
	fmd.__set_num_rows(mydata.size());
	fmd.__set_row_groups(rgs);
	fmd.__set_created_by("https://github.com/gaborcsardi/miniparquet");
	fmd.write(tproto.get());
	mem_buffer->getBuffer(&out_buffer, &out_length);
	f.write((char*) out_buffer, out_length);

  // size of the footer (without the magic bytes)
	f.write((const char*) &out_length, 4);

	// Magic bytes
	f.write("PAR1", 4);
	f.close();
}