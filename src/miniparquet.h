#pragma once

#include <string>
#include <vector>
#include <bitset>
#include <fstream>
#include <cstring>
#include "parquet/parquet_types.h"

#include <protocol/TCompactProtocol.h>
#include <transport/TBufferTransports.h>

namespace miniparquet {

class ParquetColumn {
public:
	uint64_t id;
	parquet::format::Type::type type;
	std::string name;
	parquet::format::SchemaElement* schema_element;
};

struct Int96 {
	uint32_t value[3];
};

template<class T>
class Dictionary {
public:
	std::vector<T> dict;
	Dictionary(uint64_t n_values) {
		dict.resize(n_values);
	}
	T& get(uint64_t offset) {
		if (offset >= dict.size()) {
			throw std::runtime_error("Dictionary offset out of bounds");
		} else
			return dict.at(offset);
	}
};

// todo move this to impl

class ByteBuffer { // on to the 10 thousandth impl
public:
	char* ptr = nullptr;
	uint64_t len = 0;

	void resize(uint64_t new_size, bool copy=true) {
		if (new_size > len) {
			auto new_holder = std::unique_ptr<char[]>(new char[new_size]);
			if (copy && holder != nullptr) {
				memcpy(new_holder.get(), holder.get(), len);
			}
			holder = std::move(new_holder);
			ptr = holder.get();
			len = new_size;
		}
	}
private:
	std::unique_ptr<char[]> holder = nullptr;
};

class ScanState {
public:
	uint64_t row_group_idx = 0;
	uint64_t row_group_offset = 0;
};

struct ResultColumn {
	uint64_t id;
	ByteBuffer data;
	ParquetColumn *col;
	ByteBuffer defined;
	std::vector<std::unique_ptr<char[]>> string_heap_chunks;

};

struct ResultChunk {
	std::vector<ResultColumn> cols;
	uint64_t nrows;
};

class ParquetFile {
public:
	ParquetFile(std::string filename);
	void initialize_result(ResultChunk& result);
	bool scan(ScanState &s, ResultChunk& result);
	uint64_t nrow;
	std::vector<std::unique_ptr<ParquetColumn>> columns;

private:
	void initialize(std::string filename);
	void initialize_column(ResultColumn& col, uint64_t num_rows);
	void scan_column(ScanState& state, ResultColumn& result_col);
	parquet::format::FileMetaData file_meta_data;
	std::ifstream pfile;
};

class ParquetOutFile {
public:
	ParquetOutFile(std::string filename);
	void set_num_rows(uint32_t nr);
	void schema_add_column(
		std::string name,
		parquet::format::Type::type type
	);
	void schema_add_column(
		std::string name,
		parquet::format::LogicalType logical_type
	);
	void write();

	// write out various parquet types, these must be implemented in
	// the subclass
	virtual void write_int32(std::ostream& file, uint32_t idx) = 0;
	virtual void write_double(std::ostream& file, uint32_t idx) = 0;

private:
	std::ofstream pfile;
	uint32_t num_rows, num_cols;
	bool num_rows_set;
	uint32_t total_size; // for the single row group we have for now

	std::vector<parquet::format::SchemaElement> schemas;
	std::vector<parquet::format::ColumnMetaData> column_meta_data;

	std::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem_buffer;
  apache::thrift::protocol::TCompactProtocolFactoryT
		<apache::thrift::transport::TMemoryBuffer> tproto_factory;
  std::shared_ptr<apache::thrift::protocol::TProtocol> tproto;

	void write_columns();
	void write_column(uint32_t idx);
	void write_footer();

	parquet::format::Type::type get_type_from_logical_type(
		parquet::format::LogicalType logical_type
	);
	uint32_t calculate_column_data_size(uint32_t idx);
};

}

void experiment();
