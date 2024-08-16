#include "lib/nanoparquet.h"

#include "protect.h"
#include "RParquetReader.h"

using namespace nanoparquet;
using namespace std;

extern "C" {

extern SEXP nanoparquet_call;

SEXP nanoparquet_read_pages(SEXP filesxp) {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("nanoparquet_read: Need single filename parameter");
  }

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START(R_NilValue);
  const char *fname = CHAR(STRING_ELT(filesxp, 0));
  RParquetReader f(fname);

  // first go over the pages to see how many we have
  size_t num_pages = 0;
  parquet::FileMetaData fmd = f.file_meta_data_;
  vector<parquet::RowGroup> rgs = fmd.row_groups;
  for (auto i = 0; i < rgs.size(); i++) {
    for (auto j = 0; j < rgs[i].columns.size(); j++) {
      parquet::ColumnChunk cc = rgs[i].columns[j];
      parquet::ColumnMetaData cmd = cc.meta_data;
      int64_t chunk_start = cmd.data_page_offset;
      // dict?
      if (cmd.__isset.dictionary_page_offset &&
          cmd.dictionary_page_offset >= 4) {
        chunk_start = cmd.dictionary_page_offset;
      }
      int64_t chunk_len = cmd.total_compressed_size;

      int64_t end = chunk_start + chunk_len;
      int64_t ofs = chunk_start;
      while (ofs < end) {
        pair<parquet::PageHeader, int64_t> ph =
          f.read_page_header(ofs);
        ofs += ph.second;
        ofs += ph.first.compressed_page_size;
        num_pages++;
      }
    }
  }

  const char *resnms[] = {
    "file_name",
    "row_group",
    "column",
    "page_type",
    "page_header_offset",
    "uncompressed_page_size",
    "compressed_page_size",
    "crc",
    "num_values",
    "encoding",
    "definition_level_encoding",
    "repetition_level_encoding",
    "data_offset",
    "page_header_length",
    ""
  };
  SEXP res = PROTECT(safe_mknamed_vec(resnms, &uwtoken));
  SEXP file_name = safe_allocvector_str(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 0, file_name);
  SEXP row_group = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 1, row_group);
  SEXP column = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 2, column);
  SEXP page_type = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 3, page_type);
  SEXP page_header_offset = safe_allocvector_real(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 4, page_header_offset);
  SEXP uncompressed_page_size = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 5, uncompressed_page_size);
  SEXP compressed_page_size = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 6, compressed_page_size);
  SEXP crc = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 7, crc);
  SEXP num_values = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 8, num_values);
  SEXP encoding = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 9, encoding);
  SEXP definition_level_encoding = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 10, definition_level_encoding);
  SEXP repetition_level_encoding = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 11, repetition_level_encoding);
  SEXP data_offset = safe_allocvector_real(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 12, data_offset);
  SEXP page_header_length = safe_allocvector_int(num_pages, &uwtoken);
  SET_VECTOR_ELT(res, 13, page_header_length);

  SEXP chr_file_name = PROTECT(safe_mkchar(fname, &uwtoken));

  size_t page = 0;
  for (auto i = 0; i < rgs.size(); i++) {
    for (auto j = 0; j < rgs[i].columns.size(); j++) {
      parquet::ColumnChunk cc = rgs[i].columns[j];
      parquet::ColumnMetaData cmd = cc.meta_data;
      int64_t chunk_start = cmd.data_page_offset;
      // dict?
      if (cmd.__isset.dictionary_page_offset &&
          cmd.dictionary_page_offset >= 4) {
        chunk_start = cmd.dictionary_page_offset;
      }
      int64_t chunk_len = cmd.total_compressed_size;

      int64_t end = chunk_start + chunk_len;
      int64_t ofs = chunk_start;
      while (ofs < end) {
        pair<parquet::PageHeader, int64_t> ph =
        f.read_page_header(ofs);

        SET_STRING_ELT(file_name, page, chr_file_name);
        INTEGER(row_group)[page] = i;
        INTEGER(column)[page] = j;
        INTEGER(page_type)[page] = ph.first.type;
        INTEGER(uncompressed_page_size)[page] =
          ph.first.uncompressed_page_size;
        INTEGER(compressed_page_size)[page] =
          ph.first.compressed_page_size;
        INTEGER(crc)[page] =
          ph.first.__isset.crc ? ph.first.crc : NA_INTEGER;
        if (ph.first.type == parquet::PageType::DATA_PAGE) {
          INTEGER(num_values)[page] =
            ph.first.data_page_header.num_values;
          INTEGER(encoding)[page] = ph.first.data_page_header.encoding;
          INTEGER(definition_level_encoding)[page] =
            ph.first.data_page_header.definition_level_encoding;
          INTEGER(repetition_level_encoding)[page] =
            ph.first.data_page_header.repetition_level_encoding;
        } else if (ph.first.type ==
                   parquet::PageType::DATA_PAGE_V2) {
          INTEGER(num_values)[page] =
            ph.first.data_page_header_v2.num_values;
          INTEGER(encoding)[page] = ph.first.data_page_header_v2.encoding;
          INTEGER(definition_level_encoding)[page] = NA_INTEGER;
          INTEGER(repetition_level_encoding)[page] = NA_INTEGER;
        } else if (ph.first.type ==
                   parquet::PageType::DICTIONARY_PAGE) {
          INTEGER(num_values)[page] =
            ph.first.dictionary_page_header.num_values;
          INTEGER(encoding)[page] =
            ph.first.dictionary_page_header.encoding;
          INTEGER(definition_level_encoding)[page] = NA_INTEGER;
          INTEGER(repetition_level_encoding)[page] = NA_INTEGER;
        } else if (ph.first.type ==
                   parquet::PageType::INDEX_PAGE) {
          INTEGER(num_values)[page] = NA_INTEGER;
          INTEGER(encoding)[page] = NA_INTEGER;
          INTEGER(definition_level_encoding)[page] = NA_INTEGER;
          INTEGER(repetition_level_encoding)[page] = NA_INTEGER;
        } else {
          INTEGER(num_values)[page] = NA_INTEGER;
          INTEGER(encoding)[page] = NA_INTEGER;
          INTEGER(definition_level_encoding)[page] = NA_INTEGER;
          INTEGER(repetition_level_encoding)[page] = NA_INTEGER;
        }
        REAL(page_header_offset)[page] = ofs;
        REAL(data_offset)[page] = ofs + ph.second;
        INTEGER(page_header_length)[page] = ph.second;

        ofs += ph.second;
        ofs += ph.first.compressed_page_size;
        page++;
      }
    }
  }

  UNPROTECT(3);
  return res;
  R_API_END();
}

struct PageData {
  // set in find_page
  parquet::PageType::type page_type;
  int row_group_no;
  int column_no;
  int64_t page_header_offset;
  int64_t data_offset;
  int32_t page_header_length;
  int32_t compressed_page_size;
  int32_t uncompressed_page_size;
  parquet::CompressionCodec::type codec;
  int32_t num_values;
  int32_t num_nulls;
  int32_t num_rows;
  parquet::Encoding::type encoding;
  parquet::Encoding::type definition_level_encoding;
  parquet::Encoding::type repetition_level_encoding;
  bool has_repetition_levels;
  int32_t definition_levels_byte_length;
  int32_t repetition_levels_byte_length;

  // not set in find_page, need info from the schema
  int schema_column_no;     // all columns, including internal nodes
  parquet::Type::type data_type;
  parquet::FieldRepetitionType::type repetition_type;
  bool has_definition_levels;
};

static PageData find_page(ParquetReader &file, int64_t page_header_offset) {
  PageData pd;

  parquet::FileMetaData fmd = file.file_meta_data_;
  vector<parquet::RowGroup> rgs = fmd.row_groups;
  for (auto i = 0; i < rgs.size(); i++) {
    for (auto j = 0; j < rgs[i].columns.size(); j++) {
      parquet::ColumnChunk cc = rgs[i].columns[j];
      parquet::ColumnMetaData cmd = cc.meta_data;
      int64_t chunk_start = cmd.data_page_offset;
      // dict?
      if (cmd.__isset.dictionary_page_offset &&
          cmd.dictionary_page_offset >= 4) {
        chunk_start = cmd.dictionary_page_offset;
      }
      int64_t chunk_len = cmd.total_compressed_size;

      // Maybe it is not in this chunk at all
      if (page_header_offset < chunk_start ||
          page_header_offset >= chunk_start + chunk_len) {
            continue;
      }

      int64_t end = chunk_start + chunk_len;
      int64_t ofs = chunk_start;
      while (ofs < end) {
        pair<parquet::PageHeader, int64_t> ph =
          file.read_page_header(ofs);
        if (ofs == page_header_offset) {
          pd.page_type = ph.first.type;
          pd.row_group_no = i;
          pd.column_no = j;
          pd.page_header_offset = ofs;
          pd.data_offset = ofs + ph.second;
          pd.page_header_length = ph.second;
          pd.compressed_page_size = ph.first.compressed_page_size;
          pd.uncompressed_page_size = ph.first.uncompressed_page_size;
          pd.codec = cmd.codec;
          pd.definition_level_encoding =
            (parquet::Encoding::type) NA_INTEGER;
          pd.repetition_level_encoding =
            (parquet::Encoding::type) NA_INTEGER;
          pd.definition_levels_byte_length = NA_INTEGER;
          pd.repetition_levels_byte_length = NA_INTEGER;
          pd.num_nulls = pd.num_rows = NA_INTEGER;
          if (ph.first.type == parquet::PageType::DATA_PAGE) {
            pd.num_values = ph.first.data_page_header.num_values;
            pd.encoding = ph.first.data_page_header.encoding;
            pd.definition_level_encoding =
              ph.first.data_page_header.definition_level_encoding;
            pd.repetition_level_encoding =
              ph.first.data_page_header.repetition_level_encoding;
          } else if (ph.first.type == parquet::PageType::DATA_PAGE_V2) {
            pd.num_values = ph.first.data_page_header_v2.num_values;
            pd.encoding = ph.first.data_page_header_v2.encoding;
            pd.definition_levels_byte_length =
              ph.first.data_page_header_v2.definition_levels_byte_length;
            pd.repetition_levels_byte_length =
              ph.first.data_page_header_v2.repetition_levels_byte_length;
            pd.num_nulls = ph.first.data_page_header_v2.num_nulls;
            pd.num_rows = ph.first.data_page_header_v2.num_rows;
          } else if (ph.first.type ==
                     parquet::PageType::DICTIONARY_PAGE) {
            pd.num_values = ph.first.dictionary_page_header.num_values;
            pd.encoding = ph.first.dictionary_page_header.encoding;
          }
          pd.has_repetition_levels =
            (pd.page_type == parquet::PageType::DATA_PAGE ||
             pd.page_type == parquet::PageType::DATA_PAGE_V2) &&
            cmd.path_in_schema.size() >= 2;
          return pd;
        }
        ofs += ph.second;
        ofs += ph.first.compressed_page_size;
      }
    }
  }

  throw runtime_error(
    "Could not find page at specified offset in Parquet file"
  );
}

SEXP nanoparquet_read_page(SEXP filesxp, SEXP page) {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("nanoparquet_read: Need single filename parameter");
  }
  int64_t page_header_offset = REAL(page)[0];

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START(R_NilValue);
  const char *fname = CHAR(STRING_ELT(filesxp, 0));
  RParquetReader f(fname);
  // Find where it is in the file
  PageData pd = find_page(f, page_header_offset);
  // Need to find some metadata about this column
  auto schema = f.file_meta_data_.schema;
  int leafs = 0;
  for (int i = 0; i < schema.size(); i++) {
    parquet::SchemaElement se = schema[i];
    if (se.__isset.num_children) { continue; }
    if (leafs == pd.column_no) {
      pd.schema_column_no = i;
      pd.data_type = se.type;
      // all columns but the root have one, so this must have one
      pd.repetition_type = se.repetition_type;
      pd.has_definition_levels =
        (pd.page_type == parquet::PageType::DATA_PAGE ||
         pd.page_type == parquet::PageType::DATA_PAGE_V2) &&
        se.repetition_type !=
        parquet::FieldRepetitionType::REQUIRED;
      break;
    }
    leafs++;
  }
  if (leafs != pd.column_no) {
    throw runtime_error(
      "Could not find column in schema, broken Parquet file?"
    );
  }

  const char *resnms[] = {
    "page_type",
    "row_group",
    "column",
    "page_header_offset",
    "data_page_offset",
    "page_header_length",
    "compressed_page_size",
    "uncompressed_page_size",
    "codec",
    "num_values",
    "encoding",
    "definition_level_encoding",
    "repetition_level_encoding",
    "has_repetition_levels",
    "has_definition_levels",
    "schema_column",
    "data_type",
    "repetition_type",
    "page_header",
    "data",
    "definition_levels_byte_length",
    "repetition_levels_byte_length",
    "num_nulls",
    "num_rows",
    ""
  };

  SEXP res = PROTECT(safe_mknamed_vec(resnms, &uwtoken));
  SET_VECTOR_ELT(res, 0, safe_scalarinteger(pd.page_type, &uwtoken));
  SET_VECTOR_ELT(res, 1, safe_scalarinteger(pd.row_group_no, &uwtoken));
  SET_VECTOR_ELT(res, 2, safe_scalarinteger(pd.column_no, &uwtoken));
  SET_VECTOR_ELT(res, 3, safe_scalarreal(pd.page_header_offset, &uwtoken));
  SET_VECTOR_ELT(res, 4, safe_scalarreal(pd.data_offset, &uwtoken));
  SET_VECTOR_ELT(res, 5, safe_scalarinteger(pd.page_header_length, &uwtoken));
  SET_VECTOR_ELT(res, 6, safe_scalarinteger(pd.compressed_page_size, &uwtoken));
  SET_VECTOR_ELT(res, 7, safe_scalarinteger(pd.uncompressed_page_size, &uwtoken));
  SET_VECTOR_ELT(res, 8, safe_scalarinteger(pd.codec, &uwtoken));
  SET_VECTOR_ELT(res, 9, safe_scalarinteger(NA_INTEGER, &uwtoken));
  SET_VECTOR_ELT(res, 10, safe_scalarinteger(NA_INTEGER, &uwtoken));
  SET_VECTOR_ELT(res, 11, safe_scalarinteger(NA_INTEGER, &uwtoken));
  SET_VECTOR_ELT(res, 12, safe_scalarinteger(NA_INTEGER, &uwtoken));
  if (pd.page_type == parquet::PageType::DATA_PAGE ||
      pd.page_type == parquet::PageType::DATA_PAGE_V2 ||
      pd.page_type == parquet::PageType::DICTIONARY_PAGE) {
    SET_VECTOR_ELT(res, 9, safe_scalarinteger(pd.num_values, &uwtoken));
    SET_VECTOR_ELT(res, 10, safe_scalarinteger(pd.encoding, &uwtoken));
  }
  if (pd.page_type == parquet::PageType::DATA_PAGE) {
    SET_VECTOR_ELT(res, 11, safe_scalarinteger(pd.definition_level_encoding, &uwtoken));
    SET_VECTOR_ELT(res, 12, safe_scalarinteger(pd.repetition_level_encoding, &uwtoken));
  }
  SET_VECTOR_ELT(res, 13, safe_scalarlogical(pd.has_repetition_levels, &uwtoken));
  SET_VECTOR_ELT(res, 14, safe_scalarlogical(pd.has_definition_levels, &uwtoken));
  SET_VECTOR_ELT(res, 15, safe_scalarinteger(pd.schema_column_no, &uwtoken));
  SET_VECTOR_ELT(res, 16, safe_scalarinteger(pd.data_type, &uwtoken));
  SET_VECTOR_ELT(res, 17, safe_scalarinteger(pd.repetition_type, &uwtoken));
  SET_VECTOR_ELT(res, 18, safe_allocvector_raw(pd.page_header_length, &uwtoken));
  f.read_chunk(
    pd.page_header_offset,
    pd.page_header_length,
    (int8_t*) RAW(VECTOR_ELT(res, 18))
  );
  SET_VECTOR_ELT(res, 19, safe_allocvector_raw(pd.compressed_page_size, &uwtoken));
  f.read_chunk(
    pd.data_offset,
    pd.compressed_page_size,
    (int8_t*) RAW(VECTOR_ELT(res, 19))
  );
  SET_VECTOR_ELT(res, 20, safe_scalarinteger(pd.definition_levels_byte_length, &uwtoken));
  SET_VECTOR_ELT(res, 21, safe_scalarinteger(pd.repetition_levels_byte_length, &uwtoken));
  SET_VECTOR_ELT(res, 22, safe_scalarinteger(pd.num_nulls, &uwtoken));
  SET_VECTOR_ELT(res, 23, safe_scalarinteger(pd.num_rows, &uwtoken));

  UNPROTECT(2);
  return res;
  R_API_END();
}

} // extern "C"
