#include "lib/miniparquet.h"

#include <Rdefines.h>

using namespace miniparquet;
using namespace std;

extern "C" {

SEXP miniparquet_read_pages(SEXP filesxp) {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("miniparquet_read: Need single filename parameter");
  }

  char error_buffer[8192];
  error_buffer[0] = '\0';

  try {

    const char *fname = CHAR(STRING_ELT(filesxp, 0));
    ParquetFile f(fname);

    // first go over the pages to see how many we have
    size_t num_pages = 0;
    parquet::format::FileMetaData fmd = f.file_meta_data;
    vector<parquet::format::RowGroup> rgs = fmd.row_groups;
    for (auto i = 0; i < rgs.size(); i++) {
      for (auto j = 0; j < rgs[i].columns.size(); j++) {
        parquet::format::ColumnChunk cc = rgs[i].columns[j];
        parquet::format::ColumnMetaData cmd = cc.meta_data;
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
          pair<parquet::format::PageHeader, int64_t> ph =
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
      "uncompressed_page_size",
      "compressed_page_size",
      "crc",
      "num_values",
      "encoding",
      "definition_level_encoding",
      "repetition_level_encoding",
      "page_header_offset",
      "data_offset",
      "page_header_length",
      ""
    };
    SEXP res = PROTECT(Rf_mkNamed(VECSXP, resnms));
    SEXP file_name = Rf_allocVector(STRSXP, num_pages);
    SET_VECTOR_ELT(res, 0, file_name);
    SEXP row_group = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 1, row_group);
    SEXP column = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 2, column);
    SEXP page_type = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 3, page_type);
    SEXP uncompressed_page_size = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 4, uncompressed_page_size);
    SEXP compressed_page_size = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 5, compressed_page_size);
    SEXP crc = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 6, crc);
    SEXP num_values = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 7, num_values);
    SEXP encoding = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 8, encoding);
    SEXP definition_level_encoding = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 9, definition_level_encoding);
    SEXP repetition_level_encoding = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 10, repetition_level_encoding);
    SEXP page_header_offset = Rf_allocVector(REALSXP, num_pages);
    SET_VECTOR_ELT(res, 11, page_header_offset);
    SEXP data_offset = Rf_allocVector(REALSXP, num_pages);
    SET_VECTOR_ELT(res, 12, data_offset);
    SEXP page_header_length = Rf_allocVector(INTSXP, num_pages);
    SET_VECTOR_ELT(res, 13, page_header_length);

    SEXP chr_file_name = PROTECT(Rf_mkChar(fname));

    size_t page = 0;
    for (auto i = 0; i < rgs.size(); i++) {
      for (auto j = 0; j < rgs[i].columns.size(); j++) {
        parquet::format::ColumnChunk cc = rgs[i].columns[j];
        parquet::format::ColumnMetaData cmd = cc.meta_data;
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
          pair<parquet::format::PageHeader, int64_t> ph =
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
          if (ph.first.type == parquet::format::PageType::DATA_PAGE) {
            INTEGER(num_values)[page] =
              ph.first.data_page_header.num_values;
            INTEGER(encoding)[page] = ph.first.data_page_header.encoding;
            INTEGER(definition_level_encoding)[page] =
              ph.first.data_page_header.definition_level_encoding;
            INTEGER(repetition_level_encoding)[page] =
              ph.first.data_page_header.repetition_level_encoding;
          } else if (ph.first.type ==
                     parquet::format::PageType::DICTIONARY_PAGE) {
            INTEGER(num_values)[page] =
              ph.first.dictionary_page_header.num_values;
            INTEGER(encoding)[page] =
              ph.first.dictionary_page_header.encoding;
            INTEGER(definition_level_encoding)[page] = NA_INTEGER;
            INTEGER(repetition_level_encoding)[page] = NA_INTEGER;
          } else if (ph.first.type ==
                     parquet::format::PageType::INDEX_PAGE) {
            INTEGER(num_values)[page] = NA_INTEGER;
            INTEGER(encoding)[page] = NA_INTEGER;
            INTEGER(definition_level_encoding)[page] = NA_INTEGER;
            INTEGER(repetition_level_encoding)[page] = NA_INTEGER;
          } else if (ph.first.type ==
                     parquet::format::PageType::DATA_PAGE_V2) {
            throw runtime_error("Data page v2 is not supported yet");
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

    UNPROTECT(2);
    return res;

  } catch (std::exception &ex) {
    strncpy(error_buffer, ex.what(), sizeof(error_buffer) - 1);
  }

  if (error_buffer[0] != '\0') {
    Rf_error("%s", error_buffer);
  }

  // never reached
  return R_NilValue;
}

struct PageData {
  // set in find_page
  parquet::format::PageType::type page_type;
  int row_group_no;
  int column_no;
  int64_t page_header_offset;
  int64_t data_offset;
  int32_t page_header_length;
  int32_t compressed_page_size;
  int32_t uncompressed_page_size;
  parquet::format::CompressionCodec::type codec;
  int32_t num_values;
  parquet::format::Encoding::type encoding;
  parquet::format::Encoding::type definition_level_encoding;
  parquet::format::Encoding::type repetition_level_encoding;
  bool has_repetition_levels;

  // not set in find_page, need info from the schema
  int schema_column_no;     // all columns, including internal nodes
  parquet::format::Type::type data_type;
  parquet::format::FieldRepetitionType::type repetition_type;
  bool has_definition_levels;
};

static PageData find_page(ParquetFile &file, int64_t page_header_offset) {
  PageData pd;

  parquet::format::FileMetaData fmd = file.file_meta_data;
  vector<parquet::format::RowGroup> rgs = fmd.row_groups;
  for (auto i = 0; i < rgs.size(); i++) {
    for (auto j = 0; j < rgs[i].columns.size(); j++) {
      parquet::format::ColumnChunk cc = rgs[i].columns[j];
      parquet::format::ColumnMetaData cmd = cc.meta_data;
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
        pair<parquet::format::PageHeader, int64_t> ph =
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
          if (ph.first.type == parquet::format::PageType::DATA_PAGE) {
            pd.num_values = ph.first.data_page_header.num_values;
            pd.encoding = ph.first.data_page_header.encoding;
            pd.definition_level_encoding =
              ph.first.data_page_header.definition_level_encoding;
            pd.repetition_level_encoding =
              ph.first.data_page_header.repetition_level_encoding;
          } else if (ph.first.type ==
                     parquet::format::PageType::DICTIONARY_PAGE) {
            pd.num_values = ph.first.data_page_header.num_values;
          }
          pd.has_repetition_levels = cmd.path_in_schema.size() >= 2;
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

SEXP miniparquet_read_page(SEXP filesxp, SEXP page) {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("miniparquet_read: Need single filename parameter");
  }
  int64_t page_header_offset = REAL(page)[0];

  char error_buffer[8192];
  error_buffer[0] = '\0';

  try {
    const char *fname = CHAR(STRING_ELT(filesxp, 0));
    ParquetFile f(fname);
    // Find where it is in the file
    PageData pd = find_page(f, page_header_offset);
    // Need to find some metadata about this column
    auto schema = f.file_meta_data.schema;
    int leafs = 0;
    for (int i = 0; i < schema.size(); i++) {
      parquet::format::SchemaElement se = schema[i];
      if (se.__isset.num_children) { continue; }
      if (leafs == pd.column_no) {
        pd.schema_column_no = i;
        pd.data_type = se.type;
        // all columns but the root have one, so this must have one
        pd.repetition_type = se.repetition_type;
        pd.has_definition_levels = se.repetition_type !=
          parquet::format::FieldRepetitionType::REQUIRED;
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
      ""
    };

    SEXP res = PROTECT(Rf_mkNamed(VECSXP, resnms));
    SET_VECTOR_ELT(res, 0, Rf_ScalarInteger(pd.page_type));
    SET_VECTOR_ELT(res, 1, Rf_ScalarInteger(pd.row_group_no));
    SET_VECTOR_ELT(res, 2, Rf_ScalarInteger(pd.column_no));
    SET_VECTOR_ELT(res, 3, Rf_ScalarReal(pd.page_header_offset));
    SET_VECTOR_ELT(res, 4, Rf_ScalarReal(pd.data_offset));
    SET_VECTOR_ELT(res, 5, Rf_ScalarInteger(pd.page_header_length));
    SET_VECTOR_ELT(res, 6, Rf_ScalarInteger(pd.compressed_page_size));
    SET_VECTOR_ELT(res, 7, Rf_ScalarInteger(pd.uncompressed_page_size));
    SET_VECTOR_ELT(res, 8, Rf_ScalarInteger(pd.codec));
    SET_VECTOR_ELT(res, 9, Rf_ScalarInteger(NA_INTEGER));
    SET_VECTOR_ELT(res, 10, Rf_ScalarInteger(NA_INTEGER));
    SET_VECTOR_ELT(res, 11, Rf_ScalarInteger(NA_INTEGER));
    SET_VECTOR_ELT(res, 12, Rf_ScalarInteger(NA_INTEGER));
    if (pd.page_type == parquet::format::PageType::DATA_PAGE ||
       pd.page_type == parquet::format::PageType::DICTIONARY_PAGE) {
      SET_VECTOR_ELT(res, 9, Rf_ScalarInteger(pd.num_values));
    }
    if (pd.page_type == parquet::format::PageType::DATA_PAGE) {
      SET_VECTOR_ELT(res, 10, Rf_ScalarInteger(pd.encoding));
      SET_VECTOR_ELT(res, 11, Rf_ScalarInteger(pd.definition_level_encoding));
      SET_VECTOR_ELT(res, 12, Rf_ScalarInteger(pd.repetition_level_encoding));
    }
    SET_VECTOR_ELT(res, 13, Rf_ScalarLogical(pd.has_repetition_levels));
    SET_VECTOR_ELT(res, 14, Rf_ScalarLogical(pd.has_definition_levels));
    SET_VECTOR_ELT(res, 15, Rf_ScalarInteger(pd.schema_column_no));
    SET_VECTOR_ELT(res, 16, Rf_ScalarInteger(pd.data_type));
    SET_VECTOR_ELT(res, 17, Rf_ScalarInteger(pd.repetition_type));
    SET_VECTOR_ELT(res, 18, Rf_allocVector(RAWSXP, pd.page_header_length));
    f.read_chunk(
      pd.page_header_offset,
      pd.page_header_length,
      (int8_t*) RAW(VECTOR_ELT(res, 18))
    );
    SET_VECTOR_ELT(res, 19, Rf_allocVector(RAWSXP, pd.compressed_page_size));
    f.read_chunk(
      pd.data_offset,
      pd.compressed_page_size,
      (int8_t*) RAW(VECTOR_ELT(res, 19))
    );

    UNPROTECT(1);
    return res;

  } catch (std::exception &ex) {
    strncpy(error_buffer, ex.what(), sizeof(error_buffer) - 1);
  }

  if (error_buffer[0] != '\0') {
    Rf_error("%s", error_buffer);
  }

  // never reached
  return R_NilValue;
}

} // extern "C"
