#include "lib/nanoparquet.h"

#include <Rdefines.h>

using namespace nanoparquet;
using namespace std;

extern "C" {

SEXP convert_logical_type(parquet::format::LogicalType ltype) {
  SEXP rtype = R_NilValue;
  int prot = 0;
  if (ltype.__isset.STRING) {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("STRING"));

  } else if (ltype.__isset.ENUM) {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("ENUM"));

  } else if (ltype.__isset.UUID) {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("UUID"));

  } else if (ltype.__isset.INTEGER) {
    const char *nms[] = { "type", "bit_width", "is_signed", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("INT"));
    SET_VECTOR_ELT(rtype, 1, Rf_ScalarInteger(ltype.INTEGER.bitWidth));
    SET_VECTOR_ELT(rtype, 2, Rf_ScalarLogical(ltype.INTEGER.isSigned));

  } else if (ltype.__isset.DECIMAL) {
    const char *nms[] = { "type", "scale", "precision", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("DECIMAL"));
    SET_VECTOR_ELT(rtype, 1, Rf_ScalarInteger(ltype.DECIMAL.scale));
    SET_VECTOR_ELT(rtype, 2, Rf_ScalarInteger(ltype.DECIMAL.precision));

  } else if (ltype.__isset.DATE) {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("DATE"));

  } else if (ltype.__isset.TIME) {
    const char *nms[] = { "type", "is_adjusted_to_utc", "unit", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("TIME"));
    SET_VECTOR_ELT(rtype, 1, Rf_ScalarLogical(ltype.TIME.isAdjustedToUTC));
    if (ltype.TIME.unit.__isset.MILLIS) {
      SET_VECTOR_ELT(rtype, 2, Rf_mkString("millis"));
    } else if (ltype.TIME.unit.__isset.MICROS) {
      SET_VECTOR_ELT(rtype, 2, Rf_mkString("micros"));
    } else if (ltype.TIME.unit.__isset.NANOS) {
      SET_VECTOR_ELT(rtype, 2, Rf_mkString("nanos"));
    } else {
      SET_VECTOR_ELT(rtype, 2, R_NaString);
    }
  } else if (ltype.__isset.TIMESTAMP) {
    const char *nms[] = { "type", "is_adjusted_to_utc", "unit", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("TIMESTAMP"));
    SET_VECTOR_ELT(rtype, 1, Rf_ScalarLogical(ltype.TIMESTAMP.isAdjustedToUTC));
    if (ltype.TIMESTAMP.unit.__isset.MILLIS) {
      SET_VECTOR_ELT(rtype, 2, Rf_mkString("millis"));
    } else if (ltype.TIMESTAMP.unit.__isset.MICROS) {
      SET_VECTOR_ELT(rtype, 2, Rf_mkString("micros"));
    } else if (ltype.TIMESTAMP.unit.__isset.NANOS) {
      SET_VECTOR_ELT(rtype, 2, Rf_mkString("nanos"));
    } else {
      SET_VECTOR_ELT(rtype, 2, R_NaString);
    }

  } else if (ltype.__isset.JSON) {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("JSON"));

  } else if (ltype.__isset.BSON) {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("BSON"));

  } else if (ltype.__isset.LIST) {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("LIST"));

  } else if (ltype.__isset.MAP) {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("MAP"));

  } else {
    const char *nms[] = { "type", "" };
    rtype = PROTECT(Rf_mkNamed(VECSXP, nms)); prot++;
    SET_VECTOR_ELT(rtype, 0, Rf_mkString("UNKNOWN"));
  }

  if (!Rf_isNull(rtype)) {
    SEXP cls = PROTECT(Rf_mkString("nanoparquet_logical_type")); prot++;
    Rf_setAttrib(rtype, R_ClassSymbol, cls);
  }

  UNPROTECT(prot);
  return rtype;
}

SEXP convert_key_value_metadata(const parquet::format::FileMetaData &fmd) {
  auto kvsize =
    fmd.__isset.key_value_metadata ? fmd.key_value_metadata.size() : 0;
  const char *kv_nms[] = { "key", "value", "" };
  SEXP kv = PROTECT(Rf_mkNamed(VECSXP, kv_nms));
  SEXP key = Rf_allocVector(STRSXP, kvsize);
  SET_VECTOR_ELT(kv, 0, key);
  SEXP val = Rf_allocVector(STRSXP, kvsize);
  SET_VECTOR_ELT(kv, 1, val);
  if (kvsize > 0) {
    for (R_xlen_t i = 0; i < kvsize; i++) {
      const parquet::format::KeyValue &el = fmd.key_value_metadata[i];
      SET_STRING_ELT(key, i, Rf_mkChar(el.key.c_str()));
      SET_STRING_ELT(val, i,
        el.__isset.value ? Rf_mkChar(el.value.c_str()) : NA_STRING);
    }
  }

  UNPROTECT(1);
  return kv;
}

SEXP convert_schema(const char *cfile_name,
                    vector<parquet::format::SchemaElement>& schema) {
  uint64_t nc = schema.size();
  const char *col_nms[] = {
    "file_name",
    "name",
    "type",
    "type_length",
    "repetition_type",
    "converted_type",
    "logical_type",
    "num_children",
    "scale",
    "precision",
    "field_id",
    ""
  };
  SEXP columns = PROTECT(Rf_mkNamed(VECSXP, col_nms));

  SEXP rfile_name = PROTECT(Rf_mkChar(cfile_name));
  SEXP file_name = Rf_allocVector(STRSXP, nc);
  SET_VECTOR_ELT(columns, 0, file_name);
  SEXP name = Rf_allocVector(STRSXP, nc);
  SET_VECTOR_ELT(columns, 1, name);
  SEXP type = Rf_allocVector(INTSXP, nc);
  SET_VECTOR_ELT(columns, 2, type);
  SEXP type_length = Rf_allocVector(INTSXP, nc);
  SET_VECTOR_ELT(columns, 3, type_length);
  SEXP repetition_type = Rf_allocVector(INTSXP, nc);
  SET_VECTOR_ELT(columns, 4, repetition_type);
  SEXP converted_type = Rf_allocVector(INTSXP, nc);
  SET_VECTOR_ELT(columns, 5, converted_type);
  SEXP logical_type = Rf_allocVector(VECSXP, nc);
  SET_VECTOR_ELT(columns, 6, logical_type);
  SEXP num_children = Rf_allocVector(INTSXP, nc);
  SET_VECTOR_ELT(columns, 7, num_children);
  SEXP scale = Rf_allocVector(INTSXP, nc);
  SET_VECTOR_ELT(columns, 8, scale);
  SEXP precision = Rf_allocVector(INTSXP, nc);
  SET_VECTOR_ELT(columns, 9, precision);
  SEXP field_id = Rf_allocVector(INTSXP, nc);
  SET_VECTOR_ELT(columns, 10, field_id);

  for (uint64_t idx = 0; idx < nc; idx++) {
    parquet::format::SchemaElement sch = schema[idx];
    SET_STRING_ELT(file_name, idx, rfile_name);
    SET_STRING_ELT(name, idx, Rf_mkChar(sch.name.c_str()));
    INTEGER(type)[idx] = sch.__isset.type ? sch.type : NA_INTEGER;
    INTEGER(type_length)[idx] =
      sch.__isset.type_length ? sch.type_length : NA_INTEGER;
    INTEGER(repetition_type)[idx] =
      sch.__isset.repetition_type ? sch.repetition_type : NA_INTEGER;
    INTEGER(converted_type)[idx] =
    sch.__isset.converted_type ? sch.converted_type : NA_INTEGER;
    if (sch.__isset.logicalType) {
      SET_VECTOR_ELT(logical_type, idx, convert_logical_type(sch.logicalType));
    }
    INTEGER(num_children)[idx] =
      sch.__isset.num_children ? sch.num_children : NA_INTEGER;
    INTEGER(scale)[idx] = sch.__isset.scale ? sch.scale : NA_INTEGER;
    INTEGER(precision)[idx] =
      sch.__isset.precision ? sch.precision : NA_INTEGER;
    INTEGER(field_id)[idx] = sch.__isset.field_id ? sch.field_id : NA_INTEGER;
  }

  UNPROTECT(2);
  return columns;
}

SEXP convert_row_groups(const char *cfile_name,
                        vector<parquet::format::RowGroup> &rgs) {
  const char *nms[] = {
    "file_name",
    "id",
    "total_byte_size",
    "num_rows",
    "file_offset",
    "total_compressed_size",
    "ordinal",
    ""
  };
  auto nrgs = rgs.size();
  SEXP rrgs = PROTECT(Rf_mkNamed(VECSXP, nms));
  SEXP rfile_name = PROTECT(Rf_mkChar(cfile_name));

  SET_VECTOR_ELT(rrgs, 0, Rf_allocVector(STRSXP, nrgs));
  SET_VECTOR_ELT(rrgs, 1, Rf_allocVector(INTSXP, nrgs));
  SET_VECTOR_ELT(rrgs, 2, Rf_allocVector(REALSXP, nrgs));
  SET_VECTOR_ELT(rrgs, 3, Rf_allocVector(REALSXP, nrgs));
  SET_VECTOR_ELT(rrgs, 4, Rf_allocVector(REALSXP, nrgs));
  SET_VECTOR_ELT(rrgs, 5, Rf_allocVector(REALSXP, nrgs));
  SET_VECTOR_ELT(rrgs, 6, Rf_allocVector(INTSXP, nrgs));

  for (auto i = 0; i < nrgs; i++) {
    SET_STRING_ELT(VECTOR_ELT(rrgs, 0), i, rfile_name);
    INTEGER(VECTOR_ELT(rrgs, 1))[i] = i;
    REAL(VECTOR_ELT(rrgs, 2))[i] = rgs[i].total_byte_size;
    REAL(VECTOR_ELT(rrgs, 3))[i] = rgs[i].num_rows;
    REAL(VECTOR_ELT(rrgs, 4))[i] =
      rgs[i].__isset.file_offset ? rgs[i].file_offset : NA_REAL;
    REAL(VECTOR_ELT(rrgs, 5))[i] =
      rgs[i].__isset.total_compressed_size ? rgs[i].total_compressed_size : NA_REAL;
    INTEGER(VECTOR_ELT(rrgs, 6))[i] =
      rgs[i].__isset.ordinal ? rgs[i].ordinal : NA_INTEGER;
  }

  UNPROTECT(2);
  return rrgs;
}

SEXP convert_column_chunks(const char *file_name,
                           vector<parquet::format::RowGroup> &rgs) {
  const char *nms[] = {
    "file_name",
    "row_group",
    "column",
    "file_path",
    "file_offset",
    "offset_index_offset",
    "offset_index_length",
    "column_index_offset",
    "column_index_length",
    "type",
    "encodings",
    "path_in_schema",
    "codec",
    "num_values",
    "total_uncompressed_size",
    "total_compressed_size",
    // TODO: key_value_metadata,
    "data_page_offset",
    "index_page_offset",
    "dictionary_page_offset",
    // TODO: statistics
    // TODO: encoding_stats
    ""
  };

  int nccs = 0;
  for (auto i = 0; i < rgs.size(); i++) {
    nccs += rgs[i].columns.size();
  }

  SEXP rccs = PROTECT(Rf_mkNamed(VECSXP, nms));
  SET_VECTOR_ELT(rccs,  0, Rf_allocVector(STRSXP, nccs));   // file_name
  SET_VECTOR_ELT(rccs,  1, Rf_allocVector(INTSXP, nccs));   // row_group
  SET_VECTOR_ELT(rccs,  2, Rf_allocVector(INTSXP, nccs));   // column
  SET_VECTOR_ELT(rccs,  3, Rf_allocVector(STRSXP, nccs));   // file_path
  SET_VECTOR_ELT(rccs,  4, Rf_allocVector(REALSXP, nccs));  // file_offset
  SET_VECTOR_ELT(rccs,  5, Rf_allocVector(REALSXP, nccs));  // offset_index_offset
  SET_VECTOR_ELT(rccs,  6, Rf_allocVector(INTSXP, nccs));   // offset_index_length
  SET_VECTOR_ELT(rccs,  7, Rf_allocVector(REALSXP, nccs));  // column_index_offset
  SET_VECTOR_ELT(rccs,  8, Rf_allocVector(INTSXP, nccs));   // column_index_length
  SET_VECTOR_ELT(rccs,  9, Rf_allocVector(INTSXP, nccs));   // type
  SET_VECTOR_ELT(rccs, 10, Rf_allocVector(VECSXP, nccs));   // encodings
  SET_VECTOR_ELT(rccs, 11, Rf_allocVector(VECSXP, nccs));   // path_in_schema
  SET_VECTOR_ELT(rccs, 12, Rf_allocVector(INTSXP, nccs));   // codec
  SET_VECTOR_ELT(rccs, 13, Rf_allocVector(REALSXP, nccs));  // num_values
  SET_VECTOR_ELT(rccs, 14, Rf_allocVector(REALSXP, nccs));  // total_uncompressed_size
  SET_VECTOR_ELT(rccs, 15, Rf_allocVector(REALSXP, nccs));  // total_compressed_size
  SET_VECTOR_ELT(rccs, 16, Rf_allocVector(REALSXP, nccs));  // data_page_offset
  SET_VECTOR_ELT(rccs, 17, Rf_allocVector(REALSXP, nccs));  // index_page_offset
  SET_VECTOR_ELT(rccs, 18, Rf_allocVector(REALSXP, nccs));  // dictionary_page_offset

  SEXP rfile_name = PROTECT(Rf_mkChar(file_name));

  int idx = 0;
  for (int i = 0; i < rgs.size(); i++) {
    for (int j = 0; j < rgs[i].columns.size(); j++) {
      parquet::format::ColumnChunk cc = rgs[i].columns[j];
      parquet::format::ColumnMetaData cmd = cc.meta_data;
      SET_STRING_ELT(VECTOR_ELT(rccs, 0), idx, rfile_name);
      INTEGER(VECTOR_ELT(rccs, 1))[idx] = i;
      INTEGER(VECTOR_ELT(rccs, 2))[idx] = j;
      SET_STRING_ELT(VECTOR_ELT(rccs, 3), idx,
        cc.__isset.file_path ? Rf_mkChar(cc.file_path.c_str()) : NA_STRING);
      REAL(VECTOR_ELT(rccs, 4))[idx] = cc.file_offset;
      REAL(VECTOR_ELT(rccs, 5))[idx] =
        cc.__isset.offset_index_offset ? cc.offset_index_offset : NA_REAL;
      INTEGER(VECTOR_ELT(rccs, 6))[idx] =
        cc.__isset.offset_index_length ? cc.offset_index_length : NA_INTEGER;
      REAL(VECTOR_ELT(rccs, 7))[idx] =
        cc.__isset.column_index_offset ? cc.column_index_offset : NA_REAL;
      INTEGER(VECTOR_ELT(rccs, 8))[idx] =
        cc.__isset.column_index_length ? cc.column_index_length : NA_INTEGER;
      INTEGER(VECTOR_ELT(rccs, 9))[idx] = cmd.type;
      SET_VECTOR_ELT(VECTOR_ELT(rccs, 10), idx, Rf_allocVector(INTSXP, cmd.encodings.size()));
      for (auto k = 0; k < cmd.encodings.size(); k++) {
        INTEGER(VECTOR_ELT(VECTOR_ELT(rccs, 10), idx))[k] = cmd.encodings[k];
      }
      SET_VECTOR_ELT(VECTOR_ELT(rccs, 11), idx, Rf_allocVector(STRSXP, cmd.path_in_schema.size()));
      for (auto k = 0; k < cmd.path_in_schema.size(); k++) {
        SET_STRING_ELT(VECTOR_ELT(VECTOR_ELT(rccs, 11), idx), k,
          Rf_mkChar(cmd.path_in_schema[k].c_str()));
      }
      INTEGER(VECTOR_ELT(rccs, 12))[idx] = cmd.codec;
      REAL(VECTOR_ELT(rccs, 13))[idx] = cmd.num_values;
      REAL(VECTOR_ELT(rccs, 14))[idx] = cmd.total_uncompressed_size;
      REAL(VECTOR_ELT(rccs, 15))[idx] = cmd.total_compressed_size;
      REAL(VECTOR_ELT(rccs, 16))[idx] = cmd.data_page_offset;
      REAL(VECTOR_ELT(rccs, 17))[idx] =
        cmd.__isset.index_page_offset ? cmd.index_page_offset : NA_REAL;
      REAL(VECTOR_ELT(rccs, 18))[idx] =
        cmd.__isset.dictionary_page_offset ? cmd.dictionary_page_offset : NA_REAL;

      idx++;
    }
  }

  UNPROTECT(2); // rfile_name, rccs
  return rccs;
}

SEXP nanoparquet_read_metadata(SEXP filesxp) {
  int prot = 0;

  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("nanoparquet_read: Need single filename parameter");
  }

  char error_buffer[8192];
  error_buffer[0] = '\0';

  try {

    const char *fname = CHAR(STRING_ELT(filesxp, 0));
    ParquetFile f(fname);

    const char *res_nms[] = {
      "file_meta_data",
      "schema",
      "row_groups",
      "column_chunks",
      ""
      };
    SEXP res = PROTECT(Rf_mkNamed(VECSXP, res_nms)); prot++;

    parquet::format::FileMetaData fmd = f.file_meta_data;
    const char *fmd_nms[] = {
      "file_name",
      "version",
      "num_rows",
      "key_value_metadata",
      "created_by",
      ""
    };
    SEXP rfmd = PROTECT(Rf_mkNamed(VECSXP, fmd_nms)); prot++;
    SET_VECTOR_ELT(rfmd, 0, Rf_mkString(fname));
    SET_VECTOR_ELT(rfmd, 1, Rf_ScalarInteger(fmd.version));
    SET_VECTOR_ELT(rfmd, 2, Rf_ScalarReal(fmd.num_rows));
    SET_VECTOR_ELT(rfmd, 3, convert_key_value_metadata(fmd));
    if (fmd.__isset.created_by) {
      SET_VECTOR_ELT(rfmd, 4, Rf_mkString(fmd.created_by.c_str()));
    }
    // TODO: encryption algorithm
    // TODO: footer_signing_key_metadata
    SET_VECTOR_ELT(res, 0, rfmd);
    UNPROTECT(1); prot--; // rfmd

    SET_VECTOR_ELT(res, 1, convert_schema(fname, fmd.schema));
    SET_VECTOR_ELT(res, 2, convert_row_groups(fname, fmd.row_groups));
    SET_VECTOR_ELT(res, 3, convert_column_chunks(fname, fmd.row_groups));

    UNPROTECT(prot); //res
    return res;

  } catch (std::exception &ex) {
    strncpy(error_buffer, ex.what(), sizeof(error_buffer) - 1);
  }

  if (error_buffer[0] != '\0') {
    Rf_error("%s", error_buffer);
  }

  // never reached
  UNPROTECT(prot);
  return R_NilValue;
}

SEXP nanoparquet_read_schema(SEXP filesxp) {
  int prot = 0;
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("nanoparquet_read: Need single filename parameter");
  }

  char error_buffer[8192];
  error_buffer[0] = '\0';

  try {

    SEXP cfname = PROTECT(STRING_ELT(filesxp, 0)); prot++;
    const char *fname = CHAR(cfname);
    ParquetFile f(fname);
    parquet::format::FileMetaData fmd = f.file_meta_data;
    UNPROTECT(prot);
    return convert_schema(fname, fmd.schema);

  } catch (std::exception &ex) {
    strncpy(error_buffer, ex.what(), sizeof(error_buffer) - 1);
  }

  if (error_buffer[0] != '\0') {
    Rf_error("%s", error_buffer);
  }

  // never reached
  UNPROTECT(prot);
  return R_NilValue;
}

} // extern "C"
