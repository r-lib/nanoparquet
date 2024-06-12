#include "lib/nanoparquet.h"

#include <Rdefines.h>
#include "protect.h"

using namespace nanoparquet;
using namespace std;

extern "C" {

// Does not throw C++ exceptions, so we can wrap it
SEXP convert_logical_type_(parquet::LogicalType ltype) {
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

SEXP convert_logical_type_wrapper(void *data) {
  parquet::LogicalType *ltype = (parquet::LogicalType*) data;
  return convert_logical_type_(*ltype);
}

SEXP convert_logical_type(parquet::LogicalType ltype, SEXP *uwt) {
  return R_UnwindProtect(
    convert_logical_type_wrapper,
    &ltype,
    throw_error,
    uwt,
    *uwt
  );
}

SEXP convert_key_value_metadata(const parquet::FileMetaData &fmd) {
  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();
  auto kvsize =
    fmd.__isset.key_value_metadata ? fmd.key_value_metadata.size() : 0;
  const char *kv_nms[] = { "key", "value", "" };
  SEXP kv = PROTECT(safe_mknamed_vec(kv_nms, &uwtoken));
  SEXP key = safe_allocvector_str(kvsize, &uwtoken);
  SET_VECTOR_ELT(kv, 0, key);
  SEXP val = safe_allocvector_str(kvsize, &uwtoken);
  SET_VECTOR_ELT(kv, 1, val);
  if (kvsize > 0) {
    for (R_xlen_t i = 0; i < kvsize; i++) {
      const parquet::KeyValue &el = fmd.key_value_metadata[i];
      SET_STRING_ELT(key, i, safe_mkchar(el.key.c_str(), &uwtoken));
      SET_STRING_ELT(val, i,
        el.__isset.value ? safe_mkchar(el.value.c_str(), &uwtoken) : NA_STRING);
    }
  }

  UNPROTECT(2);
  return kv;
  R_API_END()
}

SEXP convert_schema(const char *cfile_name,
                    vector<parquet::SchemaElement>& schema) {
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

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();

  uint64_t nc = schema.size();
  SEXP columns = PROTECT(safe_mknamed_vec(col_nms, &uwtoken));
  SEXP rfile_name = PROTECT(safe_mkchar(cfile_name, &uwtoken));
  SEXP file_name = safe_allocvector_str(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 0, file_name);
  SEXP name = safe_allocvector_str(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 1, name);
  SEXP type = safe_allocvector_int(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 2, type);
  SEXP type_length = safe_allocvector_int(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 3, type_length);
  SEXP repetition_type = safe_allocvector_int(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 4, repetition_type);
  SEXP converted_type = safe_allocvector_int(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 5, converted_type);
  SEXP logical_type = safe_allocvector_vec(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 6, logical_type);
  SEXP num_children = safe_allocvector_int(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 7, num_children);
  SEXP scale = safe_allocvector_int(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 8, scale);
  SEXP precision = safe_allocvector_int(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 9, precision);
  SEXP field_id = safe_allocvector_int(nc, &uwtoken);
  SET_VECTOR_ELT(columns, 10, field_id);

  for (uint64_t idx = 0; idx < nc; idx++) {
    parquet::SchemaElement sch = schema[idx];
    SET_STRING_ELT(file_name, idx, rfile_name);
    SET_STRING_ELT(name, idx, safe_mkchar(sch.name.c_str(), &uwtoken));
    INTEGER(type)[idx] = sch.__isset.type ? sch.type : NA_INTEGER;
    INTEGER(type_length)[idx] =
      sch.__isset.type_length ? sch.type_length : NA_INTEGER;
    INTEGER(repetition_type)[idx] =
      sch.__isset.repetition_type ? sch.repetition_type : NA_INTEGER;
    INTEGER(converted_type)[idx] =
    sch.__isset.converted_type ? sch.converted_type : NA_INTEGER;
    if (sch.__isset.logicalType) {
      SET_VECTOR_ELT(logical_type, idx, convert_logical_type(sch.logicalType, &uwtoken));
    }
    INTEGER(num_children)[idx] =
      sch.__isset.num_children ? sch.num_children : NA_INTEGER;
    INTEGER(scale)[idx] = sch.__isset.scale ? sch.scale : NA_INTEGER;
    INTEGER(precision)[idx] =
      sch.__isset.precision ? sch.precision : NA_INTEGER;
    INTEGER(field_id)[idx] = sch.__isset.field_id ? sch.field_id : NA_INTEGER;
  }

  UNPROTECT(3);
  return columns;
  R_API_END();
}

SEXP convert_row_groups(const char *cfile_name,
                        vector<parquet::RowGroup> &rgs) {
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

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();

  auto nrgs = rgs.size();
  SEXP rrgs = PROTECT(safe_mknamed_vec(nms, &uwtoken));
  SEXP rfile_name = PROTECT(safe_mkchar(cfile_name, &uwtoken));

  SET_VECTOR_ELT(rrgs, 0, safe_allocvector_str(nrgs, &uwtoken));
  SET_VECTOR_ELT(rrgs, 1, safe_allocvector_int(nrgs, &uwtoken));
  SET_VECTOR_ELT(rrgs, 2, safe_allocvector_real(nrgs, &uwtoken));
  SET_VECTOR_ELT(rrgs, 3, safe_allocvector_real(nrgs, &uwtoken));
  SET_VECTOR_ELT(rrgs, 4, safe_allocvector_real(nrgs, &uwtoken));
  SET_VECTOR_ELT(rrgs, 5, safe_allocvector_real(nrgs, &uwtoken));
  SET_VECTOR_ELT(rrgs, 6, safe_allocvector_int(nrgs, &uwtoken));

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

  UNPROTECT(3);
  return rrgs;
  R_API_END();
}

SEXP convert_column_chunks(const char *file_name,
                           vector<parquet::RowGroup> &rgs) {
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

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();

  int nccs = 0;
  for (auto i = 0; i < rgs.size(); i++) {
    nccs += rgs[i].columns.size();
  }

  SEXP rccs = PROTECT(safe_mknamed_vec(nms, &uwtoken));
  SET_VECTOR_ELT(rccs,  0, safe_allocvector_str(nccs, &uwtoken));   // file_name
  SET_VECTOR_ELT(rccs,  1, safe_allocvector_int(nccs, &uwtoken));   // row_group
  SET_VECTOR_ELT(rccs,  2, safe_allocvector_int(nccs, &uwtoken));   // column
  SET_VECTOR_ELT(rccs,  3, safe_allocvector_str(nccs, &uwtoken));   // file_path
  SET_VECTOR_ELT(rccs,  4, safe_allocvector_real(nccs, &uwtoken));  // file_offset
  SET_VECTOR_ELT(rccs,  5, safe_allocvector_real(nccs, &uwtoken));  // offset_index_offset
  SET_VECTOR_ELT(rccs,  6, safe_allocvector_int(nccs, &uwtoken));   // offset_index_length
  SET_VECTOR_ELT(rccs,  7, safe_allocvector_real(nccs, &uwtoken));  // column_index_offset
  SET_VECTOR_ELT(rccs,  8, safe_allocvector_int(nccs, &uwtoken));   // column_index_length
  SET_VECTOR_ELT(rccs,  9, safe_allocvector_int(nccs, &uwtoken));   // type
  SET_VECTOR_ELT(rccs, 10, safe_allocvector_vec(nccs, &uwtoken));   // encodings
  SET_VECTOR_ELT(rccs, 11, safe_allocvector_vec(nccs, &uwtoken));   // path_in_schema
  SET_VECTOR_ELT(rccs, 12, safe_allocvector_int(nccs, &uwtoken));   // codec
  SET_VECTOR_ELT(rccs, 13, safe_allocvector_real(nccs, &uwtoken));  // num_values
  SET_VECTOR_ELT(rccs, 14, safe_allocvector_real(nccs, &uwtoken));  // total_uncompressed_size
  SET_VECTOR_ELT(rccs, 15, safe_allocvector_real(nccs, &uwtoken));  // total_compressed_size
  SET_VECTOR_ELT(rccs, 16, safe_allocvector_real(nccs, &uwtoken));  // data_page_offset
  SET_VECTOR_ELT(rccs, 17, safe_allocvector_real(nccs, &uwtoken));  // index_page_offset
  SET_VECTOR_ELT(rccs, 18, safe_allocvector_real(nccs, &uwtoken));  // dictionary_page_offset

  SEXP rfile_name = PROTECT(safe_mkchar(file_name, &uwtoken));

  int idx = 0;
  for (int i = 0; i < rgs.size(); i++) {
    for (int j = 0; j < rgs[i].columns.size(); j++) {
      parquet::ColumnChunk cc = rgs[i].columns[j];
      parquet::ColumnMetaData cmd = cc.meta_data;
      SET_STRING_ELT(VECTOR_ELT(rccs, 0), idx, rfile_name);
      INTEGER(VECTOR_ELT(rccs, 1))[idx] = i;
      INTEGER(VECTOR_ELT(rccs, 2))[idx] = j;
      SET_STRING_ELT(VECTOR_ELT(rccs, 3), idx,
        cc.__isset.file_path ? safe_mkchar(cc.file_path.c_str(), &uwtoken) : NA_STRING);
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
      SET_VECTOR_ELT(VECTOR_ELT(rccs, 10), idx, safe_allocvector_int(cmd.encodings.size(), &uwtoken));
      for (auto k = 0; k < cmd.encodings.size(); k++) {
        INTEGER(VECTOR_ELT(VECTOR_ELT(rccs, 10), idx))[k] = cmd.encodings[k];
      }
      SET_VECTOR_ELT(VECTOR_ELT(rccs, 11), idx, safe_allocvector_str(cmd.path_in_schema.size(), &uwtoken));
      for (auto k = 0; k < cmd.path_in_schema.size(); k++) {
        SET_STRING_ELT(VECTOR_ELT(VECTOR_ELT(rccs, 11), idx), k,
          safe_mkchar(cmd.path_in_schema[k].c_str(), &uwtoken));
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

  UNPROTECT(3); // rfile_name, rccs, uwtoken
  return rccs;
  R_API_END();
}

SEXP nanoparquet_read_metadata(SEXP filesxp) {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("nanoparquet_read: Need single filename parameter");
  }

  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();

  const char *fname = CHAR(STRING_ELT(filesxp, 0));
  ParquetFile f(fname);

  const char *res_nms[] = {
    "file_meta_data",
    "schema",
    "row_groups",
    "column_chunks",
    ""
    };
  SEXP res = PROTECT(safe_mknamed_vec(res_nms, &uwtoken));

  parquet::FileMetaData fmd = f.file_meta_data;
  const char *fmd_nms[] = {
    "file_name",
    "version",
    "num_rows",
    "key_value_metadata",
    "created_by",
    ""
  };
  SEXP rfmd = PROTECT(safe_mknamed_vec(fmd_nms, &uwtoken));
  SET_VECTOR_ELT(rfmd, 0, safe_mkstring(fname, &uwtoken));
  SET_VECTOR_ELT(rfmd, 1, safe_scalarinteger(fmd.version, &uwtoken));
  SET_VECTOR_ELT(rfmd, 2, safe_scalarreal(fmd.num_rows, &uwtoken));
  SET_VECTOR_ELT(rfmd, 3, convert_key_value_metadata(fmd));
  if (fmd.__isset.created_by) {
    SET_VECTOR_ELT(rfmd, 4, safe_mkstring(fmd.created_by.c_str(), &uwtoken));
  } else {
    SET_VECTOR_ELT(rfmd, 4, safe_scalarstring(NA_STRING, &uwtoken));
  }
  // TODO: encryption algorithm
  // TODO: footer_signing_key_metadata
  SET_VECTOR_ELT(res, 0, rfmd);
  UNPROTECT(1); // rfmd

  SET_VECTOR_ELT(res, 1, convert_schema(fname, fmd.schema));
  SET_VECTOR_ELT(res, 2, convert_row_groups(fname, fmd.row_groups));
  SET_VECTOR_ELT(res, 3, convert_column_chunks(fname, fmd.row_groups));

  UNPROTECT(2); //res
  return res;
  R_API_END();
}

SEXP nanoparquet_read_schema(SEXP filesxp) {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_error("nanoparquet_read: Need single filename parameter");
  }

  R_API_START();
  SEXP cfname = PROTECT(STRING_ELT(filesxp, 0));
  const char *fname = CHAR(cfname);
  ParquetFile f(fname);
  parquet::FileMetaData fmd = f.file_meta_data;
  UNPROTECT(1);
  return convert_schema(fname, fmd.schema);
  R_API_END();
}

} // extern "C"
