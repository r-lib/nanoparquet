#include "lib/memstream.h"
// R headers last, because they define Realloc, Free, etc., and
// winsock2.h does as well.
#include "RParquetOutFile.h"
#include "RParquetAppender.h"
#include "r-nanoparquet.h"
#include "unwind.h"

using namespace nanoparquet;

extern SEXP nanoparquet_call;

extern "C" {

SEXP rf_nanoparquet_write(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression,
  SEXP metadata,
  SEXP required,
  SEXP options,
  SEXP schema,
  SEXP encoding,
  SEXP row_group_starts,
  SEXP call) noexcept {

  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_errorcall(call, "nanoparquet_write: filename must be a string");
  }

  int c_compression = INTEGER(compression)[0];
  parquet::CompressionCodec::type codec;
  switch (c_compression) {
  case 0:
    codec = parquet::CompressionCodec::UNCOMPRESSED;
    break;
  case 1:
    codec = parquet::CompressionCodec::SNAPPY;
    break;
  case 2:
    codec = parquet::CompressionCodec::GZIP;
    break;
  case 6:
    codec = parquet::CompressionCodec::ZSTD;
    break;
  default:
    Rf_errorcall(call, "Invalid compression type code: %d", // # nocov
                 c_compression);                            // # nocov
    break;
  }

  int dp_ver = INTEGER(rf_get_list_element(options, "write_data_page_version"))[0];
  int comp_level = INTEGER(rf_get_list_element(options, "compression_level"))[0];
  const char *cfname = CHAR(STRING_ELT(filesxp, 0));
  const int *crow_group_starts = INTEGER(row_group_starts);
  R_xlen_t nrg = Rf_xlength(row_group_starts);
  SEXP res = R_NilValue;
  PROTECT(nanoparquet_call = call);

  CPP_INIT;
  CPP_BEGIN; // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  std::vector<int64_t> row_groups(nrg);
  for (R_xlen_t i = 0; i < nrg; i++) {
    // convert to zero-based
    row_groups[i] = crow_group_starts[i] - 1;
  }

  std::string fname = cfname;
  if (fname == ":raw:") {
    MemStream ms;
    std::ostream &os = ms.stream();
    RParquetOutFile of(os, codec, comp_level, row_groups);
    of.data_page_version = dp_ver;
    of.init_metadata(dfsxp, dim, metadata, required, options, schema, encoding);
    of.write();
    R_xlen_t bufsize = ms.size();
    res = r_eval([bufsize] {
      return Rf_allocVector(RAWSXP, bufsize);
    });
    ms.copy(RAW(res), bufsize);
  } else {
    RParquetOutFile of(fname, codec, comp_level, row_groups);
    of.data_page_version = dp_ver;
    of.init_metadata(dfsxp, dim, metadata, required, options, schema, encoding);
    of.write();
  }

  CPP_END; // -------------------------------------------------------------

  nanoparquet_call = R_NilValue;
  UNPROTECT(1);
  return res;
}

SEXP rf_nanoparquet_append(
  SEXP dfsxp,
  SEXP filesxp,
  SEXP dim,
  SEXP compression,
  SEXP required,
  SEXP options,
  SEXP schema,
  SEXP encoding,
  SEXP row_group_starts,
  SEXP overwrite_last_row_group,
  SEXP call
) noexcept {
  if (TYPEOF(filesxp) != STRSXP || LENGTH(filesxp) != 1) {
    Rf_errorcall(call, "nanoparquet_append: filename must be a string");
  }

  int c_compression = INTEGER(compression)[0];
  parquet::CompressionCodec::type codec;
  switch (c_compression) {
  case 0:
    codec = parquet::CompressionCodec::UNCOMPRESSED;
    break;
  case 1:
    codec = parquet::CompressionCodec::SNAPPY;
    break;
  case 2:
    codec = parquet::CompressionCodec::GZIP;
    break;
  case 6:
    codec = parquet::CompressionCodec::ZSTD;
    break;
  default:
    Rf_errorcall(call, "Invalid compression type code: %d", // # nocov
                 c_compression);                            // # nocov
    break;
  }

  int dp_ver = INTEGER(rf_get_list_element(options, "write_data_page_version"))[0];
  int comp_level = INTEGER(rf_get_list_element(options, "compression_level"))[0];
  R_xlen_t nrg = Rf_xlength(row_group_starts);
  const char *cfname = CHAR(STRING_ELT(filesxp, 0));
  const int *prow_group_starts = INTEGER(row_group_starts);
  int coverwrite_last_row_group = LOGICAL(overwrite_last_row_group)[0];
  PROTECT(nanoparquet_call = call);

  CPP_INIT;
  CPP_BEGIN; // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  std::string fname = cfname;
  std::vector<int64_t> row_groups(nrg);
  for (R_xlen_t i = 0; i < nrg; i++) {
    // convert to zero-based
    row_groups[i] = prow_group_starts[i] - 1;
  }

  RParquetAppender appender(
    fname,
    codec,
    comp_level,
    row_groups,
    dp_ver,
    coverwrite_last_row_group
  );
  appender.init_metadata(dfsxp, dim, required, options, schema, encoding);
  appender.append();

  CPP_END; // -------------------------------------------------------------

  UNPROTECT(1);
  nanoparquet_call = R_NilValue;
  return R_NilValue;
}

SEXP rf_nanoparquet_logical_to_converted(SEXP logical_type) noexcept {
  const char *nms[] = { "converted_type", "scale", "precision", "" };
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, nms));
  SET_VECTOR_ELT(res, 0, Rf_ScalarInteger(NA_INTEGER));
  SET_VECTOR_ELT(res, 1, Rf_ScalarInteger(NA_INTEGER));
  SET_VECTOR_ELT(res, 2, Rf_ScalarInteger(NA_INTEGER));
  int *res0 = INTEGER(VECTOR_ELT(res, 0));
  int *res1 = INTEGER(VECTOR_ELT(res, 1));
  int *res2 = INTEGER(VECTOR_ELT(res, 2));

  CPP_INIT;
  CPP_BEGIN;

  parquet::SchemaElement sel;
  r_to_logical_type(logical_type, sel);
  fill_converted_type_for_logical_type(sel);

  if (sel.__isset.converted_type) {
    res0[0] = sel.converted_type;
  }
  if (sel.__isset.scale) {
    res1[0] = sel.scale;
  }
  if (sel.__isset.precision) {
    res2[0] = sel.precision;
  }

  CPP_END;

  UNPROTECT(1);
  return res;
}

SEXP rf_nanoparquet_map_to_parquet_types(SEXP df, SEXP options) noexcept {
  R_xlen_t nc = Rf_xlength(df);
  SEXP res = PROTECT(Rf_allocVector(VECSXP, nc));

  CPP_INIT;

  for (R_xlen_t cl = 0; cl < nc; cl++) {
    SEXP col = VECTOR_ELT(df, cl);
    parquet::SchemaElement sel;
    std::string rtype;
    CPP_BEGIN;
    nanoparquet_map_to_parquet_type(col, options, sel, rtype);
    CPP_END;
    SEXP typ = Rf_allocVector(VECSXP, 3);
    SET_VECTOR_ELT(res, cl, typ);
    SET_VECTOR_ELT(typ, 0, Rf_mkString(to_string(sel.type).c_str()));
    SET_VECTOR_ELT(typ, 1, Rf_mkString(rtype.c_str()));
    if (sel.__isset.logicalType) {
      SET_VECTOR_ELT(typ, 2, rf_convert_logical_type(sel.logicalType));
    } else {
      SET_VECTOR_ELT(typ, 2, R_NilValue);
    }
  }

  UNPROTECT(1);
  return res;
}

} // extern "C"
