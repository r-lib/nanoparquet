#include <iostream>

#include <Rdefines.h>

#include "flatbuffers/Message_generated.h"
#include "simdutf/simdutf.h"

#include "lib/bytebuffer.h"

using namespace org::apache::arrow::flatbuf;
using namespace flatbuffers;
using namespace std;

template <typename RootType>
bool VerifyFlatbuffers(const uint8_t* data, int64_t size) {
  Verifier verifier(
      data, static_cast<size_t>(size),
      /*max_depth=*/128,
      /*max_tables=*/static_cast<flatbuffers::uoffset_t>(8 * size));
  return verifier.VerifyBuffer<RootType>(nullptr);
}

extern "C" {

SEXP miniparquet_parse_arrow_schema_impl(uint8_t *buf, uint32_t len) {
  bool ok = VerifyFlatbuffers<Message>(buf, len);
  if (!ok) {
     return R_NilValue;
  }

  MessageT msg;
  GetMessage(buf)->UnPackTo(&msg);

  SchemaT *sch = msg.header.AsSchema();
  if (sch == nullptr) {
    return R_NilValue;
  }

  const char *nms[] = {
    "name",
    "type",
    "nullable",
    "dictionary_bit_width",
    "dictionary_signed",
    "custom_metadata",
    ""
  };
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, nms));
  uint32_t ncols = sch->fields.size();
  SET_VECTOR_ELT(res, 0, Rf_allocVector(STRSXP, ncols));
  SET_VECTOR_ELT(res, 1, Rf_allocVector(INTSXP, ncols));
  SET_VECTOR_ELT(res, 2, Rf_allocVector(LGLSXP, ncols));
  SET_VECTOR_ELT(res, 3, Rf_allocVector(INTSXP, ncols));
  SET_VECTOR_ELT(res, 4, Rf_allocVector(LGLSXP, ncols));
  SET_VECTOR_ELT(res, 5, Rf_allocVector(VECSXP, ncols));

  const char *kvnms[] = { "key", "value", "" };

  for (auto i = 0; i < sch->fields.size(); i++) {
    bool dict = sch->fields[i]->dictionary != nullptr;
    SET_STRING_ELT(
      VECTOR_ELT(res, 0),
      i,
      Rf_mkChar(sch->fields[i]->name.c_str())
    );
    INTEGER(VECTOR_ELT(res, 1))[i] = sch->fields[i]->type.type;
    LOGICAL(VECTOR_ELT(res, 2))[i] = sch->fields[i]->nullable;
    if (dict) {
      INTEGER(VECTOR_ELT(res, 3))[i] =
        sch->fields[i]->dictionary->indexType->bitWidth;
      LOGICAL(VECTOR_ELT(res, 4))[i] =
        sch->fields[i]->dictionary->indexType->is_signed;
    } else {
      INTEGER(VECTOR_ELT(res, 3))[i] = NA_INTEGER;
      LOGICAL(VECTOR_ELT(res, 4))[i] = NA_LOGICAL;
    }
    SET_VECTOR_ELT(VECTOR_ELT(res, 5), i, Rf_mkNamed(VECSXP, kvnms));
    SEXP kv = VECTOR_ELT(VECTOR_ELT(res, 5), i);
    size_t kvlen = sch->fields[i]->custom_metadata.size();
    SET_VECTOR_ELT(kv, 0, Rf_allocVector(STRSXP, kvlen));
    SET_VECTOR_ELT(kv, 1, Rf_allocVector(STRSXP, kvlen));
    for (auto j = 0; j < kvlen; j++) {
      SET_STRING_ELT(
        VECTOR_ELT(kv, 0),
        j,
        Rf_mkChar(sch->fields[i]->custom_metadata[j]->key.c_str())
      );
      SET_STRING_ELT(
        VECTOR_ELT(kv, 1),
        j,
        Rf_mkChar(sch->fields[i]->custom_metadata[j]->value.c_str())
      );
    }
  }

  SEXP kv = PROTECT(Rf_mkNamed(VECSXP, kvnms));
  size_t kvlen = sch->custom_metadata.size();
  SET_VECTOR_ELT(kv, 0, Rf_allocVector(STRSXP, kvlen));
  SET_VECTOR_ELT(kv, 1, Rf_allocVector(STRSXP, kvlen));
  for (auto i = 0; i < sch->custom_metadata.size(); i++) {
    SET_STRING_ELT(
      VECTOR_ELT(kv, 0),
      i,
      Rf_mkChar(sch->custom_metadata[i]->key.c_str())
    );
    SET_STRING_ELT(
      VECTOR_ELT(kv, 1),
      i,
      Rf_mkChar(sch->custom_metadata[i]->value.c_str())
    );
  }

  SEXP features = PROTECT(Rf_allocVector(INTSXP, sch->features.size()));
  for (auto i = 0; i < sch->features.size(); i++) {
    INTEGER(features)[i] = sch->features[i];
  }

  SEXP fres = PROTECT(Rf_allocVector(VECSXP, 4));
  SET_VECTOR_ELT(fres, 0, res);
  SET_VECTOR_ELT(fres, 1, kv);
  SET_VECTOR_ELT(fres, 2, Rf_ScalarInteger(sch->endianness));
  SET_VECTOR_ELT(fres, 3, features);

  UNPROTECT(4);
  return fres;
}

SEXP miniparquet_parse_arrow_schema(SEXP rbuf) {
  // base64 decode first
  if (TYPEOF(rbuf) != STRSXP) {
    Rf_error("Arrow schema must be a RAW vector or a string");
  }
  const char *input = (const char*) CHAR(STRING_ELT(rbuf, 0));
  size_t slen = strlen(input);

  size_t olen = simdutf::maximal_binary_length_from_base64(
    input,
    slen
  );
  ByteBuffer bbuf;
  bbuf.resize(olen);
  simdutf::result bres = simdutf::base64_to_binary(
    input,
    slen,
    (char*) bbuf.ptr
  );
  size_t rawlen = bres.count;
  uint8_t *buf = (uint8_t*) bbuf.ptr;

  if (rawlen < 4) {
    Rf_error("Invalid serialized Arrow schema");
  }
  // The first four bytes may be an optional continuation token.
  // We try to parse the schema with and without a token.
  uint32_t len = ((uint32_t *) buf)[0];
  SEXP res = R_NilValue;
  if (len <= rawlen - 4) {
    res = miniparquet_parse_arrow_schema_impl(buf + 4, len);
  }

  // If it failed, then try to skip the continuation token
  if (Rf_isNull(res)) {
    if (rawlen < 8) {
      Rf_error("Invalid serialized Arrow schema");
    }
    len = ((uint32_t*) buf)[1];
    if (len <= rawlen - 8) {
      res = miniparquet_parse_arrow_schema_impl(buf + 8, len);
    }
  }

  if (Rf_isNull(res)) {
    Rf_error("Failed to parse serialized Arrow schema");
  }

  return res;
}

}
