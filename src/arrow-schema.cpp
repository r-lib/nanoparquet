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
    "type_type",
    "type",
    "nullable",
    "dictionary",
    "custom_metadata",
    ""
  };
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, nms));
  uint32_t ncols = sch->fields.size();
  SET_VECTOR_ELT(res, 0, Rf_allocVector(STRSXP, ncols)); // name
  SET_VECTOR_ELT(res, 1, Rf_allocVector(INTSXP, ncols)); // type_type
  SET_VECTOR_ELT(res, 2, Rf_allocVector(VECSXP, ncols)); // type
  SET_VECTOR_ELT(res, 3, Rf_allocVector(LGLSXP, ncols)); // nullable
  SET_VECTOR_ELT(res, 4, Rf_allocVector(VECSXP, ncols)); // dictionary
  SET_VECTOR_ELT(res, 5, Rf_allocVector(VECSXP, ncols)); // custom_metadata
  SEXP rtype = VECTOR_ELT(res, 2);
  SEXP rdict = VECTOR_ELT(res, 4);
  const char *dictnms[] = {
    "id",
    "index_type",
    "is_ordered",
    "dictionary_kind",
    ""
  };
  const char *kvnms[] = { "key", "value", "" };
  const char *intnms[] = { "bit_width", "is_signed", "" };
  const char *fltnms[] = { "precision", "" };
  const char *decnms[] = { "precision", "scale", "bit_width", "" };
  const char *datnms[] = { "date_unit", "" };
  const char *timnms[] = { "time_unit", "bit_width", "" };
  const char *tstnms[] = { "unit", "timezone", "" };
  const char *ivlnms[] = { "unit", "" };
  const char *uninms[] = { "mode", "type_ids", "" };
  const char *fsbnms[] = { "byte_width", "" };
  const char *fslnms[] = { "list_size", "" };
  const char *mapnms[] = { "keys_sorted", "" };
  const char *durnms[] = { "unit", "" };

  for (auto i = 0; i < sch->fields.size(); i++) {
    bool dict = sch->fields[i]->dictionary != nullptr;
    SET_STRING_ELT(
      VECTOR_ELT(res, 0),
      i,
      Rf_mkChar(sch->fields[i]->name.c_str())
    );
    INTEGER(VECTOR_ELT(res, 1))[i] = sch->fields[i]->type.type;
    switch (sch->fields[i]->type.type) {
      case Type_Int:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, intnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsInt();
        SET_VECTOR_ELT(el, 0, Rf_ScalarInteger(ft->bitWidth));
        SET_VECTOR_ELT(el, 1, Rf_ScalarLogical(ft->is_signed));
        break;
      }
      case Type_FloatingPoint:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, fltnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsFloatingPoint();
        SET_VECTOR_ELT(el, 0,
          Rf_mkString(EnumNamesPrecision()[ft->precision]));
        break;
      }
      case Type_Decimal:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, decnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsDecimal();
        SET_VECTOR_ELT(el, 0, Rf_ScalarInteger(ft->precision));
        SET_VECTOR_ELT(el, 1, Rf_ScalarInteger(ft->scale));
        SET_VECTOR_ELT(el, 2, Rf_ScalarInteger(ft->bitWidth));
        break;
      }
      case Type_Date:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, datnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsDate();
        SET_VECTOR_ELT(el, 0,
          Rf_mkString(EnumNamesDateUnit()[ft->unit]));
        break;
      }
      case Type_Time:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, timnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsTime();
        SET_VECTOR_ELT(el, 0,
          Rf_mkString(EnumNamesTimeUnit()[ft->unit]));
        SET_VECTOR_ELT(el, 1, Rf_ScalarInteger(ft->bitWidth));
        break;
      }
      case Type_Timestamp:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, tstnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsTimestamp();
        SET_VECTOR_ELT(el, 0,
          Rf_mkString(EnumNamesTimeUnit()[ft->unit]));
        SET_VECTOR_ELT(el, 1, Rf_mkString(ft->timezone.c_str()));
        break;
      }
      case Type_Interval:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, ivlnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsInterval();
        SET_VECTOR_ELT(el, 0,
          Rf_mkString(EnumNamesIntervalUnit()[ft->unit]));
        break;
      }
      case Type_Union:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, uninms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsUnion();
        SET_VECTOR_ELT(el, 0,
          Rf_mkString(EnumNamesUnionMode()[ft->mode]));
        SET_VECTOR_ELT(el, 1, Rf_allocVector(INTSXP, ft->typeIds.size()));
        for (auto j = 0; j < ft->typeIds.size(); j++) {
          INTEGER(VECTOR_ELT(el, 1))[j] = ft->typeIds[j];
        }
        break;
      }
      case Type_FixedSizeBinary:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, fsbnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsFixedSizeBinary();
        SET_VECTOR_ELT(el, 0, Rf_ScalarInteger(ft->byteWidth));
        break;
      }
      case Type_FixedSizeList:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, fslnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsFixedSizeList();
        SET_VECTOR_ELT(el, 0, Rf_ScalarInteger(ft->listSize));
        break;
      }
      case Type_Map:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, mapnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsMap();
        SET_VECTOR_ELT(el, 0, Rf_ScalarLogical(ft->keysSorted));
        break;
      }
      case Type_Duration:
      {
        SET_VECTOR_ELT(rtype, i, Rf_mkNamed(VECSXP, durnms));
        SEXP el = VECTOR_ELT(rtype, i);
        auto ft = sch->fields[i]->type.AsDuration();
        SET_VECTOR_ELT(el, 0,
          Rf_mkString(EnumNamesTimeUnit()[ft->unit]));
        break;
      }
      default:
        // nothing extra for Null, Struct_, List, LargeList, ListView,
        // LargeListView, Utf8, Binary, LargeUtf8, LargeBinary, Utf8View,
        // BinaryView, Bool, RunEndEncoded
        break;
    };
    LOGICAL(VECTOR_ELT(res, 3))[i] = sch->fields[i]->nullable;
    if (dict) {
      SET_VECTOR_ELT(rdict, i, Rf_mkNamed(VECSXP, dictnms));
      SEXP rdicti = VECTOR_ELT(rdict, i);
      auto &fd = sch->fields[i]->dictionary;
      SET_VECTOR_ELT(rdicti, 0, Rf_ScalarReal(fd->id));
      SET_VECTOR_ELT(rdicti, 1, Rf_mkNamed(VECSXP, intnms));
      SEXP rdictii = VECTOR_ELT(rdicti, 1);
      SET_VECTOR_ELT(rdictii, 0, Rf_ScalarInteger(fd->indexType->bitWidth));
      SET_VECTOR_ELT(rdictii, 1, Rf_ScalarLogical(fd->indexType->is_signed));
      SET_VECTOR_ELT(rdicti, 2, Rf_ScalarLogical(fd->isOrdered));
      SET_VECTOR_ELT(rdicti, 3, Rf_ScalarInteger(fd->dictionaryKind));
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
