#ifndef R_NANOPARQUET_H
#define R_NANOPARQUET_H

#include <Rinternals.h>
#undef TYPE_BITS

#include "lib/ParquetOutFile.h"

void r_to_logical_type(SEXP logical_type, parquet::SchemaElement &sel);
void nanoparquet_map_to_parquet_type(
  SEXP x,
  SEXP options,
  parquet::SchemaElement &sel,
  std::string &rtype);

extern "C" {

static const char *type_names[] = {
  "NULL",
  "a symbol",
  "a pairlist",
  "a closure",
  "an environment",
  "a promise",
  "a language object",
  "a special function",
  "a builtin function",
  "an internal character string",
  "a logical vector",
  "",
  "",
  "an integer vector",
  "a double vector",
  "a complex vector",
  "a character vector",
  "a dot-dot-dot object",
  "an \"any\" object",
  "a list",
  "an expression",
  "a byte code object",
  "an external pointer",
  "a weak reference",
  "a raw vector",
  "an S4 object"
};

Int96 int32_to_int96(int32_t x) noexcept;
Int96 double_to_int96(double x) noexcept;
uint16_t double_to_float16(double x) noexcept;
SEXP rf_get_list_element(SEXP list, const char *str) noexcept;
SEXP nanoparquet_any_na(SEXP x) noexcept;
SEXP nanoparquet_any_null(SEXP x) noexcept;
SEXP convert_logical_type(parquet::LogicalType ltype) noexcept;

} // extern "C"

#endif // R_NANOPARQUET_H
