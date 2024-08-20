#pragma once

#include <string>
#include <Rdefines.h>

#define STR(x) STR2(x)
#define STR2(x) #x

#define R_API_START(call)                                                \
  nanoparquet_call = call;                                               \
  SEXP err_ = R_NilValue;                                                \
  char error_buffer_[8192];                                              \
  error_buffer_[0] = '\0';                                               \
  try {

#define R_API_END()                                                      \
  } catch(np_error &cont) {                                              \
    err_ = cont.uwtoken;                                                 \
  } catch(std::exception& ex) {                                          \
    strncpy(error_buffer_, ex.what(), sizeof(error_buffer_) - 1);        \
  } catch(std::string& ex) {                                             \
    strncpy(error_buffer_, ex.c_str(), sizeof(error_buffer_) - 1);       \
  } catch(...) {                                                         \
    SEXP nc = nanoparquet_call;                                          \
    nanoparquet_call = R_NilValue;                                       \
    Rf_errorcall(                                                        \
      nc,                                                                \
      "nanoparquet error @ " __FILE__ ":" STR(__LINE__)                  \
    );                                                                   \
  }                                                                      \
  if (!Rf_isNull(err_)) {                                                \
    nanoparquet_call = R_NilValue;                                       \
    R_ContinueUnwind(err_);                                              \
  } else {                                                               \
    SEXP nc = nanoparquet_call;                                          \
    nanoparquet_call = R_NilValue;                                       \
    Rf_errorcall(nc, "%s", error_buffer_);                               \
  }                                                                      \
  return R_NilValue;

struct np_error {
  SEXP uwtoken;
};

void throw_error(void *err, Rboolean jump);

SEXP wrapped_rawsxp(void *len);
SEXP wrapped_intsxp(void *len);
SEXP wrapped_realsxp(void *len);
SEXP wrapped_strsxp(void *len);
SEXP wrapped_vecsxp(void *len);
SEXP wrapped_mknamed_vec(void *data);
SEXP wrapped_mkchar(void *data);
SEXP wrapped_mkchar_len_utf8(void *data, int len);
SEXP wrapped_mkstring(void *data);
SEXP wrapped_scalarinteger(void *data);
SEXP wrapped_scalarreal(void *data);
SEXP wrapped_scalarlogical(void *data);
SEXP wrapped_scalarstring(void *data);

inline SEXP safe_allocvector_raw(R_xlen_t len, SEXP *uwt) {
  return R_UnwindProtect(wrapped_rawsxp, &len, throw_error, uwt, *uwt);
}

inline SEXP safe_allocvector_int(R_xlen_t len, SEXP *uwt) {
  return R_UnwindProtect(wrapped_intsxp, &len, throw_error, uwt, *uwt);
}

inline SEXP safe_allocvector_real(R_xlen_t len, SEXP *uwt) {
  return R_UnwindProtect(wrapped_realsxp, &len, throw_error, uwt, *uwt);
}

inline SEXP safe_allocvector_str(R_xlen_t len, SEXP *uwt) {
  return R_UnwindProtect(wrapped_strsxp, &len, throw_error, uwt, *uwt);
}

inline SEXP safe_allocvector_vec(R_xlen_t len, SEXP *uwt) {
  return R_UnwindProtect(wrapped_vecsxp, &len, throw_error, uwt, *uwt);
}

inline SEXP safe_mkchar(const char *c, SEXP *uwt) {
  return R_UnwindProtect(wrapped_mkchar, &c, throw_error, uwt, *uwt);
}

struct safe_mkchar_len_data {
  char *c;
  int len;
};

inline SEXP safe_mkstring(const char *c, SEXP *uwt) {
  return R_UnwindProtect(wrapped_mkstring, &c, throw_error, uwt, *uwt);
}

inline SEXP safe_scalarinteger(int n, SEXP *uwt) {
  return R_UnwindProtect(wrapped_scalarinteger, &n, throw_error, uwt, *uwt);
}

inline SEXP safe_scalarreal(double n, SEXP *uwt) {
  return R_UnwindProtect(wrapped_scalarreal, &n, throw_error, uwt, *uwt);
}

inline SEXP safe_scalarlogical(int n, SEXP *uwt) {
  return R_UnwindProtect(wrapped_scalarlogical, &n, throw_error, uwt, *uwt);
}

inline SEXP safe_scalarstring(SEXP x, SEXP *uwt) {
  return R_UnwindProtect(wrapped_scalarstring, &x, throw_error, uwt, *uwt);
}

inline SEXP safe_mknamed_vec(const char **names, SEXP *uwt) {
  return R_UnwindProtect(wrapped_mknamed_vec, &names, throw_error, uwt, *uwt);
}
