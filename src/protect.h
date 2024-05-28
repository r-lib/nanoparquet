#pragma once

#include <Rdefines.h>

#define STR(x) #x

#define R_API_START()                                                    \
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
    Rf_error("error in " STR(__FILE__) " @ " STR(__PRETTY_FUNCTION__));  \
  }                                                                      \
  if (!Rf_isNull(err_)) {                                                \
    R_ContinueUnwind(err_);                                              \
  } else {                                                               \
    Rf_error("%s", error_buffer_);                                       \
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
SEXP wrapped_mknamed_vec(void *data);
SEXP wrapped_mkchar(void *data);

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

inline SEXP safe_mkchar(const char *c, SEXP *uwt) {
  return R_UnwindProtect(wrapped_mkchar, &c, throw_error, uwt, *uwt);
}

inline SEXP safe_mknamed_vec(const char **names, SEXP *uwt) {
  return R_UnwindProtect(wrapped_mknamed_vec, &names, throw_error, uwt, *uwt);
}
