#include <unordered_set>
#include <cstdint>
#include <cmath>

#include <Rdefines.h>
#include "protect.h"

struct void_ptr_hash {
  inline size_t operator()(const void* p) const {
    static const size_t shift = (size_t)log2(1 + sizeof(SEXP));
    return (size_t)(p) >> shift;
  }
};

uint64_t create_dict_ptr(void** values, int *dict, uint64_t len) {
  std::unordered_set<void*, void_ptr_hash> mm;
  mm.reserve(len * 2);
  void **begin = values;
  void **end = begin + len;
  int n = 0;

  for (int i = 0; begin < end; begin++, i++) {
    auto res = mm.insert(*begin);
    if (res.second) dict[n++] = i;
  }

  return n;
}

template <typename T>
uint64_t create_dict(T* values, int *dict, uint64_t len) {
  std::unordered_set<T> mm;
  mm.reserve(len * 2);
  T *begin = values;
  T *end = begin + len;
  int n = 0;

  for (int i = 0; begin < end; begin++, i++) {
    auto res = mm.insert(*begin);
    if (res.second) dict[n++] = i;
  }

  return n;
}

extern "C" {

SEXP nanoparquet_create_dict(SEXP x) {
  R_xlen_t dictlen, len = Rf_xlength(x);
  SEXP uwtoken = PROTECT(R_MakeUnwindCont());
  R_API_START();

  SEXP dict = PROTECT(safe_allocvector_int(len, &uwtoken));
  int *idict = INTEGER(dict);
  switch (TYPEOF(x)) {
    case LGLSXP:
      dictlen = create_dict<int>(LOGICAL(x), idict, len);
      break;
    case INTSXP:
      dictlen = create_dict<int>(INTEGER(x), idict, len);
      break;
    case REALSXP:
      dictlen = create_dict<double>(REAL(x), idict, len);
      break;
    case STRSXP: {
      dictlen = create_dict_ptr((void**)STRING_PTR_RO(x),idict, len);
      break;
    }
    default:
      Rf_error("Cannot create dictionary for this type");
      break;
  }

  if (dictlen < len) {
    dict = safe_xlengthgets(dict, dictlen, &uwtoken);
  }

  UNPROTECT(2);
  return dict;
  R_API_END();
}

}
