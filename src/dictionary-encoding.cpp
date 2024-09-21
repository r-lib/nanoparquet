#include <unordered_set>
#include <unordered_map>
#include <cstdint>
#include <cmath>
#include <iostream>

#include <Rdefines.h>
#include "protect.h"

extern SEXP nanoparquet_call;

struct void_ptr_hash {
  inline size_t operator()(const void* p) const {
    static const size_t shift = (size_t)log2(1 + sizeof(SEXP));
    return (size_t)(p) >> shift;
  }
};

uint64_t create_dict_ptr(void** values, uint64_t len, void* naval) {
  std::unordered_set<void*, void_ptr_hash> mm;
  mm.reserve(len * 2);
  void **begin = values;
  void **end = begin + len;
  int n = 0;

  for (int i = 0; begin < end; begin++, i++) {
    if (*begin != naval) {
      auto res = mm.insert(*begin);
      n += res.second;
    }
  }

  return n;
}

uint64_t create_dict_real(double* values, uint64_t len) {
  std::unordered_set<double> mm;
  mm.reserve(len * 2);
  double *begin = values;
  double *end = begin + len;
  int n = 0;

  for (int i = 0; begin < end; begin++, i++) {
    if (!R_IsNA(*begin)) {
      auto res = mm.insert(*begin);
      n += res.second;
    }
  }

  return n;
}

template <typename T>
uint64_t create_dict(T* values, uint64_t len, T naval) {
  std::unordered_set<T> mm;
  mm.reserve(len * 2);
  T *begin = values;
  T *end = begin + len;
  int n = 0;

  for (int i = 0; begin < end; begin++, i++) {
    if (*begin != naval) {
      auto res = mm.insert(*begin);
      n += res.second;
    }
  }

  return n;
}

uint64_t create_dict_ptr_idx(void** values, int *dict, int *idx,
                             uint64_t len, void *naval) {
  std::unordered_map<void*, int, void_ptr_hash> mm;
  mm.reserve(len * 2);
  void **begin = values;
  void **end = begin + len;
  int n = 0;

  for (int i = 0; begin < end; begin++, i++) {
    if (*begin == naval) {
      idx[i] = NA_INTEGER;
      continue;
    }
    auto it = mm.find(*begin);
    if (it == mm.end()) {
      mm.insert(std::make_pair(*begin, n));
      idx[i] = n;
      dict[n] = i;
      n++;
    } else {
      idx[i] = it->second;
    }
  }

  return n;
}

uint64_t create_dict_real_idx(double* values, int *dict, int *idx,
                              uint64_t len, double &minval,
                              double &maxval, bool &hasminmax) {
  std::unordered_map<double, int> mm;
  mm.reserve(len * 2);
  double *begin = values;
  double *end = begin + len;
  int n = 0;

  hasminmax = false;

  for (int i = 0; begin < end; begin++, i++) {
    if (R_IsNA(*begin)) {
      idx[i] = NA_INTEGER;
      continue;
    }
    if (!hasminmax) {
      hasminmax = true;
      minval = maxval = *begin;
    }
    auto it = mm.find(*begin);
    if (it == mm.end()) {
      if (*begin < minval) minval = *begin;
      if (*begin > maxval) maxval = *begin;
      mm.insert(std::make_pair(*begin, n));
      idx[i] = n;
      dict[n] = i;
      n++;
    } else {
      idx[i] = it->second;
    }
  }

  return n;
}

template <typename T>
uint64_t create_dict_idx(T* values, int *dict, int *idx, uint64_t len,
                         T naval, T &minval, T &maxval, bool &hasminmax) {
  std::unordered_map<T, int> mm;
  mm.reserve(len * 2);
  T *begin = values;
  T *end = begin + len;
  int n = 0;

  hasminmax = false;

  for (int i = 0; begin < end; begin++, i++) {
    if (*begin == naval) {
      idx[i] = NA_INTEGER;
      continue;
    }
    if (!hasminmax) {
      hasminmax = true;
      minval = maxval = *begin;
    }
    auto it = mm.find(*begin);
    if (it == mm.end()) {
      if (*begin < minval) minval = *begin;
      if (*begin > maxval) maxval = *begin;
      mm.insert(std::make_pair(*begin, n));
      idx[i] = n;
      dict[n] = i;
      n++;
    } else {
      idx[i] = it->second;
    }
  }

  return n;
}

extern "C" {

SEXP nanoparquet_create_dict(SEXP x, SEXP rlen) {
  R_xlen_t dictlen, len = INTEGER(rlen)[0];

  switch (TYPEOF(x)) {
    case LGLSXP:
      dictlen = create_dict<int>(LOGICAL(x), len, NA_LOGICAL);
      break;
    case INTSXP:
      dictlen = create_dict<int>(INTEGER(x), len, NA_INTEGER);
      break;
    case REALSXP:
      dictlen = create_dict_real(REAL(x), len);
      break;
    case STRSXP: {
      dictlen = create_dict_ptr((void**)STRING_PTR_RO(x), len, (void*) NA_STRING);
      break;
    }
    default:
      Rf_error("Cannot create dictionary for this type");
      break;
  }

  return Rf_ScalarInteger(dictlen);
}

SEXP nanoparquet_create_dict_idx_(SEXP x, SEXP from, SEXP until) {
  int64_t cfrom = INTEGER(from)[0];
  int64_t cuntil = INTEGER(until)[0];
  int64_t len = cuntil - cfrom;
  R_xlen_t dictlen;

  SEXP idx = PROTECT(Rf_allocVector(INTSXP, len));
  SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
  int *idict = INTEGER(dict);
  int *iidx = INTEGER(idx);
  int imin, imax;
  double dmin, dmax;
  bool hasminmax = false;
  switch (TYPEOF(x)) {
    case LGLSXP:
      dictlen = create_dict_idx<int>(
        LOGICAL(x) + cfrom, iidx, idict, len, NA_LOGICAL,
        imin, imax, hasminmax
      );
      break;
    case INTSXP:
      dictlen = create_dict_idx<int>(
        INTEGER(x) + cfrom, idict, iidx, len, NA_INTEGER,
        imin, imax, hasminmax
      );
      break;
    case REALSXP:
      dictlen = create_dict_real_idx(
        REAL(x) + cfrom, idict, iidx, len,
        dmin, dmax, hasminmax
      );
      break;
    case STRSXP: {
      dictlen = create_dict_ptr_idx(
        (void**)(STRING_PTR_RO(x) + cfrom), idict, iidx, len,
        (void*) NA_STRING
      );
      break;
    }
    default:
      Rf_error("Cannot create dictionary for this type");
      break;
  }

  SEXP res = PROTECT(Rf_allocVector(VECSXP, hasminmax ? 4 : 2));
  SET_VECTOR_ELT(res, 0, dict);
  SET_VECTOR_ELT(res, 1, idx);
  if (hasminmax) {
    if (TYPEOF(x) == INTSXP) {
      SET_VECTOR_ELT(res, 2, Rf_ScalarInteger(imin));
      SET_VECTOR_ELT(res, 3, Rf_ScalarInteger(imax));
    } else if (TYPEOF(x) == REALSXP) {
      SET_VECTOR_ELT(res, 2, Rf_ScalarReal(dmin));
      SET_VECTOR_ELT(res, 3, Rf_ScalarReal(dmax));
    }
  }

  if (dictlen < len) {
    SET_VECTOR_ELT(res, 0, Rf_xlengthgets(dict, dictlen));
  }

  UNPROTECT(3);
  return res;
}

struct nanoparquet_create_dict_idx_data {
  SEXP data;
  SEXP from;
  SEXP until;
};

inline SEXP nanoparquet_create_dict_idx_wrapper(void *data) {
  struct nanoparquet_create_dict_idx_data *rdata =
    (struct nanoparquet_create_dict_idx_data*) data;
  return nanoparquet_create_dict_idx_(
    rdata->data,
    rdata->from,
    rdata->until
  );
}

SEXP nanoparquet_create_dict_idx(SEXP x, SEXP from, SEXP until, SEXP call) {

  struct nanoparquet_create_dict_idx_data data = { x, from, until };

  SEXP uwt = PROTECT(R_MakeUnwindCont());
  R_API_START(call);

  SEXP ret = R_UnwindProtect(
    nanoparquet_create_dict_idx_wrapper,
    &data,
    throw_error,
    &uwt,
    uwt
  );

  UNPROTECT(1);
  return ret;

  R_API_END();
}

// Only works for LGLSXP, we don't use it for anything else
SEXP nanoparquet_avg_run_length(SEXP x, SEXP rlen) {
  uint32_t len = INTEGER(rlen)[0] - 1;
  if (len == -1) {
    return Rf_ScalarInteger(0);
  }
  if (len == 0) {
    return Rf_ScalarInteger(1);
  }
  uint32_t num_runs = 0;
  uint32_t sum_runs = 0;
  uint32_t crl = 1; // current run length
  const int *xx = LOGICAL(x);
  for (auto i = 0; i < len; i++, xx++) {
    if (*xx == NA_LOGICAL) {
      // do nothing
    } else if (*xx == *(xx+1)) {
      crl++;
    } else {
      num_runs++;
      sum_runs += crl;
      crl = 1;
    }
  }
  // last one, we could omit this, but easier to test if included
  num_runs++;
  sum_runs += crl;

  return Rf_ScalarInteger(sum_runs / num_runs);
}

}
