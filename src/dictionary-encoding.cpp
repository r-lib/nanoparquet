#include <unordered_set>
#include <unordered_map>
#include <cstdint>
#include <cmath>
#include <iostream>
#include <cstring>

#include <Rinternals.h>
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

static inline bool STR_LESS(SEXP sc, SEXP set) {
  const char *c = CHAR(sc), *et = CHAR(set);
  size_t l = strlen(c), el = strlen(et);
  if (l == 0) return el > 0;
  if (el == 0) return false;
  int res = memcmp(c, et, l < el ? l : el);
  return res < 0 || (res == 0 && l < el);
}

static inline bool STR_MORE(SEXP sc, SEXP set) {
  const char *c = CHAR(sc), *et = CHAR(set);
  size_t l = strlen(c), el = strlen(et);
  if (l == 0) return false;
  if (el == 0) return true;
  int res = memcmp(c, et, l < el ? l : el);
  return res > 0 || (res == 0 && l > el);
}

uint64_t create_dict_str_idx(const SEXP* values, int *dict, int *idx,
                             uint64_t len, SEXP naval, SEXP &minval,
                             SEXP &maxval, bool &hasminmax) {
  std::unordered_map<void*, int, void_ptr_hash> mm;
  mm.reserve(len * 2);
  SEXP *begin = (SEXP*) values;
  SEXP *end = (SEXP*) begin + len;
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
      if (STR_LESS(*begin, minval)) minval = *begin;
      if (STR_MORE(*begin, maxval)) maxval = *begin;
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

// Create dict for VECSXP (list columns). Returns a list with:
//   res[0] = actual unique leaf values (INTSXP/REALSXP/STRSXP)
//   res[1] = flat dict indices for all non-NA leaf values
//   res[2] = per-row offsets: offsets[i] = cumulative non-NA leaf count for rows 0..i
static SEXP nanoparquet_create_dict_idx_vecsxp_(SEXP x, int64_t cfrom,
                                                int64_t cuntil) {
  int64_t len = cuntil - cfrom;

  // Detect element type from the first non-empty list element
  SEXPTYPE elt_type = NILSXP;
  for (int64_t i = cfrom; i < cuntil; i++) {
    SEXP elt = VECTOR_ELT(x, i);
    if (!Rf_isNull(elt) && Rf_xlength(elt) > 0) {
      elt_type = TYPEOF(elt);
      break;
    }
  }

  // Compute per-row offsets of non-NA leaf values
  SEXP offsets = PROTECT(Rf_allocVector(INTSXP, len + 1));
  int *ioffsets = INTEGER(offsets);
  ioffsets[0] = 0;
  for (int64_t i = 0; i < len; i++) {
    SEXP row = VECTOR_ELT(x, cfrom + i);
    int count = 0;
    if (!Rf_isNull(row)) {
      R_xlen_t rlen = Rf_xlength(row);
      if (elt_type == INTSXP) {
        int *irow = INTEGER(row);
        for (R_xlen_t j = 0; j < rlen; j++) if (irow[j] != NA_INTEGER) count++;
      } else if (elt_type == REALSXP) {
        double *drow = REAL(row);
        for (R_xlen_t j = 0; j < rlen; j++) if (!R_IsNA(drow[j])) count++;
      } else if (elt_type == STRSXP) {
        for (R_xlen_t j = 0; j < rlen; j++) if (STRING_ELT(row, j) != NA_STRING) count++;
      }
    }
    ioffsets[i + 1] = ioffsets[i] + count;
  }
  int total_leaves = ioffsets[len];

  // Build flat array of non-NA leaf values
  SEXPTYPE flat_type = (elt_type == NILSXP) ? INTSXP : elt_type;
  SEXP flat = PROTECT(Rf_allocVector(flat_type, total_leaves));
  if (elt_type == INTSXP) {
    int *iflat = INTEGER(flat);
    int k = 0;
    for (int64_t i = 0; i < len; i++) {
      SEXP row = VECTOR_ELT(x, cfrom + i);
      if (!Rf_isNull(row)) {
        int *irow = INTEGER(row);
        R_xlen_t rlen = Rf_xlength(row);
        for (R_xlen_t j = 0; j < rlen; j++) if (irow[j] != NA_INTEGER) iflat[k++] = irow[j];
      }
    }
  } else if (elt_type == REALSXP) {
    double *dflat = REAL(flat);
    int k = 0;
    for (int64_t i = 0; i < len; i++) {
      SEXP row = VECTOR_ELT(x, cfrom + i);
      if (!Rf_isNull(row)) {
        double *drow = REAL(row);
        R_xlen_t rlen = Rf_xlength(row);
        for (R_xlen_t j = 0; j < rlen; j++) if (!R_IsNA(drow[j])) dflat[k++] = drow[j];
      }
    }
  } else if (elt_type == STRSXP) {
    int k = 0;
    for (int64_t i = 0; i < len; i++) {
      SEXP row = VECTOR_ELT(x, cfrom + i);
      if (!Rf_isNull(row)) {
        R_xlen_t rlen = Rf_xlength(row);
        for (R_xlen_t j = 0; j < rlen; j++) {
          SEXP elt = STRING_ELT(row, j);
          if (elt != NA_STRING) SET_STRING_ELT(flat, k++, elt);
        }
      }
    }
  }

  // Create dict + flat indices from the flat array
  SEXP flat_idx = PROTECT(Rf_allocVector(INTSXP, total_leaves));
  SEXP flat_dict = PROTECT(Rf_allocVector(INTSXP, total_leaves));
  int *iflat_idx = INTEGER(flat_idx);
  int *iflat_dict = INTEGER(flat_dict);
  uint64_t ndictlen = 0;
  int imin = 0, imax = 0;
  double dmin = 0, dmax = 0;
  SEXP smin = R_NilValue, smax = R_NilValue;
  bool hasminmax2 = false;
  if (elt_type == INTSXP) {
    ndictlen = create_dict_idx<int>(
      INTEGER(flat), iflat_dict, iflat_idx, total_leaves,
      NA_INTEGER, imin, imax, hasminmax2
    );
  } else if (elt_type == REALSXP) {
    ndictlen = create_dict_real_idx(
      REAL(flat), iflat_dict, iflat_idx, total_leaves,
      dmin, dmax, hasminmax2
    );
  } else if (elt_type == STRSXP) {
    ndictlen = create_dict_str_idx(
      STRING_PTR_RO(flat), iflat_dict, iflat_idx, total_leaves,
      NA_STRING, smin, smax, hasminmax2
    );
  }

  // Build actual unique values vector (not positions into the original array)
  SEXP unique_vals;
  if (elt_type == INTSXP || elt_type == NILSXP) {
    unique_vals = PROTECT(Rf_allocVector(INTSXP, ndictlen));
    int *iunique = INTEGER(unique_vals);
    int *iflat_ptr = INTEGER(flat);
    for (uint64_t i = 0; i < ndictlen; i++) iunique[i] = iflat_ptr[iflat_dict[i]];
  } else if (elt_type == REALSXP) {
    unique_vals = PROTECT(Rf_allocVector(REALSXP, ndictlen));
    double *dunique = REAL(unique_vals);
    double *dflat_ptr = REAL(flat);
    for (uint64_t i = 0; i < ndictlen; i++) dunique[i] = dflat_ptr[iflat_dict[i]];
  } else {  // STRSXP
    unique_vals = PROTECT(Rf_allocVector(STRSXP, ndictlen));
    for (uint64_t i = 0; i < ndictlen; i++) {
      SET_STRING_ELT(unique_vals, i, STRING_ELT(flat, iflat_dict[i]));
    }
  }

  SEXP res = PROTECT(Rf_allocVector(VECSXP, 3));
  SET_VECTOR_ELT(res, 0, unique_vals);
  SET_VECTOR_ELT(res, 1, flat_idx);
  SET_VECTOR_ELT(res, 2, offsets);

  UNPROTECT(6); // offsets, flat, flat_idx, flat_dict, unique_vals, res
  return res;
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

  if (TYPEOF(x) == VECSXP) {
    return nanoparquet_create_dict_idx_vecsxp_(x, cfrom, cuntil);
  }

  SEXP idx = PROTECT(Rf_allocVector(INTSXP, len));
  SEXP dict = PROTECT(Rf_allocVector(INTSXP, len));
  int *idict = INTEGER(dict);
  int *iidx = INTEGER(idx);
  int imin, imax;
  double dmin, dmax;
  SEXP smin = R_NilValue, smax = R_NilValue;
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
      dictlen = create_dict_str_idx(
        STRING_PTR_RO(x) + cfrom, idict, iidx, len, NA_STRING,
        smin, smax, hasminmax
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
    } else if (TYPEOF(x) == STRSXP) {
      SET_VECTOR_ELT(res, 2, smin);
      SET_VECTOR_ELT(res, 3, smax);
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
