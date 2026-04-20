#ifndef UNWIND_H
#define UNWIND_H

#include <csetjmp>
#include <cerrno>
#include <iostream>
#include <stdexcept>

#include <R.h>
#include <Rinternals.h>
#undef TYPE_BITS

struct unwind_error {
  SEXP token;
};

#define CPP_INIT                                               \
  char errmsg_[4096];                                          \
  bool cpperr_ = false;                                        \
  bool rerr_ = false;                                          \
  SEXP err_ = R_NilValue;

#define CPP_BEGIN                                              \
  try {

#define CPP_END                                                \
  } catch(const unwind_error &err) {                           \
    rerr_ = true;                                              \
    err_ = err.token;                                          \
  } catch(const std::exception &ex) {                          \
    strncpy(errmsg_, ex.what(), sizeof(errmsg_) - 1);          \
    errmsg_[sizeof(errmsg_) - 1] = '\0';                       \
    cpperr_ = true;                                            \
  } catch(const std::string &ex) {                             \
    strncpy(errmsg_, ex.c_str(), sizeof(errmsg_) - 1);         \
    errmsg_[sizeof(errmsg_) - 1] = '\0';                       \
    cpperr_ = true;                                            \
  } catch(...) {                                               \
    snprintf(errmsg_, sizeof(errmsg_) - 1, "unwind error");    \
    errmsg_[sizeof(errmsg_) - 1] = '\0';                       \
    cpperr_ = true;                                            \
  }                                                            \
  if (rerr_) {                                                 \
    R_ContinueUnwind(err_);                                    \
  } else if (cpperr_) {                                        \
    Rf_errorcall(nanoparquet_call, "%s", errmsg_);             \
  }

template <typename F>
SEXP r_eval(F fun) {
  SEXP token = PROTECT(R_MakeUnwindCont());
  std::jmp_buf jmpbuf;
  if (setjmp(jmpbuf)) {
    struct unwind_error rerr = { token };
    throw rerr;
  }

  SEXP result = R_UnwindProtect(
    [](void* data) -> SEXP {
      auto callback = static_cast<decltype(&fun)>(data);
      return static_cast<F&&>(*callback)();
    },
    &fun,
    [](void* jmpbuf, Rboolean jump) {
      if (jump == TRUE) {
        // We need to first jump back into the C++ stacks because you
        // can't safely throw exceptions from C stack frames.
        longjmp(*static_cast<std::jmp_buf*>(jmpbuf), 1);
      }
    },
    &jmpbuf,
    token
  );

  UNPROTECT(1);
  return result;
}

template <typename F>
void r_call(F fun) {
  SEXP token = PROTECT(R_MakeUnwindCont());
  std::jmp_buf jmpbuf;
  if (setjmp(jmpbuf)) {
    struct unwind_error rerr = { token };
    throw rerr;
  }

  SEXP result = R_UnwindProtect(
    [](void* data) -> SEXP {
      auto callback = static_cast<decltype(&fun)>(data);
      static_cast<F&&>(*callback)();
      return R_NilValue;
    },
    &fun,
    [](void* jmpbuf, Rboolean jump) {
      if (jump == TRUE) {
        // We need to first jump back into the C++ stacks because you
        // can't safely throw exceptions from C stack frames.
        longjmp(*static_cast<std::jmp_buf*>(jmpbuf), 1);
      }
    },
    &jmpbuf,
    token
  );

  (void) result;
  UNPROTECT(1);
  return;
}

#endif // UNWIND_H
