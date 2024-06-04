#include <Rdefines.h>
#include "lib/memstream.h"

extern "C" {

SEXP test_memstream() {
  MemStream ms(10);
  std::ostream &os = ms.stream();

  os << "This is a test" << "\n";
  std::cerr << ms.size() << std::endl;

  os << "This is a test" << "\n";
  std::cerr << ms.size() << std::endl;

  os << "This is a test" << "\n";
  std::cerr << ms.size() << std::endl;

  os << "This is a test" << "\n";
  std::cerr << ms.size() << std::endl;

  os << "This is a test" << "\n";
  std::cerr << ms.size() << std::endl;

  SEXP res = Rf_allocVector(RAWSXP, ms.size());
  ms.copy(RAW(res), ms.size());

  return res;
}

} // extern "C"
