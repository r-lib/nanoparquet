#pragma once

#include <mutex>
#include <condition_variable>
#include <future>
#include <vector>
#include <thread>

#include "endianness.h"
#if IS_BIG_ENDIAN
#error Nanoparquet does not support big-endian platforms: https://github.com/r-lib/nanoparquet/issues/21
#endif

#include "bytebuffer.h"
#include "ParquetOutFile.h"

class semaphore {
  std::mutex m;
  std::condition_variable cv;
  int count;

public:
  semaphore(int n) : count{n} {}
  void notify() {
    std::unique_lock<std::mutex> l(m);
    ++count;
    cv.notify_one();
  }
  void wait() {
    std::unique_lock<std::mutex> l(m);
    cv.wait(l, [this]{ return count!=0; });
    --count;
  }
};

class critical_section {
  semaphore &s;
public:
  critical_section(semaphore &ss) : s{ss} { s.wait(); }
  ~critical_section() { s.notify(); }
};
