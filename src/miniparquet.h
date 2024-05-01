#pragma once

#include <cstddef>
#include <iostream>

class ByteBuffer : public std::streambuf {
public:
  char *ptr = nullptr;
  uint64_t len = 0;
  uint64_t tellp = 0;

  void resize(uint64_t new_size, bool copy = true) {
    if (new_size > len) {
      auto new_holder = std::unique_ptr<char[]>(new char[new_size]);
      if (copy && holder != nullptr) {
        memcpy(new_holder.get(), holder.get(), len);
      }
      holder = std::move(new_holder);
      ptr = holder.get();
      sptr = ptr;
      len = new_size;
    }
  }

  std::streamsize xsputn(const char* s, std::streamsize n) override {
    if (sptr == nullptr) {
      throw std::runtime_error("Cannot write to uninitialized byte buffer");
    }
    uint64_t space = len - (sptr - ptr);
    if (space < n) {
      memcpy(sptr, s, space);
      sptr += space;
      tellp += space;
      return space;
    } else {
      memcpy(sptr, s, n);
      sptr += n;
      tellp += n;
      return n;
    }
  };

  int overflow(int ch) override {
    return (int) xsputn((const char*) &ch, 1);
  }

  void reset() {
    sptr = ptr;
    tellp = 0;
  }

private:
  std::unique_ptr<char[]> holder = nullptr;
  char *sptr = nullptr;
};

#include "ParquetFile.h"
#include "ParquetOutFile.h"
