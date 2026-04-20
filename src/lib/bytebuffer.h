#pragma once
#include <cstddef>
#include <iostream>
#include <memory>
#include <cstring>
#include <cstdint>
#include <vector>

class ByteBuffer : public std::streambuf {
public:
  char *ptr = stat;
  std::streamsize len = 0;
  bool locked = false;

  ByteBuffer() {
    setp(stat, stat + sizeof(stat));
  }

  void resize(std::streamsize new_size, bool copy = true) {
    if (new_size > len) {
      auto new_holder = std::unique_ptr<char[]>(new char[new_size]);
      if (copy && holder != nullptr) {
        memcpy(new_holder.get(), holder.get(), len);
      }
      holder = std::move(new_holder);
      ptr = holder.get();
      sptr = ptr;
      len = new_size;
      setp(ptr, ptr + new_size);
    }
  }

  std::streamsize xsputn(const char* s, std::streamsize n) override {
    if (sptr == nullptr) {
      throw std::runtime_error("Cannot write to uninitialized byte buffer"); // # nocov
    }
    std::streamsize space = len - (sptr - ptr);
    if (space < n) {
      memcpy(sptr, s, space);                  // # nocov
      sptr += space;                           // # nocov
      pbump(space);                            // # nocov
      return space;                            // # nocov
    } else {
      memcpy(sptr, s, n);
      sptr += n;
      pbump(n);
      return n;
    }
  };

  int overflow(int ch) override {                          // # nocov
    return (int) xsputn((const char*) &ch, 1);             // # nocov
  }                                                        // # nocov

  void reset(std::streamsize new_size = 0, bool copy = false) {
    if (new_size > 0) {
      resize(new_size, copy);
    }
    sptr = ptr;
    setp(ptr, ptr + new_size);
  }

  void skip(std::streamsize bytes) {
    std::streamsize space = len - (sptr - ptr);
    if (space < bytes) {
      throw std::runtime_error("Cannot write past the end of the buffer");
    }
    sptr += bytes;
  }

  pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                   std::ios_base::openmode which) override {
    if (dir == std::ios_base::cur) {
      pbump(off);
    } else if (dir == std::ios_base::end) {
      setp(pbase(), epptr() + off);
    } else if (dir == std::ios_base::beg) {
      setp(pbase(), pbase() + off);
    }
    return pptr() - pbase();
  }

private:
  char stat[128];
  std::unique_ptr<char[]> holder = nullptr;
  char *sptr = stat;
};

class BufferGuard {
public:
  BufferGuard(ByteBuffer &buf) : buf(buf) {
    buf.locked = true;
  }
  ~BufferGuard() {
    buf.locked = false;
  }
  ByteBuffer &buf;
};

class BufferManager {
public:
  BufferManager(int n): bufs(n) { }
  ~BufferManager() { }
  BufferGuard claim() {
    // return a BufferGuard that is associated with a buffer
    for (auto i = 0; i < bufs.size(); i++) {
      if (!bufs[i].locked) {
        return BufferGuard(bufs[i]);
      }
    }
    throw std::runtime_error("Buffer manageer Ran out of buffers :()");
  }
  BufferGuard claim(std::streamsize size) {
    // same, but check if we have one with the right size (TODO)
    throw std::runtime_error("Not implemented yet");
  }

private:
  std::vector<ByteBuffer> bufs;
};
