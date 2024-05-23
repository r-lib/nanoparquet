#include <cstddef>
#include <iostream>
#include <memory>
#include <cstring>

class ByteBuffer : public std::streambuf {
public:
  char *ptr = nullptr;
  uint64_t len = 0;

  ByteBuffer() {
    setp(0, 0);
  }

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
      setp(ptr, ptr + new_size);
    }
  }

  std::streamsize xsputn(const char* s, std::streamsize n) override {
    if (sptr == nullptr) {
      throw std::runtime_error("Cannot write to uninitialized byte buffer"); // # nocov
    }
    uint64_t space = len - (sptr - ptr);
    if (space < n) {
      memcpy(sptr, s, space);                  // # nocov
      sptr += space;                           // # nocov
      pbump(space);
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

  void reset(uint64_t new_size = 0, bool copy = false) {
    if (new_size > 0) {
      resize(new_size, copy);
    }
    sptr = ptr;
    setp(ptr, ptr + new_size);
  }

  void skip(uint64_t bytes) {
    uint64_t space = len - (sptr - ptr);
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
  std::unique_ptr<char[]> holder = nullptr;
  char *sptr = nullptr;
};
