// I should merge this with ByteBuffer at some point
#include <cstddef>
#include <iostream>
#include <memory>
#include <cstring>
#include <vector>

// A growing buffer that can be used as an output stream.
// It stores that data in an array of buffers, each bigger than
// the previous one.

class MemStream : public std::streambuf {
  public:
    MemStream(uint64_t size = 1024 * 1024, double factor = 1.5)
      : size1(size), factor_(factor), sbuf(nullptr), sptr(0) {
      bufs.resize(num_bufs);
      std::fill(bufs.begin(), bufs.end(), nullptr);
      sizes.resize(num_bufs);
      sizes[0] = size1;
      setp(0, 0);
    }

    // no copies please!
    MemStream & operator=(const MemStream&) = delete;
    MemStream(const MemStream&) = delete;

    std::streamsize xsputn(const char* s, std::streamsize n) override {
      if (n == 0) return n;
      // the very first allocation
      if (sbuf == nullptr) {
        bufs[bufptr] = std::unique_ptr<char[]>(new char[sizes[bufptr]]);
        sbuf = bufs[bufptr].get();
        sptr = 0;
        setp(sbuf, sbuf + sizes[bufptr]);
      }
      uint64_t space = sizes[bufptr] - sptr;
      if (n > space) {
        // ovreflow, allocate new buffer
        uint64_t of = n - space;
        memcpy(sbuf + sptr, s, space);
        bufptr++;
        sizes[bufptr] = sizes[bufptr - 1] * factor_;
        if (sizes[bufptr] < of) {
          sizes[bufptr] = of;
        }
        bufs[bufptr] = std::unique_ptr<char[]>(new char[sizes[bufptr]]);
        sbuf = bufs[bufptr].get();
        memcpy(sbuf, s + space, of);
        sptr = of;
        setp(sbuf, sbuf + sizes[bufptr]);
        pbump(of);
       } else {
        memcpy(sbuf + sptr, s, n);
        sptr += n;
        pbump(n);
      }
      return n;
    }

    int overflow(int ch) override {                          // # nocov
      const char cch = (const char) ch;                      // # nocov
      return (int) xsputn(&cch, 1);                          // # nocov
    }                                                        // # nocov

    std::ostream& stream() {
      if (stream_ == nullptr) {
        stream_ = std::unique_ptr<std::ostream>(new std::ostream(this));
      }
      return *stream_;
    }

    uint64_t size() {
      uint64_t s = 0;
      for (int i = 0; i < bufptr; i++) {
        s += sizes[i];
      }
      s += sptr;
      return s;
    }

    uint64_t copy(uint8_t *dst, uint64_t len) {
      uint64_t done = 0;
      uint64_t todo = len;
      for (int i = 0; i < bufptr; i++) {
        if (todo <= sizes[i]) {
          memcpy(dst, bufs[i].get(), todo);
          done += todo;
          todo = 0;
          break;
        } else {
          memcpy(dst, bufs[i].get(), sizes[i]);
          done += sizes[i];
          todo -= sizes[i];
          dst += sizes[i];
        }
      }
      // plus the current buffer, if needed
      if (todo > 0) {
        if (todo <= sptr) {
          memcpy(dst, sbuf, todo);
          done += todo;
        } else {
          memcpy(dst, sbuf, sptr);
          done += sptr;
        }
      }
      return done;
    }

    // TODO: need seekoff() override for tellp() to work

  private:
    const int num_bufs = 50;
    uint64_t size1;
    double factor_;
    std::vector<std::unique_ptr<char[]>> bufs;
    std::vector<uint64_t> sizes;
    std::unique_ptr<std::ostream> stream_ = nullptr;

    int bufptr = 0;            // number of active buffer
    char *sbuf = nullptr;      // pointer to active buffer
    uint64_t sptr = 0;         // position inside the active buffer
};
