#include <cstdint>
#include <cstring>
#include <cmath>

class BitPacker {
public:
  BitPacker(uint8_t *buffer, uint8_t bit_width)
  : start_(buffer), buffer_(buffer), bit_width_(bit_width), tmp(0),
    bit_offset(0) {
    value_bytes = ceil(bit_width / 8.0);
  }

  inline void pack_varint(uint64_t v) {
    check_zero_offset();
    while (v >= 128) {
      uint8_t c = v | 0x80;
      *buffer_++ = c;
      v >>= 7;
    }
    *buffer_++ = v;
  }

  // value for repetition, using bit_width bits rounded up to bytes
  inline void pack_value(uint64_t v) {
    check_zero_offset();
    for (auto i = 0; i < value_bytes; i++) {
      *buffer_++ = v & 0xff;
      v >>= 8;
    }
  }

  inline void pack(uint64_t v) {
    tmp |= v << bit_offset;
    bit_offset += bit_width_;
    if (bit_offset >= 64) {
      std::memcpy(buffer_, &tmp, 8);
      buffer_ += 8;
      bit_offset -= 64;
      tmp = bit_offset == 0 ? 0 : v >> (bit_width_ - bit_offset);
    }
  }

  inline void flush() {
    if (bit_offset % 8 != 0) {
      throw std::runtime_error(                               // # nocov
        "Internal bit packer error, flushing partial bytes"
      );
    }
    std::memcpy(buffer_, &tmp, bit_offset / 8);
    buffer_ += bit_offset / 8;
    bit_offset = 0;
    tmp = 0;
  }

  inline uint32_t size() const {
    return buffer_ - start_;
  }

  inline void check_zero_offset() {
    if (bit_offset != 0) {
      throw std::runtime_error(                               // # nocov
        "Internal bit packer error, raw value with packed data"
      );
    }
  }

  uint8_t *start_;
  uint8_t *buffer_;
  uint8_t bit_width_;
  uint8_t value_bytes;
  uint64_t tmp;
  int bit_offset;
};
