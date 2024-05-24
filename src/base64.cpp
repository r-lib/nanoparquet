#include <cstdint>
#include <cstring>

#include "base64.h"
#include "base64_tables.h"
#include "lib/endianness.h"

namespace base64 {

enum endianness {
        LITTLE = 0,
        BIG = 1
};

bool match_system(endianness e) {
#if IS_BIG_ENDIAN
    return e == endianness::BIG;
#else
    return e == endianness::LITTLE;
#endif
}

bool is_ascii_white_space(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
}

inline uint32_t swap_bytes(const uint32_t word) {
  return ((word >> 24) & 0xff) |      // move byte 3 to byte 0
         ((word << 8) & 0xff0000) |   // move byte 1 to byte 2
         ((word >> 8) & 0xff00) |     // move byte 2 to byte 1
         ((word << 24) & 0xff000000); // byte 0 to byte 3
}

result base64_tail_decode(char *dst, const char *src, size_t length) {
  // This looks like 5 branches, but we expect the compiler to resolve this to a single branch:
  const uint8_t *to_base64 = base64::to_base64_value;
  const uint32_t *d0 = base64::d0;
  const uint32_t *d1 = base64::d1;
  const uint32_t *d2 = base64::d2;
  const uint32_t *d3 = base64::d3;

  const char*srcend = src + length;
  const char *srcinit = src;
  const char *dstinit = dst;

  uint32_t x;
  size_t idx;
  uint8_t buffer[4];
  while (true) {
    while (src + 4 <= srcend &&
           (x = d0[uint8_t(src[0])] | d1[uint8_t(src[1])] |
                d2[uint8_t(src[2])] | d3[uint8_t(src[3])]) < 0x01FFFFFF) {
      if(match_system(endianness::BIG)) {
        x = swap_bytes(x);
      }
      std::memcpy(dst, &x, 3); // optimization opportunity: copy 4 bytes
      dst += 3;
      src += 4;
    }
    idx = 0;
    // we need at least four characters.
    while (idx < 4 && src < srcend) {
      char c = *src;
      uint8_t code = to_base64[uint8_t(c)];
      buffer[idx] = uint8_t(code);
      if (code <= 63) {
        idx++;
      } else if (code > 64) {
        return {INVALID_BASE64_CHARACTER, size_t(src - srcinit)};
      } else {
        // We have a space or a newline. We ignore it.
      }
      src++;
    }
    if (idx != 4) {
      if (idx == 2) {
        uint32_t triple =
            (uint32_t(buffer[0]) << 3 * 6) + (uint32_t(buffer[1]) << 2 * 6);
        if(match_system(endianness::BIG)) {
          triple <<= 8;
          std::memcpy(dst, &triple, 1);
        } else {
          triple = swap_bytes(triple);
          triple >>= 8;
          std::memcpy(dst, &triple, 1);
        }
        dst += 1;

      } else if (idx == 3) {
        uint32_t triple = (uint32_t(buffer[0]) << 3 * 6) +
                          (uint32_t(buffer[1]) << 2 * 6) +
                          (uint32_t(buffer[2]) << 1 * 6);
        if(match_system(endianness::BIG)) {
          triple <<= 8;
          std::memcpy(dst, &triple, 2);
        } else {
          triple = swap_bytes(triple);
          triple >>= 8;
          std::memcpy(dst, &triple, 2);
        }
        dst += 2;
      } else if (idx == 1) {
        return {BASE64_INPUT_REMAINDER, size_t(dst - dstinit)};
      }
      return {SUCCESS, size_t(dst - dstinit)};
    }

    uint32_t triple =
        (uint32_t(buffer[0]) << 3 * 6) + (uint32_t(buffer[1]) << 2 * 6) +
        (uint32_t(buffer[2]) << 1 * 6) + (uint32_t(buffer[3]) << 0 * 6);
    if(match_system(endianness::BIG)) {
      triple <<= 8;
      std::memcpy(dst, &triple, 3);
    } else {
      triple = swap_bytes(triple);
      triple >>= 8;
      std::memcpy(dst, &triple, 3);
    }
    dst += 3;
  }
}

// Returns the number of bytes written. The destination buffer must be large
// enough. It will add padding (=) if needed.
size_t tail_encode_base64(char *dst, const char *src, size_t srclen) {
  const char *e0 = base64::e0;
  const char *e1 = base64::e1;
  const char *e2 = base64::e2;
  char *out = dst;
  size_t i = 0;
  uint8_t t1, t2, t3;
  for (; i + 2 < srclen; i += 3) {
    t1 = uint8_t(src[i]);
    t2 = uint8_t(src[i + 1]);
    t3 = uint8_t(src[i + 2]);
    *out++ = e0[t1];
    *out++ = e1[((t1 & 0x03) << 4) | ((t2 >> 4) & 0x0F)];
    *out++ = e1[((t2 & 0x0F) << 2) | ((t3 >> 6) & 0x03)];
    *out++ = e2[t3];
  }
  switch (srclen - i) {
  case 0:
    break;
  case 1:
    t1 = uint8_t(src[i]);
    *out++ = e0[t1];
    *out++ = e1[(t1 & 0x03) << 4];
    *out++ = '=';
    *out++ = '=';
    break;
  default: /* case 2 */
    t1 = uint8_t(src[i]);
    t2 = uint8_t(src[i + 1]);
    *out++ = e0[t1];
    *out++ = e1[((t1 & 0x03) << 4) | ((t2 >> 4) & 0x0F)];
    *out++ = e2[(t2 & 0x0F) << 2];
    *out++ = '=';
  }
  return (size_t)(out - dst);
}


size_t base64_length_from_binary(size_t length) {
  // We use padding to make the length a multiple of 4.
  return (length + 2)/3 * 4;
}

size_t maximal_binary_length_from_base64(const char * input, size_t length) {
  // We follow https://infra.spec.whatwg.org/#forgiving-base64-decode
  size_t padding = 0;
  if (length > 0) {
    if (input[length - 1] == '=') {
      padding++;
      if (length > 1 && input[length - 2] == '=') {
        padding++;
      }
    }
  }
  size_t actual_length = length - padding;
  if (actual_length % 4 <= 1) {
    return actual_length / 4 * 3;
  }
  // if we have a valid input, then the remainder must be 2 or 3 adding one or two extra bytes.
  return  actual_length / 4 * 3 + (actual_length %4)  - 1;
}

result base64_to_binary(const char * input, size_t length, char* output) {
  while(length > 0 && is_ascii_white_space(input[length - 1])) {
    length--;
  }
  size_t equallocation = length; // location of the first padding character if any
  size_t equalsigns = 0;
  if(length > 0 && input[length - 1] == '=') {
    length -= 1;
    equalsigns++;
    while(length > 0 && is_ascii_white_space(input[length - 1])) {
      length--;
    }
    if(length > 0 && input[length - 1] == '=') {
      equalsigns++;
      length -= 1;
    }
  }
  if(length == 0) {
    if(equalsigns > 0) {
      return {INVALID_BASE64_CHARACTER, equallocation};
    }
    return {SUCCESS, 0};
  }
  result r = base64_tail_decode(output, input, length);
  if(r.error == error_code::SUCCESS && equalsigns > 0) {
    // additional checks
    if((r.count % 3 == 0) || ((r.count % 3) + 1 + equalsigns != 4)) {
      return {INVALID_BASE64_CHARACTER, equallocation};
    }
  }
  return r;
}

size_t binary_to_base64(const char * input, size_t length, char* output) {
  return tail_encode_base64(output, input, length);
}

}
