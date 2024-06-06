#pragma once

#include <cstdint>
#include <type_traits>

#include "fastpforlib/bitpackinghelpers.h"

struct buffer {
  uint8_t *start;
  uint32_t len;
};

template <typename T>
T uleb_decode(buffer *buf) {
  T result = 0;
  uint8_t shift = 0;
  while (true) {
    if (buf->len <= 0) {
      throw runtime_error("Buffer ended while varint decoding");
    }
    auto byte = *buf->start++; buf->len--;
    result |= (byte & 127) << shift;
    if ((byte & 128) == 0) break;
    shift += 7;
    if (shift > sizeof(T) * 8) {
      throw runtime_error("Varint decoding found too large number");
    }
  }

  return result;
}

template <typename T, typename Tunsigned>
T zigzag_decode(Tunsigned val) {
  // return val & 1 ? T(-(val >> 1)) : T(val >> 1);
  return T(val >> 1) ^ -T(val & 1);
}

template <typename T>
void unpack_bits(uint8_t *buf, uint64_t len, uint8_t bw, T *values,
                 uint64_t num_values) {
  if (len < bw * num_values / 8 + ((bw * num_values) % 8 > 0)) {
    throw runtime_error(
      "Buffer too short for unpacking specified number of values"
    );
  }

  if (bw == 0) {
    memset(values, 0, num_values * sizeof(T));
    return;
  }

  // we unpack output_group_size _values_ with one call, from
  // input_group_size _bytes_
  int output_group_size = sizeof(T) * 8;
  int input_group_size = output_group_size * bw / 8;
  uint32_t bw2 = bw;
  while (num_values > output_group_size) {
    fastpforlib::fastunpack((uint32_t*) buf, values, bw2);
    num_values -= output_group_size;
    buf += input_group_size;
    values += output_group_size;
  }

  // the leftover bytes must be unpacked from a dummy buffer, into a
  // dummy buffer, because out input and/or output buffer is not long
  // enough
  if (num_values > 0) {
    unique_ptr<uint8_t[]> ib(new uint8_t[input_group_size]);
    unique_ptr<T[]> ob(new T[output_group_size]);
    int left_bytes = num_values * bw / 8 + ((bw * num_values) % 8 > 0);
    memcpy(ib.get(), buf, left_bytes);
    fastpforlib::fastunpack((uint32_t*) ib.get(), ob.get(), bw2);
    memcpy(values, ob.get(), num_values * sizeof(T));
  }
}
