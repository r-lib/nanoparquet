#pragma once

#include <cstdint>

#include "nanoparquet.h"
#include "decode-utils.h"

using namespace std;

template <typename T, typename Tunsigned>
class DbpDecoder {
public:
  DbpDecoder(buffer *buf) : buf(buf) {
    start = buf->start;
    block_size = uleb_decode<uint32_t>(buf);
    num_mb_pb = uleb_decode<uint32_t>(buf);
    num_values = uleb_decode<uint32_t>(buf);
    first_value = zigzag_decode<T, Tunsigned>(uleb_decode<Tunsigned>(buf));
    mb_size = block_size / num_mb_pb;
    // TODO: sanity check for values;
  }

  inline uint32_t size() {
    return num_values;
  }

  inline void decode(T *values) {
    if (num_values == 0) return;
    values[0] = first_value;
    values++;
    uint64_t todo = num_values - 1;
    while (todo > 0) {
      // start of a block
      T min_delta = zigzag_decode<T, Tunsigned>(uleb_decode<Tunsigned>(buf));
      if (buf->len < num_mb_pb) {
        throw runtime_error("End of buffer while DBP decoding");
      }
      vector<uint8_t> bit_widths(num_mb_pb);
      memcpy(bit_widths.data(), buf->start, num_mb_pb);
      buf->start += num_mb_pb; buf->len -= num_mb_pb;
      for (auto i = 0; todo > 0 && i < num_mb_pb; i++) {
        // start of a miniblock
        int8_t bw = bit_widths[i];
        uint64_t mb_vals = mb_size > todo ? todo : mb_size;
        uint64_t mb_len = bw * mb_vals / 8 + ((bw * mb_vals) % 8 > 0);
        if (buf->len < mb_len) {
          throw runtime_error("End of buffer while DBP decoding");
        }
        unpack_bits<Tunsigned>(
          buf->start,
          mb_len,
          bw,
          (Tunsigned*) values,
          mb_vals
        );
        for (auto i = 0; i < mb_vals; i++) {
          *values = *(values-1) + *values + min_delta;
          values++;
        }
        buf->start += mb_len; buf->len -= mb_len;
        todo -= mb_vals;
      }
    }
  }

private:
  struct buffer *buf;
  uint8_t *start;
  uint32_t block_size;
  uint32_t num_mb_pb;
  uint32_t num_values;
  uint32_t mb_size;
  T first_value;
};
