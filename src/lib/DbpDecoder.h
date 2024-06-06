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
    values_per_block = uleb_decode<uint32_t>(buf);
    mini_blocks_per_block = uleb_decode<uint32_t>(buf);
    total_value_count = uleb_decode<uint32_t>(buf);
    first_value = zigzag_decode<T, Tunsigned>(uleb_decode<Tunsigned>(buf));
    values_per_mini_block = values_per_block / mini_blocks_per_block;

    // some sanity checks, ideally these should caught and re-thrown
    // upstream with more information, e.g. file name and column name
    if (values_per_block == 0) {
      throw runtime_error(
        "zero values per block is not allowed in "
        "DELTA_BINARY_PACKED column"
      );
    }
    if (values_per_block % 128 != 0) {
      throw runtime_error(
        "the number of values in a block must be multiple of 128, but it's " +
        std::to_string(values_per_block)
      );
    }
    if (mini_blocks_per_block == 0) {
      throw runtime_error(
        "zero miniblocks per block is not allowsd in "
        "DELTA_BIANRY_PACKED column"
      );
    }
    if (values_per_mini_block % 32 != 0) {
      throw runtime_error(
        "the number of values in a miniblock must be multiple of 32, but it's " +
        std::to_string(values_per_mini_block)
      );
    }

  }

  inline uint32_t size() {
    return total_value_count;
  }

  inline void decode(T *values) {
    if (total_value_count == 0) return;
    values[0] = first_value;
    values++;
    uint64_t todo = total_value_count - 1;
    while (todo > 0) {
      // start of a block
      T min_delta = zigzag_decode<T, Tunsigned>(uleb_decode<Tunsigned>(buf));
      if (buf->len < mini_blocks_per_block) {
        throw runtime_error("End of buffer while DBP decoding");
      }
      vector<uint8_t> bit_widths(mini_blocks_per_block);
      memcpy(bit_widths.data(), buf->start, mini_blocks_per_block);
      buf->start += mini_blocks_per_block; buf->len -= mini_blocks_per_block;
      for (auto i = 0; todo > 0 && i < mini_blocks_per_block; i++) {
        // start of a miniblock
        int8_t bw = bit_widths[i];
        uint64_t mb_vals = values_per_mini_block > todo ? todo : values_per_mini_block;
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
  uint32_t values_per_block;
  uint32_t mini_blocks_per_block;
  uint32_t total_value_count;
  uint32_t values_per_mini_block;
  T first_value;
};
