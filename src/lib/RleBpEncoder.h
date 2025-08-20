#include <cstdint>
#include <cmath>

#include "fastpforlib/bitpackinghelpers.h"

class RleBpEncoder {

public:
  explicit RleBpEncoder(uint32_t bit_width)
      : bit_width(bit_width), output_(nullptr) {

    // To work out the minimum lengths, consider an output with minimal
    // length RLE encodings only, and compare that to the maximum size
    // prediction above.
    // E.g. for bit_width=2, and min_reps=6:
    // 6 values are encoded into 2 bytes, 24 values into 8, max est is 9.
    // min_reps=5 is not good:
    // 5 values are encoded into 2 bytes, 40 values into 16, max est is 15.
    // ...
    // We could actually use min_reps=2 for bit_width=7, 8, and >11
    // (but not for 9 and 10!). Maybe we'll do this later.
    switch (bit_width) {
    case 0:
    case 1:
      min_reps = 8;
      break;
    case 2:
      min_reps = 6;
      break;
    case 3:
    case 4:
      min_reps = 4;
      break;
    default:
      min_reps = 3;
      break;
    };
  }

  inline uint32_t MaxSize(uint32_t input_len) {
    // If we bit-pack everything in chunks of eight, then
    // we'll get this length
    // we might end up adding 7 padding bytes at the end
    input_len += 7;
    // round up to eight
    uint32_t efflen = input_len - input_len % 8 + 8;
    // +1 is length marker, + bit_width * 4 is for the bit packer
    uint32_t bplen = (bit_width + 1) * efflen / 8 + bit_width * 4;
    return bplen;
  }
  inline uint32_t Encode(const uint32_t *input, uint32_t input_len,
                         uint8_t *output, uint32_t output_len) {
    input_ = input;
    input_len_ = input_len;
    output_ = output;
    byte_width = ceil(bit_width / 8.0);

    uint32_t iidx = 0;
    while (iidx < input_len) {
      FindNextRuns(iidx, input_len_);
      if (bp_len > 0) {
        WriteBpRun(input_ + bp_start, bp_len);
        iidx += bp_len;
      }
      if (rle_len > 0) {
        WriteRleRun(input[rle_start], rle_len);
        iidx += rle_len;
      }
    }
    uint32_t len = output_ - output;
    if (len > output_len) {
      throw std::runtime_error("RLE/BP output buffer is not long enough");
    }
    return len;
  }

private:
  inline void FindNextRuns(uint32_t from, uint32_t until) {
    bp_start = rle_start = from;
    bp_len = rle_len = 0;
    while (rle_start < until) {
      // count repeated values from rle_Start
      uint32_t rle_end = rle_start;
      uint32_t rep_value = input_[rle_end++];
      rle_len = 1;
      while (rle_end < until && input_[rle_end++] == rep_value) {
        rle_len++;
      }
      if (rle_len >= min_reps) {
        // if we have enough, then we are done
        return;
      } else {
        // otherwise we need to bit pack 8 more values from rle_start
        // and continue looking for repeated values.
        // At the end we might have less than 8 values.
        bp_len += rle_start + 8 > until ? until - rle_start : 8;
        rle_start += 8;
        rle_len = 0;
      }
    }
    // didn't find a repeat, bit pack the rest
  }

  inline void WriteBpRun(const uint32_t* values, uint32_t count) {
    int pad = (8 - count % 8) % 8;
    WriteVarInt((((count + pad) / 8) << 1) | 1);
    while (count >= 32) {
      fastpforlib::fastpack(values, (uint32_t*) output_, bit_width);
      values += 32;
      count -= 32;
      output_ += bit_width * 4;
    }
    // And the rest, padded to a multiple of 8, as RLE/BP can only bit-pack
    // multiples of 8.
    if (count > 0) {
      memset(bpbuffer, 0, sizeof(uint32_t) * 32);
      memcpy(bpbuffer, values, sizeof(uint32_t) * count);
      fastpforlib::fastpack(bpbuffer, (uint32_t*) output_, bit_width);
      int packed_len = bit_width * (count + pad) / 8;
      output_ += packed_len;
    }
  }

  inline void WriteRleRun(uint32_t value, uint32_t len) {
    WriteVarInt(len << 1);
    WriteValue(value);
  }

  inline void WriteVarInt(uint32_t v) {
    while (v >= 128) {
      uint8_t c = v | 0x80;
      *output_++ = c;
      v >>= 7;
    }
    *output_++ = v;
  }

  // value for repetition, using bit_width bits rounded up to bytes
  inline void WriteValue(uint32_t v) {
    for (auto i = 0; i < byte_width; i++) {
      *output_++ = v & 0xff;
      // std::cerr << "pack value +1" << std::endl;
      v >>= 8;
    }
  }

private:
  uint32_t bit_width;
  uint32_t byte_width;
  int min_reps;
  const uint32_t *input_;
  uint32_t input_len_;
  uint8_t *output_;
  uint32_t bpbuffer[32];

  uint32_t bp_start;
  uint32_t bp_len;
  uint32_t rle_start;
  uint32_t rle_len;
};

inline int32_t MaxRleBpSize(const uint32_t *input, uint32_t input_len,
                     uint32_t bit_width) {
  RleBpEncoder encoder(bit_width);
  return encoder.MaxSize(input_len);
}

inline int32_t MaxRleBpSize(uint32_t input_len, uint32_t bit_width) {
  RleBpEncoder encoder(bit_width);
  return encoder.MaxSize(input_len);
}

inline uint32_t RleBpEncode(const uint32_t *input, uint32_t input_len,
                     uint32_t bit_width, uint8_t *output,
                     uint32_t output_len) {
  RleBpEncoder encoder(bit_width);
  return encoder.Encode(input, input_len, output, output_len);
}
