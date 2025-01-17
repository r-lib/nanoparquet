#include <cstdint>

#include "bitpacker.h"

static uint32_t MaxRleBpSizeSimple(uint32_t input_len, uint8_t bit_width) {
  // If we bit-pack everything in chunks of eight, then
  // we'll get this length
  // we might end up adding 7 padding bytes at the end
  input_len += 7;
  // round up to eight
  uint32_t efflen = input_len - input_len % 8 + 8;
  uint32_t bplen = (bit_width + 1) * efflen / 8; // +1 is length marker
  return bplen;
}

template<typename T>
uint32_t MaxRleBpSize(const T *input, uint32_t input_len,
                      uint8_t bit_width) {
  return MaxRleBpSizeSimple(input_len, bit_width);
}

template<typename T>
uint32_t RleBpEncode(const T *input, uint32_t input_len,
                     uint8_t bit_width, uint8_t *output,
                     uint32_t output_len) {
  uint32_t iidx = 0;

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

  int min_reps;
  switch(bit_width) {
    case 0:
    case 1: min_reps = 8; break;
    case 2: min_reps = 6; break;
    case 3:
    case 4: min_reps = 4; break;
    default: min_reps = 3; break;
  };

  if (input_len == 0) return 0;

  BitPacker bp(output, bit_width);
  while (iidx < input_len) {
    // search for a repeated value that starts at an 8-block, and
    // is repeated at least min_reps times;
    uint32_t rleidx = iidx;
    while (rleidx < input_len) {
      uint32_t sidx = rleidx;
      uint32_t reps = 1;
      T val = input[sidx++];
      while (sidx < input_len && input[sidx++] == val) {
        reps++;
      }
      if (reps >= min_reps) {
        // we have a repeat from rleidx. until then it is BP, then RLE
        if (rleidx > iidx) {
          // std::cerr << "@ " << iidx << " bp x " << rleidx - iidx << std::endl;
          bp.pack_varint(((rleidx - iidx) / 8) << 1 | 1);
          for (; iidx < rleidx; iidx++) {
            bp.pack(input[iidx]);
          }
          bp.flush();
        }
        // std::cerr << "@ " << rleidx << " rep " << val << " x " << reps << std::endl;
        bp.pack_varint(reps << 1);
        bp.pack_value(val);
        iidx += reps;
        break;
      } else {
        rleidx += 8;
      }
    }
    if (rleidx >= input_len) {
      // if we didn't find ant RLE, then BP until the end
      // we need to pad it to a multiple of eight
      // (This is not actually mentioned in the specs, but it is
      // what arrow does.)
      // std::cerr << "@ " << iidx << " bp x " << input_len - iidx << std::endl;
      uint32_t num = input_len - iidx;
      // we need pad values to get to a multiple of 8
      int pad = (8 - num % 8) % 8;
      bp.pack_varint(((num + pad) / 8) << 1 | 1);
      while (iidx < input_len) {
        bp.pack(input[iidx++]);
      }
      for (; pad > 0; pad--) {
        bp.pack(0);
      }
      bp.flush();
    }
  }

  return bp.size();
}
