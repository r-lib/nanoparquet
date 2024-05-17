
using namespace std;

// adapted from arrow parquet reader
class RleBpDecoder {
public:
  /// Create a decoder object. buffer/buffer_len is the decoded data.
  /// bit_width is the width of each value (before encoding).
  RleBpDecoder(const uint8_t *buffer, uint32_t buffer_len,
               uint32_t bit_width)
      : buffer(buffer), bit_width_(bit_width), current_value_(0),
        repeat_count_(0), literal_count_(0) {

    if (bit_width >= 64) {
      throw runtime_error("Decode bit width too large");
    }
    byte_encoded_len = ((bit_width_ + 7) / 8);
    max_val = (1 << bit_width_) - 1;
  }

  /// Gets a batch of values.  Returns the number of decoded elements.
  template <typename T> inline int GetBatch(T *values, int batch_size) {
    uint32_t values_read = 0;

    while (values_read < batch_size) {
      if (repeat_count_ > 0) {
        int repeat_batch = std::min(batch_size - values_read,
                                    static_cast<uint32_t>(repeat_count_));
        std::fill(values + values_read, values + values_read + repeat_batch,
                  static_cast<T>(current_value_));
        repeat_count_ -= repeat_batch;
        values_read += repeat_batch;
      } else if (literal_count_ > 0) {
        uint32_t literal_batch = std::min(
            batch_size - values_read, static_cast<uint32_t>(literal_count_));
        uint32_t actual_read =
            BitUnpack<T>(values + values_read, literal_batch);
        if (literal_batch != actual_read) {
          throw runtime_error("Did not find enough values");
        }
        literal_count_ -= literal_batch;
        values_read += literal_batch;
      } else {
        if (!NextCounts<T>())
          return values_read;
      }
    }
    return values_read;
  }

  template <typename T>
  inline int GetBatchSpaced(uint32_t batch_size, uint32_t null_count,
                            const uint8_t *defined, T *out) {
    //  DCHECK_GE(bit_width_, 0);
    uint32_t values_read = 0;
    uint32_t remaining_nulls = null_count;

    uint32_t d_off = 0; // defined_offset

    while (values_read < batch_size) {
      bool is_valid = defined[d_off++];

      if (is_valid) {
        if ((repeat_count_ == 0) && (literal_count_ == 0)) {
          if (!NextCounts<T>())
            return values_read;
        }
        if (repeat_count_ > 0) {
          // The current index is already valid, we don't need to check that
          // again
          uint32_t repeat_batch = 1;
          repeat_count_--;

          while (repeat_count_ > 0 &&
                 (values_read + repeat_batch) < batch_size) {
            if (defined[d_off]) {
              repeat_count_--;
            } else {
              remaining_nulls--;
            }
            repeat_batch++;

            d_off++;
          }
          std::fill(out, out + repeat_batch, static_cast<T>(current_value_));
          out += repeat_batch;
          values_read += repeat_batch;
        } else if (literal_count_ > 0) {
          int literal_batch =
              std::min(batch_size - values_read - remaining_nulls,
                       static_cast<uint32_t>(literal_count_));

          // Decode the literals
          constexpr int kBufferSize = 1024;
          T indices[kBufferSize];
          literal_batch = std::min(literal_batch, kBufferSize);
          auto actual_read = BitUnpack<T>(indices, literal_batch);

          if (actual_read != literal_batch) {
            throw runtime_error("Did not find enough values");
          }

          uint32_t skipped = 0;
          uint32_t literals_read = 1;
          *out++ = indices[0];

          // Read the first bitset to the end
          while (literals_read < literal_batch) {
            if (defined[d_off]) {
              *out = indices[literals_read];
              literals_read++;
            } else {
              skipped++;
            }
            ++out;
            d_off++;
          }
          literal_count_ -= literal_batch;
          values_read += literal_batch + skipped;
          remaining_nulls -= skipped;
        }
      } else {
        ++out;
        values_read++;
        remaining_nulls--;
      }
    }

    return values_read;
  }

private:
  const uint8_t *buffer;

  ByteBuffer unpack_buf;

  /// Number of bits needed to encode the value. Must be between 0 and 64.
  int bit_width_;
  uint64_t current_value_;
  uint32_t repeat_count_;
  uint32_t literal_count_;
  uint8_t byte_encoded_len;
  uint32_t max_val;

  // this is slow but whatever, calls are rare
  static uint8_t VarintDecode(const uint8_t *source,
                              uint32_t *result_out) {
    uint32_t result = 0;
    uint8_t shift = 0;
    uint8_t len = 0;
    while (true) {
      auto byte = *source++;
      len++;
      result |= (byte & 127) << shift;
      if ((byte & 128) == 0)
        break;
      shift += 7;
      if (shift > 32) {
        throw runtime_error("Varint-decoding found too large number");
      }
    }
    *result_out = result;
    return len;
  }

  /// Fills literal_count_ and repeat_count_ with next values. Returns false if
  /// there are no more.
  template <typename T> bool NextCounts() {
    // Read the next run's indicator int, it could be a literal or repeated run.
    // The int is encoded as a vlq-encoded value.
    uint32_t indicator_value;

    // TODO check in varint decode if we have enough buffer left
    buffer += VarintDecode(buffer, &indicator_value);

    // TODO check a bunch of lengths here against the standard

    // lsb indicates if it is a literal run or repeated run
    bool is_literal = indicator_value & 1;
    if (is_literal) {
      literal_count_ = (indicator_value >> 1) * 8;
    } else {
      repeat_count_ = indicator_value >> 1;
      // (ARROW-4018) this is not big-endian compatible, lol
      current_value_ = 0;
      for (auto i = 0; i < byte_encoded_len; i++) {
        current_value_ |= ((uint8_t)*buffer++) << (i * 8);
      }
      // sanity check
      if (current_value_ > max_val) {
        throw runtime_error(
            "Payload value bigger than allowed. Corrupted file?");
      }
    }
    // TODO complain if we run out of buffer
    return true;
  }

  static const uint32_t BITPACK_MASKS[];
  static const uint8_t BITPACK_DLEN;

  template <typename T>
  uint32_t BitUnpack(T *dest, uint32_t count) {
    assert(bit_width_ < 32);

    int8_t bitpack_pos = 0;
    auto source = buffer;
    auto mask = BITPACK_MASKS[bit_width_];

    for (uint32_t i = 0; i < count; i++) {
      T val = (*source >> bitpack_pos) & mask;
      bitpack_pos += bit_width_;
      while (bitpack_pos > BITPACK_DLEN) {
        val |=
            (*++source << (BITPACK_DLEN - (bitpack_pos - bit_width_))) & mask;
        bitpack_pos -= BITPACK_DLEN;
      }
      dest[i] = val;
    }

    buffer += bit_width_ * count / 8;
    return count;
  }
};

const uint32_t RleBpDecoder::BITPACK_MASKS[] = {
    0,         1,         3,          7,         15,       31,       63,
    127,       255,       511,        1023,      2047,     4095,     8191,
    16383,     32767,     65535,      131071,    262143,   524287,   1048575,
    2097151,   4194303,   8388607,    16777215,  33554431, 67108863, 134217727,
    268435455, 536870911, 1073741823, 2147483647};

const uint8_t RleBpDecoder::BITPACK_DLEN = 8;
