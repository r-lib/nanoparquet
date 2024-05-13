# base64

    Code
      base64_encode(1:10)
    Condition
      Error in `base64_encode()`:
      ! Invalid input in base64 encoder

---

    Code
      base64_decode(1:10)
    Condition
      Error in `base64_decode()`:
      ! Invalid input in base64 decoder

---

    Code
      base64_decode("foobar;;;")
    Condition
      Error in `base64_decode()`:
      ! Base64 decoding error at position 6

