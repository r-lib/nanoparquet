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

# temporal types

    Code
      as.data.frame(np[["columns"]])
    Output
        name type_type type nullable dictionary custom_metadata
      1    x      Date  DAY     TRUE               characte....
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      [[1]]$date_unit
      [1] "DAY"
      
      

---

    Code
      as.data.frame(np[["columns"]])
    Output
        name type_type       type nullable dictionary custom_metadata
      1    x      Time SECOND, 32     TRUE               characte....
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      [[1]]$time_unit
      [1] "SECOND"
      
      [[1]]$bit_width
      [1] 32
      
      

---

    Code
      as.data.frame(np[["columns"]])
    Output
        name type_type       type nullable dictionary custom_metadata
      1    x      Time SECOND, 32     TRUE               characte....
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      [[1]]$time_unit
      [1] "SECOND"
      
      [[1]]$bit_width
      [1] 32
      
      

---

    Code
      as.data.frame(np[["columns"]])
    Output
        name type_type       type nullable dictionary custom_metadata
      1    x  Duration NANOSECOND     TRUE               characte....
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      [[1]]$unit
      [1] "NANOSECOND"
      
      

---

    Code
      as.data.frame(np[["columns"]])
    Output
        name type_type         type nullable dictionary custom_metadata
      1    x Timestamp MICROSEC....     TRUE               characte....
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      [[1]]$unit
      [1] "MICROSECOND"
      
      [[1]]$timezone
      [1] "UTC"
      
      

---

    Code
      as.data.frame(np[["columns"]])
    Output
        name type_type type nullable   dictionary custom_metadata
      1    x      Utf8          TRUE 0, list(....    characte....
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      NULL
      

