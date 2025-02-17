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
      np[["columns"]]
    Output
      # A data frame: 1 x 6
        name  type_type type             nullable dictionary custom_metadata 
        <chr> <chr>     <I<list>>        <lgl>    <I<list>>  <I<list>>       
      1 x     Date      <named list [1]> TRUE     <NULL>     <named list [2]>
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      [[1]]$date_unit
      [1] "DAY"
      
      

---

    Code
      np[["columns"]]
    Output
      # A data frame: 1 x 6
        name  type_type type             nullable dictionary custom_metadata 
        <chr> <chr>     <I<list>>        <lgl>    <I<list>>  <I<list>>       
      1 x     Time      <named list [2]> TRUE     <NULL>     <named list [2]>
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
      np[["columns"]]
    Output
      # A data frame: 1 x 6
        name  type_type type             nullable dictionary custom_metadata 
        <chr> <chr>     <I<list>>        <lgl>    <I<list>>  <I<list>>       
      1 x     Time      <named list [2]> TRUE     <NULL>     <named list [2]>
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
      np[["columns"]]
    Output
      # A data frame: 1 x 6
        name  type_type type             nullable dictionary custom_metadata 
        <chr> <chr>     <I<list>>        <lgl>    <I<list>>  <I<list>>       
      1 x     Duration  <named list [1]> TRUE     <NULL>     <named list [2]>
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      [[1]]$unit
      [1] "NANOSECOND"
      
      

---

    Code
      np[["columns"]]
    Output
      # A data frame: 1 x 6
        name  type_type type             nullable dictionary custom_metadata 
        <chr> <chr>     <I<list>>        <lgl>    <I<list>>  <I<list>>       
      1 x     Timestamp <named list [2]> TRUE     <NULL>     <named list [2]>
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
      np[["columns"]]
    Output
      # A data frame: 1 x 6
        name  type_type type      nullable dictionary       custom_metadata 
        <chr> <chr>     <I<list>> <lgl>    <I<list>>        <I<list>>       
      1 x     Utf8      <NULL>    TRUE     <named list [4]> <named list [2]>
    Code
      np[["columns"]][["type"]]
    Output
      [[1]]
      NULL
      

