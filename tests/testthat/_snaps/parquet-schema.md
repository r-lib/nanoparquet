# parquet_type

    Code
      parquet_type("AUTO")
    Output
      $type
      [1] NA
      
    Code
      parquet_type("BOOLEAN")
    Output
      $type
      [1] "BOOLEAN"
      
    Code
      parquet_type("INT32")
    Output
      $type
      [1] "INT32"
      
    Code
      parquet_type("INT64")
    Output
      $type
      [1] "INT64"
      
    Code
      parquet_type("INT96")
    Output
      $type
      [1] "INT96"
      
    Code
      parquet_type("FLOAT")
    Output
      $type
      [1] "FLOAT"
      
    Code
      parquet_type("DOUBLE")
    Output
      $type
      [1] "DOUBLE"
      
    Code
      parquet_type("BYTE_ARRAY")
    Output
      $type
      [1] "BYTE_ARRAY"
      
    Code
      parquet_type("FIXED_LEN_BYTE_ARRAY", type_length = 10)
    Output
      $type
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 10
      
    Code
      parquet_type("STRING")
    Output
      $type
      [1] "BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "STRING"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "UTF8"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("ENUM")
    Output
      $type
      [1] "BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "ENUM"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "ENUM"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("UUID")
    Output
      $type
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "UUID"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $type_length
      [1] 16
      
      $converted_type
      [1] NA
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("INT", bit_width = 8, is_signed = TRUE)
    Output
      $type
      [1] "INT32"
      
      $logical_type
      $type
      [1] "INT"
      
      $bit_width
      [1] 8
      
      $is_signed
      [1] TRUE
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "INT_8"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("INT", bit_width = 16, is_signed = TRUE)
    Output
      $type
      [1] "INT32"
      
      $logical_type
      $type
      [1] "INT"
      
      $bit_width
      [1] 16
      
      $is_signed
      [1] TRUE
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "INT_16"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("INT", bit_width = 32, is_signed = FALSE)
    Output
      $type
      [1] "INT32"
      
      $logical_type
      $type
      [1] "INT"
      
      $bit_width
      [1] 32
      
      $is_signed
      [1] FALSE
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "UINT_32"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("INT", bit_width = 64, is_signed = FALSE)
    Output
      $type
      [1] "INT64"
      
      $logical_type
      $type
      [1] "INT"
      
      $bit_width
      [1] 64
      
      $is_signed
      [1] FALSE
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "UINT_64"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("DECIMAL", precision = 5, primitive_type = "INT32")
    Output
      $type
      [1] "INT32"
      
      $logical_type
      $type
      [1] "DECIMAL"
      
      $scale
      NULL
      
      $precision
      [1] 5
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $scale
      [1] 0
      
      $precision
      [1] 5
      
      $converted_type
      [1] "DECIMAL"
      
    Code
      parquet_type("DECIMAL", precision = 5, scale = 0, primitive_type = "INT32")
    Output
      $type
      [1] "INT32"
      
      $logical_type
      $type
      [1] "DECIMAL"
      
      $scale
      [1] 0
      
      $precision
      [1] 5
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $scale
      [1] 0
      
      $precision
      [1] 5
      
      $converted_type
      [1] "DECIMAL"
      
    Code
      parquet_type("DECIMAL", precision = 5, scale = 5, primitive_type = "INT64")
    Output
      $type
      [1] "INT64"
      
      $logical_type
      $type
      [1] "DECIMAL"
      
      $scale
      [1] 5
      
      $precision
      [1] 5
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $scale
      [1] 5
      
      $precision
      [1] 5
      
      $converted_type
      [1] "DECIMAL"
      
    Code
      parquet_type("DECIMAL", precision = 5, primitive_type = "BYTE_ARRAY")
    Output
      $type
      [1] "BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "DECIMAL"
      
      $scale
      NULL
      
      $precision
      [1] 5
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $scale
      [1] 0
      
      $precision
      [1] 5
      
      $converted_type
      [1] "DECIMAL"
      
    Code
      parquet_type("DECIMAL", precision = 5, primitive_type = "FIXED_LEN_BYTE_ARRAY",
        type_length = 5)
    Output
      $type
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "DECIMAL"
      
      $scale
      NULL
      
      $precision
      [1] 5
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $scale
      [1] 0
      
      $precision
      [1] 5
      
      $type_length
      [1] 5
      
      $converted_type
      [1] "DECIMAL"
      
    Code
      parquet_type("FLOAT16")
    Output
      $type
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "FLOAT16"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $type_length
      [1] 2
      
      $converted_type
      [1] NA
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("DATE")
    Output
      $type
      [1] "INT32"
      
      $logical_type
      $type
      [1] "DATE"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "DATE"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MILLIS")
    Output
      $type
      [1] "INT32"
      
      $logical_type
      $type
      [1] "TIME"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "TIME_MILLIS"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MICROS")
    Output
      $type
      [1] "INT64"
      
      $logical_type
      $type
      [1] "TIME"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MICROS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "TIME_MICROS"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("TIME", is_adjusted_utc = TRUE, unit = "NANOS")
    Output
      $type
      [1] "INT64"
      
      $logical_type
      $type
      [1] "TIME"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "NANOS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] NA
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS")
    Output
      $type
      [1] "INT64"
      
      $logical_type
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "TIMESTAMP_MILLIS"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS")
    Output
      $type
      [1] "INT64"
      
      $logical_type
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MICROS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "TIMESTAMP_MICROS"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "NANOS")
    Output
      $type
      [1] "INT64"
      
      $logical_type
      $type
      [1] "TIMESTAMP"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "NANOS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] NA
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("JSON")
    Output
      $type
      [1] "BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "JSON"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "JSON"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("BSON")
    Output
      $type
      [1] "BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "BSON"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $converted_type
      [1] "BSON"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      

---

    Code
      parquet_type("INT32", repetition_type = "OPTIONAL")
    Output
      $type
      [1] "INT32"
      
      $repetition_type
      [1] "OPTIONAL"
      
    Code
      parquet_type("STRING", repetition_type = "REQUIRED")
    Output
      $type
      [1] "BYTE_ARRAY"
      
      $logical_type
      $type
      [1] "STRING"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $repetition_type
      [1] "REQUIRED"
      
      $converted_type
      [1] "UTF8"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      
    Code
      parquet_type("TIME", repetition_type = "REPEATED", is_adjusted_utc = TRUE,
        unit = "MILLIS")
    Output
      $type
      [1] "INT32"
      
      $logical_type
      $type
      [1] "TIME"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
      attr(,"class")
      [1] "nanoparquet_logical_type"
      
      $repetition_type
      [1] "REPEATED"
      
      $converted_type
      [1] "TIME_MILLIS"
      
      $scale
      [1] NA
      
      $precision
      [1] NA
      

---

    Code
      parquet_type("FOO")
    Condition
      Error in `err()`:
      ! Parquet type 'FOO' is not supported by nanoparquet
    Code
      parquet_type("FIXED_LEN_BYTE_ARRAY")
    Condition
      Error in `fixed_len_byte_array()`:
      ! !is.null(type_length) is not TRUE
    Code
      parquet_type("INT", bit_width = 8)
    Condition
      Error in `int()`:
      ! !is.null(is_signed) is not TRUE
    Code
      parquet_type("INT", is_signed = TRUE)
    Condition
      Error in `int()`:
      ! !is.null(bit_width) is not TRUE
    Code
      parquet_type("INT", bit_width = 10, is_signed = TRUE)
    Condition
      Error in `int()`:
      ! bit_width %in% c(8L, 16L, 32L, 64L) is not TRUE
    Code
      parquet_type("INT", bit_width = 16, is_signed = 1)
    Condition
      Error in `int()`:
      ! is_flag(is_signed) is not TRUE
    Code
      parquet_type("DECIMAL", precision = 5)
    Condition
      Error in `decimal()`:
      ! !is.null(primitive_type) is not TRUE
    Code
      parquet_type("DECIMAL", primitive_type = "INT32")
    Condition
      Error in `decimal()`:
      ! !is.null(precision) is not TRUE
    Code
      parquet_type("DECIMAL", precision = 5 / 2, primitive_type = "INT32")
    Condition
      Error in `decimal()`:
      ! is_uint32(precision) is not TRUE
    Code
      parquet_type("DECIMAL", precision = 0, primitive_type = "INT32")
    Condition
      Error in `decimal()`:
      ! precision > 0 is not TRUE
    Code
      parquet_type("DECIMAL", precision = 5, scale = 6, primitive_type = "INT32")
    Condition
      Error in `decimal()`:
      ! is.null(scale) || (is_uint32(scale) && scale <= precision) is not TRUE
    Code
      parquet_type("DECIMAL", precision = 10, primitive_type = "INT32")
    Condition
      Error in `decimal()`:
      ! precision <= 9 is not TRUE
    Code
      parquet_type("DECIMAL", precision = 19, primitive_type = "INT64")
    Condition
      Error in `decimal()`:
      ! precision <= 18 is not TRUE
    Code
      parquet_type("DECIMAL", precision = 12, primitive_type = "FIXED_LEN_BYTE_ARRAY",
        type_length = 5)
    Condition
      Error in `decimal()`:
      ! precision <= floor(log10(2^(8 * type_length - 1) - 1)) is not TRUE
    Code
      parquet_type("TIME", is_adjusted_utc = TRUE)
    Condition
      Error in `time()`:
      ! !is.null(unit) is not TRUE
    Code
      parquet_type("TIME", unit = "MILLIS")
    Condition
      Error in `time()`:
      ! !is.null(is_adjusted_utc) is not TRUE
    Code
      parquet_type("TIME", is_adjusted_utc = 1, unit = "MILLIS")
    Condition
      Error in `time()`:
      ! is_flag(is_adjusted_utc) is not TRUE
    Code
      parquet_type("TIME", is_adjusted_utc = TRUE, unit = "FOO")
    Condition
      Error in `time()`:
      ! unit %in% c("MILLIS", "MICROS", "NANOS") is not TRUE
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = TRUE)
    Condition
      Error in `timestamp()`:
      ! !is.null(unit) is not TRUE
    Code
      parquet_type("TIMESTAMP", unit = "MILLIS")
    Condition
      Error in `timestamp()`:
      ! !is.null(is_adjusted_utc) is not TRUE
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = 1, unit = "MILLIS")
    Condition
      Error in `timestamp()`:
      ! is_flag(is_adjusted_utc) is not TRUE
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "FOO")
    Condition
      Error in `timestamp()`:
      ! unit %in% c("MILLIS", "MICROS", "NANOS") is not TRUE
    Code
      parquet_type("LIST")
    Condition
      Error in `err()`:
      ! Parquet type 'LIST' is not supported by nanoparquet
    Code
      parquet_type("MAP")
    Condition
      Error in `err()`:
      ! Parquet type 'MAP' is not supported by nanoparquet
    Code
      parquet_type("UNKNOWN")
    Condition
      Error in `err()`:
      ! Parquet type 'UNKNOWN' is not supported by nanoparquet
    Code
      parquet_type("INT32", repetition_type = TRUE)
    Condition
      Error in `parquet_type()`:
      ! is_string(repetition_type) is not TRUE
    Code
      parquet_type("INT32", repetition_type = "FOO")
    Condition
      Error in `parquet_type()`:
      ! repetition_type %in% c("REQUIRED", "OPTIONAL", "REPEATED") is not TRUE

# parquet_schema

    Code
      as.data.frame(sch)
    Output
        file_name name r_type                 type type_length repetition_type
      1      <NA> <NA>   <NA>                INT32          NA            <NA>
      2      <NA> <NA>   <NA>              BOOLEAN          NA            <NA>
      3      <NA> <NA>   <NA>                INT32          NA            <NA>
      4      <NA> <NA>   <NA>                INT64          NA            <NA>
      5      <NA> <NA>   <NA>                INT96          NA            <NA>
      6      <NA> <NA>   <NA>                FLOAT          NA            <NA>
      7      <NA> <NA>   <NA>           BYTE_ARRAY          NA            <NA>
      8      <NA> <NA>   <NA> FIXED_LEN_BYTE_ARRAY          10            <NA>
        converted_type logical_type num_children scale precision field_id
      1           <NA>                        NA    NA        NA       NA
      2           <NA>                        NA    NA        NA       NA
      3           <NA>                        NA    NA        NA       NA
      4           <NA>                        NA    NA        NA       NA
      5           <NA>                        NA    NA        NA       NA
      6           <NA>                        NA    NA        NA       NA
      7           <NA>                        NA    NA        NA       NA
      8           <NA>                        NA    NA        NA       NA

---

    Code
      as.data.frame(sch2)
    Output
        file_name name r_type                 type type_length repetition_type
      a      <NA>    a   <NA>                INT32          NA            <NA>
      b      <NA>    b   <NA>              BOOLEAN          NA            <NA>
      c      <NA>    c   <NA>                INT32          NA            <NA>
      d      <NA>    d   <NA>                INT64          NA            <NA>
      e      <NA>    e   <NA>                INT96          NA            <NA>
      f      <NA>    f   <NA>                FLOAT          NA            <NA>
      g      <NA>    g   <NA>           BYTE_ARRAY          NA            <NA>
             <NA> <NA>   <NA> FIXED_LEN_BYTE_ARRAY          10            <NA>
        converted_type logical_type num_children scale precision field_id
      a           <NA>                        NA    NA        NA       NA
      b           <NA>                        NA    NA        NA       NA
      c           <NA>                        NA    NA        NA       NA
      d           <NA>                        NA    NA        NA       NA
      e           <NA>                        NA    NA        NA       NA
      f           <NA>                        NA    NA        NA       NA
      g           <NA>                        NA    NA        NA       NA
                  <NA>                        NA    NA        NA       NA

---

    Code
      as.data.frame(sch3)
    Output
        file_name name r_type                 type type_length repetition_type
      a      <NA>    a   <NA>                INT32          NA            <NA>
      b      <NA>    b   <NA>              BOOLEAN          NA            <NA>
      c      <NA>    c   <NA>                INT32          NA            <NA>
      d      <NA>    d   <NA>                INT64          NA            <NA>
      e      <NA>    e   <NA>                INT96          NA            <NA>
      f      <NA>    f   <NA>                FLOAT          NA            <NA>
      g      <NA>    g   <NA>           BYTE_ARRAY          NA            <NA>
      h      <NA>    h   <NA> FIXED_LEN_BYTE_ARRAY          10            <NA>
        converted_type logical_type num_children scale precision field_id
      a           <NA>                        NA    NA        NA       NA
      b           <NA>                        NA    NA        NA       NA
      c           <NA>                        NA    NA        NA       NA
      d           <NA>                        NA    NA        NA       NA
      e           <NA>                        NA    NA        NA       NA
      f           <NA>                        NA    NA        NA       NA
      g           <NA>                        NA    NA        NA       NA
      h           <NA>                        NA    NA        NA       NA

---

    Code
      as.data.frame(sch4)
    Output
         file_name name r_type                 type type_length repetition_type
      1       <NA> <NA>   <NA>           BYTE_ARRAY          NA            <NA>
      2       <NA> <NA>   <NA>           BYTE_ARRAY          NA            <NA>
      3       <NA> <NA>   <NA> FIXED_LEN_BYTE_ARRAY          16            <NA>
      4       <NA> <NA>   <NA>                INT32          NA            <NA>
      5       <NA> <NA>   <NA>                INT64          NA            <NA>
      6       <NA> <NA>   <NA>                INT64          NA            <NA>
      7       <NA> <NA>   <NA> FIXED_LEN_BYTE_ARRAY           2            <NA>
      8       <NA> <NA>   <NA>                INT32          NA            <NA>
      9       <NA> <NA>   <NA>           BYTE_ARRAY          NA            <NA>
      10      <NA> <NA>   <NA>           BYTE_ARRAY          NA            <NA>
         converted_type logical_type num_children scale precision field_id
      1            UTF8       STRING           NA    NA        NA       NA
      2            ENUM         ENUM           NA    NA        NA       NA
      3            <NA>         UUID           NA    NA        NA       NA
      4           INT_8 INT, 8, TRUE           NA    NA        NA       NA
      5         UINT_64 INT, 64,....           NA    NA        NA       NA
      6         DECIMAL DECIMAL,....           NA     0         5       NA
      7            <NA>      FLOAT16           NA    NA        NA       NA
      8            DATE         DATE           NA    NA        NA       NA
      9            JSON         JSON           NA    NA        NA       NA
      10           BSON         BSON           NA    NA        NA       NA

---

    Code
      as.data.frame(sch5)
    Output
          file_name name r_type  type type_length repetition_type converted_type
      foo      <NA>  foo   <NA>  <NA>          NA            <NA>           <NA>
      bar      <NA>  bar   <NA> INT32          NA            <NA>           <NA>
          logical_type num_children scale precision field_id
      foo                        NA    NA        NA       NA
      bar                        NA    NA        NA       NA

