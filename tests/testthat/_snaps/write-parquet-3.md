# parquet_type

    Code
      parquet_type("BOOLEAN")
    Output
      [[1]]
      [1] "BOOLEAN"
      
    Code
      parquet_type("INT32")
    Output
      [[1]]
      [1] "INT32"
      
    Code
      parquet_type("INT64")
    Output
      [[1]]
      [1] "INT64"
      
    Code
      parquet_type("INT96")
    Output
      [[1]]
      [1] "INT96"
      
    Code
      parquet_type("FLOAT")
    Output
      [[1]]
      [1] "FLOAT"
      
    Code
      parquet_type("DOUBLE")
    Output
      [[1]]
      [1] "DOUBLE"
      
    Code
      parquet_type("BYTE_ARRAY")
    Output
      [[1]]
      [1] "BYTE_ARRAY"
      
    Code
      parquet_type("FIXED_LEN_BYTE_ARRAY", length = 10)
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $length
      [1] 10
      
    Code
      parquet_type("STRING")
    Output
      [[1]]
      [1] "BYTE_ARRAY"
      
      $logical_type
      [1] "STRING"
      
    Code
      parquet_type("ENUM")
    Output
      [[1]]
      [1] "BYTE_ARRAY"
      
      $logical_type
      [1] "ENUM"
      
    Code
      parquet_type("UUID")
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $logical_type
      [1] "UUID"
      
      $length
      [1] 16
      
    Code
      parquet_type("INT", bit_width = 8, is_signed = TRUE)
    Output
      [[1]]
      [1] "INT32"
      
      $logical_type
      [1] "INT"
      
      $bit_width
      [1] 8
      
      $is_signed
      [1] TRUE
      
    Code
      parquet_type("INT", bit_width = 16, is_signed = TRUE)
    Output
      [[1]]
      [1] "INT32"
      
      $logical_type
      [1] "INT"
      
      $bit_width
      [1] 16
      
      $is_signed
      [1] TRUE
      
    Code
      parquet_type("INT", bit_width = 32, is_signed = FALSE)
    Output
      [[1]]
      [1] "INT32"
      
      $logical_type
      [1] "INT"
      
      $bit_width
      [1] 32
      
      $is_signed
      [1] FALSE
      
    Code
      parquet_type("INT", bit_width = 64, is_signed = FALSE)
    Output
      [[1]]
      [1] "INT64"
      
      $logical_type
      [1] "INT"
      
      $bit_width
      [1] 64
      
      $is_signed
      [1] FALSE
      
    Code
      parquet_type("DECIMAL", precision = 5, primitive_type = "INT32")
    Output
      [[1]]
      [1] "INT32"
      
      $logical_type
      [1] "DECIMAL"
      
      $scale
      NULL
      
      $precision
      [1] 5
      
    Code
      parquet_type("DECIMAL", precision = 5, scale = 0, primitive_type = "INT32")
    Output
      [[1]]
      [1] "INT32"
      
      $logical_type
      [1] "DECIMAL"
      
      $scale
      [1] 0
      
      $precision
      [1] 5
      
    Code
      parquet_type("DECIMAL", precision = 5, scale = 5, primitive_type = "INT64")
    Output
      [[1]]
      [1] "INT64"
      
      $logical_type
      [1] "DECIMAL"
      
      $scale
      [1] 5
      
      $precision
      [1] 5
      
    Code
      parquet_type("DECIMAL", precision = 5, primitive_type = "BYTE_ARRAY")
    Output
      [[1]]
      [1] "BYTE_ARRAY"
      
      $logical_type
      [1] "DECIMAL"
      
      $scale
      NULL
      
      $precision
      [1] 5
      
    Code
      parquet_type("DECIMAL", precision = 5, primitive_type = "FIXED_LEN_BYTE_ARRAY",
        length = 5)
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $logical_type
      [1] "DECIMAL"
      
      $scale
      NULL
      
      $precision
      [1] 5
      
      $length
      [1] 5
      
    Code
      parquet_type("FLOAT16")
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $logical_type
      [1] "FLOAT16"
      
      $length
      [1] 2
      
    Code
      parquet_type("DATE")
    Output
      [[1]]
      [1] "INT32"
      
      $logical_type
      [1] "DATE"
      
    Code
      parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MILLIS")
    Output
      [[1]]
      [1] "INT32"
      
      $logical_type
      [1] "TIME"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
    Code
      parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MICROS")
    Output
      [[1]]
      [1] "INT64"
      
      $logical_type
      [1] "TIME"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MICROS"
      
    Code
      parquet_type("TIME", is_adjusted_utc = TRUE, unit = "NANOS")
    Output
      [[1]]
      [1] "INT64"
      
      $logical_type
      [1] "TIME"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "NANOS"
      
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS")
    Output
      [[1]]
      [1] "INT64"
      
      $logical_type
      [1] "TIMESTAMP"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MILLIS"
      
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS")
    Output
      [[1]]
      [1] "INT64"
      
      $logical_type
      [1] "TIMESTAMP"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "MICROS"
      
    Code
      parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "NANOS")
    Output
      [[1]]
      [1] "INT64"
      
      $logical_type
      [1] "TIMESTAMP"
      
      $is_adjusted_utc
      [1] TRUE
      
      $unit
      [1] "NANOS"
      
    Code
      parquet_type("INTERVAL")
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $logical_type
      [1] "INTERVAL"
      
      $length
      [1] 12
      
    Code
      parquet_type("JSON")
    Output
      [[1]]
      [1] "BYTE_ARRAY"
      
      $logical_type
      [1] "JSON"
      
    Code
      parquet_type("BSON")
    Output
      [[1]]
      [1] "BYTE_ARRAY"
      
      $logical_type
      [1] "BSON"
      

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
      ! !is.null(length) is not TRUE
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
        length = 5)
    Condition
      Error in `decimal()`:
      ! precision <= floor(log10(2^(8 * length - 1) - 1)) is not TRUE
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

