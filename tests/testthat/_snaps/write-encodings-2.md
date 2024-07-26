# character -> ENUM

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
             s
      1    foo
      2    bar
      3 foobar

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
             s
      1    foo
      2    bar
      3 foobar

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
             s
      1    foo
      2    bar
      3 foobar

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
             s
      1    foo
      2    bar
      3   <NA>
      4 foobar

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
             s
      1    foo
      2    bar
      3   <NA>
      4 foobar

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
             s
      1    foo
      2    bar
      3   <NA>
      4 foobar

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
           s
      1  foo
      2  foo
      3  foo
      4  foo
      5  foo
      6  foo
      7  foo
      8  foo
      9  foo
      10 foo

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
           s
      1  foo
      2  foo
      3  foo
      4  foo
      5  foo
      6  foo
      7  foo
      8  foo
      9  foo
      10 foo

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
           s
      1  foo
      2  foo
      3  foo
      4  foo
      5  foo
      6  foo
      7  foo
      8  foo
      9  foo
      10 foo

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
            s
      1   foo
      2   foo
      3   foo
      4   foo
      5  <NA>
      6   foo
      7   foo
      8   foo
      9   foo
      10  foo
      11  foo

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
            s
      1   foo
      2   foo
      3   foo
      4   foo
      5  <NA>
      6   foo
      7   foo
      8   foo
      9   foo
      10  foo
      11  foo

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
            s
      1   foo
      2   foo
      3   foo
      4   foo
      5  <NA>
      6   foo
      7   foo
      8   foo
      9   foo
      10  foo
      11  foo

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for BYTE_ARRAY column: DELTA_LENGTH_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for BYTE_ARRAY column: DELTA_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for BYTE_ARRAY column: RLE

# factor -> ENUM

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
             s
      1    foo
      2    bar
      3 foobar

---

    Code
      schema
    Output
      [1] "ENUM"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
             s
      1    foo
      2    bar
      3   <NA>
      4 foobar

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for BYTE_ARRAY column: DELTA_LENGTH_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for BYTE_ARRAY column: DELTA_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for BYTE_ARRAY column: RLE

# integer -> DECOMAL INT32

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# integer -> DECIMAL INT64

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT64 column: RLE

# double -> DECOMAL INT32

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT32"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# double -> DECIMAL INT64

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [[1]]
      [1] "DECIMAL"
      
      $precision
      [1] 2
      
      $scale
      [1] 1
      
      $primitive_type
      [1] "INT64"
      
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT64 column: RLE

# integer -> INT(8, *)

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# integer -> INT(16, *)

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# integer -> INT(64, *)

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT64 column: RLE

# double -> INT(8, TRUE)

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# double -> INT(8, FALSE)

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "UINT_8"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# double -> INT(16, TRUE)

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# double -> INT(16, FALSE)

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "UINT_16"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# double -> INT(32, TRUE)

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_32"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# double -> INT(32, FALSE)

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "UINT_32"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# double -> INT(64, TRUE)

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "INT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT64 column: RLE

# double -> INT(64, FALSE)

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
        d
      1 1
      2 2
      3 3
      4 4
      5 5

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  2
      3 NA
      4  3
      5  4
      6  5

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
         d
      1  1
      2  1
      3  1
      4  1
      5  1
      6  1
      7  1
      8  1
      9  1
      10 1

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      schema
    Output
      [1] "UINT_64"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
          d
      1   1
      2   1
      3   1
      4   1
      5   1
      6  NA
      7   1
      8   1
      9   1
      10  1
      11  1

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT64 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT64 column: RLE

# character -> UUID

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                           u
      1 00112233-4455-6677-8899-aabbccddeeff
      2 01112233-4455-6677-8899-aabbccddeeff
      3 02112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                           u
      1 00112233-4455-6677-8899-aabbccddeeff
      2 01112233-4455-6677-8899-aabbccddeeff
      3 02112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                           u
      1 00112233-4455-6677-8899-aabbccddeeff
      2 01112233-4455-6677-8899-aabbccddeeff
      3 02112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                           u
      1 00112233-4455-6677-8899-aabbccddeeff
      2 01112233-4455-6677-8899-aabbccddeeff
      3                                 <NA>
      4 02112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                           u
      1 00112233-4455-6677-8899-aabbccddeeff
      2 01112233-4455-6677-8899-aabbccddeeff
      3                                 <NA>
      4 02112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                           u
      1 00112233-4455-6677-8899-aabbccddeeff
      2 01112233-4455-6677-8899-aabbccddeeff
      3                                 <NA>
      4 02112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                            u
      1  00112233-4455-6677-8899-aabbccddeeff
      2  00112233-4455-6677-8899-aabbccddeeff
      3  00112233-4455-6677-8899-aabbccddeeff
      4  00112233-4455-6677-8899-aabbccddeeff
      5  00112233-4455-6677-8899-aabbccddeeff
      6  00112233-4455-6677-8899-aabbccddeeff
      7  00112233-4455-6677-8899-aabbccddeeff
      8  00112233-4455-6677-8899-aabbccddeeff
      9  00112233-4455-6677-8899-aabbccddeeff
      10 00112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                            u
      1  00112233-4455-6677-8899-aabbccddeeff
      2  00112233-4455-6677-8899-aabbccddeeff
      3  00112233-4455-6677-8899-aabbccddeeff
      4  00112233-4455-6677-8899-aabbccddeeff
      5  00112233-4455-6677-8899-aabbccddeeff
      6  00112233-4455-6677-8899-aabbccddeeff
      7  00112233-4455-6677-8899-aabbccddeeff
      8  00112233-4455-6677-8899-aabbccddeeff
      9  00112233-4455-6677-8899-aabbccddeeff
      10 00112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                            u
      1  00112233-4455-6677-8899-aabbccddeeff
      2  00112233-4455-6677-8899-aabbccddeeff
      3  00112233-4455-6677-8899-aabbccddeeff
      4  00112233-4455-6677-8899-aabbccddeeff
      5  00112233-4455-6677-8899-aabbccddeeff
      6  00112233-4455-6677-8899-aabbccddeeff
      7  00112233-4455-6677-8899-aabbccddeeff
      8  00112233-4455-6677-8899-aabbccddeeff
      9  00112233-4455-6677-8899-aabbccddeeff
      10 00112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                            u
      1  00112233-4455-6677-8899-aabbccddeeff
      2  00112233-4455-6677-8899-aabbccddeeff
      3  00112233-4455-6677-8899-aabbccddeeff
      4  00112233-4455-6677-8899-aabbccddeeff
      5                                  <NA>
      6  00112233-4455-6677-8899-aabbccddeeff
      7  00112233-4455-6677-8899-aabbccddeeff
      8  00112233-4455-6677-8899-aabbccddeeff
      9  00112233-4455-6677-8899-aabbccddeeff
      10 00112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      [1] "PLAIN"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"   "PLAIN"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE    PLAIN
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                            u
      1  00112233-4455-6677-8899-aabbccddeeff
      2  00112233-4455-6677-8899-aabbccddeeff
      3  00112233-4455-6677-8899-aabbccddeeff
      4  00112233-4455-6677-8899-aabbccddeeff
      5                                  <NA>
      6  00112233-4455-6677-8899-aabbccddeeff
      7  00112233-4455-6677-8899-aabbccddeeff
      8  00112233-4455-6677-8899-aabbccddeeff
      9  00112233-4455-6677-8899-aabbccddeeff
      10 00112233-4455-6677-8899-aabbccddeeff

---

    Code
      schema
    Output
      [1] "UUID"
    Code
      encoding
    Output
      [1] "RLE_DICTIONARY"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"            "PLAIN"          "RLE_DICTIONARY"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
              page_type       encoding
      1 DICTIONARY_PAGE          PLAIN
      2       DATA_PAGE RLE_DICTIONARY
    Code
      as.data.frame(read_parquet(tmp))
    Output
                                            u
      1  00112233-4455-6677-8899-aabbccddeeff
      2  00112233-4455-6677-8899-aabbccddeeff
      3  00112233-4455-6677-8899-aabbccddeeff
      4  00112233-4455-6677-8899-aabbccddeeff
      5                                  <NA>
      6  00112233-4455-6677-8899-aabbccddeeff
      7  00112233-4455-6677-8899-aabbccddeeff
      8  00112233-4455-6677-8899-aabbccddeeff
      9  00112233-4455-6677-8899-aabbccddeeff
      10 00112233-4455-6677-8899-aabbccddeeff

