# parse_encoding

    Code
      names(mtcars)
    Output
       [1] "mpg"  "cyl"  "disp" "hp"   "drat" "wt"   "qsec" "vs"   "am"   "gear"
      [11] "carb"
    Code
      parse_encoding(NULL, mtcars)
    Output
       mpg  cyl disp   hp drat   wt qsec   vs   am gear carb 
        NA   NA   NA   NA   NA   NA   NA   NA   NA   NA   NA 
    Code
      parse_encoding("PLAIN", mtcars)
    Output
          mpg     cyl    disp      hp    drat      wt    qsec      vs      am    gear 
      "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" 
         carb 
      "PLAIN" 
    Code
      parse_encoding(c(disp = "RLE_DICTIONARY"), mtcars)
    Output
                   mpg              cyl             disp               hp 
                    NA               NA "RLE_DICTIONARY"               NA 
                  drat               wt             qsec               vs 
                    NA               NA               NA               NA 
                    am             gear             carb 
                    NA               NA               NA 
    Code
      parse_encoding(c(disp = "RLE_DICTIONARY", vs = "PLAIN"), mtcars)
    Output
                   mpg              cyl             disp               hp 
                    NA               NA "RLE_DICTIONARY"               NA 
                  drat               wt             qsec               vs 
                    NA               NA               NA          "PLAIN" 
                    am             gear             carb 
                    NA               NA               NA 
    Code
      parse_encoding(c(disp = "RLE", "PLAIN"), mtcars)
    Output
          mpg     cyl    disp      hp    drat      wt    qsec      vs      am    gear 
      "PLAIN" "PLAIN"   "RLE" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" 
         carb 
      "PLAIN" 
    Code
      parse_encoding(c(disp = "RLE", "PLAIN", vs = "PLAIN"), mtcars)
    Output
          mpg     cyl    disp      hp    drat      wt    qsec      vs      am    gear 
      "PLAIN" "PLAIN"   "RLE" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" "PLAIN" 
         carb 
      "PLAIN" 

---

    Code
      parse_encoding(1:2, mtcars)
    Condition
      Error in `parse_encoding()`:
      ! `encoding` must be `NULL` or a character vector
    Code
      parse_encoding(c("PLAIN", "foobar"), mtcars)
    Condition
      Error in `parse_encoding()`:
      ! `encoding` contains at least one unknown encoding
    Code
      parse_encoding(c(foo = "PLAIN", foo = "RLE"), mtcars)
    Condition
      Error in `parse_encoding()`:
      ! names of `encoding` must be unique
    Code
      parse_encoding(c(disp = "PLAIN", foo = "RLE"), mtcars)
    Condition
      Error in `parse_encoding()`:
      ! names of `encoding` must match names of `x`

# BOOLEAN

    Code
      schema
    Output
      NULL
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
            l
      1  TRUE
      2 FALSE
      3  TRUE

---

    Code
      schema
    Output
      NULL
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
            l
      1  TRUE
      2 FALSE
      3  TRUE

---

    Code
      schema
    Output
      NULL
    Code
      encoding
    Output
      [1] "RLE"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE      RLE
    Code
      as.data.frame(read_parquet(tmp))
    Output
            l
      1  TRUE
      2 FALSE
      3  TRUE

---

    Code
      schema
    Output
      NULL
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
            l
      1  TRUE
      2 FALSE
      3    NA
      4  TRUE

---

    Code
      schema
    Output
      NULL
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
            l
      1  TRUE
      2 FALSE
      3    NA
      4  TRUE

---

    Code
      schema
    Output
      NULL
    Code
      encoding
    Output
      [1] "RLE"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE      RLE
    Code
      as.data.frame(read_parquet(tmp))
    Output
            l
      1  TRUE
      2 FALSE
      3    NA
      4  TRUE

---

    Code
      schema
    Output
      NULL
    Code
      encoding
    Output
      NULL
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE      RLE
    Code
      as.data.frame(read_parquet(tmp))
    Output
            l
      1  TRUE
      2  TRUE
      3  TRUE
      4  TRUE
      5  TRUE
      6  TRUE
      7  TRUE
      8  TRUE
      9  TRUE
      10 TRUE
      11 TRUE
      12 TRUE
      13 TRUE
      14 TRUE
      15 TRUE
      16 TRUE

---

    Code
      schema
    Output
      NULL
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
            l
      1  TRUE
      2  TRUE
      3  TRUE
      4  TRUE
      5  TRUE
      6  TRUE
      7  TRUE
      8  TRUE
      9  TRUE
      10 TRUE
      11 TRUE
      12 TRUE
      13 TRUE
      14 TRUE
      15 TRUE
      16 TRUE

---

    Code
      schema
    Output
      NULL
    Code
      encoding
    Output
      [1] "RLE"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE      RLE
    Code
      as.data.frame(read_parquet(tmp))
    Output
            l
      1  TRUE
      2  TRUE
      3  TRUE
      4  TRUE
      5  TRUE
      6  TRUE
      7  TRUE
      8  TRUE
      9  TRUE
      10 TRUE
      11 TRUE
      12 TRUE
      13 TRUE
      14 TRUE
      15 TRUE
      16 TRUE

---

    Code
      schema
    Output
      NULL
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
            l
      1  TRUE
      2  TRUE
      3  TRUE
      4  TRUE
      5  TRUE
      6  TRUE
      7  TRUE
      8  TRUE
      9    NA
      10 TRUE
      11 TRUE
      12 TRUE
      13 TRUE
      14 TRUE
      15 TRUE
      16 TRUE
      17 TRUE

---

    Code
      schema
    Output
      NULL
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
            l
      1  TRUE
      2  TRUE
      3  TRUE
      4  TRUE
      5  TRUE
      6  TRUE
      7  TRUE
      8  TRUE
      9    NA
      10 TRUE
      11 TRUE
      12 TRUE
      13 TRUE
      14 TRUE
      15 TRUE
      16 TRUE
      17 TRUE

---

    Code
      schema
    Output
      NULL
    Code
      encoding
    Output
      [1] "RLE"
    Code
      read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    Output
      [[1]]
      [1] "RLE"
      
    Code
      as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    Output
        page_type encoding
      1 DATA_PAGE      RLE
    Code
      as.data.frame(read_parquet(tmp))
    Output
            l
      1  TRUE
      2  TRUE
      3  TRUE
      4  TRUE
      5  TRUE
      6  TRUE
      7  TRUE
      8  TRUE
      9    NA
      10 TRUE
      11 TRUE
      12 TRUE
      13 TRUE
      14 TRUE
      15 TRUE
      16 TRUE
      17 TRUE

---

    Code
      write_parquet(d, tmp, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for BOOLEAN column: RLE_DICTIONARY
    Code
      write_parquet(d, tmp, encoding = "BIT_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for BOOLEAN column: BIT_PACKED
    Code
      write_parquet(d, tmp, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for BOOLEAN column: BYTE_STREAM_SPLIT

# INT32

    Code
      schema
    Output
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      NULL
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
      write_parquet(d, tmp, encoding = "DELTA_BINARY_PACKED")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: DELTA_BINARY_PACKED
    Code
      write_parquet(d, tmp, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for INT32 column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT32 column: RLE

# integer -> INT64

    Code
      schema
    Output
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

# double -> INT64

    Code
      schema
    Output
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

# integer -> INT96

    Code
      write_parquet(d, tmp, schema = schema)
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
      write_parquet(d, tmp, schema = schema, encoding = "PLAIN")
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
      write_parquet(d, tmp, schema = schema, encoding = "RLE_DICTIONARY")
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

---

    Code
      write_parquet(d, tmp, schema = schema)
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
      write_parquet(d, tmp, schema = schema, encoding = "PLAIN")
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
      write_parquet(d, tmp, schema = schema, encoding = "RLE_DICTIONARY")
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

---

    Code
      write_parquet(d, tmp, schema = schema)
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
      write_parquet(d, tmp, schema = schema, encoding = "PLAIN")
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
      write_parquet(d, tmp, schema = schema, encoding = "RLE_DICTIONARY")
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

---

    Code
      write_parquet(d, tmp, schema = schema)
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
      write_parquet(d, tmp, schema = schema, encoding = "PLAIN")
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
      write_parquet(d, tmp, schema = schema, encoding = "RLE_DICTIONARY")
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

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT96 column: RLE

# double -> INT96

    Code
      schema
    Output
      [1] "INT96"
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
      1 -4713-11-24
      2 -4713-11-24
      3 -4713-11-24
      4 -4713-11-24
      5 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1 -4713-11-24
      2 -4713-11-24
      3 -4713-11-24
      4 -4713-11-24
      5 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1 -4713-11-24
      2 -4713-11-24
      3 -4713-11-24
      4 -4713-11-24
      5 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1 -4713-11-24
      2 -4713-11-24
      3        <NA>
      4 -4713-11-24
      5 -4713-11-24
      6 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1 -4713-11-24
      2 -4713-11-24
      3        <NA>
      4 -4713-11-24
      5 -4713-11-24
      6 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1 -4713-11-24
      2 -4713-11-24
      3        <NA>
      4 -4713-11-24
      5 -4713-11-24
      6 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1  -4713-11-24
      2  -4713-11-24
      3  -4713-11-24
      4  -4713-11-24
      5  -4713-11-24
      6  -4713-11-24
      7  -4713-11-24
      8  -4713-11-24
      9  -4713-11-24
      10 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1  -4713-11-24
      2  -4713-11-24
      3  -4713-11-24
      4  -4713-11-24
      5  -4713-11-24
      6  -4713-11-24
      7  -4713-11-24
      8  -4713-11-24
      9  -4713-11-24
      10 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1  -4713-11-24
      2  -4713-11-24
      3  -4713-11-24
      4  -4713-11-24
      5  -4713-11-24
      6  -4713-11-24
      7  -4713-11-24
      8  -4713-11-24
      9  -4713-11-24
      10 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1  -4713-11-24
      2  -4713-11-24
      3  -4713-11-24
      4  -4713-11-24
      5  -4713-11-24
      6         <NA>
      7  -4713-11-24
      8  -4713-11-24
      9  -4713-11-24
      10 -4713-11-24
      11 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1  -4713-11-24
      2  -4713-11-24
      3  -4713-11-24
      4  -4713-11-24
      5  -4713-11-24
      6         <NA>
      7  -4713-11-24
      8  -4713-11-24
      9  -4713-11-24
      10 -4713-11-24
      11 -4713-11-24

---

    Code
      schema
    Output
      [1] "INT96"
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
      1  -4713-11-24
      2  -4713-11-24
      3  -4713-11-24
      4  -4713-11-24
      5  -4713-11-24
      6         <NA>
      7  -4713-11-24
      8  -4713-11-24
      9  -4713-11-24
      10 -4713-11-24
      11 -4713-11-24

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for INT96 column: RLE

# FLOAT

    Code
      schema
    Output
      [1] "FLOAT"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1 0.5
      2 1.0
      3  NA
      4 1.5
      5 2.0
      6 2.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1 0.5
      2 1.0
      3  NA
      4 1.5
      5 2.0
      6 2.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1 0.5
      2 1.0
      3  NA
      4 1.5
      5 2.0
      6 2.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6   NA
      7  0.5
      8  0.5
      9  0.5
      10 0.5
      11 0.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6   NA
      7  0.5
      8  0.5
      9  0.5
      10 0.5
      11 0.5

---

    Code
      schema
    Output
      [1] "FLOAT"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6   NA
      7  0.5
      8  0.5
      9  0.5
      10 0.5
      11 0.5

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for FLOAT column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for FLOAT column: RLE

# DOUBLE

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1 0.5
      2 1.0
      3  NA
      4 1.5
      5 2.0
      6 2.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1 0.5
      2 1.0
      3  NA
      4 1.5
      5 2.0
      6 2.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1 0.5
      2 1.0
      3  NA
      4 1.5
      5 2.0
      6 2.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6   NA
      7  0.5
      8  0.5
      9  0.5
      10 0.5
      11 0.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6   NA
      7  0.5
      8  0.5
      9  0.5
      10 0.5
      11 0.5

---

    Code
      schema
    Output
      [1] "DOUBLE"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6   NA
      7  0.5
      8  0.5
      9  0.5
      10 0.5
      11 0.5

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for DOUBLE column: BYTE_STREAM_SPLIT
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for DOUBLE column: RLE

# BYTE_ARRAY, string

    Code
      schema
    Output
      [1] "STRING"
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
      [1] "STRING"
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
      [1] "STRING"
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
      [1] "STRING"
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
      [1] "STRING"
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
      [1] "STRING"
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
      [1] "STRING"
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
      [1] "STRING"
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
      [1] "STRING"
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
      [1] "STRING"
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

---

    Code
      schema
    Output
      [1] "STRING"
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

---

    Code
      schema
    Output
      [1] "STRING"
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

# BYTE_ARRAY, RAW

    Code
      schema
    Output
      [1] "BYTE_ARRAY"
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
      1             66, 6f, 6f
      2             62, 61, 72
      3 66, 6f, 6f, 62, 61, 72

---

    Code
      schema
    Output
      [1] "BYTE_ARRAY"
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
      1             66, 6f, 6f
      2             62, 61, 72
      3 66, 6f, 6f, 62, 61, 72

---

    Code
      schema
    Output
      [1] "BYTE_ARRAY"
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
      1             66, 6f, 6f
      2             62, 61, 72
      3                   NULL
      4 66, 6f, 6f, 62, 61, 72

---

    Code
      schema
    Output
      [1] "BYTE_ARRAY"
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
      1             66, 6f, 6f
      2             62, 61, 72
      3                   NULL
      4 66, 6f, 6f, 62, 61, 72

---

    Code
      schema
    Output
      [1] "BYTE_ARRAY"
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5  66, 6f, 6f
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [1] "BYTE_ARRAY"
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5  66, 6f, 6f
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [1] "BYTE_ARRAY"
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5        NULL
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [1] "BYTE_ARRAY"
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5        NULL
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for list(raw) BYTE_ARRAY column: DELTA_LENGTH_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for list(raw) BYTE_ARRAY column: DELTA_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "PLAIN_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for list(raw) BYTE_ARRAY column: PLAIN_DICTIONARY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for list(raw) BYTE_ARRAY column: RLE

# FIXED_LEN_BYTE_ARRAY, RAW

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3       NULL
      4 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3       NULL
      4 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5  66, 6f, 6f
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5  66, 6f, 6f
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5        NULL
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5        NULL
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for list(raw) FIXED_LEN_BYTE_ARRAY column: DELTA_LENGTH_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for list(raw) FIXED_LEN_BYTE_ARRAY column: DELTA_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "PLAIN_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for list(raw) FIXED_LEN_BYTE_ARRAY column: PLAIN_DICTIONARY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for list(raw) FIXED_LEN_BYTE_ARRAY column: RLE

# FIXED_LEN_BYTE_ARRAY, FLOAT16

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1 0.5
      2 1.0
      3 1.5
      4 2.0
      5 2.5

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1 0.5
      2 1.0
      3  NA
      4 2.0
      5 2.5
      6 3.0

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1 0.5
      2 1.0
      3  NA
      4 2.0
      5 2.5
      6 3.0

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1 0.5
      2 1.0
      3  NA
      4 2.0
      5 2.5
      6 3.0

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5  0.5
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5   NA
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5   NA
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      schema
    Output
      [1] "FLOAT16"
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
      1  0.5
      2  0.5
      3  0.5
      4  0.5
      5   NA
      6  0.5
      7  0.5
      8  0.5
      9  0.5
      10 0.5

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for FIXED_LEN_BYTE_ARRAY column: DELTA_LENGTH_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for FIXED_LEN_BYTE_ARRAY column: DELTA_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for FIXED_LEN_BYTE_ARRAY column: RLE

# FIXED_LEN_BYTE_ARRAY, character

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3       NULL
      4 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3       NULL
      4 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1 66, 6f, 6f
      2 62, 61, 72
      3       NULL
      4 61, 61, 61

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5  66, 6f, 6f
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5  66, 6f, 6f
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5  66, 6f, 6f
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5        NULL
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5        NULL
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      schema
    Output
      [[1]]
      [1] "FIXED_LEN_BYTE_ARRAY"
      
      $type_length
      [1] 3
      
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
      1  66, 6f, 6f
      2  66, 6f, 6f
      3  66, 6f, 6f
      4  66, 6f, 6f
      5        NULL
      6  66, 6f, 6f
      7  66, 6f, 6f
      8  66, 6f, 6f
      9  66, 6f, 6f
      10 66, 6f, 6f

---

    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for FIXED_LEN_BYTE_ARRAY column: DELTA_LENGTH_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    Condition
      Error in `write_parquet()`:
      ! Unimplemented encoding for FIXED_LEN_BYTE_ARRAY column: DELTA_BYTE_ARRAY
    Code
      write_parquet(d, tmp, schema = schema, encoding = "RLE")
    Condition
      Error in `write_parquet()`:
      ! Unsupported encoding for FIXED_LEN_BYTE_ARRAY column: RLE

