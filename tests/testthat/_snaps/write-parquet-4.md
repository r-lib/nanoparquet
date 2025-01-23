# errors

    Code
      .Call(rf_nanoparquet_write, mtcars, tempfile(), dim(mtcars), 0L, list(character(),
      character()), rep(FALSE, ncol(mtcars)), options, map_schema_to_df(NULL, mtcars),
      rep(10L, ncol(mtcars)), 1L, sys.call())
    Condition
      Error:
      ! Unknown Praquet encoding code: 10

# force PLAIN / RLE

    Code
      read_parquet_pages(tmp)[["encoding"]]
    Output
      [1] "PLAIN" "PLAIN"

---

    Code
      read_parquet_pages(tmp)[["encoding"]]
    Output
      [1] "PLAIN"          "RLE_DICTIONARY" "PLAIN"          "RLE_DICTIONARY"

# cutoff for dict encoding decision

    Code
      read_parquet_pages(tmp)[["encoding"]]
    Output
      [1] "PLAIN"

---

    Code
      read_parquet_pages(tmp)[["encoding"]]
    Output
      [1] "PLAIN"

# write broken DECIMAL INT32

    Code
      as.data.frame(read_parquet_schema(tmp))[, -1]
    Output
          name  r_type  type type_length repetition_type converted_type logical_type
      1 schema    <NA>  <NA>          NA            <NA>           <NA>             
      2    dec integer INT32          NA        REQUIRED        DECIMAL             
        num_children scale precision field_id
      1            1    NA        NA       NA
      2           NA     2         5       NA

---

    Code
      write_parquet(d, tmp, schema = schema2)
    Condition
      Error in `write_parquet()`:
      ! Invalid Parquet file: scale is not set for DECIMAL converted type

---

    Code
      write_parquet(d, tmp, schema = schema2)
    Condition
      Error in `write_parquet()`:
      ! Invalid Parquet file: precision is not set for DECIMAL converted type

---

    Code
      write_parquet(d, tmp, schema = schema2)
    Condition
      Error in `write_parquet()`:
      ! Internal nanoparquet error, precision to high for INT32 DECIMAL

---

    Code
      write_parquet(d2, tmp, schema = schema2)
    Condition
      Error in `write_parquet()`:
      ! Internal nanoparquet error, precision to high for INT32 DECIMAL

# write broken DECIMAL INT64

    Code
      write_parquet(d, tmp, schema = schema2)
    Condition
      Error in `write_parquet()`:
      ! Internal nanoparquet error, precision to high for INT64 DECIMAL

---

    Code
      write_parquet(d2, tmp, schema = schema2)
    Condition
      Error in `write_parquet()`:
      ! Internal nanoparquet error, precision to high for INT64 DECIMAL

# write broken INT32

    Code
      write_parquet(d, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Invalid bit width for INT32: 64

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Invalid bit width for INT32: 64

# write broken UINT32

    Code
      write_parquet(d, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Invalid bit width for INT32: 64

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Invalid bit width for INT32: 64

# write broken INT64

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Invalid bit width for INT64 INT type: 32

# INT96 errors

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT96"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet INT96 type.

# FLOAT errors

    Code
      write_parquet(d, tmp, schema = parquet_schema("FLOAT"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet FLOAT type.

# DOUBLE errors

    Code
      write_parquet(d, tmp, schema = parquet_schema("DOUBLE"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet DOUBLE type.

# BYTE_ARRAY errors

    Code
      write_parquet(d, tmp, schema = parquet_schema("BYTE_ARRAY"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet BYTE_ARRAY element when writing a list column of RAW vectors.

---

    Code
      write_parquet(d2, tmp, schema = parquet_schema("BYTE_ARRAY"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet BYTE_ARRAY type.

# FIXED_LEN_BYTE_ARRAY errors

    Code
      write_parquet(d, tmp, schema = parquet_schema("UUID"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a list as a Parquet UUID (FIXED_LEN_BYTE_ARRAY) type.

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("FLOAT16"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write a list as a Parquet FLOAT16 type.

---

    Code
      write_parquet(d2, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Invalid string length: 6, expenting 3 for FIXED_LEN_TYPE_ARRAY

---

    Code
      write_parquet(d, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet BYTE_ARRAY element when writing a list column of RAW vectors.

---

    Code
      write_parquet(d3, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Invalid string length: 4, expenting 3 for FIXED_LEN_TYPE_ARRAY

---

    Code
      write_parquet(d4, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Cannot write a logical vector as a Parquet FIXED_LEN_BYTE_ARRAY type.

# BOOLEAN errors

    Code
      write_parquet(d, tmp, schema = parquet_schema("BOOLEAN"))
    Condition
      Error in `write_parquet()`:
      ! Cannot write an integer vector as a Parquet BOOLEAN type.

---

    Code
      write_parquet(d, tmp, schema = schema)
    Condition
      Error in `write_parquet()`:
      ! Cannot write an integer vector as a Parquet BOOLEAN type.

# Errors when writing a dictionary

    Code
      write_parquet(d, tmp, schema = parquet_schema("DOUBLE"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Cannot convert an integer vector to Parquet type DOUBLE.

---

    Code
      write_parquet(d2, tmp, schema = parquet_schema("BYTE_ARRAY"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Cannot convert a double vector to Parquet type BYTE_ARRAY.

---

    Code
      write_parquet(d3, tmp, schema = parquet_schema("DOUBLE"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Cannot convert a factor to Parquet type DOUBLE.

---

    Code
      write_parquet(d4, tmp, schema = schema4, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT32 DECIMAL with precision 2, scale 0: -101

---

    Code
      write_parquet(d5, tmp, schema = schema5, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT32 DECIMAL with precision 2, scale 0: 101

---

    Code
      write_parquet(d4, tmp, schema = schema4, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT64 DECIMAL with precision 2, scale 0: -101

---

    Code
      write_parquet(d5, tmp, schema = schema5, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT64 DECIMAL with precision 2, scale 0: 101

---

    Code
      write_parquet(d5, tmp, schema = parquet_schema("DOUBLE"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Cannot convert an integer vector to Parquet type DOUBLE.

# POSIXct dictionary

    Code
      as.data.frame(read_parquet(tmp))
    Output
                          d
      1 2024-08-20 12:38:00
    Code
      read_parquet_metadata(tmp)[["column_chunks"]]$encodings
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

---

    Code
      write_parquet(d, tmp, schema = parquet_schema("INT32"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Cannot convert a POSIXct vector to Parquet type INT32.

---

    Code
      as.data.frame(read_parquet(tmp))
    Output
             x
      1 1 secs
    Code
      read_parquet_metadata(tmp)[["column_chunks"]]$encodings
    Output
      [[1]]
      [1] "PLAIN"          "RLE_DICTIONARY"
      

---

    Code
      write_parquet(d2, tmp, schema = parquet_schema("INT32"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Cannot convert a difftime vector to Parquet type INT32.

# more dictionaries

    Code
      write_parquet(d4, tmp, schema = schema4, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT32 DECIMAL with precision 2, scale 0: -101.000000

---

    Code
      write_parquet(d5, tmp, schema = schema5, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT32 DECIMAL with precision 2, scale 0: 101.000000

---

    Code
      write_parquet(d5, tmp, schema = schema, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Invalid bit width for INT32: 64

---

    Code
      write_parquet(d5, tmp, schema = schema, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Invalid bit width for INT32: 64

---

    Code
      write_parquet(d6, tmp, schema = parquet_schema("INT_8"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 8: 128.000000 at column 1

---

    Code
      write_parquet(d7, tmp, schema = parquet_schema("UINT_8"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Integer value too large for INT with bit width 8: 256.000000 at column 1.

---

    Code
      write_parquet(d8, tmp, schema = parquet_schema("UINT_8"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Negative values are not allowed in unsigned INT column: -1.000000 at column 1.

---

    Code
      write_parquet(d9, tmp, schema = schema4, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Value too small for INT64 DECIMAL with precision 2, scale 0: -101.000000

---

    Code
      write_parquet(d10, tmp, schema = schema5, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Value too large for INT64 DECIMAL with precision 2, scale 0: 101.000000

---

    Code
      write_parquet(d10, tmp, schema = parquet_schema("BYTE_ARRAY"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Cannot convert a double vector to Parquet type BYTE_ARRAY.

# Even more dictionaries

    Code
      write_parquet(d, tmp, schema = parquet_schema("UUID"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Invalid UUID value in column 1

---

    Code
      write_parquet(d2, tmp, schema = sch2, encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Invalid string length in FIXED_LEN_BYTE_ARRAY column: 9, should be 3.

---

    Code
      write_parquet(d2, tmp, schema = parquet_schema("DOUBLE"), encoding = "RLE_DICTIONARY")
    Condition
      Error in `write_parquet()`:
      ! Cannot convert a character vector to Parquet type DOUBLE.

# R -> Parquet mapping error

    Code
      infer_parquet_schema(d)
    Condition
      Error in `write_parquet()`:
      ! Cannot map a raw vector to any Parquet type

# argument errors

    Code
      write_parquet(mtcars, 1:10)
    Condition
      Error in `path.expand()`:
      ! invalid 'path' argument
    Code
      write_parquet(mtcars, letters)
    Condition
      Error in `write_parquet()`:
      ! nanoparquet_write: filename must be a string

