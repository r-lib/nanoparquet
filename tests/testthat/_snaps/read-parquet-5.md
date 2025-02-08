# read a subset, edge cases

    Code
      read_parquet(tmp, col_select = integer())
    Output
      # A data frame: 32 x 0
    Code
      read_parquet(tmp, col_select = character())
    Output
      # A data frame: 32 x 0

# error if a column is requested multiple times

    Code
      read_parquet(tmp, col_select = c(1, 1))
    Condition
      Error in `read_parquet()`:
      ! Column 1 selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = c(3, 4, 5, 3))
    Condition
      Error in `read_parquet()`:
      ! Column 3 selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = c(3:4, 4:3))
    Condition
      Error in `read_parquet()`:
      ! Columns 3, 4 selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = "foo")
    Condition
      Error in `read_parquet()`:
      ! Column foo does not exist in Parquet file
    Code
      read_parquet(tmp, col_select = c("foo", "bar"))
    Condition
      Error in `read_parquet()`:
      ! Columns foo, bar do not exist in Parquet file
    Code
      read_parquet(tmp, col_select = c("nam", "nam"))
    Condition
      Error in `read_parquet()`:
      ! Column nam selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = c("cyl", "disp", "hp", "cyl"))
    Condition
      Error in `read_parquet()`:
      ! Column cyl selected multiple times in `read_parquet()`.
    Code
      read_parquet(tmp, col_select = c("cyl", "disp", "disp", "cyl"))
    Condition
      Error in `read_parquet()`:
      ! Columns cyl, disp selected multiple times in `read_parquet()`.

# mixing RLE_DICTIONARY and PLAIN

    Code
      as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    Output
              type repetition_type
      1       <NA>        REQUIRED
      2      INT32        REQUIRED
      3      INT64        REQUIRED
      4 BYTE_ARRAY        REQUIRED
      5      FLOAT        REQUIRED
      6     DOUBLE        REQUIRED
      7      INT96        REQUIRED
    Code
      as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
    Output
               page_type num_values       encoding
      1  DICTIONARY_PAGE        400          PLAIN
      2        DATA_PAGE       1024 RLE_DICTIONARY
      3        DATA_PAGE       1024          PLAIN
      4        DATA_PAGE        352          PLAIN
      5  DICTIONARY_PAGE        400          PLAIN
      6        DATA_PAGE       1024 RLE_DICTIONARY
      7        DATA_PAGE       1024          PLAIN
      8        DATA_PAGE        352          PLAIN
      9  DICTIONARY_PAGE        400          PLAIN
      10       DATA_PAGE       1024 RLE_DICTIONARY
      11       DATA_PAGE       1024          PLAIN
      12       DATA_PAGE        352          PLAIN
      13 DICTIONARY_PAGE        400          PLAIN
      14       DATA_PAGE       1024 RLE_DICTIONARY
      15       DATA_PAGE       1024          PLAIN
      16       DATA_PAGE        352          PLAIN
      17 DICTIONARY_PAGE        400          PLAIN
      18       DATA_PAGE       1024 RLE_DICTIONARY
      19       DATA_PAGE       1024          PLAIN
      20       DATA_PAGE        352          PLAIN
      21 DICTIONARY_PAGE        400          PLAIN
      22       DATA_PAGE       1024 RLE_DICTIONARY
      23       DATA_PAGE       1024          PLAIN
      24       DATA_PAGE        352          PLAIN

---

    Code
      as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    Output
              type repetition_type
      1       <NA>        REQUIRED
      2      INT32        REQUIRED
      3      INT64        REQUIRED
      4 BYTE_ARRAY        REQUIRED
      5      FLOAT        REQUIRED
      6     DOUBLE        REQUIRED
      7      INT96        REQUIRED
    Code
      as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
    Output
               page_type num_values       encoding
      1  DICTIONARY_PAGE        400          PLAIN
      2        DATA_PAGE       1024 RLE_DICTIONARY
      3        DATA_PAGE       1024 RLE_DICTIONARY
      4        DATA_PAGE        352 RLE_DICTIONARY
      5  DICTIONARY_PAGE        400          PLAIN
      6        DATA_PAGE       1024 RLE_DICTIONARY
      7        DATA_PAGE       1024 RLE_DICTIONARY
      8        DATA_PAGE        352 RLE_DICTIONARY
      9  DICTIONARY_PAGE        400          PLAIN
      10       DATA_PAGE       1024 RLE_DICTIONARY
      11       DATA_PAGE       1024 RLE_DICTIONARY
      12       DATA_PAGE        352 RLE_DICTIONARY
      13 DICTIONARY_PAGE        400          PLAIN
      14       DATA_PAGE       1024 RLE_DICTIONARY
      15       DATA_PAGE       1024 RLE_DICTIONARY
      16       DATA_PAGE        352 RLE_DICTIONARY
      17 DICTIONARY_PAGE        400          PLAIN
      18       DATA_PAGE       1024 RLE_DICTIONARY
      19       DATA_PAGE       1024 RLE_DICTIONARY
      20       DATA_PAGE        352 RLE_DICTIONARY
      21 DICTIONARY_PAGE        400          PLAIN
      22       DATA_PAGE       1024 RLE_DICTIONARY
      23       DATA_PAGE       1024 RLE_DICTIONARY
      24       DATA_PAGE        352 RLE_DICTIONARY

---

    Code
      as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    Output
              type repetition_type
      1       <NA>        REQUIRED
      2      INT32        OPTIONAL
      3      INT64        OPTIONAL
      4 BYTE_ARRAY        OPTIONAL
      5      FLOAT        OPTIONAL
      6     DOUBLE        OPTIONAL
      7      INT96        OPTIONAL
    Code
      as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
    Output
               page_type num_values       encoding
      1  DICTIONARY_PAGE       1024          PLAIN
      2        DATA_PAGE       1024 RLE_DICTIONARY
      3        DATA_PAGE       1024          PLAIN
      4        DATA_PAGE        352          PLAIN
      5  DICTIONARY_PAGE       1024          PLAIN
      6        DATA_PAGE       1024 RLE_DICTIONARY
      7        DATA_PAGE       1024          PLAIN
      8        DATA_PAGE        352          PLAIN
      9  DICTIONARY_PAGE       1024          PLAIN
      10       DATA_PAGE       1024 RLE_DICTIONARY
      11       DATA_PAGE       1024          PLAIN
      12       DATA_PAGE        352          PLAIN
      13 DICTIONARY_PAGE       1024          PLAIN
      14       DATA_PAGE       1024 RLE_DICTIONARY
      15       DATA_PAGE       1024          PLAIN
      16       DATA_PAGE        352          PLAIN
      17 DICTIONARY_PAGE       1024          PLAIN
      18       DATA_PAGE       1024 RLE_DICTIONARY
      19       DATA_PAGE       1024          PLAIN
      20       DATA_PAGE        352          PLAIN
      21 DICTIONARY_PAGE       1024          PLAIN
      22       DATA_PAGE       1024 RLE_DICTIONARY
      23       DATA_PAGE       1024          PLAIN
      24       DATA_PAGE        352          PLAIN

