# errors

    Code
      .Call(nanoparquet_write, mtcars, tempfile(), dim(mtcars), 0L, list(character(),
      character()), rep(FALSE, ncol(mtcars)), options, map_schema_to_df(NULL, mtcars),
      rep(10L, ncol(mtcars)), sys.call())
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

