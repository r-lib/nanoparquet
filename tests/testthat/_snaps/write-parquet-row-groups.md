# errors

    Code
      parquet_options(num_rows_per_row_group = "foobar")
    Condition
      Error in `as_count()`:
      ! num_rows_per_row_group must be a count, i.e. an integer scalar

---

    Code
      write_parquet(df, tmp, row_groups = "foobar")
    Condition
      Error in `parse_row_groups()`:
      ! Row groups must be specified as a growing positive integer vector, starting with 1.
    Code
      write_parquet(df, tmp, row_groups = c(100L, 1L))
    Condition
      Error in `parse_row_groups()`:
      ! Row groups must be specified as a growing positive integer vector, starting with 1.
    Code
      write_parquet(df, tmp, row_groups = c(1L, 100L))
    Condition
      Error in `write_parquet()`:
      ! Internal nanoparquet error, row index too large

# non-factors write local dictionary

    Code
      for (do in dict_ofs) {
        print(read_parquet_page(tmp, do)[["data"]])
      }
    Output
      [1] 01 00 00 00 61
      [1] 01 00 00 00 61
       [1] 01 00 00 00 61 01 00 00 00 62
      [1] 01 00 00 00 62
      [1] 01 00 00 00 62
      [1] 01 00 00 00 63
      [1] 01 00 00 00 63
      [1] 01 00 00 00 63

