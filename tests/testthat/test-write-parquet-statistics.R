test_that("null_count is written", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- test_df(missing = TRUE)
  write_parquet(
    df, tmp,
    options = parquet_options(num_rows_per_row_group = 10)
  )
  expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
  expect_snapshot(
    as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]][
      , c("row_group", "column", "null_count")
    ])
  )
})
