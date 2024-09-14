test_that("gzip compression levels", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- test_df()
  for (level in c(NA_integer_, 0:11)) {
    write_parquet(df, tmp, compression = "gzip", options = parquet_options(compression_level = level))
    expect_equal(as.data.frame(read_parquet(tmp)), as.data.frame(df))
  }
})
