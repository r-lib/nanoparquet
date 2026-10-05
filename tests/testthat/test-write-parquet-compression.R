test_that("gzip compression levels", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- test_df()
  for (level in c(NA_integer_, 0:11)) {
    write_parquet(
      df,
      tmp,
      compression = "gzip",
      options = parquet_options(compression_level = level)
    )
    expect_equal(as.data.frame(read_parquet(tmp)), as.data.frame(df))
  }
})

test_that("zstd compression levels", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- test_df()
  for (level in c(NA_integer_, (-22):(22))) {
    write_parquet(
      df,
      tmp,
      compression = "zstd",
      options = parquet_options(compression_level = level)
    )
    expect_equal(as.data.frame(read_parquet(tmp)), as.data.frame(df))
  }
})

test_that("zstd default compression level is 3", {
  df <- test_df()
  expect_identical(
    write_parquet(df, ":raw:", compression = "zstd"),
    write_parquet(
      df,
      ":raw:",
      compression = "zstd",
      options = parquet_options(compression_level = 3)
    )
  )
})
