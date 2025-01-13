test_that("read_parquet_row_group", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  un <- function(x) {
    x <- as.data.frame(x)
    rownames(x) <- NULL
    x
  }

  do <- function(df, encoding = NULL) {
    write_parquet(
      df,
      tmp,
      row_groups = c(1L, 11L, 21L, 31L)
    )
    expect_snapshot(as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1])
    expect_equal(un(read_parquet_row_group(tmp, 0L)), un(df[1:10,]))
    expect_equal(un(read_parquet_row_group(tmp, 1L)), un(df[11:20,]))
    expect_equal(un(read_parquet_row_group(tmp, 2L)), un(df[21:30,]))
    expect_equal(un(read_parquet_row_group(tmp, 3L)), un(df[31:32,]))
  }

  do(test_df(), encoding = "PLAIN")
  do(test_df(missing = TRUE), encoding = "PLAIN")
  do(test_df(), encoding = c(large = "RLE", "RLE_DICTIONARY"))
  do(test_df(missing = TRUE), encoding = c(large = "RLE", "RLE_DICTIONARY"))
})
