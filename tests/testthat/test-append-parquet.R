test_that("keep all row groups", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df1 <- test_df(missing = TRUE)
  write_parquet(df1, tmp)
  mtd1 <- read_parquet_metadata(tmp)

  df2 <- test_df()
  append_parquet(
    df2,
    tmp,
    options = parquet_options(keep_row_groups = TRUE)
  )
  df3 <- read_parquet(tmp)
  mtd2 <- read_parquet_metadata(tmp)

  df21 <- utils::head(df3, nrow(df1))
  df22 <- utils::tail(df3, nrow(df2))
  row.names(df22) <- NULL
  expect_equal(df21, df1)
  expect_equal(df22, df2)
  expect_equal(mtd1$file_meta_data[-3], mtd2$file_meta_data[-3])

  expect_equal(nrow(mtd2$row_groups), 2L)
  expect_equal(
    mtd1$column_chunks,
    utils::head(mtd2$column_chunks, nrow(mtd1$column_chunks))
  )
})

test_that("overwrite last row group", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df1 <- test_df(missing = TRUE)
  write_parquet(df1, tmp, row_groups = c(1L, 31L))
  mtd1 <- read_parquet_metadata(tmp)

  df2 <- test_df()
  append_parquet(
    df2,
    tmp,
    options = parquet_options(num_rows_per_row_group = 30)
  )
  df3 <- read_parquet(tmp)
  mtd2 <- read_parquet_metadata(tmp)

  df21 <- utils::head(df3, nrow(df1))
  df22 <- utils::tail(df3, nrow(df2))
  row.names(df22) <- NULL
  expect_equal(df21, df1)
  expect_equal(df22, df2)
  expect_equal(mtd1$file_meta_data[-3], mtd2$file_meta_data[-3])

  # the first column chunk of row group 1 is at the same position as before
  ccs1 <- mtd1$column_chunks
  ccs2 <- mtd2$column_chunks
  idx1 <- which(ccs1$row_group == 1)[1]
  idx2 <- which(ccs2$row_group == 1)[1]
  expect_equal(idx1, idx2)
  expect_equal(ccs1$file_offset[idx1], ccs2$file_offset[idx2])
})

test_that("overwrite a single row groups", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df1 <- test_df(missing = TRUE)
  write_parquet(df1, tmp)
  mtd1 <- read_parquet_metadata(tmp)

  df2 <- test_df()
  append_parquet(df2, tmp)
  df3 <- read_parquet(tmp)
  mtd2 <- read_parquet_metadata(tmp)

  df21 <- utils::head(df3, nrow(df1))
  df22 <- utils::tail(df3, nrow(df2))
  row.names(df22) <- NULL
  expect_equal(df21, df1)
  expect_equal(df22, df2)
  expect_equal(mtd1$file_meta_data[-3], mtd2$file_meta_data[-3])
})
