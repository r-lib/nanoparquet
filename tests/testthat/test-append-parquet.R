test_that("append_parquet creates file if it doesn't exist", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df1 <- test_df()
  append_parquet(df1, tmp)
  expect_true(file.exists(tmp))
  expect_equal(read_parquet(tmp), df1)
})

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

test_that("appending all-NA rows to REQUIRED columns gives a clear error", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df_valid <- data.frame(
    id = as.integer(1:5),
    value = as.numeric(1:5),
    label = c("A", "B", "C", "D", "E")
  )
  df_nas <- data.frame(
    id = as.integer(c(NA, NA, NA)),
    value = c(NA_real_, NA_real_, NA_real_),
    label = c(NA_character_, NA_character_, NA_character_)
  )

  write_parquet(df_valid, tmp)
  expect_snapshot(error = TRUE, {
    append_parquet(df_nas, tmp)
  })
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
