test_that("read a subset", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- as.data.frame(test_df(missing = TRUE))
  write_parquet(df, tmp)

  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = 1:3)),
    df[, 1:3]
  )
  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = c(1:3, 13))),
    df[, c(1:3, 13)]
  )
  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = 10:13)),
    df[, 10:13]
  )
})

test_that("read a subset, edge cases", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- as.data.frame(test_df(missing = TRUE))
  write_parquet(df, tmp)

  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = 1)),
    df[, 1, drop = FALSE]
  )
  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = 13)),
    df[, 13, drop = FALSE]
  )
  expect_snapshot({
    read_parquet(tmp, col_select = integer())
  })
})

test_that("read a subset, factor to test Arrow metadata", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- as.data.frame(test_df(missing = TRUE, factor = TRUE))
  write_parquet(df, tmp)
  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = 10:14)),
    df[, 10:14]
  )
  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = 14)),
    df[, 14, drop = FALSE]
  )
})

test_that("class", {
  withr::local_options(nanoparquet.class = NULL)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  write_parquet(test_df(), tmp)
  expect_equal(class(read_parquet(tmp)), c("tbl", "data.frame"))
  expect_equal(
    class(read_parquet(
      tmp,
      options = parquet_options(class = c("foo", "bar", "data.frame"))
    )),
    c("foo", "bar", "data.frame")
  )
  withr::local_options(nanoparquet.class = "foobar")
  expect_equal(class(read_parquet(tmp)), c("foobar", "data.frame"))
})
