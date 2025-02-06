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
  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = c("cyl", "mpg", "nam"))),
    df[, c("cyl", "mpg", "nam")]
  )
  expect_equal(
    as.data.frame(read_parquet(
      tmp,
      col_select = c("am", "gear", "carb", "large"))
    ),
    df[, c("am", "gear", "carb", "large")]
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
  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = "nam")),
    df[, "nam", drop = FALSE]
  )
  expect_snapshot({
    read_parquet(tmp, col_select = integer())
    read_parquet(tmp, col_select = character())
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

test_that("subset column order", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- as.data.frame(test_df(missing = TRUE, factor = TRUE))
  write_parquet(df, tmp)

  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = 3:1)),
    df[, 3:1]
  )
  expect_equal(
    as.data.frame(read_parquet(tmp, col_select = c("cyl", "mpg", "nam"))),
    df[, c("cyl", "mpg", "nam")]
  )
})

test_that("error if a column is requested multiple times", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- as.data.frame(test_df(missing = TRUE, factor = TRUE))
  write_parquet(df, tmp)

  expect_snapshot(error = TRUE, {
    read_parquet(tmp, col_select = c(1, 1))
    read_parquet(tmp, col_select = c(3,4,5,3))
    read_parquet(tmp, col_select = c(3:4,4:3))
    read_parquet(tmp, col_select = "foo")
    read_parquet(tmp, col_select = c("foo", "bar"))
    read_parquet(tmp, col_select = c("nam", "nam"))
    read_parquet(tmp, col_select = c("cyl", "disp", "hp", "cyl"))
    read_parquet(tmp, col_select = c("cyl", "disp", "disp", "cyl"))
  })
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

test_that("mixing RLE_DICTIONARY and PLAIN", {
  # https://github.com/r-lib/nanoparquet/issues/110
  # import pyarrow as pa
  # import pyarrow.parquet as pq
  # table = pa.table({'x': pa.array(range(2000), type=pa.int32())})
  # pq.write_table(table, 'mixed-int32.parquet', dictionary_pagesize_limit = 400)
  pf <- test_path("data/mixed-int32.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
  })
  expect_equal(read_parquet(pf)$x, 0:1999)
})
