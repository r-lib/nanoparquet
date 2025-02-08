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
  pf <- test_path("data/mixed.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
  })
  tab <- read_parquet(pf)
  expect_equal(tab$x, rep(0:399, 6))
  expect_equal(tab$y, rep(0:399, 6))
  expect_equal(tab$s, as.character(rep(0:399, 6)))
  expect_equal(tab$f, rep(0:399, 6))
  expect_equal(tab$d, rep(0:399, 6))
  expect_equal(tab$i96, rep(utcts(sprintf('%d-01-01', 1800:2199)), 6))

  pf <- test_path("data/mixed2.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
  })
  tab <- read_parquet(pf)
  expect_equal(tab$x, rep(0:399, 6))
  expect_equal(tab$y, rep(0:399, 6))
  expect_equal(tab$s, as.character(rep(0:399, 6)))
  expect_equal(tab$f, rep(0:399, 6))
  expect_equal(tab$d, rep(0:399, 6))
  expect_equal(tab$i96, rep(utcts(sprintf('%d-01-01', 1800:2199)), 6))

  skip_on_cran()
  pf <- test_path("data/mixed-miss.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
  })
  d1 <- as.data.frame(read_parquet(pf))
  d2 <- as.data.frame(arrow::read_parquet(pf))
  expect_equal(d1[,1:5], d2[,1:5])
  # arrow does not read INT86 into a time stamp, so compare manually
  expect_equal(is.na(d1[,6]), is.na(d2[,6]))
  bs6 <- utcts(sprintf('%d-01-01', 1:2400))
  bs6[is.na(d1[,6])] <- NA
  expect_equal(d1[,6], bs6)
})

test_that("mixing RLE_DICTIONARY and PLAIN, DECIMAL", {
  skip_on_cran()
  pf <- test_path("data/decimal.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
  })
  t1 <- read_parquet(pf)
  t2 <- arrow::read_parquet(pf)
  expect_equal(
    as.data.frame(t1),
    as.data.frame(t2)
  )

  pf <- test_path("data/decimal2.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
  })
  t1 <- as.data.frame(read_parquet(pf))
  t2 <- as.data.frame(arrow::read_parquet(pf))
  expect_equal(t1[,1], t2[,1])
  expect_equal(t1[,2], t2[,2])
  expect_equal(t1[,3], t2[,3])
  expect_equal(t1[,4], t2[,4])
})

test_that("mixing RLE_DICTIONARY and PLAIN, BYTE_ARRAY", {
  skip_on_cran()
  pf <- test_path("data/binary.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
  })
  t1 <- as.data.frame(read_parquet(pf))
  t2 <- as.data.frame(arrow::read_parquet(pf))
  expect_equal(t1[,1], unclass(t2[,1]))
  expect_equal(t1[,2], unclass(t2[,2]))
})

test_that("mixing RLE_DICTIONARY and PLAIN, FLOAT16", {
  skip_on_cran()
  pf <- test_path("data/float16.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf)[, c("type", "repetition_type")])
    as.data.frame(read_parquet_pages(pf)[, c("page_type", "num_values", "encoding")])
  })
  t1 <- as.data.frame(read_parquet(pf))
  t2 <- as.data.frame(arrow::read_parquet(pf))
  # arrow is buggy, even the missingness pattern is wrong :(
  expect_equal(t1[,1], rep(0:399, 3))
  expect_equal(
    which(is.na(t1[,2])),
    c(30, 66, 422, 568, 878, 947, 988, 1006, 1170, 1183) + 1
  )
  bs2 <- rep(0:399, 3)
  bs2[is.na(t1[,2])] <- NA
  expect_equal(t1[,2], bs2)
})
