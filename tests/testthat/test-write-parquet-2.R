test_that("REQ PLAIN", {
  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "1")
  withr::local_envvar(NANOPARQUEST_PAGE_SIZE = "8192") # 8k pages
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  N <- 10000
  d <- data.frame(
    stringsAsFactors = FALSE,
    i = rep(c(1L, 2L, 2L, 2L, 3L, 3L, 3L, 3L), N/8),
    r = rep(c(1, 1, 1, 1, 1, 1, 2, 10), N/8),
    l = rep(c(TRUE, FALSE, FALSE, FALSE, TRUE, FALSE, TRUE, TRUE), N/8),
    s = rep(c("A", "A", "A", "B", "C", "D", "D", "D"), N/8)
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- parquet_pages(tmp)
  expect_snapshot({
    tapply(pgs$num_values, pgs$column + 1, sum)
  })
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  write_parquet(d, tmp, compression = "snappy")
  pgs <- parquet_pages(tmp)
  pgs <- parquet_pages(tmp)
  expect_snapshot({
    tapply(pgs$num_values, pgs$column + 1, sum)
  })
  expect_equal(as.data.frame(read_parquet(tmp)), d)
})

test_that("OPT PLAIN", {
  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "1")
  withr::local_envvar(NANOPARQUEST_PAGE_SIZE = "8192") # 8k pages
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  N <- 10000
  d <- data.frame(
    stringsAsFactors = FALSE,
    i = rep(c(1L, NA, 2L, NA, NA, 3L, 3L, 3L), N/8),
    r = rep(c(1, 1, 1, 1, 1, 1, NA, NA), N/8),
    l = rep(c(TRUE, FALSE, NA, NA, TRUE, FALSE, TRUE, NA), N/8),
    s = rep(c(NA, NA, "A", "B", "C", NA, "D", NA), N/8)
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- parquet_pages(tmp)
  expect_snapshot({
    tapply(pgs$num_values, pgs$column + 1, sum)
  })
  d2 <- read_parquet(tmp)
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  write_parquet(d, tmp, compression = "snappy")
  pgs <- parquet_pages(tmp)
  expect_snapshot({
    tapply(pgs$num_values, pgs$column + 1, sum)
  })
  expect_equal(as.data.frame(read_parquet(tmp)), d)
})

test_that("REQ RLE_DICT", {
  withr::local_envvar(NANOPARQUEST_PAGE_SIZE = "8192") # 8k pages
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # the index is bit-packed, so we need a bit more of this
  N <- 100000
  d <- data.frame(
    stringsAsFactors = FALSE,
    f = rep(as.factor(c("A", "A", "B", "B", "C", "C", "D", "E")), N/8)
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- parquet_pages(tmp)
  expect_equal(sum(pgs$num_values[pgs$page_type == "DATA_PAGE"]), nrow(d))
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  write_parquet(d, tmp, compression = "snappy")
  pgs <- parquet_pages(tmp)
  expect_equal(sum(pgs$num_values[pgs$page_type == "DATA_PAGE"]), nrow(d))
  expect_equal(as.data.frame(read_parquet(tmp)), d)
})

test_that("OPT RLE_DICT", {
  withr::local_envvar(NANOPARQUEST_PAGE_SIZE = "8192") # 8k pages
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # the index is bit-packed, so we need a bit more of this
  N <- 100000
  d <- data.frame(
    stringsAsFactors = FALSE,
    f = rep(as.factor(c(NA, "A", "B", NA, NA, "C", "D", "E")), N/8)
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- parquet_pages(tmp)
  expect_equal(sum(pgs$num_values[pgs$page_type == "DATA_PAGE"]), nrow(d))
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  write_parquet(d, tmp, compression = "snappy")
  pgs <- parquet_pages(tmp)
  expect_equal(sum(pgs$num_values[pgs$page_type == "DATA_PAGE"]), nrow(d))
  expect_equal(as.data.frame(read_parquet(tmp)), d)
})

test_that("write_parquet() to memory", {
  d <- test_df(missing = TRUE)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  pm <- write_parquet(d, ":raw:")
  write_parquet(d, tmp)
  pm2 <- readBin(tmp, "raw", file.size(tmp))

  expect_equal(pm, pm2)
})