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
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    tapply(pgs$num_values, pgs$column + 1, sum)
  })
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    tapply(pgs$num_values, pgs$column + 1, sum)
  })
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  # data page v2
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "uncompressed")
  expect_equal(as.data.frame(read_parquet(tmp)), d)
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2))
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
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    tapply(pgs$num_values, pgs$column + 1, sum)
  })
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    tapply(pgs$num_values, pgs$column + 1, sum)
  })
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  # data page v2
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "uncompressed")
  expect_equal(as.data.frame(read_parquet(tmp)), d)
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2))
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
  pgs <- read_parquet_pages(tmp)
  expect_equal(sum(pgs$num_values[pgs$page_type == "DATA_PAGE"]), nrow(d))
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_equal(sum(pgs$num_values[pgs$page_type == "DATA_PAGE"]), nrow(d))
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  # data page v2
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "uncompressed")
  expect_equal(as.data.frame(read_parquet(tmp)), d)
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "snappy")
  expect_equal(as.data.frame(read_parquet(tmp)), d)
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "gzip")
  expect_equal(as.data.frame(read_parquet(tmp)), d)
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "zstd")
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
  pgs <- read_parquet_pages(tmp)
  expect_equal(sum(pgs$num_values[pgs$page_type == "DATA_PAGE"]), nrow(d))
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_equal(sum(pgs$num_values[pgs$page_type == "DATA_PAGE"]), nrow(d))
  expect_equal(as.data.frame(read_parquet(tmp)), d)

  # data page v2
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "uncompressed")
  expect_equal(as.data.frame(read_parquet(tmp)), d)
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "snappy")
  expect_equal(as.data.frame(read_parquet(tmp)), d)
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "gzip")
  expect_equal(as.data.frame(read_parquet(tmp)), d)
  write_parquet(d, tmp, options = parquet_options(write_data_page_version = 2), compression = "zstd")
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

test_that("write_parquet() to memory 2", {
  # https://github.com/r-lib/nanoparquet/issues/77
  data <- data.frame(
    TEST_1 = seq(as.Date("2000/1/1"), by = "day", length.out = 100000),
    TEST_2 = seq(1:100000),
    TEST_3 = seq(as.Date("1990/1/1"), by = "day", length.out = 100000),
    TEST_2 = seq(100000:1)
  )

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  r1 <- write_parquet(data, ":raw:")
  write_parquet(data, tmp)
  r2 <- readBin(tmp, what = "raw", file.size(tmp))
  expect_equal(r1, r2)
})

test_that("write_parquet() to stdout", {
  skip_on_cran()
  tmp1 <- tempfile(fileext = ".parquet")
  tmp2 <- tempfile(fileext = ".parquet")
  script <- tempfile(fileext = ".R")
  on.exit(unlink(c(tmp1, tmp2, script)), add = TRUE)

  txt <- c(
    if (!is_rcmd_check()) "pkgload::load_all()",
    "nanoparquet::write_parquet(mtcars, \":stdout:\")"
  )
  writeLines(txt, script)
  processx::run(rscript(), script, stdout = tmp1, stderr = NULL)
  r1 <- readBin(tmp1, "raw", file.size(tmp1))
  write_parquet(mtcars, tmp2)
  r2 <- readBin(tmp2, "raw", file.size(tmp2))
  expect_equal(file.size(tmp1), file.size(tmp2))
  expect_equal(r1, r2)
})

test_that("gzip compression", {
  d <- test_df(missing = TRUE)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(d, tmp, compression = "gzip")
  expect_equal(read_parquet_page(tmp, 4L)$codec, "GZIP")
  expect_equal(read_parquet(tmp), d);
})

test_that("zstd compression", {
  d <- test_df(missing = TRUE)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(d, tmp, compression = "zstd")
  expect_equal(read_parquet_page(tmp, 4L)$codec, "ZSTD")
  expect_equal(read_parquet(tmp), d);
})
