test_that("parquet_pages", {
  pp <- parquet_pages(test_path("data/mtcars-arrow.parquet"))
  expect_snapshot(as.data.frame(pp))
})

test_that("read_parquet_page", {
  pf <- test_path("data/mtcars-arrow.parquet")
  page <- read_parquet_page(pf, 4)
  expect_snapshot(page)
  page2 <- read_parquet_page(pf, 444)
  expect_snapshot(page2)
})

test_that("read_parquet_page for trick v2 data page", {
  pf <- test_path("data/rle_boolean_encoding.parquet")
  expect_snapshot({
    read_parquet_page(pf, 4L)
  })
})

test_that("read_parquet_page error", {
  # https://github.com/llvm/llvm-project/issues/59432
  if (is_asan()) skip("ASAN bug")
  pf <- test_path("data/mtcars-arrow.parquet")
  expect_error(read_parquet_page(pf, 5))
})

test_that("snappy", {
  for (i in 1:10) {
    x <- as.raw(as.integer(runif(1000) * 100))
    comp <- snappy_compress(x)
    uncp <- snappy_uncompress(comp)
    expect_false(length(x) == length(comp) && all(x == comp))
    expect_equal(uncp, x)
  }
})

test_that("snappy error", {
  expect_error(snappy_uncompress(charToRaw("foobar")))
})

test_that("MemStream", {
  expect_snapshot({
    .Call(test_memstream)
    cat(rawToChar(.Call(test_memstream)))
  })
})

test_that("DELTA_BIT_PACKED decoding", {
  skip_on_cran()

  dodbp <- function(df, path) {
    write_parquet(df, path)
    pyscript <- sprintf(r"[
import pyarrow
import pyarrow.parquet as pq
path = "%s"
df = pq.read_table(path)
writer = pq.ParquetWriter(path, df.schema, use_dictionary = False, column_encoding = 'DELTA_BINARY_PACKED')
writer.write_table(df)
writer.close()
]", normalizePath(path, winslash = "/"))
    pytmp <- tempfile(fileext = ".py")
    on.exit(unlink(pytmp), add = TRUE)
    writeLines(pyscript, pytmp)
    py <- if (Sys.which("python3") != "") "python3" else "python"
    processx::run(py, pytmp, stderr = "2>&1", error_on_status = FALSE)
  }

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(x = c(1:100, 500:600 * 2L))
  stat <- dodbp(d, tmp)
  expect_snapshot(stat)
  dbp <- read_parquet_page(tmp, 4L)$data
  expect_equal(dbp_decode_int(dbp), d$x)

  d <- data.frame(x = c(-(101:200) * 2L + 1:10))
  stat <- dodbp(d, tmp)
  expect_snapshot(stat)
  dbp <- read_parquet_page(tmp, 4L)$data
  expect_equal(dbp_decode_int(dbp), d$x)

  d <- data.frame(x = c(-(101:200) * 2L + 1:10, as.integer(rnorm(123)* 100L + 1000L)))
  stat <- dodbp(d, tmp)
  expect_snapshot(stat)
  dbp <- read_parquet_page(tmp, 4L)$data
  expect_equal(dbp_decode_int(dbp), d$x)

  d <- data.frame(x = 0L)
  stat <- dodbp(d, tmp)
  expect_snapshot(stat)
  dbp <- read_parquet_page(tmp, 4L)$data
  expect_equal(dbp_decode_int(dbp), d$x)

  # large integers
  d <- data.frame(x = c(
    as.integer(2^31-1 - runif(100) * 10L),
    as.integer(-2^31+1 + runif(100) * 10L)
  ))
  stat <- dodbp(d, tmp)
  expect_snapshot(stat)
  dbp <- read_parquet_page(tmp, 4L)$data
  expect_equal(dbp_decode_int(dbp), d$x)
})

test_that("DELTA_BINARY_PACKED edge cases", {
  pf <- test_path("data/issue10279_delta_encoding.parquet")
  pgs <- parquet_pages(pf)
  expect_snapshot({
    # this is BOOLEAN in the file, which should not happen AFAICT
    p1 <- read_parquet_page(pf, pgs$page_header_offset[1])
    dbp1 <- p1$data[
      (p1$definition_levels_byte_length +
      p1$repetition_levels_byte_length + 1):length(p1$data)]
    dbp_decode_int(dbp1)

    p3 <- read_parquet_page(pf, pgs$page_header_offset[3])
    dbp3 <- p3$data
    dbp_decode_int(dbp3)

    p4 <- read_parquet_page(pf, pgs$page_header_offset[4])
    dbp4 <- p4$data
    dbp_decode_int(dbp4)

    # unfortunately the minimum int32 value is NA in R
    # not sure what to do about this...
    p5<- read_parquet_page(pf, pgs$page_header_offset[5])
    dbp5 <- p5$data
    dbp_decode_int(dbp5)
  })
})

test_that("DELTA_BIANRY_PACKED INT64", {
  suppressPackageStartupMessages(library(bit64))
  pf <- test_path("data/dbp-int64.parquet")
  dt <- read_parquet_page(pf, 4L)$data
  on.exit(close(con), add = TRUE)
  len <- readBin(con <- rawConnection(dt), "integer", 1)
  dt <- dt[(len + 4 + 1):length(dt)]
  expect_snapshot({
    dbp_decode_int(dt)
  })
})
