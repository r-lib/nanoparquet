test_that("factors are written as strings", {
  mt <- test_df(factor = TRUE)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp)
  expect_snapshot(
    as.data.frame(read_parquet_schema(tmp))[
      c("name", "type", "converted_type", "logical_type")
    ]
  )
})

test_that("round trip", {
  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "1")
  mt <- test_df()
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  expect_true(all(read_parquet(tmp) == mt))
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  expect_true(all(read_parquet(tmp) == mt))
})

test_that("round trip with arrow", {
  # Don't want to skip on the parquet capability missing, because then
  # this might not be tested on the CI. So rather we skip on CRAN.
  skip_on_cran()
  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "1")
  mt <- test_df(tibble = TRUE, factor = TRUE)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  mt2 <- arrow::read_parquet(tmp)
  expect_equal(mt2, mt)
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  mt2 <- arrow::read_parquet(tmp)
  expect_equal(mt2, mt)
})

test_that("round trip with duckdb", {
  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "1")
  mt <- as.data.frame(test_df())
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  df <- duckdb:::sql(sprintf("FROM '%s'", tmp))
  expect_equal(df, mt)
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  df <- duckdb:::sql(sprintf("FROM '%s'", tmp))
  expect_equal(df, mt)
})

test_that("round trip with pandas/pyarrow", {
  skip_on_cran()
  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "1")
  mt <- test_df()
  tmp1 <- tempfile(fileext = ".parquet")
  tmp2 <- tempfile(fileext = ".parquet")
  on.exit(unlink(c(tmp1, tmp2)), add = TRUE)
  # need to create to be able to call normalizePath()
  file.create(tmp1)
  file.create(tmp2)

  py_read <- function(input, output) {
    pyscript <- sprintf(r"[
import pyarrow
import pandas
pandas.set_option("display.width", 150)
pandas.set_option("display.max_columns", 150)
pandas.set_option("display.max_colwidth", 150)
df = pandas.read_parquet("%s", engine = "pyarrow")
print(df)
print(df.dtypes)
df.to_parquet("%s", engine = "pyarrow")
]", normalizePath(input, winslash = "/"), normalizePath(output, winslash = "/"))
    pytmp <- tempfile(fileext = ".py")
    on.exit(unlink(pytmp), add = TRUE)
    writeLines(pyscript, pytmp)
    py <- if (Sys.which("python3") != "") "python3" else "python"
    processx::run(py, pytmp, stderr = "2>&1", error_on_status = FALSE)
  }

  write_parquet(mt, tmp1, compression = "uncompressed")
  pyout <- py_read(tmp1, tmp2)
  if (pyout$status != 0) {
    stop("Failed to run Python, are all packages installed?")
  }
  expect_snapshot(writeLines(pyout$stdout))

  mt2 <- read_parquet(tmp2)
  expect_equal(mt2, mt)
})

test_that("errors", {
  # https://github.com/llvm/llvm-project/issues/59432
  if (is_asan()) skip("ASAN bug")

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  mt <- mt2 <- test_df(factor = TRUE)
  mt$list <- I(replicate(nrow(mt), 1:4, simplify = FALSE))

  expect_error(write_parquet(mt, tmp))
  expect_snapshot(error = TRUE, write_parquet(mt, tmp))

  expect_error(write_parquet(mt, 1:10))
  expect_snapshot(error = TRUE, write_parquet(mt, 1:10))

  expect_error(write_parquet(mt2, tmp, metadata = "bad"))
  expect_snapshot(error = TRUE, write_parquet(mt2, tmp, metadata = "bad"))
  expect_error(write_parquet(mt2, tmp, metadata = mtcars))
  expect_snapshot(error = TRUE, write_parquet(mt2, tmp, metadata = mtcars))
})

test_that("writing metadata", {
  mt <- mt2 <- test_df()
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, metadata = c("foo" = "bar"))
  kvm <- read_parquet_metadata(tmp)$file_meta_data$key_value_metadata[[1]]
  expect_snapshot(as.data.frame(kvm)[1,])
})

test_that("strings are converted to UTF-8", {
  utf8 <- paste(
    "\u0043\u0073\u00e1\u0072\u0064\u0069",
    "\u0047\u00e1\u0062\u006f\u0072"
  )
  df <- data.frame(
    stringsAsFactors = FALSE,
    utf8 = utf8,
    latin1 = iconv(utf8, "UTf-8", "latin1")
  )
  expect_snapshot({
    charToRaw(utf8)
    charToRaw(df$utf8)
    charToRaw(df$latin1)
  })

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  write_parquet(df, tmp)
  df2 <- read_parquet(tmp)

  expect_snapshot({
    charToRaw(utf8)
    charToRaw(df2$utf8)
    charToRaw(df2$latin1)
  })
})

test_that("REQ PLAIN", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    stringsAsFactors = FALSE,
    i = c(1L, 2L, 2L, 2L, 3L, 3L, 3L, 3L),
    r = c(1, 1, 1, 1, 1, 1, 2, 10),
    l = c(TRUE, FALSE, FALSE, FALSE, TRUE, FALSE, TRUE, TRUE),
    s = c("A", "A", "A", "B", "C", "D", "D", "D")
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
    read_parquet_page(tmp, pgs$page_header_offset[2])$data
    read_parquet_page(tmp, pgs$page_header_offset[3])$data
    read_parquet_page(tmp, pgs$page_header_offset[4])$data
  })

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
    read_parquet_page(tmp, pgs$page_header_offset[2])$data
    read_parquet_page(tmp, pgs$page_header_offset[3])$data
    read_parquet_page(tmp, pgs$page_header_offset[4])$data
  })

})

test_that("OPT PLAIN", {
  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "1")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    stringsAsFactors = FALSE,
    i = c(1L, NA, 2L, NA, NA, 3L, 3L, 3L),
    r = c(1, 1, 1, 1, 1, 1, NA, NA),
    l = c(TRUE, FALSE, NA, NA, TRUE, FALSE, TRUE, NA),
    s = c(NA, NA, "A", "B", "C", NA, "D", NA)
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[1])$data)
    readBin(data[-(1:6)], "integer", sum(!is.na(d$i)))
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[2])$data)
    readBin(data[-(1:6)], "double", sum(!is.na(d$r)))
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[3])$data)
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[4])$data)
  })

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[1])$data)
    readBin(data[-(1:6)], "integer", sum(!is.na(d$i)))
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[2])$data)
    readBin(data[-(1:6)], "double", sum(!is.na(d$r)))
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[3])$data)
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[4])$data)
  })
})

test_that("REQ RLE_DICT", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    stringsAsFactors = FALSE,
    f = as.factor(c("A", "A", "B", "B", "C", "C", "D", "E"))
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[2])$data)
    rle_decode_int(data[-1], bit_width = as.integer(data[1]), nrow(d))
  })

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
    read_parquet_page(tmp, pgs$page_header_offset[2])$data
  })
})

test_that("OPT RLE_DICT", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    stringsAsFactors = FALSE,
    f = as.factor(c(NA, "A", "B", NA, NA, "C", "D", "E"))
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[2])$data)
    rle_decode_int(
      data[-(1:7)],
      bit_width = as.integer(data[7]),
      sum(!is.na(d$f))
    )
  })

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
    read_parquet_page(tmp, pgs$page_header_offset[2])$data
  })
})

test_that("REQ RLE", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    l = c(logical(30), !logical(5), logical(20), !logical(30))
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_metadata(tmp)$column_chunks$encodings
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[1])$data)
  })
  expect_equal(
    rle_decode_int(data[-(1:4)], bit_width = 1L, nrow(d)),
    d$l + 0L
  )

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_metadata(tmp)$column_chunks$encodings
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
  })
})

test_that("OPT RLE", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    l = c(NA, logical(30), !logical(5), logical(20), NA, !logical(30))
  )

  write_parquet(d, tmp, compression = "uncompressed")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_metadata(tmp)$column_chunks$encodings
    data <- print(read_parquet_page(tmp, pgs$page_header_offset[1])$data)
  })
  expect_equal(
    rle_decode_int(data[-(1:4)], bit_width = 1L, nrow(d)),
    0L + !is.na(d$l)
  )
  on.exit(close(con), add = TRUE)
  def_len <- readBin(con <- rawConnection(data), "integer", 1)
  expect_equal(
    rle_decode_int(
      data[-(1:(8+def_len))],
      bit_width = 1L,
      length(na.omit(d$l))
    ),
    0L + c(na.omit(d$l))
  )

  write_parquet(d, tmp, compression = "snappy")
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_metadata(tmp)$column_chunks$encodings
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
  })
})

test_that("Factor levels not in the data", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    f = factor(sample(letters[1:5], 100, replace = TRUE), levels = letters)
  )

  write_parquet(d, tmp)
  d2 <- read_parquet(tmp)
  expect_s3_class(d2$f, "factor")
  expect_equal(levels(d2$f), letters)
  pgs <- read_parquet_pages(tmp)
  expect_snapshot({
    read_parquet_page(tmp, pgs$page_header_offset[1])$data
  })
  data <- read_parquet_page(tmp, pgs$page_header_offset[2])$data
  expect_equal(
    rle_decode_int(data[-1], as.integer(data[1]), nrow(d)),
    as.integer(d$f) -1L
  )
})

test_that("write Date", {
  skip_on_cran()

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    d = c(Sys.Date() - 1, Sys.Date(), Sys.Date() + 1)
  )
  write_parquet(d, tmp)

  d2 <- arrow::read_parquet(tmp)
  expect_equal(d$d, d2$d)
})

test_that("write hms", {
  skip_on_cran()

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    h = hms::hms(1, 2, 3)
  )
  write_parquet(d, tmp)

  d2 <- arrow::read_parquet(tmp)
  expect_equal(d2$h, d$h)
})

test_that("write POSIXct", {
  skip_on_cran()

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    h = .POSIXct(Sys.time(), tz = "UTC")
  )
  write_parquet(d, tmp)

  d2 <- arrow::read_parquet(tmp)
  expect_equal(d$h, d2$h)
})

test_that("write difftime", {
  skip_on_cran()

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # Fractional seconds are kept
  d <- data.frame(
    h = as.difftime(10 + 1/9, units = "secs")
  )
  write_parquet(d, tmp)

  d2 <- arrow::read_parquet(tmp)
  expect_equal(d$h, d2$h)

  # Other units are converted to secs
  tmp2 <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp2), add = TRUE)

  d <- data.frame(
    h = as.difftime(10, units = "mins")
  )
  write_parquet(d, tmp2)
  d2 <- arrow::read_parquet(tmp2)

  expect_snapshot({
    as.data.frame(d2)
  })
})
