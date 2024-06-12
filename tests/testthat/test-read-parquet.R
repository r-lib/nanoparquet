alltypes_plain <- structure(
  list(
    id = c(4L, 5L, 6L, 7L, 2L, 3L, 0L, 1L),
    bool_col = c(TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE),
    tinyint_col = c(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L),
    smallint_col = c(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L),
    int_col = c(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L),
    bigint_col = c(0, 10, 0, 10, 0, 10, 0, 10),
    float_col = c(
      0, 1.10000002384186, 0, 1.10000002384186,
      0, 1.10000002384186, 0, 1.10000002384186
    ),
    double_col = c(0, 10.1, 0, 10.1, 0, 10.1, 0, 10.1),
    date_string_col = c(
      "03/01/09", "03/01/09", "04/01/09", "04/01/09",
      "02/01/09", "02/01/09", "01/01/09", "01/01/09"
    ),
    string_col = c("0", "1", "0", "1", "0", "1", "0", "1"),
    timestamp_col = structure(c(
      1235865600, 1235865660, 1238544000, 1238544060,
      1233446400, 1233446460, 1230768000, 1230768060
    ), class = c("POSIXct", "POSIXt"), tzone = "UTC")
  ),
  row.names = c(NA, -8L),
  class = "data.frame"
)

alltypes_plain_snappy <- structure(
  list(
    id = 6:7,
    bool_col = c(TRUE, TRUE),
    tinyint_col = 0:1,
    smallint_col = 0:1,
    int_col = 0:1,
    bigint_col = c(0, 10),
    float_col = c(0, 1.10000002384186),
    double_col = c(0, 10.1),
    date_string_col = c("04/01/09", "04/01/09"),
    string_col = c("0", "1"),
    timestamp_col = structure(
      c(1238544000, 1238544060),
      class = c("POSIXct", "POSIXt"),
      tzone = "UTC"
    )
  ),
  row.names = c(NA, -2L),
  class = "data.frame"
)

data_comparable <- function(df1, df2, dlt = .0001) {
  df1 <- as.data.frame(df1, stringsAsFactors = F)
  df2 <- as.data.frame(df2, stringsAsFactors = F)
  if (!identical(dim(df1), dim(df2))) {
    return(FALSE)
  }
  for (col_i in length(df1)) {
    col1 <- df1[[col_i]]
    col2 <- df2[[col_i]]
    if (is.numeric(col1)) {
      # reference answers are rounded to two decimals
      col1 <- round(col1, 2)
      col2 <- round(col2, 2)
      if (any(abs(col1 - col2) > col1 * dlt)) {
        return(FALSE)
      }
    } else {
      col1 <- trimws(as.character(col1))
      col2 <- trimws(as.character(col2))
      if (any(col1 != col2)) {
        return(FALSE)
      }
    }
  }
  return(TRUE)
}

test_that("various error cases", {
  # https://github.com/llvm/llvm-project/issues/59432
  if (is_asan()) skip("ASAN bug")
  expect_error(res <- read_parquet(""))
  expect_error(res <- read_parquet("DONTEXIST"))
  tf <- tempfile()
  expect_error(res <- read_parquet(tf))
  expect_error(res <- read_parquet(c(tf, tf)))
})

test_that("basic reading works", {
  res <- read_parquet(test_path("data/alltypes_plain.parquet"))
  expect_true(data_comparable(alltypes_plain, res))
})

test_that("basic reading works with snappy", {
  res <- read_parquet(test_path("data/alltypes_plain.snappy.parquet"))
  expect_true(data_comparable(alltypes_plain_snappy, res))
})

test_that("read factors, marked by Arrow", {
  res <- read_parquet(test_path("data/factor.parquet"))
  expect_snapshot({
    as.data.frame(res[1:5,])
    sapply(res, class)
  })
})

test_that("Can't parse Arrow schema", {
  expect_snapshot(
    arrow_find_special(base64_encode("foobar"), "myfile")
  )
})

test_that("round trip with arrow", {
  # Don't want to skip on the parquet capability missing, because then
  # this might not be tested on the CI. So rather we skip on CRAN.
  skip_on_cran()
  mt <- test_df(factor = TRUE)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  arrow::write_parquet(mt, tmp, compression = "uncompressed")
  expect_equal(read_parquet(tmp), mt)
  unlink(tmp)

  arrow::write_parquet(mt, tmp, compression = "snappy")
  expect_equal(read_parquet(tmp), mt)
})

test_that("round trip with duckdb", {
  skip_on_cran()
  # https://github.com/llvm/llvm-project/issues/59432
  if (is_asan()) skip("ASAN bug")
  mt <- test_df()
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  drv <- duckdb::duckdb()
  con <- DBI::dbConnect(drv)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbWriteTable(con, "mtcars", as.data.frame(mt))

  DBI::dbExecute(con, DBI::sqlInterpolate(con,
    "COPY mtcars TO ?filename (FORMAT 'parquet', COMPRESSION 'uncompressed')",
    filename = tmp
  ))
  expect_equal(read_parquet(tmp), mt)
  unlink(tmp)

  DBI::dbExecute(con, DBI::sqlInterpolate(con,
    "COPY mtcars TO ?filename (FORMAT PARQUET, COMPRESSION 'snappy')",
    filename = tmp
  ))
  arrow::write_parquet(mt, tmp, compression = "snappy")
  expect_equal(read_parquet(tmp), mt)
})

test_that("read Date", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    d = c(Sys.Date() - 1, Sys.Date(), Sys.Date() + 1)
  )
  write_parquet(d, tmp)

  d2 <- read_parquet(tmp)
  expect_s3_class(d2$d, "Date")
  expect_equal(d$d, d2$d)
})

test_that("read hms", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    h = hms::hms(1, 2, 3)
  )
  write_parquet(d, tmp)

  d2 <- read_parquet(tmp)
  expect_s3_class(d2$h, "hms")
  expect_equal(d$h, d2$h)
})

test_that("read hms in MICROS", {
  pf <- test_path("data/timetz.parquet")
  expect_snapshot({
    as.data.frame(read_parquet(pf))
  })
})

test_that("read POSIXct", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    h = .POSIXct(Sys.time(), tz = "UTC")
  )
  write_parquet(d, tmp)

  d2 <- read_parquet(tmp)
  expect_s3_class(d$h, "POSIXct")
  expect_equal(d$h, d2$h)
})

test_that("read POSIXct in MILLIS", {
  skip_on_cran() # arrow
  # This file has UTC = FALSE, so the exact result depends on the current
  # time zone. But it should match Arrow.
  pf <- test_path("data/timestamp-ms.parquet")
  d1 <- read_parquet(pf)
  d2 <- arrow::read_parquet(pf)
  expect_equal(
    as.data.frame(d1),
    as.data.frame(d2)
  )
})

test_that("read difftime", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # Fractional seconds are kept
  d <- data.frame(
    h = as.difftime(10 + 1/9, units = "secs")
  )
  write_parquet(d, tmp)

  d2 <- read_parquet(tmp)
  expect_s3_class(d2$h, "difftime")
  expect_equal(d$h, d2$h)

  # Other units are converted to secs
  d <- data.frame(
    h = as.difftime(10, units = "mins")
  )
  write_parquet(d, tmp)
  d2 <- read_parquet(tmp)
  expect_snapshot({
    as.data.frame(d2)
  })
})

test_that("RLE BOOLEAN", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    l = c(
      logical(30),
      !logical(5),
      logical(20),
      !logical(30)
    )
  )

  write_parquet(d, tmp)
  expect_equal(
    unclass(parquet_metadata(tmp)$column_chunks$encodings),
    list("RLE")
  )

  expect_equal(as.data.frame(read_parquet(tmp)), d)

  # larger DF

  d <- data.frame(
    l = c(
      logical(runif(1) * 3000),
      !logical(runif(1) * 50),
      logical(runif(1) * 2000),
      !logical(runif(1) * 3000)
    )
  )

  write_parquet(d, tmp)
  expect_equal(
    unclass(parquet_metadata(tmp)$column_chunks$encodings),
    list("RLE")
  )

  expect_equal(as.data.frame(read_parquet(tmp)), d)
})

test_that("read GZIP compressed files", {
  pf <- test_path("data/gzip.parquet")
  expect_snapshot({
    as.data.frame(read_parquet(pf))
  })
})

test_that("V2 data pages", {
  pf <- test_path("data/parquet_go.parquet")
  expect_snapshot({
    as.data.frame(read_parquet(pf))
  })
})

test_that("V2 data page with missing values", {
  skip_on_cran()
  pf <- test_path("data/duckdb-bug1589.parquet")
  expect_equal(
    as.data.frame(read_parquet(pf)),
    as.data.frame(arrow::read_parquet(pf))
  )
})

test_that("Tricky V2 data page", {
  # has repetition levels to be ignored and uncompressed
  # definition levels
  pf <- test_path("data/rle_boolean_encoding.parquet")
  expect_snapshot({
    as.data.frame(read_parquet(pf))
  })
})

test_that("zstd", {
  pf <- test_path("data/zstd.parquet")
  expect_true(all(parquet_metadata(pf)$column_chunks$codec == "ZSTD"))
  pf2 <- test_path("data/gzip.parquet")
  expect_equal(read_parquet(pf), read_parquet(pf2))
})

test_that("zstd with data page v2", {
  pf <- test_path("data/zstd-v2.parquet")
  expect_true(all(parquet_metadata(pf)$column_chunks$codec == "ZSTD"))
  expect_true(
    all(parquet_pages(pf)$page_type %in% c("DICTIONARY_PAGE", "DATA_PAGE_V2"))
  )
  pf2 <- test_path("data/gzip.parquet")
  expect_equal(read_parquet(pf), read_parquet(pf2))
})

test_that("DELTA_BIANRY_PACKED encoding", {
  suppressPackageStartupMessages(library(bit64))
  pf <- test_path("data/dbp-int32.parquet")
  expect_snapshot({
    parquet_metadata(pf)$column_chunks$encodings
    read_parquet(pf)
  })

  pf2 <- test_path("data/dbp-int32-missing.parquet")
  expect_snapshot({
    parquet_metadata(pf2)$column_chunks$encodings
    read_parquet(pf2)
  })

  pf3 <- test_path("data/dbp-int64.parquet")
  expect_snapshot({
    parquet_metadata(pf3)$column_chunks$encodings
    read_parquet(pf3)
  })
})

test_that("UUID columns", {
  pf <- test_path("data/uuid-arrow.parquet")
  expect_snapshot({
    as.data.frame(read_parquet(pf))
  })
})

test_that("DELTA_LENGTH_BYTE_ARRAY encoding", {
  pf <- test_path("data/delta_length_byte_array.parquet")
  dlba <- read_parquet(pf)
  expect_snapshot({
    as.data.frame(dlba)[1:10,]
    rle(nchar(dlba$FRUIT))
  })
})

test_that("DELTA_BYTE_ARRAY encoding", {
  skip_on_cran()
  pf <- test_path("data/delta_byte_array.parquet")
  dba <- read_parquet(pf)
  expect_snapshot({
    as.data.frame(dba)[1:5,]
  })
  expect_equal(
    as.data.frame(arrow::read_parquet(pf)),
    as.data.frame(dba)
  )
})
