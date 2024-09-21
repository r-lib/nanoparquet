test_that("null_count is written", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- test_df(missing = TRUE)
  write_parquet(
    df, tmp,
    options = parquet_options(num_rows_per_row_group = 10)
  )
  expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
  expect_snapshot(
    as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]][
      , c("row_group", "column", "null_count")
    ])
  )
})

test_that("min/max for integers", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = c(
    sample(1:5),
    sample(c(1:3, -100L, 100L)),
    sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
    rep(NA_integer_, 3)
  ))

  as_int <- function(x) {
    sapply(x, function(xx) xx %&&% readBin(xx, what = "integer") %||% NA_integer_)
  }

  do <- function(encoding = "PLAIN",...) {
    write_parquet(
      df, tmp,
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    list(
      as_int(mtd[["min_value"]]),
      as_int(mtd[["max_value"]]),
      mtd[["is_min_value_exact"]],
      mtd[["is_max_value_exact"]]
    )
  }
  expect_snapshot(do(compression = "snappy"))
  expect_snapshot(do(compression = "uncompressed"))

  # dictionary
  enc <- ifelse(map_chr(df, class) == "integer", "RLE_DICTIONARY", "PLAIN")
  expect_snapshot(do(encoding = enc, compression = "snappy"))
  expect_snapshot(do(encoding = enc, compression = "uncompressed"))
})

test_that("min/max for DATEs", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  do <- function(...) {
    df <- data.frame(
      day = rep(as.Date("2024-09-16") - 10:1, each = 10),
      count = 1:100
    )
    df$day[c(1, 20, 25, 40)] <- as.Date(NA_character_)
    write_parquet(
      df, tmp,
      options = parquet_options(num_rows_per_row_group = 20),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    minv <- mtd[mtd$column == 0, "min_value"]
    maxv <- mtd[mtd$column == 0, "max_value"]
    list(
      as.data.frame(read_parquet_schema(tmp)[, -1]),
      as.Date(map_int(minv, readBin, what = "integer", n = 1), origin = "1970-01-01"),
      as.Date(map_int(maxv, readBin, what = "integer", n = 1), origin = "1970-01-01")
    )
  }

  expect_snapshot(do())
})

test_that("min/max for double -> signed integers", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = as.double(c(
    sample(1:5),
    sample(c(1:3, -100L, 100L)),
    sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
    rep(NA_integer_, 3)
  )))

  as_int <- function(x) {
    sapply(x, function(xx) xx %&&% readBin(xx, what = "integer") %||% NA_integer_)
  }

  do <- function(encoding = "PLAIN",...) {
    write_parquet(
      df, tmp,
      schema = parquet_schema(x = "INT32"),
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    list(
      as_int(mtd[["min_value"]]),
      as_int(mtd[["max_value"]]),
      mtd[["is_min_value_exact"]],
      mtd[["is_max_value_exact"]]
    )
  }
  expect_snapshot(do(compression = "snappy"))
  expect_snapshot(do(compression = "uncompressed"))

  # dictionary
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed"))
})

test_that("min/max for double -> unsigned integers", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = as.double(c(
    sample(1:5),
    sample(c(1:3, 1L, 100L)),
    sample(c(0L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
    rep(NA_integer_, 3)
  )))

  as_int <- function(x) {
    sapply(x, function(xx) xx %&&% readBin(xx, what = "integer") %||% NA_integer_)
  }

  do <- function(encoding = "PLAIN",...) {
    write_parquet(
      df, tmp,
      schema = parquet_schema(x = "UINT_32"),
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    list(
      as_int(mtd[["min_value"]]),
      as_int(mtd[["max_value"]]),
      mtd[["is_min_value_exact"]],
      mtd[["is_max_value_exact"]]
    )
  }
  expect_snapshot(do(compression = "snappy"))
  expect_snapshot(do(compression = "uncompressed"))

  # dictionary
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed"))
})

test_that("minmax for double -> INT32 TIME(MULLIS)", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  # IDK what's the point of signed TIME, but it seems to be allowed, the
  # sort order is signed
  df <- data.frame(x = hms::as_hms(c(
    sample(1:5),
    sample(c(1:3, -100L, 100L)),
    sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
    rep(NA_integer_, 3)
  )))

  as_int <- function(x) {
    sapply(x, function(xx) xx %&&% readBin(xx, what = "integer") %||% NA_integer_)
  }

  do <- function(encoding = "PLAIN",...) {
    write_parquet(
      df, tmp,
      schema = parquet_schema(x = "TIME_MILLIS"),
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    list(
      as_int(mtd[["min_value"]]),
      as_int(mtd[["max_value"]]),
      mtd[["is_min_value_exact"]],
      mtd[["is_max_value_exact"]]
    )
  }
  expect_snapshot(do(compression = "snappy"))
  expect_snapshot(do(compression = "uncompressed"))

  # dictionary
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed"))
})
