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
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed"))
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

test_that("min/max for DOUBLE", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = as.double(c(
    sample(1:5),
    sample(c(1:3, -100, 100)),
    sample(c(-1000, NA_real_, 1000, NA_real_, NA_real_)),
    rep(NA_real_, 3)
  )))

  as_dbl <- function(x) {
    sapply(x, function(xx) xx %&&% readBin(xx, what = "double") %||% NA_real_)
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
      as_dbl(mtd[["min_value"]]),
      as_dbl(mtd[["max_value"]]),
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

test_that("min/max for FLOAT", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = as.double(c(
    sample(1:5),
    sample(c(1:3, -100, 100)),
    sample(c(-1000, NA_real_, 1000, NA_real_, NA_real_)),
    rep(NA_real_, 3)
  )))

  as_flt <- function(x) {
    sapply(x, function(xx) xx %&&% .Call(read_float, xx) %||% NA_real_)
  }

  do <- function(encoding = "PLAIN",...) {
    write_parquet(
      df, tmp,
      schema = parquet_schema(x = "FLOAT"),
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    list(
      as_flt(mtd[["min_value"]]),
      as_flt(mtd[["max_value"]]),
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

test_that("min/max for integer -> INT64", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = c(
    sample(1:5),
    sample(c(1:3, -100L, 100L)),
    sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
    rep(NA_integer_, 3)
  ))

  as_int64 <- function(x) {
    sapply(x, function(xx) xx %&&% .Call(read_int64, xx) %||% NA_real_)
  }

  do <- function(encoding = "PLAIN",...) {
    write_parquet(
      df, tmp,
      schema = parquet_schema(x = "INT64"),
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    list(
      as_int64(mtd[["min_value"]]),
      as_int64(mtd[["max_value"]]),
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

test_that("min/max for REALSXP -> INT64", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = as.double(c(
    sample(1:5),
    sample(c(1:3, -100L, 100L)),
    sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
    rep(NA_integer_, 3)
  )))

  as_int64 <- function(x) {
    sapply(x, function(xx) xx %&&% .Call(read_int64, xx) %||% NA_real_)
  }

  do <- function(encoding = "PLAIN",...) {
    write_parquet(
      df, tmp,
      schema = parquet_schema(x = "INT64"),
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    list(
      as_int64(mtd[["min_value"]]),
      as_int64(mtd[["max_value"]]),
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

test_that("min/max for STRING", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = c(
    sample(letters[1:5]),
    sample(c(letters[1:3], "!!!", "~~~")),
    sample(c("!", NA_character_, "~", NA_character_, NA_character_)),
    rep(NA_character_, 3)
  ))

  as_str <- function(x) {
    sapply(x, function(xx) xx %&&% rawToChar(xx) %||% NA_character_)
  }

  do <- function(encoding = "PLAIN", type = "STRING", ...) {
    write_parquet(
      df, tmp,
      schema = parquet_schema(x = type),
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    expect_equal(read_parquet_schema(tmp)$logical_type[[2]]$type, type)
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    list(
      as_str(mtd[["min_value"]]),
      as_str(mtd[["max_value"]]),
      mtd[["is_min_value_exact"]],
      mtd[["is_max_value_exact"]]
    )
  }
  expect_snapshot(do(compression = "snappy"))
  expect_snapshot(do(compression = "uncompressed"))

  # dictionary
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed"))

  expect_snapshot(do(compression = "snappy", type = "JSON"))
  expect_snapshot(do(compression = "uncompressed", type = "JSON"))

  # dictionary
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy", type = "JSON"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed", type = "JSON"))

  expect_snapshot(do(compression = "snappy", type = "BSON"))
  expect_snapshot(do(compression = "uncompressed", type = "BSON"))

  # dictionary
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy", type = "BSON"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed", type = "BSON"))

  expect_snapshot(do(compression = "snappy", type = "ENUM"))
  expect_snapshot(do(compression = "uncompressed", type = "ENUM"))

  # dictionary
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy", type = "ENUM"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed", type = "ENUM"))
})

test_that("min/max for REALSXP -> TIMESTAMP (INT64)", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  now <- 0L
  df <- data.frame(x = .POSIXct(as.double(c(
    sample(now + 1:5),
    sample(c(now + c(1:3, -100L, 100L))),
    sample(c(now - 1000L, NA_integer_, now + 1000L, NA_integer_, NA_integer_)),
    rep(NA_integer_, 3)
  )), tz = "UTC"))

  as_int64 <- function(x) {
    sapply(x, function(xx) xx %&&% .Call(read_int64, xx) %||% NA_real_)
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
      as_int64(mtd[["min_value"]]),
      as_int64(mtd[["max_value"]]),
      mtd[["is_min_value_exact"]],
      mtd[["is_max_value_exact"]]
    )
  }
  expect_snapshot(do(compression = "snappy"))
  expect_snapshot(do(compression = "uncompressed"))
return()
  # dictionary
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "snappy"))
  expect_snapshot(do(encoding = "RLE_DICTIONARY", compression = "uncompressed"))
})
