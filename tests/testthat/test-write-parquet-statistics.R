test_that("null_count is written", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- test_df(missing = TRUE)
  write_parquet(
    df,
    tmp,
    options = parquet_options(num_rows_per_row_group = 10)
  )
  expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
  expect_snapshot(
    as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]][,
      c("row_group", "column", "null_count")
    ])
  )
})

test_that("min/max for integers", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(
    x = c(
      sample(1:5),
      sample(c(1:3, -100L, 100L)),
      sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
      rep(NA_integer_, 3)
    )
  )

  as_int <- function(x) {
    sapply(x, function(xx) {
      xx %&&% readBin(xx, what = "integer") %||% NA_integer_
    })
  }

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
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

test_that("min/max for logicals", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  raw_false <- as.raw(0)
  raw_true <- as.raw(1)

  stats <- function() {
    mtd <- read_parquet_metadata(tmp)[["column_chunks"]]
    list(min = unclass(mtd$min_value), max = unclass(mtd$max_value))
  }
  minmax <- function(x, ...) {
    write_parquet(data.frame(x = x), tmp, ...)
    expect_equal(read_parquet(tmp)$x, x)
    stats()
  }

  opts <- parquet_options(num_rows_per_row_group = 4)
  required <- c(TRUE, FALSE, TRUE, FALSE, rep(TRUE, 4), rep(FALSE, 4))
  optional <- c(NA, TRUE, FALSE, NA, rep(TRUE, 4), rep(NA, 4))
  for (encoding in c("PLAIN", "RLE")) {
    expect_equal(
      minmax(required, encoding = encoding, options = opts),
      list(
        min = list(raw_false, raw_true, raw_false),
        max = list(raw_true, raw_true, raw_false)
      )
    )
    expect_equal(
      minmax(optional, encoding = encoding, options = opts),
      list(
        min = list(raw_false, raw_true, NULL),
        max = list(raw_true, raw_true, NULL)
      )
    )
  }

  expect_equal(
    minmax(required, options = parquet_options(write_minmax_values = FALSE)),
    list(min = list(NULL), max = list(NULL))
  )

  write_parquet(data.frame(x = rep(TRUE, 4)), tmp)
  append_parquet(
    data.frame(x = rep(FALSE, 4)),
    tmp,
    options = parquet_options(keep_row_groups = TRUE)
  )
  expect_equal(
    stats(),
    list(min = list(raw_true, raw_false), max = list(raw_true, raw_false))
  )

  withr::local_envvar(NANOPARQUET_PAGE_SIZE = "1024")
  x <- rep(c(TRUE, FALSE), each = 10000)
  for (encoding in c("PLAIN", "RLE")) {
    expect_equal(
      minmax(x, encoding = encoding),
      list(min = list(raw_false), max = list(raw_true))
    )
    pages <- read_parquet_pages(tmp)
    expect_gt(sum(pages$page_type == "DATA_PAGE"), 1)
  }
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
      df,
      tmp,
      options = parquet_options(num_rows_per_row_group = 20),
      ...
    )
    expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    minv <- mtd[mtd$column == 0, "min_value"]
    maxv <- mtd[mtd$column == 0, "max_value"]
    list(
      as.data.frame(read_parquet_schema(tmp)[, -1]),
      as.Date(
        map_int(minv, readBin, what = "integer", n = 1),
        origin = "1970-01-01"
      ),
      as.Date(
        map_int(maxv, readBin, what = "integer", n = 1),
        origin = "1970-01-01"
      )
    )
  }

  expect_snapshot(do())
})

test_that("min/max for double -> signed integers", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(
    x = as.double(c(
      sample(1:5),
      sample(c(1:3, -100L, 100L)),
      sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
      rep(NA_integer_, 3)
    ))
  )

  as_int <- function(x) {
    sapply(x, function(xx) {
      xx %&&% readBin(xx, what = "integer") %||% NA_integer_
    })
  }

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
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
  df <- data.frame(
    x = as.double(c(
      sample(1:5),
      sample(c(1:3, 1L, 100L)),
      sample(c(0L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
      rep(NA_integer_, 3)
    ))
  )

  as_int <- function(x) {
    sapply(x, function(xx) {
      xx %&&% readBin(xx, what = "integer") %||% NA_integer_
    })
  }

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
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
  df <- data.frame(
    x = hms::as_hms(c(
      sample(1:5),
      sample(c(1:3, -100L, 100L)),
      sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
      rep(NA_integer_, 3)
    ))
  )

  as_int <- function(x) {
    sapply(x, function(xx) {
      xx %&&% readBin(xx, what = "integer") %||% NA_integer_
    })
  }

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
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
  df <- data.frame(
    x = as.double(c(
      sample(1:5),
      sample(c(1:3, -100, 100)),
      sample(c(-1000, NA_real_, 1000, NA_real_, NA_real_)),
      rep(NA_real_, 3)
    ))
  )

  as_dbl <- function(x) {
    sapply(x, function(xx) xx %&&% readBin(xx, what = "double") %||% NA_real_)
  }

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
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
  df <- data.frame(
    x = as.double(c(
      sample(1:5),
      sample(c(1:3, -100, 100)),
      sample(c(-1000, NA_real_, 1000, NA_real_, NA_real_)),
      rep(NA_real_, 3)
    ))
  )

  as_flt <- function(x) {
    sapply(x, function(xx) xx %&&% .Call(read_float, xx) %||% NA_real_)
  }

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
      schema = parquet_schema(x = "FLOAT"),
      encoding = encoding,
      options = parquet_options(num_rows_per_row_group = 5),
      ...
    )
    expect_equal(as.data.frame(read_parquet(tmp)), as.data.frame(df))
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
  df <- data.frame(
    x = c(
      sample(1:5),
      sample(c(1:3, -100L, 100L)),
      sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
      rep(NA_integer_, 3)
    )
  )

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
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
  df <- data.frame(
    x = as.double(c(
      sample(1:5),
      sample(c(1:3, -100L, 100L)),
      sample(c(-1000L, NA_integer_, 1000L, NA_integer_, NA_integer_)),
      rep(NA_integer_, 3)
    ))
  )

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
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
  df <- data.frame(
    x = c(
      sample(letters[1:5]),
      sample(c(letters[1:3], "!!!", "~~~")),
      sample(c("!", NA_character_, "~", NA_character_, NA_character_)),
      rep(NA_character_, 3)
    )
  )

  as_str <- function(x) {
    sapply(x, function(xx) xx %&&% rawToChar(xx) %||% NA_character_)
  }

  do <- function(encoding = "PLAIN", type = "STRING", ...) {
    write_parquet(
      df,
      tmp,
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
  expect_snapshot(do(
    encoding = "RLE_DICTIONARY",
    compression = "snappy",
    type = "JSON"
  ))
  expect_snapshot(do(
    encoding = "RLE_DICTIONARY",
    compression = "uncompressed",
    type = "JSON"
  ))

  expect_snapshot(do(compression = "snappy", type = "BSON"))
  expect_snapshot(do(compression = "uncompressed", type = "BSON"))

  # dictionary
  expect_snapshot(do(
    encoding = "RLE_DICTIONARY",
    compression = "snappy",
    type = "BSON"
  ))
  expect_snapshot(do(
    encoding = "RLE_DICTIONARY",
    compression = "uncompressed",
    type = "BSON"
  ))

  expect_snapshot(do(compression = "snappy", type = "ENUM"))
  expect_snapshot(do(compression = "uncompressed", type = "ENUM"))

  # dictionary
  expect_snapshot(do(
    encoding = "RLE_DICTIONARY",
    compression = "snappy",
    type = "ENUM"
  ))
  expect_snapshot(do(
    encoding = "RLE_DICTIONARY",
    compression = "uncompressed",
    type = "ENUM"
  ))
})

test_that("min/max for REALSXP -> TIMESTAMP (INT64)", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  now <- 0L
  df <- data.frame(
    x = .POSIXct(
      as.double(c(
        sample(now + 1:5),
        sample(c(now + c(1:3, -100L, 100L))),
        sample(c(
          now - 1000L,
          NA_integer_,
          now + 1000L,
          NA_integer_,
          NA_integer_
        )),
        rep(NA_integer_, 3)
      )),
      tz = "UTC"
    )
  )

  do <- function(encoding = "PLAIN", ...) {
    write_parquet(
      df,
      tmp,
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

test_that("min/max for dictionary encoded TIMESTAMP (#169)", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  ts <- .POSIXct(1209506400, tz = "UTC")
  df <- data.frame(x = rep(ts, 16))

  # a constant column is dictionary encoded, and the min/max values must be
  # in microseconds, just like the values in the dictionary page
  write_parquet(df, tmp)
  mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
  expect_true("RLE_DICTIONARY" %in% mtd[["encodings"]][[1]])
  expect_equal(as_int64(mtd[["min_value"]]), as.numeric(ts) * 1000 * 1000)
  expect_equal(as_int64(mtd[["max_value"]]), as.numeric(ts) * 1000 * 1000)
})

test_that("min/max for dictionary encoded difftime", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = as.difftime(rep(c(5, 10), 8), units = "secs"))

  write_parquet(df, tmp, encoding = "RLE_DICTIONARY")
  expect_equal(as.data.frame(read_parquet(tmp)), as.data.frame(df))
  mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
  # difftime is written in nanoseconds
  expect_equal(as_int64(mtd[["min_value"]]), 5 * 1000 * 1000 * 1000)
  expect_equal(as_int64(mtd[["max_value"]]), 10 * 1000 * 1000 * 1000)
})

test_that("min/max for dictionary encoded integer64", {
  skip_if_not_installed("bit64")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  vals <- bit64::as.integer64(c(-1234567890123, 9876543210, 1))
  df <- data.frame(x = vals[c(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3)])

  write_parquet(df, tmp, encoding = "RLE_DICTIONARY")
  expect_equal(as.data.frame(read_parquet(tmp)), as.data.frame(df))
  mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
  expect_equal(as_int64(mtd[["min_value"]]), -1234567890123)
  expect_equal(as_int64(mtd[["max_value"]]), 9876543210)
})

test_that("min/max for dictionary encoded negative integer64 with NA", {
  skip_if_not_installed("bit64")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(x = bit64::as.integer64(rep(c(-1, -2, NA), 4)))

  write_parquet(df, tmp, encoding = "RLE_DICTIONARY")
  expect_equal(as.data.frame(read_parquet(tmp)), as.data.frame(df))
  mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
  expect_equal(as_int64(mtd[["min_value"]]), -2)
  expect_equal(as_int64(mtd[["max_value"]]), -1)
})

test_that("min/max for multi-page column chunks", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  n <- 2000
  df <- data.frame(
    int = c(-1000L, 1000L, seq_len(n - 2)),
    dbl = c(-1000, 1000, seq_len(n - 2) / 10)
  )
  df$date <- as.Date(df$int, origin = "2000-01-01")
  df$time <- as.POSIXct(df$dbl, origin = "2000-01-01", tz = "UTC")

  minmax <- function() {
    write_parquet(df, tmp, encoding = "PLAIN")
    mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
    mtd[, c("column", "min_value", "max_value")]
  }
  single <- minmax()
  withr::local_envvar(NANOPARQUET_PAGE_SIZE = "1024")
  multi <- minmax()

  pages <- read_parquet_pages(tmp)
  expect_gt(min(table(pages$column[pages$page_type == "DATA_PAGE"])), 1)
  expect_equal(multi, single)
})
