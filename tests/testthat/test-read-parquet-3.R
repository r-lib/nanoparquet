test_that("TIMESTAMP", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  ts <- structure(1724186255.52948, class = c("POSIXct", "POSIXt"))
  d <- data.frame(ts = ts)
  sch <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS"))
  write_parquet(d, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })

  sch2 <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS"))
  write_parquet(d, tmp, schema = sch2)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })

  sch3 <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "NANOS"))
  write_parquet(d, tmp, schema = sch3)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })
})

test_that("TIMESTAMP, dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  ts <- structure(1724186255.52948, class = c("POSIXct", "POSIXt"))
  d <- data.frame(ts = ts)
  sch <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS"))
  write_parquet(d, tmp, schema = sch, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })

  sch2 <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS"))
  write_parquet(d, tmp, schema = sch2, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })

  sch3 <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "NANOS"))
  write_parquet(d, tmp, schema = sch3, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })
})

test_that("TIMESTAMP_MILLIS, TIMESTAMP_MICROS", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  ts <- structure(1724186255.52948, class = c("POSIXct", "POSIXt"))
  d <- data.frame(ts = ts)
  sch <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS"))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
  })

  sch <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS"))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
  })
})

test_that("TIMESTAMP_MILLIS, TIMESTAMP_MICROS, dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  ts <- structure(1724186255.52948, class = c("POSIXct", "POSIXt"))
  d <- data.frame(ts = ts)
  sch <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS"))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })

  sch <- parquet_schema(list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS"))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })
})

test_that("TIME", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  dt <- hms::hms(1,2,3)
  d <- data.frame(dt = dt)
  sch <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "MILLIS"))
  write_parquet(d, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })

  sch2 <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "MICROS"))
  write_parquet(d, tmp, schema = sch2)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })

  sch3 <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "NANOS"))
  write_parquet(d, tmp, schema = sch3)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })
})

test_that("TIME_MILLIS, TIME_MICROS", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  dt <- hms::hms(1,2,3)
  d <- data.frame(dt = dt)
  sch <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "MILLIS"))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })

  sch <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "MICROS"))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })
})

test_that("TIME, dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  dt <- hms::hms(1,2,3)
  d <- data.frame(dt = dt)
  sch <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "MILLIS"))
  write_parquet(d, tmp, schema = sch, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })

  sch2 <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "MICROS"))
  write_parquet(d, tmp, schema = sch2, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })

  sch3 <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "NANOS"))
  write_parquet(d, tmp, schema = sch3, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })
})

test_that("TIME_MILLIS, TIME_MICROS, dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  dt <- hms::hms(1,2,3)
  d <- data.frame(dt = dt)
  sch <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "MILLIS"))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })

  sch <- parquet_schema(list("TIME", is_adjusted_utc = TRUE, unit = "MICROS"))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
  })
})
