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
