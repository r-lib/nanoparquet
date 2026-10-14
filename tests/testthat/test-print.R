test_that("obj_sum.nanoparquet_logical_type", {
  sch <- parquet_schema(
    "INT_8",
    list("DECIMAL", scale = 1, precision = 3, primitive_type = "INT32"),
    list("TIME", is_adjusted_utc = TRUE, unit = "MILLIS"),
    list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS")
  )
  expect_snapshot({
    lapply(sch$logical_type, obj_sum.nanoparquet_logical_type)
  })

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  write_parquet(data.frame(x = as.POSIXct("2024-01-01", tz = "UTC")), tmp)
  expect_snapshot(
    obj_sum.nanoparquet_logical_type(read_parquet_schema(tmp)$logical_type[[2]])
  )
})
