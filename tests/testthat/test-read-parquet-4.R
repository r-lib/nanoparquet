test_that("DECIMAL converted type", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = 100)
  sch <- parquet_schema(list("DECIMAL", primitive_type = "INT32", precision = 5, scale = 2))
  sch$logical_type[1] <- list(NULL)
  write_parquet(d, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })

  d2 <- data.frame(d = 100L)
  sch$logical_type[1] <- list(NULL)
  write_parquet(d2, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })
})

test_that("DECIMAL in BA dict", {
  pf <- test_path("data/decimals.parquet")
  expect_snapshot({
    as.data.frame(read_parquet(pf))
  })
})

return()

test_that("DECIMAL in BA", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = as.double(100:110))
  sch <- parquet_schema(list("DECIMAL", primitive_type = "BYTE_ARRAY", precision = 5, scale = 2))
  write_parquet(d, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })

  d2 <- data.frame(d = 100:110)
  sch <- parquet_schema(list("DECIMAL", primitive_type = "BYTE_ARRAY", precision = 5, scale = 2))
  write_parquet(d2, tmp, schema = sch)
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })
})

test_that("DECIMAL in BA, dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = as.double(100:110))
  sch <- parquet_schema(list("DECIMAL", primitive_type = "BYTE_ARRAY", precision = 5, scale = 2))
  write_parquet(d, tmp, schema = sch, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })

  d2 <- data.frame(d = 100:110)
  sch <- parquet_schema(list("DECIMAL", primitive_type = "BYTE_ARRAY", precision = 5, scale = 2))
  write_parquet(d2, tmp, schema = sch, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_schema(tmp))[["logical_type"]]
  })
})
