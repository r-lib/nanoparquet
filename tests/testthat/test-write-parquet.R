test_that("round trip", {
  mt <- cbind(nam = rownames(mtcars), mtcars)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  expect_true(all(read_parquet(tmp) == mt))
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  expect_true(all(read_parquet(tmp) == mt))
})

test_that("round trip with arrow", {
  mt <- cbind(nam = rownames(mtcars), mtcars)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  expect_true(all(arrow::read_parquet(tmp) == mt))
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  expect_true(all(arrow::read_parquet(tmp) == mt))
})

test_that("round trip with duckdb", {
  mt <- cbind(nam = rownames(mtcars), mtcars)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  df <- duckdb:::sql(sprintf("FROM '%s'", tmp))
  expect_true(all(df == mt))
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  df <- duckdb:::sql(sprintf("FROM '%s'", tmp))
  expect_true(all(df == mt))
})
