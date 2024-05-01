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
