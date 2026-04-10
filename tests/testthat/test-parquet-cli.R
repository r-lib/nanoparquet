test_that("parquet CLI can read files written by write_parquet()", {
  skip_without_parquet_cli()

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mtcars, tmp)
  res <- processx::run("parquet", c("cat", tmp), error_on_status = FALSE)
  expect_equal(res$status, 0L)
})
