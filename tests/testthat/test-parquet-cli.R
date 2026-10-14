test_that("parquet CLI can read files written by write_parquet()", {
  skip_without_parquet_cli()

  tmp <- withr::local_tempfile(fileext = ".parquet")

  write_parquet(mtcars, tmp)
  res <- processx::run("parquet", c("cat", tmp), error_on_status = FALSE)
  expect_equal(res$status, 0L)
})
