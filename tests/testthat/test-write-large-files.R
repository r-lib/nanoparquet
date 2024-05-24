test_that("write large file", {
  if (Sys.getenv("NANOPARQUET_ALL_TESTS") != "true") {
    expect_true(TRUE)
    return()
  }
  mt <- test_df(missing = TRUE, factor = TRUE)
  larger <- do.call("rbind", replicate(1000, mt, simplify = FALSE))
  big <- do.call("rbind", replicate(100, larger, simplify = FALSE))
  large <- do.call("rbind", replicate(10, big, simplify = FALSE))

  tmp <- tempfile(fileext = ".parquet")
  system.time(write_parquet(large, tmp))
  system.time(large2 <- read_parquet(tmp))
  expect_equal(large, large2)
})
