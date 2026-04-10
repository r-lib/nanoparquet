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

test_that("write >4GB file: uint32_t/int32_t overflow (issue #143)", {
  if (Sys.getenv("NANOPARQUET_ALL_TESTS") != "true") {
    expect_true(TRUE)
    return()
  }

  # Reproduces issue #143: file offsets and column sizes were stored as
  # uint32_t/int32_t, overflowing once the file exceeded 4 GB (2^32 bytes).
  # 270M rows x 2 double columns x 8 bytes = ~4.3 GB uncompressed, crossing
  # both the 2^31 threshold (int32_t column_bytes) and the 2^32 threshold
  # (uint32_t write_columns end).

  n <- 270e6L
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- data.frame(a = as.double(seq_len(n)), b = as.double(seq_len(n)) * 2.0)
  write_parquet(df, tmp, compression = "uncompressed")
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df, df2)
})
