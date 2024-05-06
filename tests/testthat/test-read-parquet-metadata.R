test_that("read_parquet_metadata", {
  df <- test_df()
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp))
  write_parquet(df, tmp, compression = "uncompressed")
  mtd <- read_parquet_metadata(tmp)
  mtd$file_meta_data$key_value_metadata <-
    as.data.frame(mtd$file_meta_data$key_value_metadata)
  expect_snapshot({
    mtd$file_meta_data
    as.data.frame(mtd$schema)
    as.data.frame(mtd$row_groups)
    as.data.frame(mtd$column_chunks)
  })
})
