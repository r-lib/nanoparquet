test_that("read_parquet_metadata", {
  df <- test_df()
  mkdirp(tmpdir <- tempfile())
  tmp <- file.path(tmpdir, "test.parquet")
  on.exit(unlink(tmpdir, recursive = TRUE), add = TRUE)
  write_parquet(df, tmp, compression = "uncompressed")

  wd <- getwd()
  on.exit(setwd(wd), add = TRUE)
  setwd(tmpdir)

  mtd <- read_parquet_metadata("test.parquet")
  mtd$file_meta_data$key_value_metadata <-
    as.data.frame(mtd$file_meta_data$key_value_metadata)
  expect_snapshot({
    mtd$file_meta_data
    as.data.frame(mtd$schema)
    as.data.frame(mtd$row_groups)
    as.data.frame(mtd$column_chunks)
  })

  sch <- read_parquet_schema("test.parquet")
  expect_snapshot(as.data.frame(sch))
})
