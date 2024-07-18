test_that("parquet_metadata", {
  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "1")
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
    as.data.frame(mtd$file_meta_data)
    as.data.frame(mtd$schema)
    as.data.frame(mtd$row_groups)
    as.data.frame(mtd$column_chunks)
  })

  sch <- read_parquet_schema("test.parquet")
  expect_snapshot(as.data.frame(sch))
})

test_that("ENUM type", {
  pf <- test_path("data/enum.parquet")
  sch <- read_parquet_schema(pf)
  expect_snapshot(as.data.frame(sch))
})

test_that("UUID type", {
  pf <- test_path("data/uuid-arrow.parquet")
  sch <- read_parquet_schema(pf)
  expect_snapshot(as.data.frame(sch))
})

test_that("DATE type", {
  pf <- test_path("data/date.parquet")
  sch <- read_parquet_schema(pf)
  expect_snapshot(as.data.frame(sch))
})

test_that("DECIMAL type", {
  pf <- test_path("data/decimals.parquet")
  sch <- read_parquet_schema(pf)
  expect_snapshot(as.data.frame(sch))
  expect_snapshot(sch$logical_type)
})

test_that("TIME type", {
  pf <- test_path("data/timetz.parquet")
  sch <- read_parquet_schema(pf)
  expect_snapshot(as.data.frame(sch))
  expect_snapshot(sch$logical_type)
})

test_that("TIMESTAMP type", {
  pf <- test_path("data/timestamp.parquet")
  sch <- read_parquet_schema(pf)
  expect_snapshot(as.data.frame(sch))
  expect_snapshot(sch$logical_type)
})

test_that("LIST type", {
  pf <- test_path("data/nested_lists.snappy.parquet")
  sch <- read_parquet_schema(pf)
  expect_snapshot(as.data.frame(sch))
})

test_that("MAP type", {
  pf <- test_path("data/map.parquet")
  sch <- read_parquet_schema(pf)
  expect_snapshot(as.data.frame(sch))
})

test_that("key-value metadata", {
  pf <- test_path("data/mtcars-arrow.parquet")
  mtd <- read_parquet_metadata(pf)
  expect_snapshot(
    as.data.frame(mtd$file_meta_data$key_value_metadata[[1]])
  )
})

test_that("parquet_info", {
  library(pillar)
  expect_snapshot({
    read_parquet_info(test_path("data/enum.parquet"))
    read_parquet_info(test_path("data/factor.parquet"))
    read_parquet_info(test_path("data/decimals.parquet"))
  })
})
