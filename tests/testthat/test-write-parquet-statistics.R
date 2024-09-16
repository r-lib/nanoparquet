test_that("null_count is written", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- test_df(missing = TRUE)
  write_parquet(
    df, tmp,
    options = parquet_options(num_rows_per_row_group = 10)
  )
  expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
  expect_snapshot(
    as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]][
      , c("row_group", "column", "null_count")
    ])
  )
})

test_that("min/max for integers", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  df <- test_df(missing = TRUE)
  df <- df[order(df$cyl), ]
  rownames(df) <- NULL

  write_parquet(
    df, tmp,
    encoding = "PLAIN",
    options = parquet_options(num_rows_per_row_group = 5)
  )
  expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
  mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
  expect_snapshot(
    mtd[mtd$column == 2, c("row_group", "column", "min_value", "max_value")]
  )

  # dictionary
  enc <- ifelse(map_chr(df, class) == "integer", "RLE_DICTIONARY", "PLAIN")
  write_parquet(
    df, tmp,
    encoding = enc,
    options = parquet_options(num_rows_per_row_group = 5)
  )
  expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
  mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
  expect_snapshot(
    mtd[mtd$column == 2, c("row_group", "column", "min_value", "max_value")]
  )
})

test_that("min/max for DATEs", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(
    day = rep(as.Date("2024-09-16") - 10:1, each = 10),
    count = 1:100
  )
  write_parquet(
    df, tmp,
    options = parquet_options(num_rows_per_row_group = 20)
  )
  expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
  mtd <- as.data.frame(read_parquet_metadata(tmp)[["column_chunks"]])
  minv <- mtd[mtd$column == 0, "min_value"]
  maxv <- mtd[mtd$column == 0, "max_value"]
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[,-1])
    as.Date(map_int(minv, readBin, what = "integer", n = 1))
    as.Date(map_int(maxv, readBin, what = "integer", n = 1))
  })
})
