test_that("plain and simple", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(
    test_df(),
    tmp,
    row_groups = c(1L, 11L, 21L, 31L),
    encoding = "PLAIN"
  )

  # 4 row groups
  expect_snapshot(as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1])

  expect_snapshot({
    as.data.frame(read_parquet_row_group(tmp, 0L))
    as.data.frame(read_parquet_row_group(tmp, 1L))
    as.data.frame(read_parquet_row_group(tmp, 2L))
    as.data.frame(read_parquet_row_group(tmp, 3L))
  })
})

test_that("dicts", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(
    test_df(),
    tmp,
    row_groups = c(1L, 11L, 21L, 31L),
    encoding = c(large = "RLE", "RLE_DICTIONARY")
  )

  # 4 row groups, dicts
  expect_snapshot({
    as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
  })

  expect_snapshot({
    as.data.frame(read_parquet_row_group(tmp, 0L))
    as.data.frame(read_parquet_row_group(tmp, 1L))
    as.data.frame(read_parquet_row_group(tmp, 2L))
    as.data.frame(read_parquet_row_group(tmp, 3L))
  })
})
