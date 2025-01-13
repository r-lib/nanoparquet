test_that("plain and simple", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- as.data.frame(test_df())
  write_parquet(
    df,
    tmp,
    row_groups = c(1L, 11L, 21L, 31L),
    encoding = "PLAIN"
  )

  # 4 row groups
  expect_snapshot(as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1])

  un <- function(x) {
    x <- as.data.frame(x)
    rownames(x) <- NULL
    x
  }

  expect_equal(un(read_parquet_row_group(tmp, 0L)), un(df[1:10,]))
  expect_equal(un(read_parquet_row_group(tmp, 1L)), un(df[11:20,]))
  expect_equal(un(read_parquet_row_group(tmp, 2L)), un(df[21:30,]))
  expect_equal(un(read_parquet_row_group(tmp, 3L)), un(df[31:32,]))
})

test_that("dicts", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- test_df()
  write_parquet(
    df,
    tmp,
    row_groups = c(1L, 11L, 21L, 31L),
    encoding = c(large = "RLE", "RLE_DICTIONARY")
  )

  # 4 row groups, dicts
  expect_snapshot({
    as.data.frame(read_parquet_metadata(tmp)$row_groups)[-1]
  })

  un <- function(x) {
    x <- as.data.frame(x)
    rownames(x) <- NULL
    x
  }

  expect_equal(un(read_parquet_row_group(tmp, 0L)), un(df[1:10,]))
  expect_equal(un(read_parquet_row_group(tmp, 1L)), un(df[11:20,]))
  expect_equal(un(read_parquet_row_group(tmp, 2L)), un(df[21:30,]))
  expect_equal(un(read_parquet_row_group(tmp, 3L)), un(df[31:32,]))
})
