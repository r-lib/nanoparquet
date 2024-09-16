test_that("errors", {
  expect_snapshot(error = TRUE, {
    parquet_options(num_rows_per_row_group = "foobar")
  })

  df <- test_df()
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  expect_snapshot(error = TRUE, {
    write_parquet(df, tmp, row_groups = "foobar")
    write_parquet(df, tmp, row_groups = c(100L, 1L))
    write_parquet(df, tmp, row_groups = c(1L, 100L))
  })
})

test_that("row groups", {
  tmp1 <- tempfile(fileext = ".parquet")
  tmp2 <- tempfile(fileext = ".parquet")
  on.exit(unlink(c(tmp1, tmp2)), add = TRUE)

  df <- test_df()
  write_parquet(df, tmp1, row_groups = 1L)
  write_parquet(df, tmp2, row_groups = c(1L, 16L))
  expect_equal(read_parquet(tmp1), read_parquet(tmp2))
  expect_equal(nrow(read_parquet_metadata(tmp2)[["row_groups"]]), 2L)

  unlink(tmp2)
  write_parquet(df, tmp2, row_groups = seq_len(nrow(df)))
  expect_equal(read_parquet(tmp1), read_parquet(tmp2))
  expect_equal(nrow(read_parquet_metadata(tmp2)[["row_groups"]]), nrow(df))

  unlink(tmp2)
  withr::local_options(nanoparquet.num_rows_per_row_group = 10L)
  write_parquet(df, tmp2)
  expect_equal(read_parquet(tmp1), read_parquet(tmp2))
  expect_equal(nrow(read_parquet_metadata(tmp2)[["row_groups"]]), 4L)
})

test_that("grouped df", {
  df <- test_df()
  attr(df, "groups") <- data.frame(
    cyl = c(4L, 6L, 8L),
    .rows = I(list(
      c(3L, 8L, 9L, 18L, 19L, 20L, 21L, 26L, 27L, 28L, 32L),
      c(1L, 2L, 4L, 6L, 10L, 11L, 30L),
      c(5L, 7L, 12L, 13L, 14L, 15L, 16L, 17L, 22L, 23L, 24L, 25L, 29L, 31L)
    ))
  )

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  expect_snapshot(write_parquet(df, tmp))
  expect_equal(nrow(read_parquet_metadata(tmp)[["row_groups"]]), 3L)
  expect_snapshot(as.data.frame(read_parquet(tmp)[, c("nam", "cyl")]))
})

test_that("factors & factor levels", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(
    f = factor(c(rep("a", 100), rep("b", 100), rep("c", 100)))
  )
  withr::local_options(nanoparquet.num_rows_per_row_group = 50L)
  write_parquet(df, tmp)
  expect_equal(as.data.frame(read_parquet(tmp)), df)
  # the same dict is written into every dicitonary page
  pgs <- read_parquet_pages(tmp)
  dict_ofs <- pgs[["page_header_offset"]][
    pgs[["page_type"]] == "DICTIONARY_PAGE"
  ]
  dict_data <- read_parquet_page(tmp, dict_ofs[1])[["data"]]
  for (do in dict_ofs) {
    expect_equal(dict_data, read_parquet_page(tmp, do)[["data"]])
  }
})

test_that("non-factors write local dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(
    stringsAsFactors = FALSE,
    f = c(rep("a", 100), rep("b", 100), rep("c", 100))
  )
  withr::local_options(nanoparquet.num_rows_per_row_group = 40L)
  write_parquet(df, tmp)
  expect_equal(as.data.frame(read_parquet(tmp)), df)
  pgs <- read_parquet_pages(tmp)
  dict_ofs <- pgs[["page_header_offset"]][
    pgs[["page_type"]] == "DICTIONARY_PAGE"
  ]
  expect_snapshot({
    for (do in dict_ofs) {
      print(read_parquet_page(tmp, do)[["data"]])
    }
  })
})

test_that("strings in a dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- test_df()
  write_parquet(
    df, tmp,
    encoding = c(large = "RLE", "RLE_DICTIONARY"),
    options = parquet_options(num_rows_per_row_group=10)
  )
  expect_equal(as.data.frame(df), as.data.frame(read_parquet(tmp)))
})
