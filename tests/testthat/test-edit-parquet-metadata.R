test_that("edit_parquet_metadata adds a new key", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(data.frame(x = 1:3), tmp)
  edit_parquet_metadata(tmp, c(author = "Alice"))

  kv <- read_parquet_metadata(tmp)$file_meta_data$key_value_metadata[[1]]
  expect_true("author" %in% kv$key)
  expect_equal(kv$value[kv$key == "author"], "Alice")
})

test_that("edit_parquet_metadata updates an existing key", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(data.frame(x = 1:3), tmp, metadata = c(author = "Alice"))
  edit_parquet_metadata(tmp, c(author = "Bob"))

  kv <- read_parquet_metadata(tmp)$file_meta_data$key_value_metadata[[1]]
  expect_equal(kv$value[kv$key == "author"], "Bob")
  expect_equal(sum(kv$key == "author"), 1L)
})

test_that("edit_parquet_metadata removes a key via NA value", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(
    data.frame(x = 1:3), tmp,
    metadata = c(author = "Alice", version = "1"),
    options = parquet_options(write_arrow_metadata = FALSE)
  )
  edit_parquet_metadata(tmp, c(version = NA_character_))

  kv <- read_parquet_metadata(tmp)$file_meta_data$key_value_metadata[[1]]
  expect_false("version" %in% kv$key)
  expect_true("author" %in% kv$key)
})

test_that("edit_parquet_metadata clears all metadata with NULL", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(
    data.frame(x = 1:3), tmp,
    metadata = c(author = "Alice"),
    options = parquet_options(write_arrow_metadata = FALSE)
  )
  edit_parquet_metadata(tmp, NULL)

  kv <- read_parquet_metadata(tmp)$file_meta_data$key_value_metadata[[1]]
  expect_equal(nrow(kv), 0L)
})

test_that("edit_parquet_metadata accepts a data frame", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(data.frame(x = 1:3), tmp,
    options = parquet_options(write_arrow_metadata = FALSE))
  edit_parquet_metadata(
    tmp,
    data.frame(key = c("k1", "k2"), value = c("v1", "v2"))
  )

  kv <- read_parquet_metadata(tmp)$file_meta_data$key_value_metadata[[1]]
  expect_equal(kv$value[kv$key == "k1"], "v1")
  expect_equal(kv$value[kv$key == "k2"], "v2")
})

test_that("edit_parquet_metadata preserves untouched keys", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(
    data.frame(x = 1:3), tmp,
    metadata = c(a = "1", b = "2"),
    options = parquet_options(write_arrow_metadata = FALSE)
  )
  edit_parquet_metadata(tmp, c(b = "99"))

  kv <- read_parquet_metadata(tmp)$file_meta_data$key_value_metadata[[1]]
  expect_equal(kv$value[kv$key == "a"], "1")
  expect_equal(kv$value[kv$key == "b"], "99")
})

test_that("edit_parquet_metadata data is readable after editing", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(x = 1:10, y = letters[1:10])
  write_parquet(df, tmp)
  edit_parquet_metadata(tmp, c(tag = "test"))
  edit_parquet_metadata(tmp, c(tag = NA_character_))
  edit_parquet_metadata(tmp, NULL)

  result <- read_parquet(tmp)
  expect_equal(result$x, df$x)
  expect_equal(result$y, df$y)
})

test_that("edit_parquet_metadata truncates file when footer shrinks", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(
    data.frame(x = 1:3), tmp,
    metadata = c(a = "x", b = "y", c = "z"),
    options = parquet_options(write_arrow_metadata = FALSE)
  )
  size_before <- file.size(tmp)
  edit_parquet_metadata(tmp, NULL)
  size_after <- file.size(tmp)

  expect_lt(size_after, size_before)
  # data still intact
  expect_equal(read_parquet(tmp)$x, 1:3)
})

test_that("edit_parquet_metadata rejects invalid metadata argument", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(data.frame(x = 1L), tmp)
  expect_error(edit_parquet_metadata(tmp, 1:3))
  expect_error(edit_parquet_metadata(tmp, c("no_name")))
})
