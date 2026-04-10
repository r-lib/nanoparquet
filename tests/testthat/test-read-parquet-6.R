test_that("REPEATED columns, no LIST", {
  pf <- test_path("data/repeated_primitive_no_list_no_nest.parquet")
  expect_snapshot({
    as.data.frame(read_parquet(pf))
  })
})

test_that("Only 3-layer LIST is supported for now", {
  pf <- test_path("data/old_list_structure.parquet")
  expect_snapshot(error = TRUE, {
    as.data.frame(read_parquet(pf))
  })
})

test_that("LIST", {
  expect_snapshot({
    as.data.frame(read_parquet(test_path("data/list-req-req.parquet")))
    as.data.frame(read_parquet(test_path("data/list-req-opt.parquet")))
    as.data.frame(read_parquet(test_path("data/list-opt-req.parquet")))
    as.data.frame(read_parquet(test_path("data/list-opt-opt.parquet")))
  })

  expect_snapshot({
    as.data.frame(read_parquet(test_path("data/list-v2-req-req.parquet")))
    as.data.frame(read_parquet(test_path("data/list-v2-req-opt.parquet")))
    as.data.frame(read_parquet(test_path("data/list-v2-opt-req.parquet")))
    as.data.frame(read_parquet(test_path("data/list-v2-opt-opt.parquet")))
  })

  elts <- c(
    "has_repetition_levels",
    "has_definition_levels",
    "num_values",
    "num_rows"
  )

  pf <- test_path("data/repeated_primitive_no_list_no_nest.parquet")
  pgoff <- read_parquet_pages(pf)$page_header_offset[4]
  expect_snapshot({
    read_parquet_page(pf, pgoff)[elts]
  })

  pf2 <- test_path("data/list-req-req.parquet")
  pgoff2 <- read_parquet_pages(pf2)$page_header_offset[2]
  expect_snapshot({
    read_parquet_page(pf2, pgoff2)[elts]
  })

  pf3 <- test_path("data/list-req-opt.parquet")
  pgoff3 <- read_parquet_pages(pf3)$page_header_offset[2]
  expect_snapshot({
    read_parquet_page(pf3, pgoff3)[elts]
  })

  pf4 <- test_path("data/list-opt-req.parquet")
  pgoff4 <- read_parquet_pages(pf4)$page_header_offset[2]
  expect_snapshot({
    read_parquet_page(pf4, pgoff4)[elts]
  })

  pf5 <- test_path("data/list-opt-opt.parquet")
  pgoff5 <- read_parquet_pages(pf5)$page_header_offset[2]
  expect_snapshot({
    read_parquet_page(pf5, pgoff5)[elts]
  })
})

test_that("write and read list(integer())", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:4)
  df$x <- list(1L, c(2L, 3L), NULL, c(4L, NA_integer_, 6L))
  write_parquet(df, tmp)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("write and read list(double())", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:4)
  df$x <- list(1.5, c(2.5, 3.5), NULL, c(4.5, NA_real_, 6.5))
  write_parquet(df, tmp)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("write and read list(character())", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:4)
  df$x <- list("a", c("b", "c"), NULL, c("d", NA_character_, "f"))
  write_parquet(df, tmp)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(integer()) multiple data pages", {
  withr::local_envvar(NANOPARQUET_PAGE_SIZE = "1024")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  N <- 1000
  pattern <- list(1L, c(2L, 3L), NULL, c(4L, NA_integer_, 6L))
  df <- data.frame(id = seq_len(N))
  df$x <- rep_len(pattern, N)

  write_parquet(df, tmp)
  pgs <- read_parquet_pages(tmp)
  expect_gt(sum(pgs$page_type == "DATA_PAGE"), 1)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)

  # data page v2
  write_parquet(df, tmp, options = parquet_options(write_data_page_version = 2))
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(double()) multiple data pages", {
  withr::local_envvar(NANOPARQUET_PAGE_SIZE = "1024")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  N <- 1000
  pattern <- list(1.5, c(2.5, 3.5), NULL, c(4.5, NA_real_, 6.5))
  df <- data.frame(id = seq_len(N))
  df$x <- rep_len(pattern, N)

  write_parquet(df, tmp)
  pgs <- read_parquet_pages(tmp)
  expect_gt(sum(pgs$page_type == "DATA_PAGE"), 1)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)

  # data page v2
  write_parquet(df, tmp, options = parquet_options(write_data_page_version = 2))
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(character()) multiple data pages", {
  withr::local_envvar(NANOPARQUET_PAGE_SIZE = "1024")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  N <- 1000
  pattern <- list("a", c("b", "c"), NULL, c("d", NA_character_, "f"))
  df <- data.frame(id = seq_len(N))
  df$x <- rep_len(pattern, N)

  write_parquet(df, tmp)
  pgs <- read_parquet_pages(tmp)
  expect_gt(sum(pgs$page_type == "DATA_PAGE"), 1)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)

  # data page v2
  write_parquet(df, tmp, options = parquet_options(write_data_page_version = 2))
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(integer()) multiple row groups", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:8)
  df$x <- list(1L, c(2L, 3L), NULL, c(4L, NA_integer_, 6L),
               integer(0), c(7L, 8L), NULL, 9L)
  write_parquet(df, tmp, row_groups = c(1L, 4L))
  expect_equal(nrow(read_parquet_metadata(tmp)[["row_groups"]]), 2L)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(double()) multiple row groups", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:8)
  df$x <- list(1.5, c(2.5, 3.5), NULL, c(4.5, NA_real_, 6.5),
               double(0), c(7.5, 8.5), NULL, 9.5)
  write_parquet(df, tmp, row_groups = c(1L, 4L))
  expect_equal(nrow(read_parquet_metadata(tmp)[["row_groups"]]), 2L)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(character()) multiple row groups", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:8)
  df$x <- list("a", c("b", "c"), NULL, c("d", NA_character_, "f"),
               character(0), c("g", "h"), NULL, "i")
  write_parquet(df, tmp, row_groups = c(1L, 4L))
  expect_equal(nrow(read_parquet_metadata(tmp)[["row_groups"]]), 2L)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(integer()) many row groups", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  pattern <- list(1L, c(2L, 3L), NULL, c(4L, NA_integer_, 6L),
                  integer(0), c(7L, 8L), NULL, 9L)
  df <- data.frame(id = 1:40)
  df$x <- rep_len(pattern, 40)
  write_parquet(df, tmp, row_groups = seq(1L, 40L, by = 4L))
  expect_equal(nrow(read_parquet_metadata(tmp)[["row_groups"]]), 10L)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(double()) many row groups", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  pattern <- list(1.5, c(2.5, 3.5), NULL, c(4.5, NA_real_, 6.5),
                  double(0), c(7.5, 8.5), NULL, 9.5)
  df <- data.frame(id = 1:40)
  df$x <- rep_len(pattern, 40)
  write_parquet(df, tmp, row_groups = seq(1L, 40L, by = 4L))
  expect_equal(nrow(read_parquet_metadata(tmp)[["row_groups"]]), 10L)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(character()) many row groups", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  pattern <- list("a", c("b", "c"), NULL, c("d", NA_character_, "f"),
                  character(0), c("g", "h"), NULL, "i")
  df <- data.frame(id = 1:40)
  df$x <- rep_len(pattern, 40)
  write_parquet(df, tmp, row_groups = seq(1L, 40L, by = 4L))
  expect_equal(nrow(read_parquet_metadata(tmp)[["row_groups"]]), 10L)
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
})

test_that("list(integer()) dictionary encoding", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:4)
  df$x <- list(1L, c(2L, 3L), NULL, c(4L, NA_integer_, 6L))
  df$y <- c("a", "b", "a", "b")

  write_parquet(df, tmp, encoding = c(x = "RLE_DICTIONARY"))
  pgs <- read_parquet_pages(tmp)
  expect_true(any(pgs$page_type == "DICTIONARY_PAGE"))
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
  expect_equal(df2$y, df$y)
})

test_that("list(double()) dictionary encoding", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:4)
  df$x <- list(1.5, c(2.5, 3.5), NULL, c(4.5, NA_real_, 6.5))
  df$y <- c("a", "b", "a", "b")

  write_parquet(df, tmp, encoding = c(x = "RLE_DICTIONARY"))
  pgs <- read_parquet_pages(tmp)
  expect_true(any(pgs$page_type == "DICTIONARY_PAGE"))
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
  expect_equal(df2$y, df$y)
})

test_that("list(character()) dictionary encoding", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(id = 1:4)
  df$x <- list("a", c("b", "c"), NULL, c("d", NA_character_, "f"))
  df$y <- c("a", "b", "a", "b")

  write_parquet(df, tmp, encoding = c(x = "RLE_DICTIONARY"))
  pgs <- read_parquet_pages(tmp)
  expect_true(any(pgs$page_type == "DICTIONARY_PAGE"))
  df2 <- as.data.frame(read_parquet(tmp))
  expect_equal(df2$id, df$id)
  expect_equal(df2$x, df$x)
  expect_equal(df2$y, df$y)
})

test_that("write and read zero columns, zero rows", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame()
  write_parquet(df, tmp)
  df2 <- read_parquet(tmp)
  expect_equal(ncol(df2), 0L)
  expect_equal(nrow(df2), 0L)
  expect_equal(
    read_parquet_metadata(tmp)$file_meta_data$num_rows,
    0L
  )
})

test_that("write and read zero rows", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(
    i = integer(),
    d = double(),
    s = character(),
    l = logical()
  )
  write_parquet(df, tmp)
  df2 <- read_parquet(tmp)
  expect_equal(nrow(df2), 0L)
  expect_equal(ncol(df2), 4L)
  expect_equal(names(df2), c("i", "d", "s", "l"))
  expect_equal(
    read_parquet_metadata(tmp)$file_meta_data$num_rows,
    0L
  )
})
