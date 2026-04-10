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
