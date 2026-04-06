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
