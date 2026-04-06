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
})
