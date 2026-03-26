test_that("REPEATED columns, no LIST", {
  pf <- test_path("data/repeated_primitive_no_list.parquet")
  expect_snapshot({
    as.data.frame(read_parquet(pf, col_select = 1:2))
  })
})
