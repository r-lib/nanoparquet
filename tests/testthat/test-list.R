test_that("LIST schema", {
  pf <- test_path("data/list.parquet")
  expect_snapshot({
    as.data.frame(read_parquet_schema(pf))
  })
})
