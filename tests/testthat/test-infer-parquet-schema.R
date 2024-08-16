test_that("infer_parquet_schema", {
  expect_snapshot({
    as.data.frame(infer_parquet_schema(test_df(missing = FALSE)))
  })
  expect_snapshot({
    as.data.frame(infer_parquet_schema(test_df(missing = TRUE)))
  })
})
