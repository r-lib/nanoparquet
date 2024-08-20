test_that("infer_parquet_schema", {
  expect_snapshot({
    as.data.frame(infer_parquet_schema(test_df(missing = FALSE)))
  })
  expect_snapshot({
    as.data.frame(infer_parquet_schema(test_df(missing = TRUE)))
  })
})

test_that("logical_to_converted", {
  expect_snapshot(error = TRUE, {
    logical_to_converted(list("DECIMAL"))
    logical_to_converted(list("TIME", TRUE, "SECS"))
    logical_to_converted(list("TIMESTAMP", TRUE, "SECS"))
    logical_to_converted(list("INT"))
    logical_to_converted(list("FOOBAR"))
  })
})
