test_that("deprecated warnings", {
  pf <- test_path("data/alltypes_plain.parquet")
  expect_snapshot(
    sch <- parquet_schema(pf)
  )
  expect_equal(sch, read_parquet_schema(pf))

  expect_snapshot(
    pct <- parquet_column_types(mtcars)
  )

  expect_snapshot(
    mtd <- parquet_metadata(pf)
  )
  expect_equal(mtd, read_parquet_metadata(pf))

  expect_snapshot(
    info <- parquet_info(pf)
  )
  expect_equal(info, read_parquet_info(pf))
})
