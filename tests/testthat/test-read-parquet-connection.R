test_that("not open", {
  pf <- test_path("data/factor.parquet")
  con <- file(pf)
  expect_equal(
    read_parquet(con),
    read_parquet(pf)
  )
  # A closed (=invalid in R) connection will error here
  expect_error(isOpen(con))
})

test_that("open", {
  pf <- test_path("data/factor.parquet")
  con <- file(pf, open = "rb")
  on.exit(close(con), add = TRUE)
  expect_equal(
    read_parquet(con),
    read_parquet(pf)
  )
  expect_true(isOpen(con))
})

test_that("raw, opened", {
  pf <- test_path("data/factor.parquet")
  bts <- readBin(pf, what = "raw", n = file.size(pf))
  con <- rawConnection(bts, open = "rb")
  on.exit(close(con), add = TRUE)
  expect_equal(
    read_parquet(con),
    read_parquet(pf)
  )
  expect_true(isOpen(con))
})
