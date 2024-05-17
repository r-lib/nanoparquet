test_that("parquet_pages", {
  pp <- parquet_pages(test_path("data/mtcars-arrow.parquet"))
  expect_snapshot(as.data.frame(pp))
})

test_that("read_parquet_page", {
  pf <- test_path("data/mtcars-arrow.parquet")
  expect_error(read_parquet_page(pf, 5))
  page <- read_parquet_page(pf, 4)
  expect_snapshot(page)
  page2 <- read_parquet_page(pf, 444)
  expect_snapshot(page2)
})

test_that("snappy", {
  for (i in 1:10) {
    x <- as.raw(as.integer(runif(1000) * 100))
    comp <- snappy_compress(x)
    uncp <- snappy_uncompress(comp)
    expect_false(length(x) == length(comp) && all(x == comp))
    expect_equal(uncp, x)
  }
})

test_that("snappy error", {
  expect_error(snappy_uncompress(charToRaw("foobar")))
})