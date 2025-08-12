test_that("rle_decode", {
  # arrow might be broken on CRAN
  skip_on_cran()
  skip_without("arrow")
  tmp <- tempfile(fileext = ".parquet")
  d <- data.frame(
    x = as.factor(sample(letters, 100, replace = TRUE))
  )
  arrow::write_parquet(d, tmp)
  pages <- read_parquet_pages(tmp)
  data_page <- read_parquet_page(tmp, pages$page_header_offset[2])
  data <- data_page$data
  def_len <- readBin(data, "int", n = 1)
  data <- data[-(1:(def_len + 4L))]
  bw <- as.integer(data[1])
  data <- data[-1]
  idx <- rle_decode_int(data, bit_width = bw, length = nrow(d))
  # parquet indices are zero based, hence the -1L
  expect_equal(idx, as.integer(d$x) - 1L)
})

test_that("rle_encode", {
  chk <- function(x) {
    r <- rle_encode_int(x)
    x2 <- rle_decode_int(r, attr(r, "bit_width"), length(x))
    expect_equal(x2, x)
  }
  chk(c(0:16, rep(1L, 20), 0:16, rep(2L, 20)))
})

test_that("edge cases", {
  chk <- function(x) {
    r <- rle_encode_int(x)
    x2 <- rle_decode_int(r, attr(r, "bit_width"), length(x))
    expect_equal(x2, x)
  }
  chk(integer())
  chk(0L)
  chk(1L)
  chk(7L)
  chk(8L)
  chk(100L)
  chk(1:2)
  chk(1:3)
  chk(1:4)
  chk(1:5)
  chk(1:6)
  chk(1:7)
  chk(1:8)
  chk(1:9)
  chk(0:1)
  chk(0:2)
  chk(0:3)
  chk(0:4)
  chk(0:5)
  chk(0:6)
  chk(0:7)
  chk(0:8)
  chk(0:9)
  for (l in c(2, 7, 8, 9, 16, 256, 512, 513, 100000)) {
    chk(rep(0L, l))
    chk(rep(1L, l))
  }
})
