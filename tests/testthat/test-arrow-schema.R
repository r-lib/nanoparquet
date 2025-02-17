test_that("base64", {
  x <- serialize(mtcars, connection = NULL)
  x64 <- base64_encode(x)
  expect_equal(base64_decode(x64), x)
  expect_equal(base64_decode(charToRaw(x64)), x)

  # spaces are allowed
  x64s <- paste0(
    substr(x64, 1, 1000),
    "\n ",
    substr(x64, 1001, nchar(x64))
  )
  expect_equal(base64_decode(x64s), x)

  # strings are ok as well
  expect_equal(base64_decode(base64_encode("foo")), charToRaw("foo"))

  # errors, the `expect_error()`s are included because covr dor not work
  # correctly for `expect_snapshot()`, it seems.
  expect_error(base64_encode(1:10))
  expect_error(base64_decode(1:10))
  expect_error(base64_decode("foobar;;;"))

  expect_snapshot(error = TRUE, base64_encode(1:10))
  expect_snapshot(error = TRUE, base64_decode(1:10))
  expect_snapshot(error = TRUE, base64_decode("foobar;;;"))
})

test_that("factor_bits", {
  fct <- as.factor(letters)
  mockery::stub(factor_bits, "length", 100)
  expect_equal(factor_bits(fct), 8L)
  mockery::stub(factor_bits, "length", 127)
  expect_equal(factor_bits(fct), 8L)
  mockery::stub(factor_bits, "length", 128)
  expect_equal(factor_bits(fct), 16L)
  mockery::stub(factor_bits, "length", 32767)
  expect_equal(factor_bits(fct), 16L)
  mockery::stub(factor_bits, "length", 32768)
  expect_equal(factor_bits(fct), 32L)
  mockery::stub(factor_bits, "length", 2147483647)
  expect_equal(factor_bits(fct), 32L)
  mockery::stub(factor_bits, "length", 2147483648)
  expect_equal(factor_bits(fct), 64L)
})

test_that("temporal types", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  withr::local_options(nanoparquet.write_arrow_metadata = TRUE)

  # Date
  df_date <- data.frame(x = Sys.Date())
  write_parquet(df_date, tmp)
  np <- read_arrow_schema(tmp)
  expect_snapshot({
    np[["columns"]]
    np[["columns"]][["type"]]
  })

  # hms, integer
  # it is unclear if this ever comes up in practice
  df_hmsi <- data.frame(
    x = structure(0L, units = "secs", class = c("hms", "difftime"))
  )
  write_parquet(df_hmsi, tmp)
  np <- read_arrow_schema(tmp)
  expect_snapshot({
    np[["columns"]]
    np[["columns"]][["type"]]
  })

  # hms, double
  df_hmsd <- data.frame(x = hms::hms(0))
  write_parquet(df_hmsd, tmp)
  np <- read_arrow_schema(tmp)
  expect_snapshot({
    np[["columns"]]
    np[["columns"]][["type"]]
  })

  # difftime
  df_difftime <- data.frame(x = as.difftime(1, units = "secs"))
  write_parquet(df_difftime, tmp)
  np <- read_arrow_schema(tmp)
  expect_snapshot({
    np[["columns"]]
    np[["columns"]][["type"]]
  })

  # POSIXct
  df_posixct <- data.frame(x = Sys.time())
  write_parquet(df_posixct, tmp)
  np <- read_arrow_schema(tmp)
  expect_snapshot({
    np[["columns"]]
    np[["columns"]][["type"]]
  })

  # factor
  df_factor <- data.frame(x = as.factor(c("a", "a")))
  write_parquet(df_factor, tmp)
  np <- read_arrow_schema(tmp)
  expect_snapshot({
    np[["columns"]]
    np[["columns"]][["type"]]
  })
})
