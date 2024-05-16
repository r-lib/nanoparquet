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
