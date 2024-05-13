test_that("is_rcmd_check", {
  withr::local_envvar(NOT_CRAN = "true")
  expect_false(is_rcmd_check())

  withr::local_envvar(
    NOT_CRAN = NA_character_,
    "_R_CHECK_PACKAGE_NAME_" = NA_character_
  )
  expect_false(is_rcmd_check())

  withr::local_envvar(
    NOT_CRAN = NA_character_,
    "_R_CHECK_PACKAGE_NAME_" = "foo"
  )
  expect_true(is_rcmd_check())
})

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

  # errors
  expect_snapshot(error = TRUE, base64_encode(1:10))
  expect_snapshot(error = TRUE, base64_decode(1:10))
  expect_snapshot(error = TRUE, base64_decode("foobar;;;"))
})
