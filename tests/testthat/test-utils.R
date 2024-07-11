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

test_that("is_flag", {
  expect_true(is_flag(TRUE))
  expect_true(is_flag(FALSE))

  expect_false(is_flag(c(TRUE, TRUE)))
  expect_false(is_flag(1))
  expect_false(is_flag(NA))
})

test_that("is_string", {
  expect_true(is_string("a"))

  expect_false(is_string(c("a", "b")))
  expect_false(is_string(NA_character_))
  expect_false(is_string(1))
  expect_false(is_string(NULL))
})

test_that("is_uint32", {
  expect_true(is_uint32(0))
  expect_true(is_uint32(0L))
  expect_true(is_uint32(100))
  expect_true(is_uint32(100L))
  expect_true(is_uint32(4294967295))
  expect_true(is_uint32(4000000000))
  expect_true(is_uint32(2147483647))
  expect_true(is_uint32(2147483647L))

  expect_false(is_uint32(1/2))
  expect_false(is_uint32(-1))
  expect_false(is_uint32(4294967296))
  expect_false(is_uint32("a"))
  expect_false(is_uint32(NA_integer_))
  expect_false(is_uint32(NA_real_))
  expect_false(is_uint32("foo"))
})
