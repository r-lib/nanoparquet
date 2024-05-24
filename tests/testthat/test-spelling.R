test_that("spelling", {
  skip_on_cran()
  pkg <- test_package_root()
  expect_snapshot({
    spelling::spell_check_package(pkg)
  })
})