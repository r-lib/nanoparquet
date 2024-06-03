test_that("spelling", {
  skip_on_cran()
  skip_on_covr()
  pkg <- test_package_root()
  expect_snapshot({
    spelling::spell_check_package(pkg)
  })
})
