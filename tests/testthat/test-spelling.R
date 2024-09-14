test_that("spelling", {
  skip_on_cran()
  skip_on_covr()

  pkg <- test_path("../../")
  if (!file.exists(file.path(pkg, "DESCRIPTION"))) {
    pkg <- file.path(pkg, "00_pkg_src", .packageName)
  }

  expect_snapshot({
    spelling::spell_check_package(pkg)
  })
})
