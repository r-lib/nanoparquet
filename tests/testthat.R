if (.Platform$r_arch == "i386" && .Platform$OS.type == "windows") {
  message("Skipping tests on Windows i386")
} else {
  testthat::test_check("nanoparquet", reporter = "progress")
}
