test_that("polars can read temporal types", {
  skip_without_polars()

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  do <- function(df, path = tmp) {
    write_parquet(df, path)
    pyscript <- sprintf(r"[
import polars as pl
pl.read_parquet("%s")
    ]", normalizePath(path, winslash = "/"))
    pytmp <- tempfile(fileext = ".py")
    on.exit(unlink(pytmp), add = TRUE)
    writeLines(pyscript, pytmp)
    py <- if (Sys.which("python3") != "") "python3" else "python"
    processx::run(py, pytmp, stderr = "2>&1")
  }

  # Date
  df_date <- data.frame(x = Sys.Date())
  expect_silent(do(df_date))

  # hms, integer
  # it is unclear if this ever comes up in practice
  df_hmsi <- data.frame(
    x = structure(0L, units = "secs", class = c("hms", "difftime"))
  )
  expect_silent(do(df_hmsi))

  # hms, double
  df_hmsd <- data.frame(x = hms::hms(0))
  expect_silent(do(df_hmsd))

  # difftime
  df_difftime <- data.frame(x = as.difftime(1, units = "secs"))
  expect_silent(do(df_difftime))

  # POSIXct
  df_posixct <- data.frame(x = Sys.time())
  expect_silent(do(df_posixct))

  # factor
  df_factor <- data.frame(x = as.factor(c("a", "a")))
  expect_silent(do(df_factor))
})
