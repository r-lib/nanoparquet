test_that("round trip", {
  mt <- test_df()
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  expect_true(all(read_parquet(tmp) == mt))
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  expect_true(all(read_parquet(tmp) == mt))
})

test_that("round trip with arrow", {
  # Don't want to skip on the parquet capability missing, because then
  # this might not be tested on the CI. So rather we skip on CRAN.
  skip_on_cran()
  mt <- test_df(tibble = TRUE)
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  expect_equal(arrow::read_parquet(tmp), mt)
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  expect_equal(arrow::read_parquet(tmp), mt)
})

test_that("round trip with duckdb", {
  mt <- test_df()
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  write_parquet(mt, tmp, compression = "uncompressed")
  df <- duckdb:::sql(sprintf("FROM '%s'", tmp))
  expect_equal(df, mt)
  unlink(tmp)

  write_parquet(mt, tmp, compression = "snappy")
  df <- duckdb:::sql(sprintf("FROM '%s'", tmp))
  expect_equal(df, mt)
})

test_that("round trip with pandas/pyarrow", {
  skip_on_cran()
  mt <- test_df()
  tmp1 <- tempfile(fileext = ".parquet")
  tmp2 <- tempfile(fileext = ".parquet")
  on.exit(unlink(c(tmp1, tmp2)), add = TRUE)

  py_read <- function(input, output) {
    pyscript <- sprintf(r"[
import pyarrow
import pandas
df = pandas.read_parquet("%s", engine = "pyarrow")
print(df)
print(df.dtypes)
df.to_parquet("%s", engine = "pyarrow")
]", input, output)
    pytmp <- tempfile(fileext = ".py")
    on.exit(unlink(pytmp), add = TRUE)
    writeLines(pyscript, pytmp)
    py <- if (Sys.which("python3") != "") "python3" else "python"
    processx::run(py, pytmp)
  }

  write_parquet(mt, tmp1, compression = "uncompressed")
  pyout <- py_read(tmp1, tmp2)
  expect_snapshot(writeLines(pyout$stdout))

  mt2 <- read_parquet(tmp2)
  expect_equal(mt2, mt)
})
