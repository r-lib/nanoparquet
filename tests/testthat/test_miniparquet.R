alltypes_plain <- structure(
  list(
    id = c(4L, 5L, 6L, 7L, 2L, 3L, 0L, 1L),
    bool_col = c(TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE),
    tinyint_col = c(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L),
    smallint_col = c(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L),
    int_col = c(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L),
    bigint_col = c(0, 10, 0, 10, 0, 10, 0, 10),
    float_col = c(
      0, 1.10000002384186, 0, 1.10000002384186,
      0, 1.10000002384186, 0, 1.10000002384186
    ),
    double_col = c(0, 10.1, 0, 10.1, 0, 10.1, 0, 10.1),
    date_string_col = c(
      "03/01/09", "03/01/09", "04/01/09", "04/01/09",
      "02/01/09", "02/01/09", "01/01/09", "01/01/09"
    ),
    string_col = c("0", "1", "0", "1", "0", "1", "0", "1"),
    timestamp_col = structure(c(
      1235865600, 1235865660, 1238544000, 1238544060,
      1233446400, 1233446460, 1230768000, 1230768060
    ), class = c("POSIXct", "POSIXt"), tzone = "UTC")
  ),
  row.names = c(NA, -8L),
  class = "data.frame"
)

alltypes_plain_snappy <- structure(
  list(
    id = 6:7,
    bool_col = c(TRUE, TRUE),
    tinyint_col = 0:1,
    smallint_col = 0:1,
    int_col = 0:1,
    bigint_col = c(0, 10),
    float_col = c(0, 1.10000002384186),
    double_col = c(0, 10.1),
    date_string_col = c("04/01/09", "04/01/09"),
    string_col = c("0", "1"),
    timestamp_col = structure(
      c(1238544000, 1238544060),
      class = c("POSIXct", "POSIXt"),
      tzone = "UTC"
    )
  ),
  row.names = c(NA, -2L),
  class = "data.frame"
)

data_comparable <- function(df1, df2, dlt = .0001) {
  df1 <- as.data.frame(df1, stringsAsFactors = F)
  df2 <- as.data.frame(df2, stringsAsFactors = F)
  if (!identical(dim(df1), dim(df2))) {
    return(FALSE)
  }
  for (col_i in length(df1)) {
    col1 <- df1[[col_i]]
    col2 <- df2[[col_i]]
    if (is.numeric(col1)) {
      # reference answers are rounded to two decimals
      col1 <- round(col1, 2)
      col2 <- round(col2, 2)
      if (any(abs(col1 - col2) > col1 * dlt)) {
        return(FALSE)
      }
    } else {
      col1 <- trimws(as.character(col1))
      col2 <- trimws(as.character(col2))
      if (any(col1 != col2)) {
        return(FALSE)
      }
    }
  }
  return(TRUE)
}

test_that("various error cases", {
  expect_error(res <- parquet_read(""))
  expect_error(res <- parquet_read("DONTEXIST"))
  tf <- tempfile()
  expect_error(res <- parquet_read(tf))
  expect_error(res <- parquet_read(c(tf, tf)))
})

test_that("basic reading works", {
  res <- parquet_read(test_path("data/alltypes_plain.parquet"))
  expect_true(data_comparable(alltypes_plain, res))
})

test_that("basic reading works with snappy", {
  res <- parquet_read(test_path("data/alltypes_plain.snappy.parquet"))
  expect_true(data_comparable(alltypes_plain_snappy, res))
})
