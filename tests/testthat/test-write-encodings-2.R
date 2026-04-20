test_that("character -> ENUM", {
  schema <- "ENUM"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }
  do(data.frame(s = c("foo", "bar", "foobar")))
  do(data.frame(s = c("foo", "bar", NA, "foobar")))
  do(data.frame(s = rep("foo", 10)))
  d <- data.frame(s = rep("foo", 11))
  d[["s"]][5] <- NA
  do(d)

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    # unsupported for BYTE_ARRAY
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("factor -> ENUM", {
  # a factor is always a dictionary
  schema <- "ENUM"

  d <- data.frame(s = as.factor(c("foo", "bar", "foobar")))
  test_write(d, schema)

  d <- data.frame(s = as.factor(c("foo", "bar", NA, "foobar")))
  test_write(d, schema)

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    # unsupported for BYTE_ARRAY
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("integer -> DECOMAL INT32", {
  schema <- list("DECIMAL", precision = 2, scale = 1, primitive_type = "INT32")

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = 1:5))
  do(data.frame(d = c(1:2, NA, 3:5)))
  do(data.frame(d = rep(1L, 10)))
  do(d <- data.frame(d = c(rep(1L, 5), NA, rep(1L, 5))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("integer -> DECIMAL INT64", {
  schema <- list("DECIMAL", precision = 2, scale = 1, primitive_type = "INT64")

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
  }

  do(data.frame(d = 1:5))
  do(data.frame(d = c(1:2, NA, 3:5)))
  do(data.frame(d = rep(1L, 10)))
  do(d <- data.frame(d = c(rep(1L, 5), NA, rep(1L, 5))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> DECOMAL INT32", {
  schema <- list("DECIMAL", precision = 2, scale = 1, primitive_type = "INT32")

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = rep(1, 10)))
  do(d <- data.frame(d = c(rep(1, 5), NA, rep(1, 5))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> DECIMAL INT64", {
  schema <- list("DECIMAL", precision = 2, scale = 1, primitive_type = "INT64")

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = rep(1, 10)))
  do(d <- data.frame(d = c(rep(1, 5), NA, rep(1, 5))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("integer -> INT(8, *)", {
  schema <- "INT_8"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = 1:5))
  do(data.frame(d = c(1:2, NA, 3:5)))
  do(data.frame(d = rep(1L, 10)))
  do(d <- data.frame(d = c(rep(1L, 5), NA, rep(1L, 5))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("integer -> INT(16, *)", {
  schema <- "INT_16"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = 1:5))
  do(data.frame(d = c(1:2, NA, 3:5)))
  do(data.frame(d = rep(1L, 10)))
  do(d <- data.frame(d = c(rep(1L, 5), NA, rep(1L, 5))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("integer -> INT(64, *)", {
  schema <- "INT_64"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = 1:5))
  do(data.frame(d = c(1:2, NA, 3:5)))
  do(data.frame(d = rep(1L, 10)))
  do(d <- data.frame(d = c(rep(1L, 5), NA, rep(1L, 5))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT64
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT(8, TRUE)", {
  schema <- "INT_8"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT(8, FALSE)", {
  schema <- "UINT_8"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT(16, TRUE)", {
  schema <- "INT_16"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT(16, FALSE)", {
  schema <- "UINT_16"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT(32, TRUE)", {
  schema <- "INT_32"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT(32, FALSE)", {
  schema <- "UINT_32"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT(64, TRUE)", {
  schema <- "INT_64"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT(64, FALSE)", {
  schema <- "UINT_64"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, encoding = "PLAIN")
    test_write(d, schema, encoding = "RLE_DICTIONARY")
  }

  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("character -> UUID", {
  schema <- "UUID"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
  }

  do(data.frame(
    u = c(
      "00112233-4455-6677-8899-aabbccddeeff",
      "01112233-4455-6677-8899-aabbccddeeff",
      "02112233-4455-6677-8899-aabbccddeeff"
    )
  ))
  do(data.frame(
    u = c(
      "00112233-4455-6677-8899-aabbccddeeff",
      "01112233-4455-6677-8899-aabbccddeeff",
      NA,
      "02112233-4455-6677-8899-aabbccddeeff"
    )
  ))
  do(data.frame(u = rep("00112233-4455-6677-8899-aabbccddeeff", 10)))
  d <- data.frame(u = rep("00112233-4455-6677-8899-aabbccddeeff", 10))
  d[[1]][5] <- NA
  do(d)
})
