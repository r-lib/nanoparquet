test_that("parse_encoding", {
  expect_snapshot({
    names(mtcars)
    parse_encoding(NULL, mtcars)
    parse_encoding("PLAIN", mtcars)
    parse_encoding(c(disp = "RLE_DICTIONARY"), mtcars)
    parse_encoding(c(disp = "RLE_DICTIONARY", vs = "PLAIN"), mtcars)
    parse_encoding(c(disp = "RLE", "PLAIN"), mtcars)
    parse_encoding(c(disp = "RLE", "PLAIN", vs = "PLAIN"), mtcars)
  })

  expect_snapshot(error = TRUE, {
    parse_encoding(1:2, mtcars)
    parse_encoding(c("PLAIN", "foobar"), mtcars)
    parse_encoding(c(foo = "PLAIN", foo = "RLE"), mtcars)
    parse_encoding(c(disp = "PLAIN", foo = "RLE"), mtcars)
  })
})

test_that("BOOLEAN", {
  do <- function(d) {
    test_write(d)
    test_write(d, encoding = "PLAIN")
    test_write(d, encoding = "RLE")
  }
  do(data.frame(l = c(TRUE, FALSE, TRUE)))
  do(data.frame(l = c(TRUE, FALSE, NA, TRUE)))
  do(data.frame(l = rep(TRUE, 16)))
  do(data.frame(l = c(rep(TRUE, 8), NA, rep(TRUE, 8))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  d <- data.frame(l = c(rep(TRUE, 8), NA, rep(TRUE, 8)))
  expect_snapshot(error = TRUE, {
    # not implemented
    write_parquet(d, tmp, encoding = "RLE_DICTIONARY")
    write_parquet(d, tmp, encoding = "BIT_PACKED")
    # invalid for BOOLEAN
    write_parquet(d, tmp, encoding = "BYTE_STREAM_SPLIT")
  })
})

test_that("INT32", {
  do <- function(d) {
    test_write(d)
    test_write(d, encoding = "PLAIN")
    test_write(d, encoding = "RLE_DICTIONARY")
  }
  do(data.frame(d = 1:5))
  do(data.frame(d = c(1:2, NA, 3:5)))
  do(data.frame(d = rep(1L, 10)))
  do(data.frame(d = c(rep(1L, 5), NA, rep(1L, 5))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  d <- data.frame(d = c(rep(1L, 5), NA, rep(1L, 5)))
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, encoding = "DELTA_BINARY_PACKED")
    write_parquet(d, tmp, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for INT32
    write_parquet(d, tmp, encoding = "RLE")
  })
})

test_that("integer -> INT64", {
  schema <- "INT64"

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
    # unsupported for INT64
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT64", {
  schema <- "INT64"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
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
    # unsupported for INT64
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("integer -> INT96", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema("INT96")

  d <- data.frame(d = 1:5)
  expect_snapshot({
    write_parquet(d, tmp, schema = schema)
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]

    write_parquet(d, tmp, schema = schema, encoding = "PLAIN")
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]

    write_parquet(d, tmp, schema = schema, encoding = "RLE_DICTIONARY")
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
  })

  d <- data.frame(d = c(1:2, NA, 3:5))
  expect_snapshot({
    write_parquet(d, tmp, schema = schema)
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]

    write_parquet(d, tmp, schema = schema, encoding = "PLAIN")
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]

    write_parquet(d, tmp, schema = schema, encoding = "RLE_DICTIONARY")
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
  })

  d <- data.frame(d = rep(1L, 10))
  expect_snapshot({
    write_parquet(d, tmp, schema = schema)
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]

    write_parquet(d, tmp, schema = schema, encoding = "PLAIN")
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]

    write_parquet(d, tmp, schema = schema, encoding = "RLE_DICTIONARY")
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
  })

  d <- data.frame(d = c(rep(1L, 5), NA, rep(1L, 5)))
  expect_snapshot({
    write_parquet(d, tmp, schema = schema)
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]

    write_parquet(d, tmp, schema = schema, encoding = "PLAIN")
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]

    write_parquet(d, tmp, schema = schema, encoding = "RLE_DICTIONARY")
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
  })

  expect_snapshot(error = TRUE, {
    # unsupported for INT96
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("double -> INT96", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- "INT96"

  # TODO: fix tests
  if (.Platform$OS.type == "windows" && getRversion() < "4.2.0") {
    skip("Needs INT96 read w/o converting to time")
  }

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
  }
  do(data.frame(d = as.double(1:5)))
  do(data.frame(d = as.double(c(1:2, NA, 3:5))))
  do(data.frame(d = as.double(rep(1L, 10))))
  do(d <- data.frame(d = as.double(c(rep(1L, 5), NA, rep(1L, 5)))))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # unsupported for INT96
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("FLOAT", {
  schema <- "FLOAT"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
  }
  do(data.frame(d = 1:5 / 2))
  do(data.frame(d = c(1:2 / 2, NA, 3:5 / 2)))
  do(data.frame(d = rep(1, 10) / 2))
  do(d <- data.frame(d = c(rep(1, 5) / 2, NA, rep(1, 5) / 2)))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for FLOAT
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("DOUBLE", {
  schema <- "DOUBLE"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
  }
  do(data.frame(d = 1:5 / 2))
  do(data.frame(d = c(1:2 / 2, NA, 3:5/2)))
  do(data.frame(d = rep(1, 10) / 2))
  do(d <- data.frame(d = c(rep(1, 5) / 2, NA, rep(1, 5) / 2)))

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "BYTE_STREAM_SPLIT")
    # unsupported for DOUBLE
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("BYTE_ARRAY, string", {
  schema <- "STRING"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
  }
  do(data.frame(s = c("foo", "bar", "foobar")))
  do(data.frame(s = c("foo", "bar", NA, "foobar")))
  do(data.frame(d = rep("foo", 10)))
  d<- data.frame(d = rep("foo", 10))
  d[["d"]][5] <- NA
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

test_that("BYTE_ARRAY, RAW", {
  schema <- "BYTE_ARRAY"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
  }

  do(data.frame(s = I(lapply(c("foo", "bar", "foobar"), charToRaw))))
  do(data.frame(s = I(list(
    charToRaw("foo"),
    charToRaw("bar"),
    NULL,
    charToRaw("foobar")
  ))))
  do(data.frame(d = I(lapply(rep("foo", 10), charToRaw))))
  d <- data.frame(d = I(lapply(rep("foo", 10), charToRaw)))
  d[["d"]][5] <- list(NULL)
  do(d)

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    write_parquet(d, tmp, schema = schema, encoding = "PLAIN_DICTIONARY")
    # unsupported for BYTE_ARRAY
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("FIXED_LEN_BYTE_ARRAY, RAW", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- list("FIXED_LEN_BYTE_ARRAY", type_length = 3)

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
  }
  do(data.frame(s = I(lapply(c("foo", "bar", "aaa"), charToRaw))))
  d <- data.frame(s = I(lapply(c("foo", "bar", "aaa", "aaa"), charToRaw)))
  d[["s"]][3] <- list(NULL)
  do(d)
  do(data.frame(d = I(lapply(rep("foo", 10), charToRaw))))
  d <- data.frame(d = I(lapply(rep("foo", 10), charToRaw)))
  d[["d"]][5] <- list(NULL)
  do(d)

  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    write_parquet(d, tmp, schema = schema, encoding = "PLAIN_DICTIONARY")
    # unsupported for FIXED_LEN_BYTE_ARRAY
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("FIXED_LEN_BYTE_ARRAY, FLOAT16", {
  schema <- "FLOAT16"

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
  }
  do(data.frame(d = 1:5/2))
  d <- data.frame(d = 1:6/2)
  d[["d"]][3] <- NA
  do(d)
  do(data.frame(d = rep(1/2, 10)))
  d <- data.frame(d = rep(1/2, 10))
  d[["d"]][5] <- NA
  do(d)

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    # unsupported for FIXED_LEN_BYTE_ARRAY
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})

test_that("FIXED_LEN_BYTE_ARRAY, character", {
  schema <- list("FIXED_LEN_BYTE_ARRAY", type_length = 3)

  do <- function(d) {
    test_write(d, schema)
    test_write(d, schema, "PLAIN")
    test_write(d, schema, "RLE_DICTIONARY")
  }
  do(data.frame(s = c("foo", "bar", "aaa")))
  do(data.frame(s = c("foo", "bar", NA, "aaa")))
  do(data.frame(d = rep("foo", 10)))
  d <- data.frame(d = rep("foo", 10))
  d[["d"]][5] <- NA
  do(d)

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(schema)
  expect_snapshot(error = TRUE, {
    # not implemented yet
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_LENGTH_BYTE_ARRAY")
    write_parquet(d, tmp, schema = schema, encoding = "DELTA_BYTE_ARRAY")
    # unsupported for FIXED_LEN_BYTE_ARRAY
    write_parquet(d, tmp, schema = schema, encoding = "RLE")
  })
})
