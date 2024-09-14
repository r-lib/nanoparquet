test_that("errors", {
  options <- parquet_options()
  expect_snapshot(error = TRUE, {
   .Call(
      nanoparquet_write, mtcars, tempfile(), dim(mtcars), 0L,
      list(character(), character()), rep(FALSE, ncol(mtcars)),
      options, map_schema_to_df(NULL, mtcars), rep(10L, ncol(mtcars)), 1L,
      sys.call()
   )
  })
})

test_that("force PLAIN / RLE", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  d <- data.frame(d1 = 1:100, d2 = rep(1L, 100))

  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "true")
  write_parquet(d, tmp)
  expect_snapshot({
    read_parquet_pages(tmp)[["encoding"]]
  })

  withr::local_envvar(
    NANOPARQUET_FORCE_PLAIN = NA_character_,
    NANOPARQUET_FORCE_RLE = "true"
  )
  write_parquet(d, tmp)
  expect_snapshot({
    read_parquet_pages(tmp)[["encoding"]]
  })
})

test_that("cutoff for dict encoding decision", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  d <- data.frame(d1 = c(1:10000, rep(1L, 100000)))
  write_parquet(d, tmp)
  expect_snapshot({
    read_parquet_pages(tmp)[["encoding"]]
  })
  d <- data.frame(d1 = c(rep(c(TRUE, FALSE), 5000), rep(TRUE, 100000)))
  write_parquet(d, tmp)
  expect_snapshot({
    read_parquet_pages(tmp)[["encoding"]]
  })
})

test_that("write broken DECIMAL INT32", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(dec = 1:5)
  d2 <- data.frame(dec = as.double(1:5))

  schema <- parquet_schema(
    list("DECIMAL", precision = 5, scale = 2, primitive_type = "INT32")
  )
  schema$logical_type[1] <- list(NULL)

  # works w/o logical type
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
  })

  # no scale?
  schema2 <- schema
  schema2$scale <- NA_integer_
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = schema2)
  })

  # no precision?
  schema2 <- schema
  schema2$precision <- NA_integer_
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = schema2)
  })

  # bad precision?
  schema2 <- schema
  schema2$precision <- 10L
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = schema2)
  })
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema2)
  })
})

test_that("write broken DECIMAL INT64", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(dec = 1:5)
  d2 <- data.frame(dec = as.double(1:5))

  schema <- parquet_schema(
    list("DECIMAL", precision = 5, scale = 2, primitive_type = "INT64")
  )
  schema$logical_type[1] <- list(NULL)
  schema2 <- schema
  schema2$precision <- 19L
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = schema2)
  })
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema2)
  })
})

test_that("write broken INT32", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(dec = 1:5)
  d2 <- data.frame(dec = as.double(1:5))

  schema <- parquet_schema("INT_32")
  schema$logical_type[[1]]$bit_width <- 64L

  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = schema)
  })
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
})

test_that("write broken UINT32", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(dec = 1:5)
  d2 <- data.frame(dec = as.double(1:5))

  schema <- parquet_schema("UINT_32")
  schema$logical_type[[1]]$bit_width <- 64L

  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = schema)
  })
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
})

test_that("write broken INT64", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d2 <- data.frame(dec = as.double(1:5))

  schema <- parquet_schema("INT_64")
  schema$logical_type[[1]]$bit_width <- 32

  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
})

test_that("INT96 errors", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(c = c(TRUE, FALSE))

  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT96"))
  })
})

test_that("FLOAT errors", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(c = c(TRUE, FALSE))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("FLOAT"))
  })
})

test_that("DOUBLE errors", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(c = c(TRUE, FALSE))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("DOUBLE"))
  })
})

test_that("BYTE_ARRAY errors", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(c = I(list(c(TRUE, FALSE))))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("BYTE_ARRAY"))
  })

  d2 <- data.frame(c = c(TRUE, FALSE))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = parquet_schema("BYTE_ARRAY"))
  })
})

test_that("FIXED_LEN_BYTE_ARRAY errors", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(c = I(list(c(TRUE, FALSE))))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UUID"))
  })

  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("FLOAT16"))
  })

  d2 <- data.frame(c = c("foo", "bar", "foobar"))
  schema <- parquet_schema(list("FIXED_LEN_BYTE_ARRAY", type_length = 3))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })

  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = schema)
  })

  d3 <- data.frame(c = I(list(charToRaw("foo"), charToRaw("foo2"))))
  expect_snapshot(error = TRUE, {
    write_parquet(d3, tmp, schema = schema)
  })

  d4 <- data.frame(c = c(TRUE, FALSE))
  expect_snapshot(error = TRUE, {
    write_parquet(d4, tmp, schema = schema)
  })
})

test_that("BOOLEAN errors", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(c = 1:5)
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("BOOLEAN"))
  })

  schema <- parquet_schema(list("BOOLEAN", repetition_type = "OPTIONAL"))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = schema)
  })
})

test_that("Errors when writing a dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(c = 1:5)
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("DOUBLE"), encoding = "RLE_DICTIONARY")
  })

  d2 <- data.frame(c = as.double(1:5))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = parquet_schema("BYTE_ARRAY"), encoding = "RLE_DICTIONARY")
  })

  d3 <- data.frame(c = as.factor(letters))
  expect_snapshot(error = TRUE, {
    write_parquet(d3, tmp, schema = parquet_schema("DOUBLE"), encoding = "RLE_DICTIONARY")
  })

  # too small value for DECIMAL INT32
  d4 <- data.frame(c = -101L)
  schema4 <- parquet_schema(list("DECIMAL", precision = 2, primitive_type = "INT32"))
  expect_snapshot(error = TRUE, {
    write_parquet(d4, tmp, schema = schema4, encoding = "RLE_DICTIONARY")
  })
  # too larse values for DECIMAL INT32
  d5 <- data.frame(c = 101L)
  schema5 <- parquet_schema(list("DECIMAL", precision = 2, primitive_type = "INT32"))
  expect_snapshot(error = TRUE, {
    write_parquet(d5, tmp, schema = schema5, encoding = "RLE_DICTIONARY")
  })

  # too small value for DECIMAL INT64
  d4 <- data.frame(c = -101L)
  schema4 <- parquet_schema(list("DECIMAL", precision = 2, primitive_type = "INT64"))
  expect_snapshot(error = TRUE, {
    write_parquet(d4, tmp, schema = schema4, encoding = "RLE_DICTIONARY")
  })
  # too larse values for DECIMAL INT64
  d5 <- data.frame(c = 101L)
  schema5 <- parquet_schema(list("DECIMAL", precision = 2, primitive_type = "INT64"))
  expect_snapshot(error = TRUE, {
    write_parquet(d5, tmp, schema = schema5, encoding = "RLE_DICTIONARY")
  })

  # no INTSXP -> DOUBLE conversion
  d5 <- data.frame(c = 1:5)
  expect_snapshot(error = TRUE, {
    write_parquet(d5, tmp, schema = parquet_schema("DOUBLE"), encoding = "RLE_DICTIONARY")
  })
})

test_that("POSIXct dictionary", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = structure(1724157480.12919, class = c("POSIXct", "POSIXt")))
  write_parquet(d, tmp, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    read_parquet_metadata(tmp)[["column_chunks"]]$encodings
  })

  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT32"), encoding = "RLE_DICTIONARY")
  })

  d2 <- data.frame(x = as.difftime(1, units = "secs"))
  write_parquet(d2, tmp, encoding = "RLE_DICTIONARY")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
    read_parquet_metadata(tmp)[["column_chunks"]]$encodings
  })

  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = parquet_schema("INT32"), encoding = "RLE_DICTIONARY")
  })
})

test_that("more dictionaries", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # too small value for DECIMAL INT32
  d4 <- data.frame(c = as.double(-101L))
  schema4 <- parquet_schema(list("DECIMAL", precision = 2, primitive_type = "INT32"))
  expect_snapshot(error = TRUE, {
    write_parquet(d4, tmp, schema = schema4, encoding = "RLE_DICTIONARY")
  })
  # too larse values for DECIMAL INT32
  d5 <- data.frame(c = as.double(101L))
  schema5 <- parquet_schema(list("DECIMAL", precision = 2, primitive_type = "INT32"))
  expect_snapshot(error = TRUE, {
    write_parquet(d5, tmp, schema = schema5, encoding = "RLE_DICTIONARY")
  })

  # invalid bit width
  schema <- parquet_schema("INT_32")
  schema$logical_type[[1]]$bit_width <- 64L
  expect_snapshot(error = TRUE, {
    write_parquet(d5, tmp, schema = schema, encoding = "RLE_DICTIONARY")
  })
  schema <- parquet_schema("UINT_32")
  schema$logical_type[[1]]$bit_width <- 64L
  expect_snapshot(error = TRUE, {
    write_parquet(d5, tmp, schema = schema, encoding = "RLE_DICTIONARY")
  })

  # too large value
  d6 <- data.frame(c = 128.0)
  expect_snapshot(error = TRUE, {
    write_parquet(d6, tmp, schema = parquet_schema("INT_8"), encoding = "RLE_DICTIONARY")
  })

  # too large value
  d7 <- data.frame(c = 256.0)
  expect_snapshot(error = TRUE, {
    write_parquet(d7, tmp, schema = parquet_schema("UINT_8"), encoding = "RLE_DICTIONARY")
  })

  # too small value
  d8 <- data.frame(c = -1)
  expect_snapshot(error = TRUE, {
    write_parquet(d8, tmp, schema = parquet_schema("UINT_8"), encoding = "RLE_DICTIONARY")
  })

  # too small value for DECIMAL INT64
  d9 <- data.frame(c = as.double(-101L))
  schema4 <- parquet_schema(list("DECIMAL", precision = 2, primitive_type = "INT64"))
  expect_snapshot(error = TRUE, {
    write_parquet(d9, tmp, schema = schema4, encoding = "RLE_DICTIONARY")
  })
  # too larse values for DECIMAL INT32
  d10 <- data.frame(c = as.double(101L))
  schema5 <- parquet_schema(list("DECIMAL", precision = 2, primitive_type = "INT64"))
  expect_snapshot(error = TRUE, {
    write_parquet(d10, tmp, schema = schema5, encoding = "RLE_DICTIONARY")
  })

  # no DOUBLE -> BYTE_ARRAY conversion
  expect_snapshot(error = TRUE, {
    write_parquet(d10, tmp, schema = parquet_schema("BYTE_ARRAY"), encoding = "RLE_DICTIONARY")
  })
})

test_that("Even more dictionaries", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # bad UUID value
  d <- data.frame(x = "not-a-uuid")
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UUID"), encoding = "RLE_DICTIONARY")
  })

  # invalid string length in FIXED_LEN_BYTE_ARRAY
  d2 <- data.frame(x = c("foo", "thisisbad"))
  sch2 <- parquet_schema(list("FIXED_LEN_BYTE_ARRAY", type_length = 3))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = sch2, encoding = "RLE_DICTIONARY")
  })

  # no CHARSXP -> DOUBLE conversion
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = parquet_schema("DOUBLE"), encoding = "RLE_DICTIONARY")
  })
})

test_that("R -> Parquet mapping error", {
  d <- data.frame(x = raw(10))
  expect_snapshot(error = TRUE, {
    infer_parquet_schema(d)
  })
})

test_that("argument errors", {
  expect_snapshot(error = TRUE, {
    write_parquet(mtcars, 1:10)
    write_parquet(mtcars, letters)
  })
})