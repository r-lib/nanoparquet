test_that("write_parquet -> INT32", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # integer -> INT32
  d <- data.frame(d = c(1:10, NA))
  write_parquet(d, tmp, schema = parquet_schema("INT32"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d <- data.frame(d = c(NA, rep(1:2, 50)))
  write_parquet(d, tmp, schema = parquet_schema("INT32"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  # logical -> INT32
  d <- data.frame(d = c(NA, (1:10 %% 2 == 0)))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT32"))
  })

  # string -> INT32
  d <- data.frame(d = c(NA, paste(1:10)))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT32"))
  })
})

test_that("write_parquet -> INT64", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # integer -> INT64
  d <- data.frame(d = c(NA, 1:10))
  write_parquet(d, tmp, schema = parquet_schema("INT64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d <- data.frame(d = c(NA, rep(1:2, 50)))
  write_parquet(d, tmp, schema = parquet_schema("INT64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  # double -> INT64
  d <- data.frame(d = c(NA, as.double(1:10)))
  write_parquet(d, tmp, schema = parquet_schema("INT64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d <- data.frame(d = c(NA, as.double(rep(1:2, 50))))
  write_parquet(d, tmp, schema = parquet_schema("INT64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  # logical -> INT32
  d <- data.frame(d = (1:10 %% 2 == 0))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT64"))
  })

  # string -> INT32
  d <- data.frame(d = paste(1:10))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT64"))
  })

  # list -> INT32
  d <- data.frame(d = I(as.list(1:10)))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT64"))
  })
})

test_that("write_parquet -> INT96", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # integer -> INT96
  d <- data.frame(d = c(-5:5, NA))
  write_parquet(d, tmp, schema = parquet_schema("INT96"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    read_parquet_page(tmp, 4L)$data
  })
  d <- data.frame(d = c(rep(-1:1, 30), NA))
  write_parquet(d, tmp, schema = parquet_schema("INT96"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    read_parquet_page(tmp, 4L)$data
  })

  # double -> INT96
  d <- data.frame(d = as.double(c(-5:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("INT96"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    read_parquet_page(tmp, 4L)$data
  })
  d <- data.frame(d = as.double(c(rep(-1:1, 30), NA)))
  write_parquet(d, tmp, schema = parquet_schema("INT96"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    read_parquet_page(tmp, 4L)$data
  })
})

test_that("write_parquet -> FLOAT", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # double -> FLOAT
  d <- data.frame(d = c(as.double(-5:5), NA))
  write_parquet(d, tmp, schema = parquet_schema("FLOAT"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d <- data.frame(d = c(as.double(rep(-1:1, 30)), NA))
  write_parquet(d, tmp, schema = parquet_schema("FLOAT"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("write_parquet -> BYTE_ARRAY", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # character -> BYTE_ARRAY
  d <- data.frame(d = c("foo", "bar", "foobar", NA))
  write_parquet(
    d,
    tmp,
    schema = parquet_schema("BYTE_ARRAY")
  )
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("write_parquet -> FIXED_LEN_BYTE_ARRAY", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # character -> FIXED_LEN_BYTE_ARRAY
  d <- data.frame(d = c("foo", "bar", "aaa", NA))
  write_parquet(
    d,
    tmp,
    schema = parquet_schema(list("FIXED_LEN_BYTE_ARRAY", type_length = 3))
  )
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("write_parquet -> STRING", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # character -> STRING
  d <- data.frame(d = c("foo", "bar", "foobar", NA))
  write_parquet(d, tmp, schema = parquet_schema("STRING"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("write_parquet -> ENUM", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # character -> ENUM
  d <- data.frame(d = c("foo", "bar", "foobar", NA))
  write_parquet(d, tmp, schema = parquet_schema("ENUM"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  # factor -> ENUM
  d <- data.frame(d = as.factor(c("foo", "bar", "foobar", NA)))
  write_parquet(d, tmp, schema = parquet_schema("ENUM"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("write_parquet -> DECIMAL INT32", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # integer -> DECIMAL INT32
  d <- data.frame(d = c(-5:5, NA))
  schema <- parquet_schema(list(
    "DECIMAL",
    primitive_type = "INT32",
    scale = 0,
    precision = 3
  ))
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- data.frame(d = c(999L, 1000L))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
  d2 <- data.frame(d = -c(999L, 1000L))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })

  schema <- parquet_schema(list(
    "DECIMAL",
    primitive_type = "INT32",
    scale = 2,
    precision = 3
  ))
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- data.frame(d = c(9L, 10L))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
  d2 <- data.frame(d = -c(9L, 10L))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })

  # double -> DECIMAL INT32
  d <- data.frame(d = c(-5:5 / 2, NA))
  schema <- parquet_schema(list(
    "DECIMAL",
    primitive_type = "INT32",
    scale = 1,
    precision = 3
  ))
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- data.frame(d = c(99.9, 100))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
  d2 <- data.frame(d = -c(99.9, 100))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })

  schema <- parquet_schema(list(
    "DECIMAL",
    primitive_type = "INT32",
    scale = 2,
    precision = 3
  ))
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- data.frame(d = c(9.99, 10))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
  d2 <- data.frame(d = -c(9.99, 10))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
})

test_that("write_parquet -> DECIMAL INT64", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # integer -> DECIMAL INT64
  d <- data.frame(d = c(-5:5, NA))
  schema <- parquet_schema(list(
    "DECIMAL",
    primitive_type = "INT64",
    scale = 0,
    precision = 3
  ))
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- data.frame(d = c(999L, 1000L))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
  d2 <- data.frame(d = -c(999L, 1000L))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })

  schema <- parquet_schema(list(
    "DECIMAL",
    primitive_type = "INT64",
    scale = 2,
    precision = 3
  ))
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- data.frame(d = c(9L, 10L))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
  d2 <- data.frame(d = -c(9L, 10L))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })

  # double -> DECIMAL INT64
  d <- data.frame(d = c(-5:5 / 2, NA))
  schema <- parquet_schema(list(
    "DECIMAL",
    primitive_type = "INT64",
    scale = 1,
    precision = 3
  ))
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- data.frame(d = c(99.9, 100))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
  d2 <- data.frame(d = -c(99.9, 100))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })

  schema <- parquet_schema(list(
    "DECIMAL",
    primitive_type = "INT64",
    scale = 2,
    precision = 3
  ))
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- data.frame(d = c(9.99, 10))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
  d2 <- data.frame(d = -c(9.99, 10))
  expect_snapshot(error = TRUE, {
    write_parquet(d2, tmp, schema = schema)
  })
})

test_that("smaller integers", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = c(-5:5, NA))
  write_parquet(d, tmp, schema = parquet_schema("INT_8"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = 127:128)
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_8"))
  })
  d <- data.frame(d = -(127:129))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_8"))
  })

  d <- data.frame(d = c(-5:5, NA))
  write_parquet(d, tmp, schema = parquet_schema("INT_16"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = 32767:32768)
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_16"))
  })
  d <- data.frame(d = -(32767:32769))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_16"))
  })

  d <- data.frame(d = c(0:5, NA))
  write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = 255:256)
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
  })
  d <- data.frame(d = c(0L, -1L))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
  })

  d <- data.frame(d = c(0:5, NA))
  write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = 65536:65536)
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
  })
  d <- data.frame(d = c(0L, -1L))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
  })
})

test_that("double to smaller integers", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = as.double(c(-5:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("INT_8"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = as.double(127:128))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_8"))
  })
  d <- data.frame(d = -as.double((127:129)))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_8"))
  })

  d <- data.frame(d = as.double(c(-5:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("INT_16"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = as.double(32767:32768))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_16"))
  })
  d <- data.frame(d = -as.double(32767:32769))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_16"))
  })

  d <- data.frame(d = as.double(c(-5:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("INT_32"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = c(2^31 - 1, 2^31))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_32"))
  })
  d <- data.frame(d = c(-2^31, -2^31 - 1))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT_32"))
  })

  d <- data.frame(d = as.double(c(0:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = as.double(255:256))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
  })
  d <- data.frame(d = c(0, -1))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_8"))
  })

  d <- data.frame(d = as.double(c(0:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = as.double(65535:65536))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
  })
  d <- data.frame(d = c(0, -1))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_16"))
  })

  d <- data.frame(d = as.double(c(0:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("UINT_32"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = c(2^32 - 1, 2^32))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_32"))
  })
  d <- data.frame(d = c(0, -1))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_32"))
  })
})

test_that("double to INT(64, *)", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = as.double(c(-5:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("INT_64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = c(9223372036854770000, 9223372036854774273))
  expect_snapshot(
    error = TRUE,
    {
      write_parquet(d, tmp, schema = parquet_schema("INT_64"))
    },
    transform = redact_maxint64
  )
  d <- data.frame(d = -c(9223372036854770000, 9223372036854774273))
  expect_snapshot(
    error = TRUE,
    {
      write_parquet(d, tmp, schema = parquet_schema("INT_64"))
    },
    transform = redact_maxint64
  )

  d <- data.frame(d = as.double(c(0:5, NA)))
  write_parquet(d, tmp, schema = parquet_schema("UINT_64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = c(18446744073709450591.0, 18446744073709550592.0))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_64"))
  })
  d <- data.frame(d = c(0, -1))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UINT_64"))
  })
})

test_that("integer64 round-trip and read_int64_type option", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    x = bit64::as.integer64(c(1e15, -1e15, NA, 2^31))
  )
  write_parquet(d, tmp)

  res_dbl <- read_parquet(tmp)
  expect_equal(class(res_dbl$x), "numeric")
  expect_snapshot(as.data.frame(res_dbl))

  opts_i64 <- parquet_options(read_int64_type = "integer64")
  res_i64 <- read_parquet(tmp, options = opts_i64)
  expect_s3_class(res_i64$x, "integer64")
  expect_true(is.na(res_i64$x[3]))
  expect_equal(res_i64$x[c(1, 2, 4)], d$x[c(1, 2, 4)])

  opts_i64b <- parquet_options(read_int64_type = "bit64::integer64")
  res_i64b <- read_parquet(tmp, options = opts_i64b)
  expect_identical(res_i64b$x, res_i64$x)

  s_dbl <- read_parquet_schema(tmp)
  expect_equal(s_dbl$r_type[s_dbl$name == "x"], "double")

  s_i64 <- read_parquet_schema(tmp, options = opts_i64)
  expect_equal(s_i64$r_type[s_i64$name == "x"], "integer64")

  d2 <- data.frame(x = bit64::as.integer64(c(1L, 2L, NA)))
  write_parquet(d2, tmp, schema = parquet_schema("INT_64"))
  res2 <- read_parquet(tmp, options = opts_i64)
  expect_s3_class(res2$x, "integer64")
  expect_true(is.na(res2$x[3]))
  expect_equal(res2$x[c(1, 2)], d2$x[c(1, 2)])
})

test_that("JSON", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = c("foo", "bar", "foobar", NA))
  write_parquet(d, tmp, schema = parquet_schema("JSON"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("UUID", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = c(rep("00112233-4455-6677-8899-aabbccddeeff", 3), NA))
  write_parquet(d, tmp, schema = parquet_schema("UUID"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(d = c("00112233-4455-6677-8899-aabbccddeeff", "foo"))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("UUID"))
  })
})

test_that("FLOAT16", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(c = c(0, 1, 2, NA, -1, NaN, -2, -Inf, Inf, 1 / 2))
  write_parquet(d, tmp, schema = parquet_schema("FLOAT16"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("NaN", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(d = c(1, 2, NaN, 4, 5))
  write_parquet(d, tmp)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("list of RAW to BYTE_ARRAY", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    d = I(list(
      charToRaw("foo"),
      charToRaw("bar"),
      charToRaw("foobar")
    ))
  )
  write_parquet(d, tmp)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(
    d = I(list(
      charToRaw("foo"),
      NULL,
      charToRaw("bar"),
      charToRaw("foobar"),
      NULL
    ))
  )
  write_parquet(d, tmp)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("list of RAW to FIXED_LEN_BYTE_ARRAY", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(list("FIXED_LEN_BYTE_ARRAY", type_length = 3))

  d <- data.frame(
    d = I(list(
      charToRaw("foo"),
      charToRaw("bar"),
      charToRaw("aaa")
    ))
  )
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  d <- data.frame(
    d = I(list(
      charToRaw("foo"),
      NULL,
      charToRaw("bar"),
      charToRaw("aaa"),
      NULL
    ))
  )
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("blob::blob to BYTE_ARRAY", {
  skip_without("blob")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(
    d = blob::blob(
      charToRaw("foo"),
      charToRaw("bar"),
      charToRaw("foobar")
    )
  )
  write_parquet(d, tmp)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- read_parquet(tmp)
  expect_s3_class(d2$d, "blob")
  expect_equal(d2$d, d$d)

  # with NAs (NULL entries in blob)
  d_na <- data.frame(
    d = blob::blob(
      charToRaw("foo"),
      NULL,
      charToRaw("bar"),
      charToRaw("foobar"),
      NULL
    )
  )
  write_parquet(d_na, tmp)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d3 <- read_parquet(tmp)
  expect_s3_class(d3$d, "blob")
  expect_equal(d3$d, d_na$d)
})

test_that("blob::blob to FIXED_LEN_BYTE_ARRAY", {
  skip_without("blob")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema(list("FIXED_LEN_BYTE_ARRAY", type_length = 3))

  d <- data.frame(
    d = blob::blob(
      charToRaw("foo"),
      charToRaw("bar"),
      charToRaw("aaa")
    )
  )
  write_parquet(d, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d2 <- read_parquet(tmp)
  expect_s3_class(d2$d, "blob")
  expect_equal(d2$d, d$d)

  # with NAs
  d_na <- data.frame(
    d = blob::blob(
      charToRaw("foo"),
      NULL,
      charToRaw("bar"),
      charToRaw("aaa"),
      NULL
    )
  )
  write_parquet(d_na, tmp, schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d3 <- read_parquet(tmp)
  expect_s3_class(d3$d, "blob")
  expect_equal(d3$d, d_na$d)
})
