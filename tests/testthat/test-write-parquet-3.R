test_that("write_parquet -> INT32", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # integer -> INT32
  d <- data.frame(d = 1:10)
  write_parquet(d, tmp, schema = parquet_schema("INT32"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d <- data.frame(d = rep(1:2, 50))
  write_parquet(d, tmp, schema = parquet_schema("INT32"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  # double -> INT32
  d <- data.frame(d = 1:10/2)
  d2 <- data.frame(d = rep(1:2, 50)/ 2)
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT32"))
    write_parquet(d2, tmp, schema = parquet_schema("INT32"))
  })

  # logical -> INT32
  d <- data.frame(d = (1:10 %% 2 == 0))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT32"))
  })

  # string -> INT32
  d <- data.frame(d = paste(1:10))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT32"))
  })

  # list -> INT32
  d <- data.frame(d = I(as.list(1:10)))
  expect_snapshot(error = TRUE, {
    write_parquet(d, tmp, schema = parquet_schema("INT32"))
  })
})

test_that("write_parquet -> INT64", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # integer -> INT64
  d <- data.frame(d = 1:10)
  write_parquet(d, tmp, schema = parquet_schema("INT64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d <- data.frame(d = rep(1:2, 50))
  write_parquet(d, tmp, schema = parquet_schema("INT64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  # double -> INT64
  d <- data.frame(d = as.double(1:10))
  write_parquet(d, tmp, schema = parquet_schema("INT64"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d <- data.frame(d = as.double(rep(1:2, 50)))
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
  d <- data.frame(d = -5:5)
  write_parquet(d, tmp, schema = parquet_schema("INT96"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    read_parquet_page(tmp, 4L)$data
  })
  d <- data.frame(d = rep(-1:1, 30))
  write_parquet(d, tmp, schema = parquet_schema("INT96"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    read_parquet_page(tmp, 4L)$data
  })

  # double -> INT64
  d <- data.frame(d = as.double(-5:5))
  write_parquet(d, tmp, schema = parquet_schema("INT96"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    read_parquet_page(tmp, 4L)$data
  })
  d <- data.frame(d = as.double(rep(-1:1, 30)))
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
  d <- data.frame(d = as.double(-5:5))
  write_parquet(d, tmp, schema = parquet_schema("FLOAT"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
  d <- data.frame(d = as.double(rep(-1:1, 30)))
  write_parquet(d, tmp, schema = parquet_schema("FLOAT"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })
})

test_that("write_parquet -> FIXED_LEN_BYTE_ARRAY", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  # character -> FIXED_LEN_BYTE_ARRAY
  d <- data.frame(d = c("foo", "bar", "aaa"))
  write_parquet(
    d, tmp,
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
  d <- data.frame(d = c("foo", "bar", "foobar"))
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
  d <- data.frame(d = c("foo", "bar", "foobar"))
  write_parquet(d, tmp, schema = parquet_schema("ENUM"))
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp)[, -1])
    as.data.frame(read_parquet(tmp))
  })

  # factor -> ENUM
  d <- data.frame(d = as.factor(c("foo", "bar", "foobar")))
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
  d <- data.frame(d = -5:5)
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
  d <- data.frame(d = -5:5 / 2)
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
  d <- data.frame(d = -5:5)
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
  d <- data.frame(d = -5:5 / 2)
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
