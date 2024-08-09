# temporary...
read_parquet <- read_parquet2

# -------------------------------------------------------------------------
# Primitive types

test_that("BOOLEAN", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(i = rep(c(TRUE, FALSE), 3))
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(1,3,5)] <- NA
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet(tmp))
  })

  # longer
  d <- data.frame(i = rep(c(TRUE, FALSE), 100))
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    read_parquet(tmp)[["i"]]
  })

  d[["i"]][c(1,3,5,50,99)] <- NA
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    read_parquet(tmp)[["i"]]
  })
})

test_that("INT32", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(i = 1:10)
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(2,6,10)] <- NA
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
  })

  # dict
  d <- data.frame(i = rep(1:2, 20))
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(1,5,10,20,39)] <- NA
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet(tmp))
  })
})

test_that("INT64", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema("INT64")

  d <- data.frame(i = 1:10)
  write_parquet(d, tmp, compression = "uncompressed", schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(2,6,10)] <- NA
  write_parquet(d, tmp, compression = "uncompressed", schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  # dict
  d <- data.frame(i = rep(1:2, 20))
  write_parquet(d, tmp, compression = "uncompressed", schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(1,5,10,20,39)] <- NA
  write_parquet(d, tmp, compression = "uncompressed", schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })
})

# TODO: INT96, once we have a way to not convert it to POSIXct

test_that("FLOAT", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema <- parquet_schema("FLOAT")

  d <- data.frame(i = 1:10/2)
  write_parquet(d, tmp, compression = "uncompressed", schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(2,6,10)] <- NA
  write_parquet(d, tmp, compression = "uncompressed", schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  # dict
  d <- data.frame(i = rep(1:2, 20)/2)
  write_parquet(d, tmp, compression = "uncompressed", schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(1,5,10,20,39)] <- NA
  write_parquet(d, tmp, compression = "uncompressed", schema = schema)
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet(tmp))
  })
})

test_that("DOUBLE", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(i = 1:10/2)
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(2,6,10)] <- NA
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet(tmp))
  })

  # dict
  d <- data.frame(i = rep(1:2, 20)/2)
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet(tmp))
  })

  d[["i"]][c(1,5,10,20,39)] <- NA
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet(tmp))
  })
})

test_that("BYTE_ARRAY", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(l = I(list(
    as.raw(1:10),
    as.raw(11:20),
    as.raw(1:3),
    as.raw(1:1)
  )))
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  # missing
  d[["l"]][3] <- list(NULL)
  write_parquet(d, tmp, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  # dict
  d <- data.frame(s = c("foo", "bar", "foobar"))
  write_parquet(
    d,
    tmp,
    compression = "uncompressed",
    encoding = "RLE_DICTIONARY"
  )
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  # missing
  d <- data.frame(s = c("foo", "bar", NA, "foobar"))
  write_parquet(
    d,
    tmp,
    compression = "uncompressed",
    encoding = "RLE_DICTIONARY"
  )
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })
})

test_that("FIXED_LEN_BYTE_ARRAY", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- data.frame(l = I(list(
    as.raw(1:10),
    as.raw(11:20),
    as.raw(1:10),
    as.raw(21:30)
  )))
  schema <- parquet_schema(list("FIXED_LEN_BYTE_ARRAY", type_length = 10))
  write_parquet(d, tmp, schema = schema, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  # missing
  d[["l"]][3] <- list(NULL)
  write_parquet(d, tmp, schema = schema, compression = "uncompressed")
  expect_snapshot({
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  # dict
  d <- data.frame(s = c("foo", "bar", "aaa"))
  schema <- parquet_schema(list("FIXED_LEN_BYTE_ARRAY", type_length = 3))
  write_parquet(
    d,
    tmp,
    schema = schema,
    compression = "uncompressed",
    encoding = "RLE_DICTIONARY"
  )
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })

  # missing
  d <- data.frame(s = c("foo", "bar", NA, "aaa"))
  schema <- parquet_schema(list("FIXED_LEN_BYTE_ARRAY", type_length = 3))
  write_parquet(
    d,
    tmp,
    schema = schema,
    compression = "uncompressed",
    encoding = "RLE_DICTIONARY"
  )
  expect_snapshot({
    as.data.frame(read_parquet_pages(tmp))[["page_type"]]
    as.data.frame(read_parquet_schema(tmp))[, -1]
    as.data.frame(read_parquet(tmp))
  })
})

# -------------------------------------------------------------------------
# Compression

test_that("snappy compression", {
  skip("not yet")
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

  d <- test_df()
  write_parquet(d, tmp, compression = "snappy", encoding = "PLAIN")
  expect_equal(read_parquet(tmp), d)

  # dict
#  write_parquet(d, tmp, compression = "snappy", encoding = "RLE_DICTIONARY")
#  expect_equal(read_parquet(tmp), d)
})
