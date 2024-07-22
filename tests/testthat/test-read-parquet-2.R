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

})

test_that("FIXED_LEN_BYTE_ARRAY", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)

})

# -------------------------------------------------------------------------
# Logicaltypes
