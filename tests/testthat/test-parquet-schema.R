test_that("parquet_type", {
  expect_snapshot({
    parquet_type("AUTO")
    parquet_type("BOOLEAN")
    parquet_type("INT32")
    parquet_type("INT64")
    parquet_type("INT96")
    parquet_type("FLOAT")
    parquet_type("DOUBLE")
    parquet_type("BYTE_ARRAY")
    parquet_type("FIXED_LEN_BYTE_ARRAY", type_length = 10)

    parquet_type("STRING")
    parquet_type("ENUM")
    parquet_type("UUID")
    parquet_type("INT", bit_width = 8, is_signed = TRUE)
    parquet_type("INT", bit_width = 16, is_signed = TRUE)
    parquet_type("INT", bit_width = 32, is_signed = FALSE)
    parquet_type("INT", bit_width = 64, is_signed = FALSE)
    parquet_type("DECIMAL", precision = 5, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 5, scale = 0, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 5, scale = 5, primitive_type = "INT64")
    parquet_type("DECIMAL", precision = 5, primitive_type = "BYTE_ARRAY")
    parquet_type(
      "DECIMAL",
      precision = 5,
      primitive_type = "FIXED_LEN_BYTE_ARRAY",
      type_length = 5
    )
    parquet_type("FLOAT16")
    parquet_type("DATE")
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MILLIS")
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MICROS")
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "NANOS")
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS")
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS")
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "NANOS")
    parquet_type("JSON")
    parquet_type("BSON")
  })

  expect_snapshot({
    parquet_type("INT32", repetition_type = "OPTIONAL")
    parquet_type("STRING", repetition_type = "REQUIRED")
    parquet_type(
      "TIME",
      repetition_type = "REPEATED",
      is_adjusted_utc = TRUE,
      unit = "MILLIS"
    )
  })

  expect_snapshot(error = TRUE, {
    parquet_type("FOO")
    parquet_type("FIXED_LEN_BYTE_ARRAY")
    parquet_type("INT", bit_width = 8)
    parquet_type("INT", is_signed = TRUE)
    parquet_type("INT", bit_width = 10, is_signed = TRUE)
    parquet_type("INT", bit_width = 16, is_signed = 1)
    parquet_type("DECIMAL", precision = 5)
    parquet_type("DECIMAL", primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 5 / 2, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 0, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 5, scale = 6, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 10, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 19, primitive_type = "INT64")
    parquet_type(
      "DECIMAL",
      precision = 12,
      primitive_type = "FIXED_LEN_BYTE_ARRAY",
      type_length = 5
    )
    parquet_type("TIME", is_adjusted_utc = TRUE)
    parquet_type("TIME", unit = "MILLIS")
    parquet_type("TIME", is_adjusted_utc = 1, unit = "MILLIS")
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "FOO")
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE)
    parquet_type("TIMESTAMP", unit = "MILLIS")
    parquet_type("TIMESTAMP", is_adjusted_utc = 1, unit = "MILLIS")
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "FOO")
    parquet_type("LIST")
    parquet_type("MAP")
    parquet_type("UNKNOWN")
    parquet_type("INT32", repetition_type = TRUE)
    parquet_type("INT32", repetition_type = "FOO")
  })

  # Need this as well for covr, which does not handle stop()
  # in snapshots, apparently
  expect_error(parquet_type("FOO"), "not supported by nanoparquet")
  expect_error(parquet_type("LIST"), "not supported by nanoparquet")
  expect_error(parquet_type("MAP"), "not supported by nanoparquet")
  expect_error(parquet_type("UNKNOWN"), "not supported by nanoparquet")
})

test_that("parquet_type converted type shortcuts", {
  expect_equal(
    parquet_type("INT_8"),
    parquet_type("INT", bit_width = 8, is_signed = TRUE)
  )
  expect_equal(
    parquet_type("INT_16"),
    parquet_type("INT", bit_width = 16, is_signed = TRUE)
  )
  expect_equal(
    parquet_type("INT_32"),
    parquet_type("INT", bit_width = 32, is_signed = TRUE)
  )
  expect_equal(
    parquet_type("INT_64"),
    parquet_type("INT", bit_width = 64, is_signed = TRUE)
  )

  expect_equal(
    parquet_type("UINT_8"),
    parquet_type("INT", bit_width = 8, is_signed = FALSE)
  )
  expect_equal(
    parquet_type("UINT_16"),
    parquet_type("INT", bit_width = 16, is_signed = FALSE)
  )
  expect_equal(
    parquet_type("UINT_32"),
    parquet_type("INT", bit_width = 32, is_signed = FALSE)
  )
  expect_equal(
    parquet_type("UINT_64"),
    parquet_type("INT", bit_width = 64, is_signed = FALSE)
  )

  expect_equal(
    parquet_type("TIME_MICROS"),
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MICROS")
  )
  expect_equal(
    parquet_type("TIME_MILLIS"),
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MILLIS")
  )

  expect_equal(
    parquet_type("TIMESTAMP_MICROS"),
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS")
  )
  expect_equal(
    parquet_type("TIMESTAMP_MILLIS"),
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS")
  )
})

test_that("parquet_schema", {
  sch <- parquet_schema(
    "INT32",
    "BOOLEAN",
    "INT32",
    "INT64",
    "INT96",
    "FLOAT",
    "BYTE_ARRAY",
    list("FIXED_LEN_BYTE_ARRAY", type_length = 10)
  )
  expect_snapshot(as.data.frame(sch))

  sch2 <- parquet_schema(
    a = "INT32",
    b = "BOOLEAN",
    c = "INT32",
    d = "INT64",
    e = "INT96",
    f = "FLOAT",
    g = "BYTE_ARRAY",
    list("FIXED_LEN_BYTE_ARRAY", type_length = 10)
  )
  expect_snapshot(as.data.frame(sch2))

  sch3 <- parquet_schema(
    a = "INT32",
    b = "BOOLEAN",
    c = "INT32",
    d = "INT64",
    e = "INT96",
    f = "FLOAT",
    g = "BYTE_ARRAY",
    h = list("FIXED_LEN_BYTE_ARRAY", type_length = 10)
  )
  expect_snapshot(as.data.frame(sch3))

  sch4 <- parquet_schema(
    "STRING",
    "ENUM",
    "UUID",
    list("INTEGER", bit_width = 8, is_signed = TRUE),
    list("INTEGER", bit_width = 64, is_signed = FALSE),
    list("DECIMAL", precision = 5, primitive_type = "INT64"),
    "FLOAT16",
    "DATE",
    "JSON",
    "BSON"
  )
  expect_snapshot(as.data.frame(sch4))

  sch5 <- parquet_schema(
    foo = "AUTO",
    bar = "INT32"
  )
  expect_snapshot(as.data.frame(sch5))
})
