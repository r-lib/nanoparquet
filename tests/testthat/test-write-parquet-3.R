test_that("parquet_type", {
  expect_snapshot({
    parquet_type("BOOLEAN")
    parquet_type("INT32")
    parquet_type("INT64")
    parquet_type("INT96")
    parquet_type("FLOAT")
    parquet_type("DOUBLE")
    parquet_type("BYTE_ARRAY")
    parquet_type("FIXED_LEN_BYTE_ARRAY", length = 10)

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
      length = 5
    )
    parquet_type("FLOAT16")
    parquet_type("DATE")
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MILLIS")
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "MICROS")
    parquet_type("TIME", is_adjusted_utc = TRUE, unit = "NANOS")
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS")
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS")
    parquet_type("TIMESTAMP", is_adjusted_utc = TRUE, unit = "NANOS")
    parquet_type("INTERVAL")
    parquet_type("JSON")
    parquet_type("BSON")
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
    parquet_type("DECIMAL", precision = 5/2, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 0, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 5, scale = 6, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 10, primitive_type = "INT32")
    parquet_type("DECIMAL", precision = 19, primitive_type = "INT64")
    parquet_type(
      "DECIMAL",
      precision = 12,
      primitive_type = "FIXED_LEN_BYTE_ARRAY",
      length = 5
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
  })
})
