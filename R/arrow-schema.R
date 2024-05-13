apply_arrow_schema <- function(tab, file) {
  mtd <- parquet_metadata(file)
  kv <- mtd$file_meta_data$key_value_metadata[[1]]
  if ("ARROW:schema" %in% kv$key) {
    amd <- tryCatch(
      parse_arrow_schema(kv$value[match("ARROW:schema", kv$key)]),
      error = function(e) {
        warning(sprintf(
          "Failed to parse Arrow schema from parquet file at '%s'",
          file
        ), call. = TRUE)
        NULL
      }
    )
    if (is.null(amd)) {
      return(tab)
    }
    fct <- which(amd$type == "Utf8" & !is.na(amd$dictionary_bit_width))
    for (idx in fct) {
      tab[[idx]] <- as.factor(tab[[idx]])
    }
  }
  tab
}

arrow_types <- c(
  NONE = 0L,
  Null = 1L,
  Int = 2L,
  FloatingPoint = 3L,
  Binary = 4L,
  Utf8 = 5L,
  Bool = 6L,
  Decimal = 7L,
  Date = 8L,
  Time = 9L,
  Timestamp = 10L,
  Interval = 11L,
  List = 12L,
  Struct_ = 13L,
  Union = 14L,
  FixedSizeBinary = 15L,
  FixedSizeList = 16L,
  Map = 17L,
  Duration = 18L,
  LargeBinary = 19L,
  LargeUtf8 = 20L,
  LargeList = 21L,
  RunEndEncoded = 22L,
  BinaryView = 23L,
  Utf8View = 24L,
  ListView = 25L,
  LargeListView = 26L
)

parse_arrow_schema <- function(schema) {
  ret <- .Call(miniparquet_parse_arrow_schema, schema)
  ret$type <- names(arrow_types)[ret$type + 1L]
  ret <- as.data.frame(ret)
  class(ret) <- c("tbl", class(ret))
  ret
}

base64_decode <- function(x) {
  .Call(miniparquet_base64_decode, x)
}

base64_encode <- function(x) {
  .Call(miniparquet_base64_encode, x)
}
