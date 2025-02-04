test_df <- function(tibble = FALSE, factor = FALSE, missing = FALSE) {
  df <- cbind(nam = rownames(mtcars), mtcars)
  df$cyl <- as.integer(df$cyl)
  df$large <- df$cyl >= 6
  if (factor) {
    df$fac <- as.factor(tolower(substr(df$nam, 1, 1)))
  }
  rownames(df) <- NULL

  if (missing) {
    for (i in seq_len(ncol(df))) {
      if (i <= nrow(df)) {
        df[i,i] <- NA
      } else {
        df[1,i] <- NA
      }
    }
  }

  if (tibble) {
    class(df) <- c("tbl_df", "tbl", "data.frame")
  } else {
    class(df) <- c("tbl", "data.frame")
  }
  df
}

test_write <- function(d, schema = NULL, encoding = NULL) {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  schema1 <- if (!is.null(schema)) parquet_schema(schema)
  write_parquet(d, tmp, schema = schema1, encoding = encoding)

  expect_snapshot({
    schema
    encoding
    read_parquet_metadata(tmp)[["column_chunks"]][["encodings"]]
    as.data.frame(read_parquet_pages(tmp))[, c("page_type", "encoding")]
    as.data.frame(read_parquet(tmp))
  })
}

redact_maxint64 <- function(x) {
  gsub("922337203685477[0-9][0-9][0-9][0-9]", "922337203685477xxxx", x)
}
