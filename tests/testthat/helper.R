skip_without_parquet_cli <- function() {
  skip_on_cran()
  if (Sys.which("parquet") == "") {
    skip("parquet CLI not found on PATH")
  }
}

skip_without_cargo <- function() {
  skip_on_cran()
  if (Sys.which("cargo") == "") {
    skip("cargo not found on PATH")
  }
}

skip_without_pyarrow <- function() {
  skip_on_cran()
  if (tolower(Sys.getenv("_R_CHECK_FORCE_SUGGESTS_")) != "false") {
    return()
  }
  pyscript <- r"[
    import pyarrow
    import pyarrow.parquet as pq
  ]"
  pytmp <- tempfile(fileext = ".py")
  on.exit(unlink(pytmp), add = TRUE)
  writeLines(pyscript, pytmp)
  py <- if (Sys.which("python3") != "") "python3" else "python"
  res <- tryCatch(
    processx::run(py, pytmp, stderr = "2>&1"),
    error = function(err) err
  )
  if (inherits(res, "error")) {
    skip("missing pyarrow")
  }
}

skip_without_polars <- function() {
  skip_on_cran()
  if (tolower(Sys.getenv("_R_CHECK_FORCE_SUGGESTS_")) != "false") {
    return()
  }
  pyscript <- r"[
    import polars
  ]"
  pytmp <- tempfile(fileext = ".py")
  on.exit(unlink(pytmp), add = TRUE)
  writeLines(pyscript, pytmp)
  py <- if (Sys.which("python3") != "") "python3" else "python"
  res <- tryCatch(
    processx::run(py, pytmp, stderr = "2>&1"),
    error = function(err) err
  )
  if (inherits(res, "error")) {
    skip("missing polars in Python")
  }
}

skip_without <- function(pkgs) {
  if (any(c("arrow", "duckdb") %in% pkgs)) {
    skip_on_cran()
  }
  if (
    "duckdb" %in%
      pkgs &&
      getRversion() < "4.2.0" &&
      .Platform$OS.type == "windows"
  ) {
    skip("duckdb requires R 4.2.0 on Windows")
  }
  if (tolower(Sys.getenv("_R_CHECK_FORCE_SUGGESTS_")) != "false") {
    return()
  }
  ok <- vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)
  if (any(!ok)) {
    skip(paste0("missing ", paste(pkgs[!ok], collapse = ", ")))
  }
}

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
        df[i, i] <- NA
      } else {
        df[1, i] <- NA
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

rscript <- function() {
  if (.Platform$OS.type == "windows") {
    file.path(R.home("bin"), "Rscript.exe")
  } else {
    file.path(R.home("bin"), "Rscript")
  }
}

redact_maxint64 <- function(x) {
  gsub("922337203685477[0-9][0-9][0-9][0-9]", "922337203685477xxxx", x)
}

utcts <- function(x) {
  as.POSIXct(as.POSIXlt(as.Date(x), tz = "UTC"))
}

make_nested_list_parquet <- function(filename, depth, rows = NULL, ...) {
  # Write a Parquet file with a single column 'a' that is a list nested to
  # `depth` levels (depth=1 -> list<int32>, depth=2 -> list<list<int32>>).
  #
  # Args:
  #   filename  output .parquet path
  #   depth     nesting depth, must be >= 1
  #   rows      optional row data; if NULL a small default example is used.
  #             Must be an R list of rows, each nested to `depth` levels with
  #             integer values at the leaves.

  if (depth < 1L) {
    stop("depth must be >= 1")
  }

  # Build the Arrow type recursively: list_of(list_of(...(int32())...))
  make_type <- function(d) {
    if (d == 0L) arrow::int32() else arrow::list_of(make_type(d - 1L))
  }

  # Default data: 3 rows that exercise empty lists at every level.
  # At depth 1: list(1:3, integer(0), 4L)
  # At depth d: wraps depth d-1 rows as [normal+empty, empty, singleton].
  make_rows <- function(d) {
    if (d == 1L) {
      list(1:3, integer(0), 4L)
    } else {
      inner <- make_rows(d - 1L)
      list(
        list(inner[[1]], inner[[2]]),
        list(),
        list(inner[[3]])
      )
    }
  }

  if (is.null(rows)) {
    rows <- make_rows(depth)
  }

  arr <- arrow::Array$create(rows, type = make_type(depth))
  arrow::write_parquet(arrow::arrow_table(a = arr), filename, ...)
}

# Write a Parquet file with a single list<int32> column 'a', with controllable
# repetition types for the outer list and its elements.
#
# list_nullable    = TRUE  -> outer list field is OPTIONAL (may be NULL)
#                  = FALSE -> outer list field is REQUIRED (never NULL)
# element_nullable = TRUE  -> list elements are OPTIONAL (may be NULL)
#                  = FALSE -> list elements are REQUIRED (never NULL)
#
# rows: optional data; must be an R list of integer vectors (no NAs when
#       element_nullable = FALSE, no NULLs when list_nullable = FALSE).
make_list_parquet <- function(
  filename,
  list_nullable = TRUE,
  element_nullable = TRUE,
  rows = NULL
) {
  elem_field <- arrow::field(
    "item",
    arrow::int32(),
    nullable = element_nullable
  )
  list_type <- arrow::list_of(elem_field)

  if (is.null(rows)) {
    rows <- list(1:3, integer(0), 4L)
  }

  arr <- arrow::Array$create(rows, type = list_type)

  col_field <- arrow::field("a", list_type, nullable = list_nullable)
  tbl <- arrow::arrow_table(a = arr, schema = arrow::schema(col_field))

  arrow::write_parquet(tbl, filename)
}

read_parquet_duckdb <- function(file) {
  duckdb::sql_query(sprintf("FROM '%s'", file))
}
