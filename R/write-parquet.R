#' Write a data frame to a Parquet file
#'
#' Writes the contents of an R data frame into a Parquet file.
#'
#' `write_parquet()` converts string columns to UTF-8 encoding by calling
#' [base::enc2utf8()]. It does the same for factor levels.
#'
#' @param x Data frame to write.
#' @param file Path to the output file. If this is the string `":raw:"`,
#'   then the data frame is written to a memory buffer, and the memory
#'   buffer is returned as a raw vector.
#' @param schema Parquet schema. Specify a schema to tweak the default
#'   nanoparquet R -> Parquet type mappings. Use [parquet_schema()] to
#'   create a schema that you can use here, or [read_parquet_schema()] to
#'   use the schema of a Parquet file.
#' @param compression Compression algorithm to use. Currently `"snappy"`
#'   (the default), `"gzip"`, `"zstd"`, and `"uncompressed"` are supported.
#' @param encoding Encoding to use. Possible values:
#'   * If `NULL`, the appropriate encoding is selected automatically:
#'     `RLE` or `PLAIN` for `BOOLEAN` columns, `RLE_DICTIONARY` for other
#'     columns with many repeated values, and `PLAIN` otherwise.
#'   * If It is a single (unnamed) character string, then it'll be used
#'     for all columns.
#'   * If it is an unnamed character vector of encoding names of the same
#'     length as the number of columns in the data frame, then those
#'     encodings will be used for each column.
#'   * If it is a named character vector, then the named must be unique
#'     and each name must match a column name, to specify the encoding of
#'     that column. The special empty name (`""`) applies to the rest of
#'     the columns. If there is no empty name, the rest of the columns
#'     will use the default encoding.
#'
#'   If `NA_character_` is specified for a column, the default encoding is
#'   used for the column.
#'
#'   If a specified encoding is invalid for a certain column type,
#'   or nanoparquet does not implement it, `write_parquet()` throws an
#'   error.
#'
#'   This version of nanoparquet supports the following encodings:
#'   `r paste("\u0060", names(nanoparquet:::encodings), "\u0060", collapse = ", ")`.
#'
#'   See [parquet-encodings] for more about encodings.
#' @param metadata Additional key-value metadata to add to the file.
#'   This must be a named character vector, or a data frame with columns
#'   character columns called `key` and `value`.
#' @param row_groups Row groups of the Parquet file. If `NULL`, then the
#'   `num_rows_per_row_group` option is used from the `options` argument,
#'   see [parquet_options()]. Otherwise it must be an integer vector,
#'   specifying the starts of the row groups.
#' @param options Nanoparquet options, see [parquet_options()].
#' @return `NULL`, unless `file` is `":raw:"`, in which case the Parquet
#'   file is returned as a raw vector.
#'
#' @export
#' @seealso [read_parquet_metadata()], [read_parquet()].
#' @examplesIf FALSE
#' # add row names as a column, because `write_parquet()` ignores them.
#' mtcars2 <- cbind(name = rownames(mtcars), mtcars)
#' write_parquet(mtcars2, "mtcars.parquet")

write_parquet <- function(
  x,
  file,
  schema = NULL,
  compression = c("snappy", "gzip", "zstd", "uncompressed"),
  encoding = NULL,
  metadata = NULL,
  row_groups = NULL,
  options = parquet_options()) {

  file <- path.expand(file)

  codecs <- c("uncompressed" = 0L, "snappy" = 1L, "gzip" = 2L, "zstd" = 6L)
  compression <- codecs[match.arg(compression)]
  if (is.na(options[["compression_level"]])) {
    # -1 is an allowed value for zstd, so we set the default here
    if (compression == "zstd") {
      options[["compression_level"]] <- 3L
    } else {
      options[["compression_level"]] <- -1L
    }
  }

  dim <- as.integer(dim(x))

  x <- prepare_df(x)

  schema <- map_schema_to_df(schema, x, options)

  if (is.null(metadata)) {
    metadata <- list(character(), character())
  } else if (is.data.frame(metadata)) {
    stopifnot(ncol(metadata) == 2)
  } else {
    stopifnot(
      is.character(metadata),
      length(names(metadata)) == length(metadata)
    )
    metadata <- list(names(metadata), unname(metadata))
  }

  if (options[["write_arrow_metadata"]]) {
    if (! "ARROW:schema" %in% metadata[[1]]) {
      metadata[[1]] <- c(metadata[[1]], "ARROW:schema")
      metadata[[2]] <- c(metadata[[2]], encode_arrow_schema(x))
    }
  }

  # if schema has REQUIRED, but the column has NAs, that's an error
  rt <- schema[["repetition_type"]]
  req <- !is.na(rt) & rt == "REQUIRED"
  hasna <- vapply(x, any_na, logical(1))
  bad <- which(req & hasna)
  if (length(bad) > 0) {
    stop(
      "Parquet schema does not allow missing values for column",
      if (length(bad) > 1) "s", ":", paste(names(x)[bad], collapse = ", ")
    )
  }
  schema[["repetition_type"]][is.na(rt)] <-
    ifelse(hasna[is.na(rt)], "OPITONAL", "REQUIRED")
  required <- schema[["repetition_type"]] == "REQUIRED"

  encoding <- parse_encoding(encoding, x)

  row_groups <- row_groups %||%
    default_row_groups(x, schema, compression, encoding, options)
  row_group_starts <- parse_row_groups(x, row_groups)
  x <- row_group_starts[[1]]
  row_group_starts <- row_group_starts[[2]]

  res <- .Call(
    nanoparquet_write,
    x,
    file,
    dim,
    compression,
    metadata,
    required,
    options,
    schema,
    encodings[encoding],
    row_group_starts,
    sys.call()
  )

  if (is.null(res)) {
    invisible()
  } else {
    res
  }
}

parse_encoding <- function(encoding, x) {
  stopifnot(
    "`encoding` must be `NULL` or a character vector" =
      is.null(encoding) || is.character(encoding),
    "`encoding` contains at least one unknown encoding" =
      all(is.na(encoding) | encoding %in% names(encodings))
  )

  if (is.null(encoding)) {
    structure(rep(NA_character_, length(x)), names = names(x))

  } else if (is_named(encoding)) {
    stopifnot(
      "names of `encoding` must be unique" =
        !anyDuplicated(names(encoding)),
      "names of `encoding` must match names of `x`" =
        all(names(encoding) %in% c(names(x), ""))
    )
    def <- c(encoding[names(encoding) == ""], NA_character_)[1]
    encoding <- encoding[names(encoding) != ""]
    res <- structure(rep(def, length(x)), names = names(x))
    res[names(encoding)] <- encoding
    res

  } else if (length(encoding) == 1) {
    structure(rep(encoding, length(x)), names = names(x))

  } else {
    stopifnot(length(encoding) == length(x))
    structure(encoding, names = names(x))
  }
}

# we should refine this later
default_row_groups <- function(x, schema, compression, encoding, options) {
  n <- options[["num_rows_per_row_group"]]
  seq(1L, nrow(x), by = n)
}

parse_row_groups <- function(x, rg) {
  if (!is.integer(rg) || anyNA(rg) || any(rg <= 0) ||
      any(diff(rg) <= 0) || rg[1] != 1L) {
    stop(
      "Row groups must be specified as a growing positive integer ",
      "vector, starting with 1."
    )
  }
  list(x = x, row_groups = rg)
}

prepare_df <- function(x) {
  # convert strings to UTF-8
  strs <- which(vapply(x, is.character, logical(1)))
  for (idx in strs) {
    x[[idx]] <- enc2utf8(x[[idx]])
  }
  # factor levels as well
  fctrs <- which(vapply(x, "inherits", "factor", FUN.VALUE = logical(1)))
  for (idx in fctrs) {
    levels(x[[idx]]) <- enc2utf8(levels(x[[idx]]))
  }

  # Date must be integer
  dates <- which(vapply(x, "inherits", "Date", FUN.VALUE = logical(1)))
  for (idx in dates) {
    # this keeps the class
    mode(x[[idx]]) <- "integer"
  }

  # Convert hms to double
  hmss <- which(vapply(x, "inherits", "hms", FUN.VALUE = logical(1)))
  for (idx in hmss) {
    # this keeps the class
    mode(x[[idx]]) <- "double"
  }

  # Make sure POSIXct is double
  posixcts <- which(vapply(x, "inherits", "POSIXct", FUN.VALUE = logical(1)))
  for (idx in posixcts) {
    # keeps the class
    mode(x[[idx]]) <- "double"
  }

  # difftime must be saved in seconds
  difftimes <- which(vapply(x, "inherits", "difftime", FUN.VALUE = logical(1)))
  for (idx in setdiff(difftimes, hmss)) {
    x[[idx]] <- as.difftime(
      as.double(x[[idx]], units = "secs"),
      units = "secs"
    )
  }

  x
}