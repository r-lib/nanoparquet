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
#' @param compression Compression algorithm to use. Currently `"snappy"`
#'   (the default), `"gzip"`, `"zstd"`, and `"uncompressed"` are supported.
#' @param metadata Additional key-value metadata to add to the file.
#'   This must be a named character vector, or a data frame with columns
#'   character columns called `key` and `value`.
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
  compression = c("snappy", "gzip", "zstd", "uncompressed"),
  metadata = NULL,
  options = parquet_options()) {

  file <- path.expand(file)
  codecs <- c("uncompressed" = 0L, "snappy" = 1L, "gzip" = 2L, "zstd" = 6L)
  compression <- codecs[match.arg(compression)]
  dim <- as.integer(dim(x))

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

  # Convert hms to milliseconds in integer
  hmss <- which(vapply(x, "inherits", "hms", FUN.VALUE = logical(1)))
  for (idx in hmss) {
    # convert seconds and milliseconds
    x[[idx]][] <- x[[idx]] * 1000
    # this keeps the class
    mode(x[[idx]]) <- "integer"
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

  # easier here than calling back to R
  required <- !vapply(x, anyNA, logical(1))

  res <- .Call(
    nanoparquet_write,
    x,
    file,
    dim,
    compression,
    metadata,
    required,
    options
  )

  if (is.null(res)) {
    invisible()
  } else {
    res
  }
}
