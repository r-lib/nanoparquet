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
#' @seealso [parquet_metadata()], [read_parquet()].
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

parquet_type <- function(type, length = NULL, bit_width = NULL,
                         is_signed = NULL, precision = NULL, scale = NULL,
                         is_adjusted_utc = NULL, unit = NULL,
                         primitive_type = NULL) {

  fixed_len_byte_array <- function() {
    stopifnot(
      ! is.null(length),
      is_uint32(length)
    )
    r <- list("FIXED_LEN_BYTE_ARRAY", length = as.double(length))
    length <<- NULL
    r
  }

  int <- function() {
    stopifnot(
      ! is.null(bit_width),
      bit_width %in% c(8L, 16L, 32L, 64L),
      ! is.null(is_signed),
      is_flag(is_signed)
    )
    r <- list(
      if (bit_width <= 32L) "INT32" else "INT64",
      logical_type = "INT",
      bit_width = as.integer(bit_width),
      is_signed = is_signed
    )
    bit_width <<- NULL
    is_signed <<- NULL
    r
  }

  decimal <- function() {
    stopifnot(
      ! is.null(precision),
      is_uint32(precision),
      precision > 0,
      is.null(scale) || (is_uint32(scale) && scale <= precision),
      ! is.null(primitive_type),
      is_string(primitive_type),
      primitive_type %in%
        c("INT32", "INT64", "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY")
    )
    if (primitive_type == "INT32") {
      stopifnot(precision <= 9)
    } else if (primitive_type == "INT64") {
      stopifnot(precision <= 18)
    } else if (primitive_type == "FIXED_LEN_BYTE_ARRAY") {
      stopifnot(
        ! is.null(length),
        is_uint32(length),
        precision <= floor(log10(2^(8 * length - 1) - 1))
      )
    }
    r <- list(
      primitive_type,
      logical_type = "DECIMAL",
      scale = scale,
      precision = precision
    )
    precision <<- NULL
    scale <<- NULL
    if (primitive_type == "FIXED_LEN_BYTE_ARRAY") {
      r[["length"]] <- length
      length <<- NULL
    }
    primitive_type <<- NULL
    r
  }

  time <- function() {
    stopifnot(
      ! is.null(is_adjusted_utc),
      is_flag(is_adjusted_utc),
      ! is.null(unit),
      is_string(unit),
      unit %in% c("MILLIS", "MICROS", "NANOS")
    )
    r <- list(
      if (unit == "MILLIS") "INT32" else "INT64",
      logical_type = "TIME",
      is_adjusted_utc = is_adjusted_utc,
      unit = unit
    )
    is_adjusted_utc <<- NULL
    unit <<- NULL
    r
  }

  timestamp <- function() {
    stopifnot(
      ! is.null(is_adjusted_utc),
      is_flag(is_adjusted_utc),
      ! is.null(unit),
      is_string(unit),
      unit %in% c("MILLIS", "MICROS", "NANOS")
    )
    r <- list(
      "INT64",
      logical_type = "TIMESTAMP",
      is_adjusted_utc = is_adjusted_utc,
      unit = unit
    )
    is_adjusted_utc <<- NULL
    unit <<- NULL
    r
  }

  err <- function(typename) {
    stop("Parquet type '", typename, "' is not supported by nanoparquet")
  }

  ptype <- switch (type,
    # primitive types
    BOOLEAN = list("BOOLEAN"),
    INT32 = list("INT32"),
    INT64 = list("INT64"),
    INT96 = list("INT96"),
    FLOAT = list("FLOAT"),
    DOUBLE = list("DOUBLE"),
    BYTE_ARRAY = list("BYTE_ARRAY"),
    FIXED_LEN_BYTE_ARRAY = fixed_len_byte_array(),

    # logical types
    STRING = list("BYTE_ARRAY", logical_type = "STRING"),
    ENUM = list("BYTE_ARRAY", logical_type = "ENUM"),
    UUID = list("FIXED_LEN_BYTE_ARRAY", logical_type = "UUID", length = 16L),
    INT = int(),
    DECIMAL = decimal(),
    FLOAT16 = list(
      "FIXED_LEN_BYTE_ARRAY",
      logical_type = "FLOAT16",
      length = 2
    ),
    DATE = list("INT32", logical_type = "DATE"),
    TIME = time(),
    TIMESTAMP = timestamp(),
    INTERVAL = list(
      "FIXED_LEN_BYTE_ARRAY",
      logical_type = "INTERVAL",
      length = 12L
    ),
    JSON = list("BYTE_ARRAY", logical_type = "JSON"),
    BSON = list("BYTE_ARRAY", logical_type = "BSON"),
    LIST = err("LIST"),
    MAP = err("MAP"),
    UNKNOWN = err("UNKNOWN"),

    # bail
    err(type)
  )

  # check that every non-NULL parameter is used
  stopifnot(
    "Unused Parquet type parameter 'length'." = is.null(length),
    "Unused Parquet type parameter 'bit_width'." = is.null(bit_width),
    "Unused Parquet type parameter 'is_signed'." = is.null(is_signed),
    "Unused Parquet type parameter 'precision'." = is.null(precision),
    "Unused Parquet type parameter 'scale'." = is.null(scale),
    "Unused Parquet type parameter 'is_adjusted_utc'." = is.null(is_adjusted_utc),
    "Unused Parquet type parameter 'unit'." = is.null(unit),
    "Unused Parquet type parameter 'primitive_type'." = is.null(primitive_type)
  )

  ptype
}
