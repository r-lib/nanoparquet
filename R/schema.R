#' Create a Parquet schema
#'
#' @param ... Parquet type specifications, see below.
#'   For backwards compatibility, you can supply a file name
#'   here, and then `parquet_schema` behaves as [read_parquet_schema()].
#'
#' @export

parquet_schema <- function(...) {
	args <- list(...)
	if (length(args) == 1 && !is.null(args[[1]]) && file.exists(args[[1]]) &&
      (is.null(names(args)) || names(args)[1] %in% c("", "file"))) {
		warning(
			"Using `parquet_schema()` to read the schema from a file is deprecated. ",
			"Use `read_parquet_schema()` instead."
		)
		read_parquet_schema(args[[1]])
	} else {
    if (!is.null(file)) {
      args <- c(list(file = file))
    }
		parquet_schema_create(args)
	}
}

parquet_type <- function(type, type_length = NULL, bit_width = NULL,
                         is_signed = NULL, precision = NULL, scale = NULL,
                         is_adjusted_utc = NULL, unit = NULL,
                         primitive_type = NULL) {

  fixed_len_byte_array <- function() {
    stopifnot(
      ! is.null(type_length),
      is_uint32(type_length)
    )
    r <- list("FIXED_LEN_BYTE_ARRAY", type_length = as.double(type_length))
    type_length <<- NULL
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
        ! is.null(type_length),
        is_uint32(type_length),
        precision <= floor(log10(2^(8 * type_length - 1) - 1))
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
      r[["type_length"]] <- type_length
      type_length <<- NULL
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
    UUID = list("FIXED_LEN_BYTE_ARRAY", logical_type = "UUID", type_length = 16L),
    INT = int(),
    DECIMAL = decimal(),
    FLOAT16 = list(
      "FIXED_LEN_BYTE_ARRAY",
      logical_type = "FLOAT16",
      type_length = 2
    ),
    DATE = list("INT32", logical_type = "DATE"),
    TIME = time(),
    TIMESTAMP = timestamp(),
    INTERVAL = list(
      "FIXED_LEN_BYTE_ARRAY",
      logical_type = "INTERVAL",
      type_length = 12L
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
    "Unused Parquet type parameter 'type_length'." = is.null(type_length),
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

parquet_type <- function(type, type_length = NULL, bit_width = NULL,
                         is_signed = NULL, precision = NULL, scale = NULL,
                         is_adjusted_utc = NULL, unit = NULL,
                         primitive_type = NULL) {

  fixed_len_byte_array <- function() {
    stopifnot(
      ! is.null(type_length),
      is_uint32(type_length)
    )
    r <- list("FIXED_LEN_BYTE_ARRAY", type_length = as.double(type_length))
    type_length <<- NULL
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
        ! is.null(type_length),
        is_uint32(type_length),
        precision <= floor(log10(2^(8 * type_length - 1) - 1))
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
      r[["type_length"]] <- type_length
      type_length <<- NULL
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
    UUID = list("FIXED_LEN_BYTE_ARRAY", logical_type = "UUID", type_length = 16L),
    INT = int(),
    DECIMAL = decimal(),
    FLOAT16 = list(
      "FIXED_LEN_BYTE_ARRAY",
      logical_type = "FLOAT16",
      type_length = 2
    ),
    DATE = list("INT32", logical_type = "DATE"),
    TIME = time(),
    TIMESTAMP = timestamp(),
    INTERVAL = list(
      "FIXED_LEN_BYTE_ARRAY",
      logical_type = "INTERVAL",
      type_length = 12L
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
    "Unused Parquet type parameter 'type_length'." = is.null(type_length),
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

parquet_schema_create <- function(args) {
  # TODO
}
