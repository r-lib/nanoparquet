#' Create a Parquet schema
#'
#' You can use this schema to specify how to write out a data frame to
#' a Parquet file with [write_parquet()].
#'
#' @param ... Parquet type specifications, see below.
#'   For backwards compatibility, you can supply a file name
#'   here, and then `parquet_schema` behaves as [read_parquet_schema()].
#' @return Data frame with the same columns as [read_parquet_schema()]:
#'   `r paste0("\u0060", names(parquet_schema("INT32")), "\u0060", collapse = ", ")`.
#' @details
#' A schema is a list of potentially named type specifications. A schema
#' is stored in a data frame. Each (potentially named) argument of
#' `parquet_schema` may be a character scalar, or a list. Parameterized
#' types need to be specified as a list. Primitive Parquet types may be
#' specified as a string or a list.
#'
#' # Possible types:
#'
#' Special type:
#'
#' * `"AUTO"`: this is not a Parquet type, but it tells [write_parquet()]
#'   to map the R type to Parquet automatically, using the default mapping
#'   rules.
#'
#' Primitive Parquet types:
#'
#' * `"BOOLEAN"`
#' * `"INT32"`
#' * `"INT64"`
#' * `"INT96"`
#' * `"FLOAT"`
#' * `"DOUBLE"`
#' * `"BYTE_ARRAY"`
#' * `"FIXED_LEN_BYTE_ARRAY"`: fixed-length byte array. It needs a
#'   `type_length` parameter, an integer between 0 and 2^31-1.
#'
#' Parquet logical types:
#'
#' * `"STRING"`
#' * `"ENUM"`
#' * `"UUID"`
#' * `"INTEGER"`: signed or unsigned integer. It needs a `bit_width` and
#'   an `is_signed` parameter. `bit_width` must be 8, 16, 32 or 64.
#'   `is_signed` must be `TRUE` or `FALSE`.
#' * `"INT"`: same as `"INTEGER"`. The Parquet documentation uses `"INT"`,
#'   but the actual specification uses `"INTEGER"`. Both are supported in
#'   nanoparquet.
#' * `"DECIMAL"`: decimal number of specified scale and precision.
#'   It needs the `precision` and `primitive_type` parameters. Also
#'   supports the `scale` parameter, it defaults to zero if not specified.
#' * `"FLOAT16"`
#' * `"DATE"`
#' * `"TIME"`: needs an `is_adjusted_utc` (`TRUE` or `FALSE`) and a
#'   `unit` parameter. `unit` must be `"MILLIS"`, `"MICROS"` or `"NANOS"`.
#' * `"TIMESTAMP"`: needs an `is_adjusted_utc` (`TRUE` or `FALSE`) and a
#'   `unit` parameter. `unit` must be `"MILLIS"`, `"MICROS"` or `"NANOS"`.
#' * `"JSON"`
#' * `"BSON"`
#' * `"LIST"`: list of some other type. It needs an `element` parameter,
#'   which is a type specification for the list elements. Currently
#'   `element` can be only `"INT32"`, `"DOUBLE"` or `"STRING"`.
#'   Also, currently nanoparquet always write LIST columns that are
#'   `"OPTIONAL"`, both for the list elements and the list itself.
#'
#' Logical types `MAP`, and `UNKNOWN` are not supported currently.
#'
#' Converted types are deprecated in the Parquet specification in favor of
#' logical types, but `parquet_schema()` accepts some converted types as a
#' syntactic shortcut for the corresponding logical types:
#'
#' * `INT_8` mean `list("INT", bit_width = 8, is_signed = TRUE)`.
#' * `INT_16` mean `list("INT", bit_width = 16, is_signed = TRUE)`.
#' * `INT_32` mean `list("INT", bit_width = 32, is_signed = TRUE)`.
#' * `INT_64` mean `list("INT", bit_width = 64, is_signed = TRUE)`.
#' * `TIME_MICROS` means `list("TIME", is_adjusted_utc = TRUE, unit = "MICROS")`.
#' * `TIME_MILLIS` means `list("TIME", is_adjusted_utc = TRUE, unit = "MILLIS")`.
#' * `TIMESTAMP_MICROS` means `list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS")`.
#' * `TIMESTAMP_MILLIS` means `list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS")`.
#' * `UINT_8` means `list("INT", bit_width = 8, is_signed = FALSE)`.
#' * `UINT_16` means `list("INT", bit_width = 16, is_signed = FALSE)`.
#' * `UINT_32` means `list("INT", bit_width = 32, is_signed = FALSE)`.
#' * `UINT_64` means `list("INT", bit_width = 64, is_signed = FALSE)`.
#'
#' ## Missing values
#'
#' Each type might also have a `repetition_type` parameter, with possible
#' values `"REQUIRED"`, `"OPTIONAL"` or `"REPEATED"`. `"REQUIRED"` columns
#' do not allow missing values. Missing values are allowed in `"OPTIONAL"`
#' columns. `"REPEATED"` columns are currently not supported in
#' [write_parquet()].
#'
#' @export
#' @examples
#' parquet_schema(
#'   c1 = "INT32",
#'   c2 = list("INT", bit_width = 64, is_signed = TRUE),
#'   c3 = list("STRING", repetition_type = "OPTIONAL"),
#'   l = list("LIST", element = "DOUBLE")
#' )

parquet_schema <- function(...) {
  args <- list(...)
  if (
    length(args) == 1 &&
      is_string(args[[1]]) &&
      !is.null(args[[1]]) &&
      file.exists(args[[1]]) &&
      (is.null(names(args)) || names(args)[1] %in% c("", "file"))
  ) {
    warning(
      "Using `parquet_schema()` to read the schema from a file is deprecated. ",
      "Use `read_parquet_schema()` instead."
    )
    read_parquet_schema(args[[1]])
  } else {
    parquet_schema_create(args)
  }
}

parquet_schema_create <- function(types) {
  if (length(types) == 0) {
    ptdf <- data.frame(
      file_name = character(),
      r_col = integer(),
      name = character(),
      r_type = character(),
      type = character(),
      type_length = integer(),
      repetition_type = character(),
      converted_type = character(),
      logical_type = I(list()),
      num_children = integer(),
      scale = integer(),
      precision = integer(),
      field_id = integer()
    )
    class(ptdf) <- c("nanoparquet_schema", "tbl", class(ptdf))
    return(ptdf)
  }
  ptypes <- lapply(types, function(t) do.call(parquet_type, as.list(t)))
  nms <- names(types) %||% rep("", length(types))
  nms[nms == ""] <- NA_character_

  normalize <- function(x, nm) {
    na <- rep(NA_character_, length(x$type))
    nai <- rep(NA_integer_, length(x$type))
    x$file_name <- na
    if (is.null(x$name)) {
      x$name <- nm
    } else {
      x$name[1] <- nm
    }
    x$type <- x$type %||% NA_character_
    x$r_type <- na
    x$type_length <- x$type_length %||% nai
    x$repetition_type <- x$repetition_type %||% na
    x$converted_type <- x$converted_type %||% na
    x$num_children <- x$num_children %||% nai
    x$scale <- x$scale %||% nai
    x$precision <- x$precision %||% nai
    x$field_id <- nai
    x
  }

  ptypes <- mapply(FUN = normalize, ptypes, nms, SIMPLIFY = FALSE)

  r_col <- unlist(mapply(
    function(x, idx) rep(as.integer(idx), length(x$type)),
    ptypes,
    seq_along(ptypes),
    SIMPLIFY = FALSE
  ))

  ptdf <- data.frame(
    file_name = unlist(lapply(ptypes, "[[", "file_name")),
    r_col = r_col,
    name = unlist(lapply(ptypes, "[[", "name")),
    r_type = unlist(lapply(ptypes, "[[", "r_type")),
    type = unlist(lapply(ptypes, "[[", "type")),
    type_length = unlist(lapply(ptypes, "[[", "type_length")),
    repetition_type = unlist(lapply(ptypes, "[[", "repetition_type")),
    converted_type = unlist(lapply(ptypes, "[[", "converted_type")),
    logical_type = if (length(ptypes) == 0) {
      I(list())
    } else {
      I(unlist(
        lapply(ptypes, function(x) {
          lt <- x$logical_type
          if (is.null(lt) || inherits(lt, "nanoparquet_logical_type")) {
            list(lt)
          } else {
            lt
          }
        }),
        recursive = FALSE,
        use.names = FALSE
      ))
    },
    num_children = unlist(lapply(ptypes, "[[", "num_children")),
    scale = unlist(lapply(ptypes, "[[", "scale")),
    precision = unlist(lapply(ptypes, "[[", "precision")),
    field_id = unlist(lapply(ptypes, "[[", "field_id"))
  )

  class(ptdf) <- c("nanoparquet_schema", "tbl", class(ptdf))
  ptdf
}

parquet_type <- function(
  type,
  type_length = NULL,
  bit_width = NULL,
  is_signed = NULL,
  precision = NULL,
  scale = NULL,
  is_adjusted_utc = NULL,
  unit = NULL,
  primitive_type = NULL,
  repetition_type = NULL,
  encoding = NULL,
  element = NULL
) {
  fixed_len_byte_array <- function() {
    stopifnot(
      !is.null(type_length),
      is_uint32(type_length)
    )
    r <- list(
      type = "FIXED_LEN_BYTE_ARRAY",
      type_length = as.integer(type_length)
    )
    type_length <<- NULL
    r
  }

  int <- function() {
    stopifnot(
      !is.null(bit_width),
      bit_width %in% c(8L, 16L, 32L, 64L),
      !is.null(is_signed),
      is_flag(is_signed)
    )
    r <- list(
      type = if (bit_width <= 32L) "INT32" else "INT64",
      logical_type = nanoparquet_logical_type(list(
        type = "INT",
        bit_width = as.integer(bit_width),
        is_signed = is_signed
      ))
    )
    bit_width <<- NULL
    is_signed <<- NULL
    r
  }

  decimal <- function() {
    stopifnot(
      !is.null(precision),
      is_uint32(precision),
      precision > 0,
      is.null(scale) || (is_uint32(scale) && scale <= precision),
      !is.null(primitive_type),
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
        !is.null(type_length),
        is_uint32(type_length),
        precision <= floor(log10(2^(8 * type_length - 1) - 1))
      )
    }
    r <- list(
      type = primitive_type,
      logical_type = nanoparquet_logical_type(list(
        type = "DECIMAL",
        scale = if (!is.null(scale)) as.integer(scale),
        precision = as.integer(precision)
      )),
      scale = as.integer(scale %||% 0),
      precision = as.integer(precision)
    )
    precision <<- NULL
    scale <<- NULL
    if (primitive_type == "FIXED_LEN_BYTE_ARRAY") {
      r[["type_length"]] <- as.integer(type_length)
      type_length <<- NULL
    }
    primitive_type <<- NULL
    r
  }

  time <- function() {
    stopifnot(
      !is.null(is_adjusted_utc),
      is_flag(is_adjusted_utc),
      !is.null(unit),
      is_string(unit),
      unit %in% c("MILLIS", "MICROS", "NANOS")
    )
    r <- list(
      type = if (unit == "MILLIS") "INT32" else "INT64",
      logical_type = nanoparquet_logical_type(list(
        type = "TIME",
        is_adjusted_utc = is_adjusted_utc,
        unit = unit
      ))
    )
    is_adjusted_utc <<- NULL
    unit <<- NULL
    r
  }

  timestamp <- function() {
    stopifnot(
      !is.null(is_adjusted_utc),
      is_flag(is_adjusted_utc),
      !is.null(unit),
      is_string(unit),
      unit %in% c("MILLIS", "MICROS", "NANOS")
    )
    r <- list(
      type = "INT64",
      logical_type = nanoparquet_logical_type(list(
        type = "TIMESTAMP",
        is_adjusted_utc = is_adjusted_utc,
        unit = unit
      ))
    )
    is_adjusted_utc <<- NULL
    unit <<- NULL
    r
  }

  nested_list <- function() {
    stopifnot(!is.null(element))
    if (is.list(element) && !is.null(element$repetition_type)) {
      stop("Custom repetition_type is not yet supported for LIST columns")
    }
    r <- do.call("parquet_type", as.list(element))
    element <<- NULL
    r
  }

  err <- function(typename) {
    # nocov start this is probably a covr bug that this is not covered
    stop("Parquet type '", typename, "' is not supported by nanoparquet")
    # nocov end
  }

  ptype <- switch(
    type,
    # dummy type to denote auto-detection
    AUTO = list(type = NA_character_),

    # primitive types
    BOOLEAN = list(type = "BOOLEAN"),
    INT32 = list(type = "INT32"),
    INT64 = list(type = "INT64"),
    INT96 = list(type = "INT96"),
    FLOAT = list(type = "FLOAT"),
    DOUBLE = list(type = "DOUBLE"),
    BYTE_ARRAY = list(type = "BYTE_ARRAY"),
    FIXED_LEN_BYTE_ARRAY = fixed_len_byte_array(),

    # logical types
    STRING = list(
      type = "BYTE_ARRAY",
      logical_type = nanoparquet_logical_type(list(type = "STRING"))
    ),
    ENUM = list(
      type = "BYTE_ARRAY",
      logical_type = nanoparquet_logical_type(list(type = "ENUM"))
    ),
    UUID = list(
      type = "FIXED_LEN_BYTE_ARRAY",
      logical_type = nanoparquet_logical_type(list(type = "UUID")),
      type_length = 16L
    ),
    INTEGER = int(),
    INT = int(),
    DECIMAL = decimal(),
    FLOAT16 = list(
      type = "FIXED_LEN_BYTE_ARRAY",
      logical_type = nanoparquet_logical_type(list(type = "FLOAT16")),
      type_length = 2L
    ),
    DATE = list(
      type = "INT32",
      logical_type = nanoparquet_logical_type(list(type = "DATE"))
    ),
    TIME = time(),
    TIMESTAMP = timestamp(),
    # There is no INTERVAL logical type? What's going on here?
    # INTERVAL = list(
    #   type = "FIXED_LEN_BYTE_ARRAY",
    #   logical_type = nanoparquet_logical_type(list(type = "INTERVAL")),
    #   type_length = 12L
    # ),
    JSON = list(
      type = "BYTE_ARRAY",
      logical_type = nanoparquet_logical_type(list(type = "JSON"))
    ),
    BSON = list(
      type = "BYTE_ARRAY",
      logical_type = nanoparquet_logical_type(list(type = "BSON"))
    ),
    LIST = {
      nl <- nested_list()
      list(
        name = c(NA_character_, "list", "element"),
        type = c(NA_character_, NA_character_, nl$type),
        logical_type = list(
          nanoparquet_logical_type(list(type = "LIST")),
          NULL,
          nl$logical_type
        ),
        converted_type = c(
          "LIST",
          NA_character_,
          nl$converted_type %||% NA_character_
        ),
        repetition_type = c("OPTIONAL", "REPEATED", "OPTIONAL"),
        num_children = c(1L, 1L, NA_integer_)
      )
    },
    MAP = err("MAP"),
    UNKNOWN = err("UNKNOWN"),

    # some converted types as shortcuts
    "INT_8" = {
      bit_width <- 8
      is_signed <- TRUE
      int()
    },
    "INT_16" = {
      bit_width <- 16
      is_signed <- TRUE
      int()
    },
    "INT_32" = {
      bit_width <- 32
      is_signed <- TRUE
      int()
    },
    "INT_64" = {
      bit_width <- 64
      is_signed <- TRUE
      int()
    },

    "TIME_MICROS" = {
      is_adjusted_utc <- TRUE
      unit <- "MICROS"
      time()
    },
    "TIME_MILLIS" = {
      is_adjusted_utc <- TRUE
      unit <- "MILLIS"
      time()
    },
    "TIMESTAMP_MICROS" = {
      is_adjusted_utc <- TRUE
      unit <- "MICROS"
      timestamp()
    },
    "TIMESTAMP_MILLIS" = {
      is_adjusted_utc <- TRUE
      unit <- "MILLIS"
      timestamp()
    },

    "UINT_8" = {
      bit_width <- 8
      is_signed <- FALSE
      int()
    },
    "UINT_16" = {
      bit_width <- 16
      is_signed <- FALSE
      int()
    },
    "UINT_32" = {
      bit_width <- 32
      is_signed <- FALSE
      int()
    },
    "UINT_64" = {
      bit_width <- 64
      is_signed <- FALSE
      int()
    },

    # bail
    err(type)
  )

  if (!is.null(repetition_type)) {
    stopifnot(
      is_string(repetition_type),
      repetition_type %in% c("REQUIRED", "OPTIONAL", "REPEATED")
    )
    ptype[["repetition_type"]] <- repetition_type
  }

  # check that every non-NULL parameter is used
  stopifnot(
    "Unused Parquet type parameter 'type_length'." = is.null(type_length),
    "Unused Parquet type parameter 'bit_width'." = is.null(bit_width),
    "Unused Parquet type parameter 'is_signed'." = is.null(is_signed),
    "Unused Parquet type parameter 'precision'." = is.null(precision),
    "Unused Parquet type parameter 'scale'." = is.null(scale),
    "Unused Parquet type parameter 'is_adjusted_utc'." = is.null(
      is_adjusted_utc
    ),
    "Unused Parquet type parameter 'unit'." = is.null(unit),
    "Unused Parquet type parameter 'primitive_type'." = is.null(primitive_type)
  )

  if (!is.null(ptype[["logical_type"]])) {
    if (is.null(ptype[["converted_type"]])) {
      ct <- logical_to_converted(ptype[["logical_type"]])
      ptype[names(ct)] <- ct
    }
  }

  ptype
}

nanoparquet_logical_type <- function(x) {
  class(x) <- "nanoparquet_logical_type"
  x
}

logical_to_converted <- function(logical_type) {
  res <- .Call(rf_nanoparquet_logical_to_converted, logical_type)
  res[["converted_type"]] <- names(ctype_names)[res[["converted_type"]] + 1]
  res
}

map_schema_to_df <- function(schema, df, options) {
  stopifnot(
    is.null(schema) || is.data.frame(schema),
    is.data.frame(df)
  )
  # Remove root node (r_col = NA, present only in file-read schemas).
  schema <- schema[!is.na(schema[["r_col"]]), ]

  # All remaining rows are passed to C++. Use r_col to find the top-level row
  # of each R column (first row per r_col group), which carries the column name.
  rcol <- schema[["r_col"]]
  top <- !duplicated(rcol)
  nms <- schema$name[top]

  if (is.null(schema)) {
    # no schema at all, everything auto-detected
    schema <- do.call(
      "parquet_schema",
      structure(as.list(rep("AUTO", ncol(df))), names = names(df))
    )
  } else if (!is.null(nms) && !anyNA(nms) && !any(nms == "")) {
    # all properly named — match against df columns
    dfmiss <- setdiff(nms, names(df))
    if (length(dfmiss) > 0) {
      stop(
        "Parquet schema column",
        if (length(dfmiss) > 1) "s",
        " missing from the data: ",
        paste(dfmiss, collapse = ", ")
      )
    }
    # add AUTO rows for df columns not in the schema
    smiss <- setdiff(names(df), nms)
    auto <- do.call(
      "parquet_schema",
      structure(as.list(rep("AUTO", length(smiss))), names = smiss)
    )
    # Reorder all rows (grouped by r_col) to match df column order.
    top_rcols <- rcol[top]
    schema_nms <- setdiff(names(df), smiss)
    col_order <- match(schema_nms, nms)
    ordered_rows <- unlist(lapply(col_order, function(i) {
      which(rcol == top_rcols[i])
    }))
    schema <- schema[ordered_rows, ]
    # Renumber r_col: each column gets its position in the df.
    new_rcol <- unlist(lapply(seq_along(col_order), function(i) {
      rep(match(schema_nms[i], names(df)), sum(rcol == top_rcols[col_order[i]]))
    }))
    schema[["r_col"]] <- new_rcol
    # Append AUTO rows (one row each, r_col = position in df).
    if (nrow(auto) > 0) {
      auto[["r_col"]] <- match(smiss, names(df))
      schema <- rbind(schema, auto)
    }
    schema <- schema[order(schema[["r_col"]]), ]
  } else {
    # unnamed / partially named
    n_top <- sum(top)
    if (n_top != ncol(df)) {
      stop(
        "Parquet schema size does not match data size. ",
        "They must match, unless the schema is fully named."
      )
    }
    if (is.null(nms) || all(is.na(nms) | nms == "")) {
      # no names at all, ok
    } else {
      # some names, those must match
      if (is.na(nms) | nms == names(df)) {
        stop(
          "Parquet schema names do not fully match data frame names."
        )
      }
    }
  }

  schema[["type"]] <- type_names[schema[["type"]]]
  schema[["repetition_type"]] <- repetition_types[schema[["repetition_type"]]]
  schema[["converted_type"]] <- ctype_names[schema[["converted_type"]]]

  schema
}
