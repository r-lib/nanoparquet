read_arrow_schema <- function(file) {
  mtd <- read_parquet_metadata(file)
  kvm <- mtd[["file_meta_data"]][["key_value_metadata"]][[1]]
  if ("ARROW:schema" %in% kvm[["key"]]) {
    as <- kvm[["value"]][match("ARROW:schema", kvm[["key"]])]
    parse_arrow_schema(as)
  }
}

apply_arrow_schema <- function(tab, file, arrow_schema, dicts, types,
                               col_select) {
  if (is.na(arrow_schema)) {
    return(tab)
  }
  spec <- arrow_find_special(arrow_schema, file, col_select)
  for (idx in spec$factor) {
    clevels <- Reduce(union, dicts[[idx]])
    tab[[idx]] <- factor(tab[[idx]], levels = clevels)
  }
  for (idx in spec$difftime) {
    # only if INT64, otherwise hms, probably
    if (types[[idx]] != 2) next
    mult <- switch(
      spec$columns$type[[idx]]$unit,
      SECOND = 1,
      MILLISECOND = 1000,
      MICROSECOND = 1000 * 1000,
      NANOSECOND = 1000 * 1000 * 1000,
      stop("Unknown Arrow time unit")
    )
    tab[[idx]] <- as.difftime(tab[[idx]] / mult, units = "secs")
  }
  tab
}

arrow_find_special <- function(asch, file, col_select = NULL) {
  amd <- tryCatch(
    parse_arrow_schema(asch)$columns,
    error = function(e) {
      warning(sprintf(
        "Failed to parse Arrow schema from parquet file at '%s'",
        file
      ), call. = TRUE)
      NULL
    }
  )
  if (is.null(amd)) {
    return(list())
  }
  # Subset of columns?
  if (!is.null(col_select)) {
    amd <- amd[col_select, , drop = FALSE]
  }
  # If the type is Utf8 and it is a dictionary, then it is a factor
  fct <- which(
    amd$type_type == "Utf8" & !vapply(amd$dictionary, is.null, logical(1))
  )
  dft <- which(amd$type_type == "Duration")
  list(factor = fct, difftime = dft, columns = amd)
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

float_precision_names <- c(
  HALF = 0L,
  SINGLE = 1L,
  DOUBLE = 2L
)

date_unit_names <- c(
  DAY = 0L,
  MILLISECOND = 1L

)

time_unit_names <- c(
  SECOND = 0L,
  MILLISECOND = 1L,
  MICROSECOND = 2L,
  NANOSECOND = 3L
)

interval_unit_names <- c(
  YEAR_MONTH = 0L,
  DAY_TIME = 1L,
  MONTH_DAY_NANO = 2L
)

union_mode_names <- c(
  Sparse = 0L,
  Dense = 1L
)

endianness_names <- c(
  Little = 0L,
  Big = 1L
)

features_names <- c(
  UNUSED = 0L,
  DICTIONARY_REPLACEMENT = 1L,
  COMPRESSED_BODY = 2L
)

dict_kind_names <- c(
  "DenseArray" = 0L
)

parse_arrow_schema <- function(schema) {
  ret <- .Call(nanoparquet_parse_arrow_schema, schema)

  columns <- ret[[1]]
  columns$type_type <- names(arrow_types)[columns$type_type + 1L]
  columns$type <- I(columns$type)
  columns$dictionary <- I(columns$dictionary)
  columns$custom_metadata <- I(columns$custom_metadata)
  columns <- as.data.frame(columns)
  class(columns) <- c("tbl", class(columns))

  kv <- ret[[2]]
  kv <- as.data.frame(kv)
  class(kv) <- c("tbl", class(kv))

  endianness <- ret[[3]]
  endianness <- names(endianness_names)[endianness + 1L]

  features <- ret[[4]]
  features <- names(features_names)[features + 1L]

  list(
    columns = columns,
    custom_metadata = kv,
    endianness = endianness,
    features = features
  )
}

# factors -> Utf8 with dictionary
# INTSXP -> Int(32, true)
# REALSXP -> FloatingPoint(DOUBLE)
# STRSXP -> Utf8
# LGLSXP -> Bool

encode_arrow_schema_r <- function(df, schema) {
  endianness <- capitalize(.Platform$endian)
  dates <- vapply(df, function(c) inherits(c, "Date"), logical(1))
  hmss <- vapply(df, function(c) inherits(c, "hms"), logical(1))
  psxcts <- vapply(df, function(c) inherits(c, "POSIXct"), logical(1))
	fctrs <- vapply(df, function(c) inherits(c, "factor"), logical(1))
  dfts <- vapply(df, function(c) !inherits(c, "hms") && inherits(c, "difftime"), logical(1))
  typemap <- c(
    "integer" = "Int",
    "double" = "FloatingPoint",
    "character" = "Utf8",
    "logical" = "Bool",
    "list" = "Binary"
  )
  dftypes <- vapply(df, typeof, character(1))
  artypes <- typemap[dftypes]
  artypes[dates] <- "Date"
  artypes[hmss] <- "Time"
  artypes[psxcts] <- "Timestamp"
  artypes[fctrs] <- "Utf8"
  artypes[dfts] <- "Duration"
  if (anyNA(artypes)) {
    stop(
      "Unsuppoted types when writing Parquet file: ",
      paste(unique(dftypes[is.na(artypes)]), collapse = ", ")
    )
  }
  typeattr <- list(
    "Int" = list(bit_width = 32L, is_signed = TRUE),
    "FloatingPoint" = list(precision = "DOUBLE"),
    "Utf8" = NULL,
    "Bool" = NULL,
    "Duration" = list(unit = "NANOSECOND"),
    "Time" = list(unit = "SECOND", bit_width = 32L),
    "Date" = list(unit = "DAY"),
    "Timestamp" = list(unit = "MICROSECOND", timezone = "UTC")
  )
  schema <- list(
    columns = data.frame(
      stringsAsFactors = FALSE,
      name = names(df),
      type_type = artypes,
      type = I(unname(typeattr[artypes])),
      nullable = rep(TRUE, ncol(df)),
      dictionary = I(replicate(ncol(df), NULL, simplify = FALSE)),
      custom_metadata = I(replicate(ncol(df), list(), simplify = FALSE))
    ),
    custom_metadata = list(NULL, NULL),
    endianness = endianness,
    features = character()
  )
  for (idx in which(fctrs)) {
    schema$columns$dictionary[[idx]] <- list(
      id = 0,
      index_type = list(bit_width = factor_bits(df[[idx]]), is_signed = TRUE),
      is_ordered = is.ordered(df[[idx]]),
      dictionary_kind = "DenseArray"
    )
  }
  schema
}

# Replace strings with numeric IDs, so we can use them in C++
fill_arrow_schema_enums_type <- function(type_type, type) {
  switch(type_type,
    "FloatingPoint" = {
      type$precision <- float_precision_names[type$precision]
    },
    "Date" = {
      type$unit <- date_unit_names[type$unit]
    },
    "Time" = ,
    "Timestamp" = ,
    "Duration" = {
      type$unit <- time_unit_names[type$unit]
    },
    "Interval" = {
      type$unit <- interval_unit_names[type$unit]
    },
    "Union" = {
      type$mode <- union_mode_names[type$mode]
    },
    # otherwise nothing to do
    NULL
  )

  type
}

fill_arrow_schema_enums_dict <- function(dict) {
  if (!is.null(dict)) {
    dict$dictionary_kind <-dict_kind_names[dict$dictionary_kind]
  }
  dict
}

fill_arrow_schema_enums <- function(schema) {
  schema$columns$type[] <- mapply(
    schema$columns$type_type,
    schema$columns$type,
    FUN = fill_arrow_schema_enums_type,
    SIMPLIFY = FALSE,
    USE.NAMES = FALSE
  )
  schema$columns$type_type <- arrow_types[schema$columns$type_type]
  schema$columns$dictionary[] <- lapply(
    schema$columns$dictionary,
    fill_arrow_schema_enums_dict
  )
  schema$endianness <- endianness_names[schema$endianness]
  schema$features <- features_names[schema$features]
  schema
}

encode_arrow_schema<- function(df) {
  schema <- encode_arrow_schema_r(df)
  schema <- fill_arrow_schema_enums(schema)
  rawenc <- .Call(nanoparquet_encode_arrow_schema, schema)
  enc <- base64_encode(rawenc)
  enc
}

# how many bits do we need for this factor in a signed integer
# Arrow only supports 8, 16, 32 and 64.
factor_bits <- function(x) {
  l <- length(levels(x))
  if (l < 2^(8-1)) {
    8L
  } else if (l < 2^(16-1)) {
    16L
  } else if (l < 2^(32-1)) {
    32L
  } else {
    64L
  }
}

base64_decode <- function(x) {
  .Call(nanoparquet_base64_decode, x)
}

base64_encode <- function(x) {
  .Call(nanoparquet_base64_encode, x)
}
