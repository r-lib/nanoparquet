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
#'   buffer is returned as a raw vector. If `":stdout:"`, then it is
#'   written to the standard output. (When writing to the standard output,
#'   special care is needed to make sure no regular R output gets mixed
#'   up with the Parquet bytes!)
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

  compression <- parse_compression(compression, options)

  dim <- as.integer(dim(x))

  x <- prepare_write_df(x)

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

  schema <- check_schema_required_cols(x, schema)
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

parse_compression <- function(
  compression = c("snappy", "gzip", "zstd", "uncompressed"),
  options) {

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
  compression
}

prepare_write_df <- function(x) {
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

check_schema_required_cols <- function(x, schema) {
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
  schema
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
  default_size <- options[["num_rows_per_row_group"]]
  seq(1L, nrow(x), by = default_size)
}

# this one as well
default_append_row_groups <- function(x, crnt_metadata, schema,
                                      compression, encoding, options) {
  default_size <- options[["num_rows_per_row_group"]]
  crnt_sizes <- crnt_metadata$row_groups$num_rows
  last_size <- utils::tail(crnt_sizes, 1)
  crnt <- utils::head(c(1L, cumsum(crnt_sizes) + 1L), -1)
  if (last_size + nrow(x) > default_size) {
    # create new row group(s)
    crnt_rows <- crnt_metadata$file_meta_data$num_rows
    new <- seq(1L, nrow(x) + last_size, by = default_size)[-1] + crnt_rows - last_size
    crnt <- c(crnt, new)
  }
  as.integer(crnt)
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

#' Append a data frame to an existing Parquet file
#'
#' The schema of the data frame must be compatible with the schema of
#' the file.
#'
#' @section Warning:
#' This function is **not** atomic! If it is interrupted, it may leave
#' the file in a corrupt state. To work around this create a copy of the
#' original file, append the new data to the copy, and then rename the
#' new, extended file to the original one.
#'
#' @section About row groups:
#' A Parquet file may be partitioned into multiple row groups, and indeed
#' most large Parquet files are. `append_parquet()` is only able to update
#' the existing file along the row group boundaries. There are two
#' possibilities:
#'
#' - `append_parquet()` keeps all existing row groups in `file`, and
#'   creates new row groups for the new data. This mode can be forced by
#'   the `keep_row_groups` option in `options`, see [parquet_options()].
#' - Alternatively, `write_parquet` will overwrite the _last_ row group in
#'   file, with its existing contents plus the (beginning of) the new data.
#'   This mode makes more sense if the last row group is small, because
#'   many small row groups are inefficient.
#'
#' By default `append_parquet` chooses between the two modes automatically,
#' aiming to create row groups with at least `num_rows_per_row_group`
#' (see [parquet_options()]) rows. You can customize this behavior with
#' the `keep_row_groups` options and the `row_groups` argument.
#'
#' @param x Data frame to append.
#' @param file Path to the output file.
#' @param compression Compression algorithm to use for the newly written
#'   data. See [write_parquet()].
#' @param encoding Encoding to use for the newly written data. It does not
#'   have to be the same as the encoding of data in `file`. See
#'   [write_parquet()] for possible values.
#' @param row_groups Row groups of the new, extended Parquet file.
#'   [append_parquet()] can only change the last existing row group, and
#'   if `row_groups` is specified, it has respect this. I.e. if the
#'   existing file has `n` rows, and the last row group starts at `k`
#'   (`k <= n`), then the first row group in `row_groups` that refers to
#'   the new data must start at `k` or `n+1`.
#'   (It is simpler to specify `num_rows_per_row_group` in `options`, see
#'   [parquet_options()] instead of `row_groups`. Only use `row_groups` if
#'   you need complete control.)
#' @param options Nanoparquet options, for the new data, see
#'   [parquet_options()]. The `keep_row_groups` option also affects whether
#'   `append_parquet()` overwrites existing row groups in `file`.
#'
#' @export
#' @seealso [write_parquet()].

append_parquet <- function(
  x,
  file,
  compression = c("snappy", "gzip", "zstd", "uncompressed"),
  encoding = NULL,
  row_groups = NULL,
  options = parquet_options()
) {

  file <- path.expand(file)
  compression <- parse_compression(compression, options)

  x <- prepare_write_df(x)

  mtd <- read_parquet_metadata(file)
  schema <- read_parquet_schema(file)
  schema <- map_schema_to_df(schema, x, list())
  schema <- check_schema_required_cols(x, schema)
  required <- schema[["repetition_type"]] == "REQUIRED"
  encoding <- parse_encoding(encoding, x)

  nrow_file <- as.integer(mtd$file_meta_data$num_rows)
  row_groups <- row_groups %||% if (options[["keep_row_groups"]]) {
    c(1L, nrow_file + default_row_groups(
      x, schema, compression, encoding, options
    ))
  } else {
    default_append_row_groups(
      x, mtd, schema, compression, encoding, options
    )
  }
  row_group_starts <- parse_row_groups(x, row_groups)
  x <- row_group_starts[[1]]
  row_group_starts <- row_group_starts[[2]]

  # check if we are extending the last row group of the original file
  nrow_total <- nrow_file + nrow(x)
  extend_last_row_group <- ! (nrow_file + 1L) %in% row_group_starts

  # if yes, prepend the last row group of the file to the new data
  if (extend_last_row_group) {
    num_rgs_file <- nrow(mtd$row_groups)
    x_prep <- read_parquet_row_group(file, num_rgs_file - 1L)
    x <- rbind(x_prep, x)
    nrow_keep <- nrow_file - nrow(x_prep)
  } else {
    nrow_keep <- nrow_file
  }

  # these indices refer to the new data
  row_group_starts <- row_group_starts[row_group_starts > nrow_keep] -
    nrow_keep

  dim <- c(dim(x), nrow_total)

  res <- .Call(
    nanoparquet_append,
    x,
    file,
    as.integer(dim),
    compression,
    required,
    options,
    schema,
    encodings[encoding],
    as.integer(row_group_starts),
    extend_last_row_group,
    sys.call()
  )

  if (is.null(res)) {
    invisible()
  } else {
    res
  }
}