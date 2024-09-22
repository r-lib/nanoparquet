#' Nanoparquet options
#'
#' Create a list of nanoparquet options.
#' @param class The extra class or classes to add to data frames created
#'   in [read_parquet()]. By default nanoparquet adds the `"tbl"` class,
#'   so data frames are printed differently if the pillar package is
#'   loaded.
#' @param compression_level The compression level in [write_parquet()].
#'   `NA` is the default, and it specifies the default
#'   compression level of each method. `Inf` always selects the highest
#'   possible compression level. More details:
#'   * Snappy does not support compression levels currently.
#'   * GZIP supports levels from 0 (uncompressed), 1 (fastest), to 9 (best).
#'     The default is 6.
#'   * ZSTD allows positive levels up to 22 currently. 20 and above require
#'     more memory. Negative levels are also allowed, the lower the level,
#'     the faster the speed, at the cost of compression. Currently the
#'     smallest level is -131072. The default level is 3.
#' @param num_rows_per_row_group The number of rows to put into a row
#'   group, if row groups are not specified explicitly. It should be
#'   an integer scalar. Defaults to 10 million.
#' @param use_arrow_metadata `TRUE` or `FALSE`. If `TRUE`, then
#'   [read_parquet()] and [read_parquet_schema()] will make use of the Apache
#'   Arrow metadata to assign R classes to Parquet columns.
#'   This is currently used to detect factor columns, and to detect
#'   "difftime" columns.
#'
#'   If this option is `FALSE`:
#'   - "factor" columns are read as character vectors.
#'   - "difftime" columns are read as real numbers, meaning one
#'     of seconds, milliseconds, microseconds or nanoseconds. Impossible
#'     to tell which without using the Arrow metadata.
#' @param write_arrow_metadata Whether to add the Apache Arrow types as
#'   metadata to the file [write_parquet()].
#' @param write_data_page_version Data version to write by default.
#'   Possible values are 1 and 2. Default is 1.
#' @param write_minmax_values Whether to write minimum and maximum values
#'   per row group, for data types that support this in [write_parquet()].
#'   However, nanoparquet currently does not support minimum and maximum
#'   values for the `DECIMAL`, `UUID` and `FLOAT16` logical types and the
#'   `BOOLEAN`, `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` primitive types
#'   if they are writing without a logical type. Currently the default
#'   is `TRUE`.
#'
#' @return List of nanoparquet options.
#'
#' @export
#' @examplesIf FALSE
#' # the effect of using Arrow metadata
#' tmp <- tempfile(fileext = ".parquet")
#' d <- data.frame(
#'   fct = as.factor("a"),
#'   dft = as.difftime(10, units = "secs")
#' )
#' write_parquet(d, tmp)
#' read_parquet(tmp, options = parquet_options(use_arrow_metadata = TRUE))
#' read_parquet(tmp, options = parquet_options(use_arrow_metadata = FALSE))

parquet_options <- function(
  class = getOption("nanoparquet.class", "tbl"),
  compression_level = getOption("nanoparquet.compression_level", NA_integer_),
  num_rows_per_row_group = getOption("nanoparquet.num_rows_per_row_group", 10000000L),
  use_arrow_metadata = getOption("nanoparquet.use_arrow_metadata", TRUE),
  write_arrow_metadata = getOption("nanoparquet.write_arrow_metadata", TRUE),
  write_data_page_version = getOption("nanoparquet.write_data_page_version", 1L),
  write_minmax_values = getOption("nanoparquet.write_minmax_values", TRUE)
) {
  stopifnot(is.character(class))
  stopifnot(is_flag(use_arrow_metadata))
  stopifnot(is_flag(write_arrow_metadata))
  stopifnot(
    identical(write_data_page_version, 1) ||
    identical(write_data_page_version, 2) ||
    identical(write_data_page_version, 1L) ||
    identical(write_data_page_version, 2L)
  )
  stopifnot(is_flag(write_minmax_values))
  num_rows_per_row_group <- as_count(
    num_rows_per_row_group,
    "num_rows_per_row_group"
  )
  if (identical(compression_level, Inf)) {
    compression_level <- 100000L
  } else if (identical(compression_level, NA) ||
             identical(compression_level, NA_integer_) ||
             identical(compression_level, NA_real_)) {
    compression_level <- NA_integer_
  } else {
    compression_level <- as_integer_scalar(compression_level, "compression_level")
  }

  list(
    class = class,
    compression_level = compression_level,
    num_rows_per_row_group = num_rows_per_row_group,
    use_arrow_metadata = use_arrow_metadata,
    write_arrow_metadata = write_arrow_metadata,
    write_data_page_version = as.integer(write_data_page_version),
    write_minmax_values = write_minmax_values
  )
}
