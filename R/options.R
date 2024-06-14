#' Nanoparquet options
#'
#' Create a list of nanoparquet options.
#' @param class The extra class or classes to add to data frames created
#'   in [read_parquet()]. By default nanoparquet adds the `"tbl"` class,
#'   so data frames are printed differently if the pillar package is
#'   loaded.
#' @param use_arrow_metadata `TRUE` or `FALSE`. If `TRUE`, then
#'   [read_parquet()] and [parquet_columns()] will make use of the Apache
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
  use_arrow_metadata = getOption("nanoparquet.use_arrow_metadata", TRUE),
  write_arrow_metadata = getOption("nanoparquet.write_arrow_metadata", TRUE)
) {
  stopifnot(is.character(class))
  stopifnot(is_flag(use_arrow_metadata))
  stopifnot(is_flag(write_arrow_metadata))

  list(
    class = class,
    use_arrow_metadata = use_arrow_metadata,
    write_arrow_metadata = write_arrow_metadata
  )
}

is_flag <- function(x) {
  is.logical(x) && length(x) == 1 && !is.na(x)
}
