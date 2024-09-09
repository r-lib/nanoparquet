#' Nanoparquet options
#'
#' Create a list of nanoparquet options.
#' @param class The extra class or classes to add to data frames created
#'   in [read_parquet()]. By default nanoparquet adds the `"tbl"` class,
#'   so data frames are printed differently if the pillar package is
#'   loaded.
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
#' @param row_groups_at Integer vector, the start positions of the row
#'   groups when writing a Parquet file in [write_parquet()]. Give at most
#'   one of `row_groups_at`, `num_rows_per_row_group` and `num_row_groups`.
#'   See 'Row groups' below.
#' @param num_rows_per_row_group Number of rows to include in each row
#'   group. It is used as a hint, so the actual numbers might slightly
#'   differ. Give at most one of `row_groups_at`, `num_rows_per_row_group`
#'   and `num_row_groups`. See 'Row groups' below.
#' @param num_row_groups Number of row groups to write. It is used as a
#'   hint, so the actualy number might slightly differ. Give at most one of
#'   `row_groups_at`, `num_rows_per_row_group` and `num_row_groups`.
#'   See 'Row groups' below.
#'
#' @return List of nanoparquet options.
#'
#' ## Row groups
#'
#' By default [write_parquet()] determines the number of row groups to
#' write automatically, trying to make each column chunk about at most
#' 100MiB.
#'
#' To manually specify the number of row groups, use the `options` argument
#' of [write_parquet()] and one of `row_groups_at`,
#' `num_rows_per_row_group` or `num_row_groups`. (See them above.)
#' It is an error to specify more than one of them in `parquet_options()`.
#'
#' You can also set one or more of the `nanoparquet.row_groups_at`,
#' `nanoparquet.num_rows_per_row_group` or `nanoparquet.num_row_groups`
#' options. The first takes precedence over the other two and the second
#' takes precedence over the third.
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
  write_arrow_metadata = getOption("nanoparquet.write_arrow_metadata", TRUE),
  write_data_page_version = getOption("nanoparquet.write_data_page_version", 1L),
  row_groups_at = NULL,
  num_rows_per_row_group = NULL,
  num_row_groups = NULL
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

  # at least two must be NULL
  if ((is.null(row_groups_at) + is.null(num_rows_per_row_group) +
      is.null(num_row_groups)) < 2) {
    stop("You can only specify at most one of the row group options.")
  }
  stopifnot(is.null(row_groups_at) || is_row_group_sequence(row_groups_at))
  stopifnot(is.null(num_rows_per_row_group) || is_count(num_rows_per_row_group, min = 1L))
  stopifnot(is.null(num_row_groups) || is_count(num_row_groups, min = 1L))
  rg_opt <- if (!is.null(row_groups_at)) {
    list(row_groups_at = row_groups_at)
  } else if (!is.null(num_rows_per_row_group)) {
    list(num_rows_per_row_group = num_rows_per_row_group)
  } else if (!is.null(num_row_groups)) {
    list(num_row_groups = num_row_groups)
  } else if (!is.null(o <- getOption("row_groups_at"))) {
    list(row_groups_at = o)
  } else if (!is.null(o <- getOption("num_rows_per_row_group"))) {
    list(num_rows_per_row_group = o)
  } else if (!is.null(o <- getOption("num_row_groups"))) {
    list(num_row_groups = o)
  }

  c(list(
      class = class,
      use_arrow_metadata = use_arrow_metadata,
      write_arrow_metadata = write_arrow_metadata,
      write_data_page_version = as.integer(write_data_page_version)
    ),
    rg_opt
  )
}
