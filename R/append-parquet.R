#' Append a data frame to a Parquet file
#'
#' TODO
#'
#' @param x Data frame to write.
#' @param file Path to the output file.
#' @param mode How the merging should be performed. Possible values are:
#'   * `auto`: the mode is selected automatically based on the sizes of
#'     the existing row groups, pages, and the appended data frame.
#'   * `merge_last_page`: merge the first row group of the data frame to
#'     the last page of the Parquet file.
#'   * `new_pages`: add the first row group of the data frame as new pages
#'     to the last row group of the Parquet file.
#'   * `new_row_groups`: add new row groups.
#' @param compression Compression algorithm to use for the new data.
#'   This also applies to the merged pages, if any. Possible values are
#'   as for [write_parquet()], plus two new values:
#'   * `auto`: choose automatically. Tries to choose the same algorithm
#'   that is used in the file. If that algorithm is not supported, it will
#'   use a different one.
#'   * `same`: Use the same algorithm as in the file. If the file uses
#'   multiple algorithms, the first supported one is selected. If none of
#'   them are supported, it throws an error.
#' @inheritParams write_parquet
#'
#' @export

append_parquet <- function(
  x,
  file,
  mode = c("auto", "merge_last_page", "new_pages", "new_row_groups"),
  compression = c("auto", "same", "snappy", "gzip", "zstd", "uncompressed"),
  encoding = NULL,
  metadata = NULL,
  row_groups = NULL,
  options = parquet_options()) {

  file <- path.expand(file)
  schema <- read_parquet_schema(file)

  x <- prepare_df(x)

  row_groups <- row_groups %||%
    default_row_groups(x, schema, compression, encoding, options)
  row_group_starts <- parse_row_groups(x, row_groups)
  x <- row_group_starts[[1]]
  row_group_starts <- row_group_starts[[2]]


}
