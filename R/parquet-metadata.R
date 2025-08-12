type_names <- c(
  BOOLEAN = 0L,
  INT32 = 1L,
  INT64 = 2L,
  INT96 = 3L,
  FLOAT = 4L,
  DOUBLE = 5L,
  BYTE_ARRAY = 6L,
  FIXED_LEN_BYTE_ARRAY = 7L
)
ctype_names <- c(
  UTF8 = 0L,
  MAP = 1L,
  MAP_KEY_VALUE = 2L,
  LIST = 3L,
  ENUM = 4L,
  DECIMAL = 5L,
  DATE = 6L,
  TIME_MILLIS = 7L,
  TIME_MICROS = 8L,
  TIMESTAMP_MILLIS = 9L,
  TIMESTAMP_MICROS = 10L,
  UINT_8 = 11L,
  UINT_16 = 12L,
  UINT_32 = 13L,
  UINT_64 = 14L,
  INT_8 = 15L,
  INT_16 = 16L,
  INT_32 = 17L,
  INT_64 = 18L,
  JSON = 19L,
  BSON = 20L,
  INTERVAL = 21L
)

repetition_types <- c(
  REQUIRED = 0L,
  OPTIONAL = 1L,
  REPEATED = 2L
)

encodings <- c(
  PLAIN = 0L,
  GROUP_VAR_INT = 1L, # was never used, now deprecated
  PLAIN_DICTIONARY = 2L,
  RLE = 3L,
  BIT_PACKED = 4L,
  DELTA_BINARY_PACKED = 5L,
  DELTA_LENGTH_BYTE_ARRAY = 6L,
  DELTA_BYTE_ARRAY = 7L,
  RLE_DICTIONARY = 8L,
  BYTE_STREAM_SPLIT = 9L
)

codecs <- c(
  UNCOMPRESSED = 0L,
  SNAPPY = 1L,
  GZIP = 2L,
  LZO = 3L,
  BROTLI = 4L,
  LZ4 = 5L,
  ZSTD = 6L
)

page_types <- c(
  DATA_PAGE = 0L,
  INDEX_PAGE = 1L,
  DICTIONARY_PAGE = 2L,
  DATA_PAGE_V2 = 3
)

format_schema_result <- function(mtd, sch, options) {
  sch$type <- names(type_names)[sch$type + 1L]
  sch$converted_type <-
    names(ctype_names)[sch$converted_type + 1L]
  sch$repetition_type <-
    names(repetition_types)[sch$repetition_type + 1L]
  sch$logical_type <- I(sch$logical_type)
  sch <- as.data.frame(sch)
  sch <- add_r_type_to_schema(mtd, sch, options)
  class(sch) <- c("nanoparquet_schema", "tbl", class(sch))

  sch
}

#' Read the metadata of a Parquet file
#'
#' This function should work on all files, even if [read_parquet()] is
#' unable to read them, because of an unsupported schema, encoding,
#' compression or other reason.
#'
#' @param file Path to a Parquet file.
#' @param options Options that potentially alter the default Parquet to R
#'   type mappings, see [parquet_options()].
#' @return A named list with entries:
#'   * `file_meta_data`: a data frame with file meta data:
#'     - `file_name`: file name.
#'     - `version`: Parquet version, an integer.
#'     - `num_rows`: total number of rows.
#'     - `key_value_metadata`: list column of a data frames with two
#'       character columns called `key` and `value`. This is the key-value
#'       metadata of the file. Arrow stores its schema here.
#'     - `created_by`: A string scalar, usually the name of the software
#'       that created the file.
#'   * `schema`:
# -- If YOU UPDATE THIS, ALSO UPDATE parquet_schema BELOW -----------------
#'     data frame, the schema of the file. It has one row for
#'     each node (inner node or leaf node). For flat files this means one
#'     root node (inner node), always the first one, and then one row for
#'     each "real" column. For nested schemas, the rows are in depth-first
#'     search order. Most important columns are:
#'     - `file_name`: file name.
#'     - `name`: column name.
#'     - `r_type`: the R type that corresponds to the Parquet type.
#'       Might be `NA` if [read_parquet()] cannot read this column. See
#'       [nanoparquet-types] for the type mapping rules.
#'     - `r_type`:
#'     - `type`: data type. One of the low level data types.
#'     - `type_length`: length for fixed length byte arrays.
#'     - `repettion_type`: character, one of `REQUIRED`, `OPTIONAL` or
#'       `REPEATED`.
#'     - `logical_type`: a list column, the logical types of the columns.
#'       An element has at least an entry called `type`, and potentially
#'       additional entries, e.g. `bit_width`, `is_signed`, etc.
#'     - `num_children`: number of child nodes. Should be a non-negative
#'       integer for the root node, and `NA` for a leaf node.
# -------------------------------------------------------------------------
#'   * `$row_groups`: a data frame, information about the row groups.
#'     Some important columns:
#'     - `file_name`: file name.
#'     - `id`: row group id, integer from zero to number of row groups
#'       minus one.
#'     - `total_byte_size`: total uncompressed size of all column data.
#'     - `num_rows`: number of rows.
#'     - `file_offset`: where the row group starts in the file. This is
#'       optional, so it might be `NA`.
#'     - `total_compressed_size`: total byte size of all compressed
#'       (and potentially encrypted) column data in this row group.
#'       This is optional, so it might be `NA`.
#'     - `ordinal`: ordinal position of the row group in the file, starting
#'       from zero. This is optional, so it might be `NA`. If `NA`, then
#'       the order of the row groups is as they appear in the metadata.
# -------------------------------------------------------------------------
#'   * `$column_chunks`: a data frame, information about all column chunks,
#'     across all row groups. Some important columns:
#'     - `file_name`: file name.
#'     - `row_group`: which row group this chunk belongs to.
#'     - `column`: which leaf column this chunks belongs to. The order is
#'       the same as in `$schema`, but only leaf columns (i.e. columns with
#'       `NA` children) are counted.
#'     - `file_path`: which file the chunk is stored in. `NA` means the
#'       same file.
#'     - `file_offset`: where the column chunk begins in the file.
#'     - `type`: low level parquet data type.
#'     - `encodings`: encodings used to store this chunk. It is a list
#'       column of character vectors of encoding names. Current possible
#'       encodings: `r paste0('"', names(encodings), '"', collapse = ", ")`.
#'     - `path_in_scema`: list column of character vectors. It is simply
#'       the path from the root node. It is simply the column name for
#'       flat schemas.
#'     - `codec`: compression codec used for the column chunk. Possible
#'       values are: `r paste0('"', names(codecs), '"', collapse = ", ")`.
#'     - `num_values`: number of values in this column chunk.
#'     - `total_uncompressed_size`: total uncompressed size in bytes.
#'     - `total_compressed_size`: total compressed size in bytes.
#'     - `data_page_offset`: absolute position of the first data page of
#'       the column chunk in the file.
#'     - `index_page_offset`: absolute position of the first index page of
#'       the column chunk in the file, or `NA` if there are no index pages.
#'     - `dictionary_page_offset`: absolute position of the first
#'       dictionary page of the column chunk in the file, or `NA` if there
#'       are no dictionary pages.
#'     - `null_count`: the number of missing values in the column chunk.
#'       It may be `NA`.
#'     - `min_value`: list column of raw vectors, the minimum value of the
#'       column, in binary. If `NULL`, then then it is not specified.
#'       This column is experimental.
#'     - `max_value`: list column of raw vectors, the maximum value of the
#'       column, in binary. If `NULL`, then then it is not specified.
#'       This column is experimental.
#'     - `is_min_value_exact`: whether the minimum value is an actual
#'       value of a column, or a bound. It may be `NA`.
#'     - `is_max_value_exact`: whether the maximum value is an actual
#'       value of a column, or a bound. It may be `NA`.
#'
#' @export
#' @seealso [read_parquet_info()] for a much shorter summary.
#'   [read_parquet_schema()] for column information.
#'   [read_parquet()] to read, [write_parquet()] to write Parquet files,
#'   [nanoparquet-types] for the R <-> Parquet type mappings.
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
#' nanoparquet::read_parquet_metadata(file_name)

read_parquet_metadata <- function(file, options = parquet_options()) {
  file <- path.expand(file)
  res <- .Call(nanoparquet_read_metadata, file)

  res$file_meta_data$key_value_metadata <-
    as.data.frame(res$file_meta_data$key_value_metadata)
  class(res$file_meta_data$key_value_metadata) <-
    c("tbl", class(res$file_meta_data$key_value_metadata))
  res$file_meta_data$key_value_metadata <-
    I(list(res$file_meta_data$key_value_metadata))
  res$file_meta_data <- as.data.frame(res$file_meta_data)
  class(res$file_meta_data) <- c("tbl", class(res$file_meta_data))

  res$schema <- format_schema_result(res, res$schema, options)

  res$row_groups <- as.data.frame(res$row_groups)
  class(res$row_groups) <- c("tbl", class(res$row_groups))

  res$column_chunks$type <- names(type_names)[res$column_chunks$type + 1L]
  res$column_chunks$encodings <- lapply(
    res$column_chunks$encodings,
    function(ec) {
      names(encodings)[ec + 1L]
    }
  )
  res$column_chunks$codec <- names(codecs)[res$column_chunks$codec + 1L]
  res$column_chunks$encodings <- I(res$column_chunks$encodings)
  res$column_chunks$path_in_schema <- I(res$column_chunks$path_in_schema)
  res$column_chunks$min_value <- I(res$column_chunks$min_value)
  res$column_chunks$max_value <- I(res$column_chunks$max_value)
  res$column_chunks <- as.data.frame(res$column_chunks)
  class(res$column_chunks) <- c("tbl", class(res$column_chunks))

  res
}

#' @export
#' @rdname read_parquet_metadata

parquet_metadata <- function(file) {
  warning(
    "`parquet_metadata()` is deprecated. ",
    "Please use `read_parquet_metadata()` instead."
  )
  read_parquet_metadata(file)
}

#' Read the schema of a Parquet file
#'
#' This function should work on all files, even if [read_parquet()] is
#' unable to read them, because of an unsupported schema, encoding,
#' compression or other reason.
#'
#' @param file Path to a Parquet file.
#' @param options Return value of [parquet_options()], options that
#'   potentially modify the Parquet to R type mappings.
#' @return
# -- If YOU UPDATE THIS, ALSO UPDATE read_parquet_metadata ABOVE ----------
#'     Data frame, the schema of the file. It has one row for
#'     each node (inner node or leaf node). For flat files this means one
#'     root node (inner node), always the first one, and then one row for
#'     each "real" column. For nested schemas, the rows are in depth-first
#'     search order. Most important columns are:
#'     - `file_name`: file name.
#'     - `name`: column name.
#'     - `r_type`: the R type that corresponds to the Parquet type.
#'       Might be `NA` if [read_parquet()] cannot read this column. See
#'       [nanoparquet-types] for the type mapping rules.
#'     - `type`: data type. One of the low level data types.
#'     - `type_length`: length for fixed length byte arrays.
#'     - `repettion_type`: character, one of `REQUIRED`, `OPTIONAL` or
#'       `REPEATED`.
#'     - `logical_type`: a list column, the logical types of the columns.
#'       An element has at least an entry called `type`, and potentially
#'       additional entries, e.g. `bit_width`, `is_signed`, etc.
#'     - `num_children`: number of child nodes. Should be a non-negative
#'       integer for the root node, and `NA` for a leaf node.
# -------------------------------------------------------------------------
#'
#' @seealso [read_parquet_metadata()] to read more metadata,
#'   [read_parquet_info()] to show only basic information.
#'   [read_parquet()], [write_parquet()], [nanoparquet-types].
#' @export

read_parquet_schema <- function(file, options = parquet_options()) {
  file <- path.expand(file)
  mtd <- read_parquet_metadata(file, options = options)
  mtd$schema
}

#' Short summary of a Parquet file
#'
#' @param file Path to a Parquet file.
#' @return Data frame with columns:
#'   * `file_name`: file name.
#'   * `num_cols`: number of (leaf) columns.
#'   * `num_rows`: number of rows.
#'   * `num_row_groups`: number of row groups.
#'   * `file_size`: file size in bytes.
#'   * `parquet_version`: Parquet version.
#'   * `created_by`: A string scalar, usually the name of the software
#'       that created the file. `NA` if not available.
#'
#' @seealso [read_parquet_metadata()] to read more metadata,
#'   [read_parquet_schema()] for column information.
#'   [read_parquet()], [write_parquet()], [nanoparquet-types].
#' @export

read_parquet_info <- function(file) {
  file <- path.expand(file)
  mtd <- read_parquet_metadata(file)
  info <- data.frame(
    stringsAsFactors = FALSE,
    file_name = file,
    num_cols = sum(is.na(mtd$schema$num_children)),
    num_rows = mtd$file_meta_data$num_rows,
    num_row_groups = nrow(mtd$row_groups),
    file_size = file.size(file),
    parquet_version = mtd$file_meta_data$version,
    created_by = mtd$file_meta_data$created_by
  )
  class(info) <- c("tbl", class(info))
  info
}

#' @export
#' @rdname read_parquet_info

parquet_info <- function(file) {
  warning(
    "`parquet_info()` is deprecated, please use `read_parquet_info() ",
    "instead."
  )
  read_parquet_info(file)
}
