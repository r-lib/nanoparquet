#' Read a Parquet file into a data frame
#'
#' Converts the contents of the named Parquet file to a R data frame.
#'
#' @param file Path to a Parquet file.
#' @return A `data.frame` with the file's contents.
#' @export
#' @seealso [read_parquet_metadata()], [write_parquet()].
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "miniparquet")
#' parquet_df <- miniparquet::read_parquet(file_name)
#' print(str(parquet_df))

read_parquet <- function(file) {
	file <- path.expand(file)
	res <- .Call(miniparquet_read, file)
	# some data.frame dress up
	attr(res, "row.names") <- c(NA_integer_, as.integer(-1 * length(res[[1]])))
	class(res) <- "data.frame"
	res
}

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
	GROUP_VAR_INT = 1L,    # was never used, now deprecated
	PLAIN_DICTIONARY = 2L,
	RLE = 3L,
	BIT_PACKED = 4L,
	DELTA_BINARY_PACKED = 5L,
	DELTA_LENGTH_BYTE_ARRAY = 6L,
	DELTA_BYTE_ARRAY = 7L,
	RLE_DICTIONARY = 8L
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

#' Read the metadata of a Parquet file
#'
#' This function should work on all files, even if [read_parquet()] is
#' unabled to read them, because of an unsupported schema, encoding,
#' compression or other reason.
#'
#' @param file Path to a Parquet file.
#' @return A named list with entries:
#'   * `file_meta_data`: a named list with file meta data:
#'     - `version`: Parquet version, an integer.
#'     - `num_rows`: total number of rows.
#'     - `key_value_metadata`: a data frame with two character
#'       columns called `key` and `value`. This is the key-value metadata
#'       of the file. Arrow stores its schema here.
#'     - `created_by`: A string scalar, usually the name of the software
#'       that created the file.
#'     - `encryption_algorithm`: not implemented yet.
#'     - `footer_signing_key_metadata`: not implemented yet.
#'   * `schema`: data frame, the schema of the file. It has one row for
#'     each node (inner node or leaf node). For flat files this means one
#'     root node (inner node), always the first one, and then one row for
#'     each "real" column. For nested schemas, the rows are in depth-first
#'     search order. Most important columns are:
#'     - `name`: column name.
#'     - `type`: data type. One of the low level data types.
#'     - `type_length`: length for fixed length byte arrays.
#'     - `repettion_type`: character, one of `REQUIRED`, `OPTIONAL` or
#'       `REPEATED`.
#'     - `logical_type`: a list column, the logical types of the columns.
#'       An element has at least an entry called `type`, and potentially
#'       additional entries, e.g. `bit_width`, `is_signed`, etc.
#'     - `num_children`: number of child nodes. Should be a non-negative
#'       integer for the root node, and `NA` for a leaf node.
#'   * `$row_groups`: a data frame, information about the row groups.
#'   * `$column_chunks`: a data frame, information about all column chunks.
#'
#' @export
#' @seealso [read_parquet()], [write_parquet()].
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "miniparquet")
#' miniparquet::read_parquet_metadata(file_name)

read_parquet_metadata <- function(file) {
	file <- path.expand(file)
	res <- .Call(miniparquet_read_schema, file)

	res$schema$type <- names(type_names)[res$schema$type + 1L]
	res$schema$converted_type <-
		names(ctype_names)[res$schema$converted_type + 1L]
	res$schema$repetition_type <-
		names(repetition_types)[res$schema$repetition_type + 1L]
	res$schema$logical_type <-	I(res$schema$logical_type)
	res$schema <- as.data.frame(res$schema)
	class(res$schema) <- c("tbl", class(res$schema))
	res$file_meta_data$key_value_metadata <-
		as.data.frame(res$file_meta_data$key_value_metadata)
	class(res$file_meta_data$key_value_metadata) <-
		c("tbl", class(res$file_meta_data$key_value_metadata))

	res$row_groups <- as.data.frame(res$row_groups)
	class(res$row_groups) <- c("tbl", class(res$row_groups))

	res$column_chunks$encodings <- lapply(
		res$column_chunks$encodings,
		function(ec) { names(encodings)[ec + 1L] }
	)
	res$column_chunks$codec <- names(codecs)[res$column_chunks$codec + 1L]
	res$column_chunks$encodings <- I(res$column_chunks$encodings)
	res$column_chunks$path_in_schema <- I(res$column_chunks$path_in_schema)
	res$column_chunks <- as.data.frame(res$column_chunks)
	class(res$column_chunks) <- c("tbl", class(res$column_chunks))

	res
}

#' Write a data frame to a Parquet file
#'
#' Writes the contents of an R data frame into a Parquet file.
#'
#' @param x Data frame to write.
#' @param file Path to the output file.
#' @param compression Compression algorithm to use. Currently only
#'   `"snappy"` (the default) and `"uncompressed"` are supported.
#' @return `NULL`
#'
#' @export
#' @seealso [read_parquet_metadata()], [read_parquet()].
#' @examplesIf !miniparquet:::is_rcmd_check()
#' # add row names as a column, because `write_parquet()` ignores them.
#' mtcars2 <- cbind(name = rownames(mtcars), mtcars)
#' write_parquet(mtcars2, "mtcars.parquet")
#' \dontshow{if (Sys.getenv("NOT_CRAN") == "true") unlink("mtcars.parquet")}

write_parquet <- function(
	x,
	file,
	compression = c("snappy", "uncompressed")) {

  file <- path.expand(file)
	codecs <- c("uncompressed" = 0L, "snappy" = 1L)
	compression <- codecs[match.arg(compression)]
	dim <- as.integer(dim(x))
	invisible(.Call(miniparquet_write, x, file, dim, compression))
}
