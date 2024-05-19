#' Read a Parquet file into a data frame
#'
#' Converts the contents of the named Parquet file to a R data frame.
#'
#' @param file Path to a Parquet file.
#' @return A `data.frame` with the file's contents.
#' @export
#' @seealso [parquet_metadata()], [write_parquet()], [nanoparquet-types].
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
#' parquet_df <- nanoparquet::read_parquet(file_name)
#' print(str(parquet_df))

read_parquet <- function(file) {
	file <- path.expand(file)
	res <- .Call(nanoparquet_read, file)
	if (!identical(getOption("nanoparquet.use_arrow_metadata"), FALSE)) {
		res <- apply_arrow_schema(res, file)
	}
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

page_types <- c(
  DATA_PAGE = 0L,
  INDEX_PAGE = 1L,
  DICTIONARY_PAGE = 2L,
  DATA_PAGE_V2 = 3
)

format_schema_result <- function(sch) {
  sch$type <- names(type_names)[sch$type + 1L]
  sch$converted_type <-
    names(ctype_names)[sch$converted_type + 1L]
  sch$repetition_type <-
    names(repetition_types)[sch$repetition_type + 1L]
  sch$logical_type <- I(sch$logical_type)
  sch <- as.data.frame(sch)
  class(sch) <- c("tbl", class(sch))

	sch
}

#' Read the metadata of a Parquet file
#'
#' This function should work on all files, even if [read_parquet()] is
#' unabled to read them, because of an unsupported schema, encoding,
#' compression or other reason.
#'
#' @param file Path to a Parquet file.
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
#'
#' @export
#' @seealso [parquet_schema()] only reads the schema of the file,
#'   [read_parquet()], [write_parquet()], [nanoparquet-types].
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
#' nanoparquet::parquet_metadata(file_name)

parquet_metadata <- function(file) {
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

	res$schema <- format_schema_result(res$schema)

	res$row_groups <- as.data.frame(res$row_groups)
	class(res$row_groups) <- c("tbl", class(res$row_groups))

	res$column_chunks$type <- names(type_names)[res$column_chunk$type + 1L]
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

#' Read the schema of a Parquet file
#'
#' This function should work on all files, even if [read_parquet()] is
#' unabled to read them, because of an unsupported schema, encoding,
#' compression or other reason.
#'
#' @param file Path to a Parquet file.
#' @return
# -- If YOU UPDATE THIS, ALSO UPDATE parquet_metadata ABOVE ---------------
#'     Data frame, the schema of the file. It has one row for
#'     each node (inner node or leaf node). For flat files this means one
#'     root node (inner node), always the first one, and then one row for
#'     each "real" column. For nested schemas, the rows are in depth-first
#'     search order. Most important columns are:
#'     - `file_name`: file name.
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
# -------------------------------------------------------------------------
#'
#' @seealso [parquet_metadata()] reads more metadata,
#'   [read_parquet()], [write_parquet()], [nanoparquet-types].
#' @export

parquet_schema <- function(file) {
	file <- path.expand(file)
	res <- .Call(nanoparquet_read_schema, file)
	res <- format_schema_result(res)
	res
}

#' Write a data frame to a Parquet file
#'
#' Writes the contents of an R data frame into a Parquet file.
#'
#' `write_parquet()` converts string columns to UTF-8 encoding by calling
#' [base::enc2utf8()]. It does the same for factor levels.
#'
#' @param x Data frame to write.
#' @param file Path to the output file.
#' @param compression Compression algorithm to use. Currently only
#'   `"snappy"` (the default) and `"uncompressed"` are supported.
#' @param metadata Additional key-value metadata to add to the file.
#'   This must be a named character vector, or a data frame with columns
#'   character columns called `key` and `value`.
#' @return `NULL`
#'
#' @export
#' @seealso [parquet_metadata()], [read_parquet()].
#' @examplesIf !nanoparquet:::is_rcmd_check()
#' # add row names as a column, because `write_parquet()` ignores them.
#' mtcars2 <- cbind(name = rownames(mtcars), mtcars)
#' write_parquet(mtcars2, "mtcars.parquet")
#' \dontshow{if (Sys.getenv("NOT_CRAN") == "true") unlink("mtcars.parquet")}

write_parquet <- function(
	x,
	file,
	compression = c("snappy", "uncompressed"),
	metadata = NULL) {

  file <- path.expand(file)
	codecs <- c("uncompressed" = 0L, "snappy" = 1L)
	compression <- codecs[match.arg(compression)]
	dim <- as.integer(dim(x))

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

	if (!identical(getOption("nanoparquet.write_arrow_metadata"), FALSE)) {
		if (! "ARROW:schema" %in% metadata[[1]]) {
			metadata[[1]] <- c(metadata[[1]], "ARROW:schema")
			metadata[[2]] <- c(metadata[[2]], encode_arrow_schema(x))
		}
	}

	# convert strings to UTF-8
	strs <- which(vapply(x, is.character, logical(1)))
	for (idx in strs) {
		x[[idx]] <- enc2utf8(x[[idx]])
	}
	# factor levels as well
	fctrs <- which(vapply(x, function(c) inherits(c, "factor"), logical(1)))
	for (idx in fctrs) {
		levels(x[[idx]]) <- enc2utf8(levels(x[[idx]]))
	}

	invisible(.Call(nanoparquet_write, x, file, dim, compression, metadata))
}
