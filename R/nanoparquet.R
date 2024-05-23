#' Read a Parquet file into a data frame
#'
#' Converts the contents of the named Parquet file to a R data frame.
#'
#' @param file Path to a Parquet file.
#' @return A `data.frame` with the file's contents.
#' @export
#' @seealso See [write_parquet()] to write Parquet files,
#'   [nanoparquet-types] for the R <-> Parquet type mapping.
#'   See [parquet_info()], for general information,
#'   [parquet_columns()] and [parquet_schema()] for information about the
#'   columns, and [parquet_metadata()] for the complete metadata.
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
#' parquet_df <- nanoparquet::read_parquet(file_name)
#' print(str(parquet_df))

read_parquet <- function(file) {
	file <- path.expand(file)
	res <- .Call(nanoparquet_read, file)
	dicts <- res[[2]]
	types <- res[[3]]
	res <- res[[1]]
	if (!identical(getOption("nanoparquet.use_arrow_metadata"), FALSE)) {
		res <- apply_arrow_schema(res, file, dicts, types)
	}

	# convert hms from milliseconds to seconds, also integer -> double
	hmss <- which(vapply(res, "inherits", "hms", FUN.VALUE = logical(1)))
	for (idx in hmss) {
		res[[idx]] <- structure(
			unclass(res[[idx]]) / 1000,
			class = class(res[[idx]])
		)
	}

	# convert POSIXct from milliseconds to seconds
	posixcts <- which(vapply(res, "inherits", "POSIXct", FUN.VALUE = logical(1)))
	for (idx in posixcts) {
		res[[idx]][] <- structure(
			unclass(res[[idx]]) / 1000,
			class = class(res[[idx]])
		)
	}

	# some data.frame dress up
	attr(res, "row.names") <- c(NA_integer_, as.integer(-1 * length(res[[1]])))
	class(res) <- c(getOption("nanoparquet.class", "tbl"), "data.frame")
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
#' unable to read them, because of an unsupported schema, encoding,
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
#' @seealso [parquet_info()] for a much shorter summary.
#'   [parquet_columns()] and [parquet_schema()] for column information.
#'   [read_parquet()] to read, [write_parquet()] to write Parquet files,
#'   [nanoparquet-types] for the R <-> Parquet type mappings.
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
#' unable to read them, because of an unsupported schema, encoding,
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
#' @seealso [parquet_metadata()] to read more metadata,
#'   [parquet_columns()] to show the columns R would read,
#'   [parquet_info()] to show only basic information.
#'   [read_parquet()], [write_parquet()], [nanoparquet-types].
#' @export

parquet_schema <- function(file) {
	file <- path.expand(file)
	res <- .Call(nanoparquet_read_schema, file)
	res <- format_schema_result(res)
	res
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
#' @seealso [parquet_metadata()] to read more metadata,
#'   [parquet_columns()] and [parquet_schema()] for column information.
#'   [read_parquet()], [write_parquet()], [nanoparquet-types].
#' @export

parquet_info <- function(file) {
	file <- path.expand(file)
	mtd <- parquet_metadata(file)
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

#' Parquet file column information
#'
#' It includes the leaf columns, i.e. the columns that [read_parquet()]
#' would read.
#'
#' @param file Path to a Parquet file.
#' @return Data frame with columns:
#'   * `file_name`: file name.
#'   * `name`: column name.
#'   * `type`: (low level) Parquet data type.
#'   * `r_type`: the R type that corresponds to the Parquet type.
#'     Might be `NA` if [read_parquet()] cannot read this column. See
#'     [nanoparquet-types] for the type mapping rules.
#'   * `repetition_type`: whether the column in `REQUIRED` (cannot be
#'     `NA`) or `OPTIONAL` (may be `NA`). `REPEATED` columns are not
#'     currently supported by nanoparquet.
#'   * `logical_type`: Parquet logical type in a list column.
#'      An element has at least an entry called `type`, and potentially
#'      additional entries, e.g. `bit_width`, `is_signed`, etc.
#'
#' @seealso [parquet_metadata()] to read more metadata,
#'   [parquet_info()] for a very short summary.
#'   [parquet_schema()] for the complete Parquet schema.
#'   [read_parquet()], [write_parquet()], [nanoparquet-types].
#' @export

parquet_columns <- function(file) {
  mtd <- parquet_metadata(file)
	sch <- mtd$schema

	kv <- mtd$file_meta_data$key_value_metadata[[1]]

	type_map <- c(
		BOOLEAN = "logical",
		INT32 = "integer",
		INT64 = "double",
		DOUBLE = "double",
		FLOAT = "double",
		INT96 = "POSIXct",
		FIXED_LENGTH_BYTE_ARRAY = NA,
		BYTE_ARRAY = "character"
	)

	# keep leaf columns only, arrow schema is for leaf columns
	sch <- sch[is.na(sch$num_children) | sch$num_children == 0L, ]
	sch$r_type <- unname(type_map[sch$type])

	# detected from Arrow schema
	if (!identical(getOption("nanoparquet.use_arrow_metadata"), FALSE)) {
		spec <- if ("ARROW:schema" %in% kv$key) {
			kv <- mtd$file_meta_data$key_value_metadata[[1]]
			arrow_find_special(
				kv$value[match("ARROW:schema", kv$key)],
				file
			)
		}
		if (length(spec$factor)) sch$r_type[spec$factor] <- "factor"
		if (length(spec$difftime)) sch$r_type[spec$difftime] <- "difftime"
	}

	# TODO: this is duplicated in the C++ code
	# our own conversions
	dates <- vapply(
		sch$logical_type,
		function(lt) !is.null(lt$type) && lt$type == "DATE",
		logical(1)
	) | sch$converted_type == "DATE"
	sch$r_type[dates] <- "Date"

	hmss <- vapply(
		sch$logical_type,
		function(lt) !is.null(lt$type) && lt$type == "TIME",
		logical(1)
	) | sch$converted_type == "TIME_MILLIS"
	sch$r_type[hmss] <- "hms"

	poscts <- vapply(
		sch$logical_type,
		function(lt) !is.null(lt) && lt$type == "TIMESTAMP",
		logical(1)
	) | sch$converted_type == "TIMESTAMP_MICROS"
	sch$r_type[poscts] <- "POSIXct"

	sch$r_type[
		sch$type == "FIXED_LEN_BYTE_ARRAY" &
		sch$converted_type == "DECIMAL"] <- "double"
	cols <- c(
		"file_name",
		"name",
		"type",
		"r_type",
		"repetition_type",
		"logical_type"
	)
	sch[, cols]
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
#' \dontshow{if (!nanoparquet:::is_rcmd_check()) unlink("mtcars.parquet")}

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

	# Convert hms to milliseconds in integer
	hmss <- which(vapply(x, "inherits", "hms", FUN.VALUE = logical(1)))
	for (idx in hmss) {
		# convert seconds and milliseconds
		x[[idx]][] <- x[[idx]] * 1000
		# this keeps the class
		mode(x[[idx]]) <- "integer"
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

	# easier here than calling back to R
	required <- !vapply(x, anyNA, logical(1))

	invisible(.Call(
		nanoparquet_write,
		x,
		file,
		dim,
		compression,
		metadata,
		required
	))
}
