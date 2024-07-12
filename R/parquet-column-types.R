#' Map between R and Parquet data types
#'
#' Note that this function is now deprecated. Please use
#' [read_parquet_schema()] for files, and [infer_parquet_schema()] for
#' data frames.
#'
#' This function works two ways. It can map the R types of a data frame to
#' Parquet types, to see how [write_parquet()] would write out the data
#' frame. It can also map the types of a Parquet file to R types, to see
#' how [read_parquet()] would read the file into R.
#'
#' @param x Path to a Parquet file, or a data frame.
#' @param options Nanoparquet options, see [parquet_options()].
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
#' @seealso [read_parquet_metadata()] to read more metadata,
#'   [read_parquet_info()] for a very short summary.
#'   [read_parquet_schema()] for the complete Parquet schema.
#'   [read_parquet()], [write_parquet()], [nanoparquet-types].
#' @export

parquet_column_types <- function(x, options = parquet_options()) {
	warning(
		"`parquet_column_types()` is deprecated, please use ",
		"`read_parquet_schema()` or `parquet_schema()` instead."
  )
	if (is.character(x)) {
		parquet_column_types_file(x, options)
	} else if (is.data.frame(x)) {
		infer_parquet_schema(x, options)
	} else {
		stop("`x` must be a file name or a data frame in `parquet_column_types()`")
	}
}

parquet_column_types_file <- function(file, options) {
  mtd <- read_parquet_metadata(file)
	sch <- mtd$schema
	add_r_type_to_schema(mtd, sch, options)
}

add_r_type_to_schema <- function(mtd, sch, options) {
	kv <- mtd$file_meta_data$key_value_metadata[[1]]

	type_map <- c(
		BOOLEAN = "logical",
		INT32 = "integer",
		INT64 = "double",
		DOUBLE = "double",
		FLOAT = "double",
		INT96 = "POSIXct",
		FIXED_LEN_BYTE_ARRAY = "raw",
		BYTE_ARRAY = "raw"
	)

	sch$r_type <- unname(type_map[sch$type])

	sch$r_type[
		sch$type == "FIXED_LEN_BYTE_ARRAY" &
		sch$converted_type == "DECIMAL"] <- "double"
	sch$r_type[
		vapply(sch$logical_type, function(x) {
			!is.null(x$type) && x$type %in% c("STRING", "ENUM", "UUID")
		}, logical(1)) |
		sch$converted_type == "UTF8"] <- "character"

	# detected from Arrow schema
	if (options[["use_arrow_metadata"]]) {
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

	cols <- c(
		"file_name",
		"name",
		"r_type",
		setdiff(colnames(sch), c("file_name", "name"))
	)
	sch[, cols]
}
