#' Read a Parquet file into a data frame
#'
#' Converts the contents of the named Parquet file to a R data frame.
#'
#' @param file Path to a Parquet file.
#' @param options Nanoparquet options, see [parquet_options()].
#' @return A `data.frame` with the file's contents.
#' @export
#' @seealso See [write_parquet()] to write Parquet files,
#'   [nanoparquet-types] for the R <-> Parquet type mapping.
#'   See [parquet_info()], for general information,
#'   [parquet_column_types()] and [parquet_schema()] for information about the
#'   columns, and [parquet_metadata()] for the complete metadata.
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
#' parquet_df <- nanoparquet::read_parquet(file_name)
#' print(str(parquet_df))

read_parquet <- function(file, options = parquet_options()) {
	file <- path.expand(file)
	res <- .Call(nanoparquet_read, file)
	dicts <- res[[2]]
	types <- res[[3]]
	res <- res[[1]]
	if (options[["use_arrow_metadata"]]) {
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
	class(res) <- c(options[["class"]], "data.frame")
	res
}
