#' Read a Parquet file into a data frame
#'
#' Converts the contents of the named Parquet file to a R data frame.
#'
#' @param file Path to a Parquet file. It may also be an R connection,
#'   in which case it first reads all data from the connection, writes
#'   it into a temporary file, then reads the temporary file, and
#'   deletes it. The connection might be open, it which case it must be
#'   a binary connection. If it is not open, then `read_parquet()` will
#'   open it and also close it in the end.
#' @param options Nanoparquet options, see [parquet_options()].
#' @return A `data.frame` with the file's contents.
#' @export
#' @seealso See [write_parquet()] to write Parquet files,
#'   [nanoparquet-types] for the R <-> Parquet type mapping.
#'   See [read_parquet_info()], for general information,
#'   [read_parquet_schema()] for information about the
#'   columns, and [read_parquet_metadata()] for the complete metadata.
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
#' parquet_df <- nanoparquet::read_parquet(file_name)
#' print(str(parquet_df))

read_parquet <- function(file, options = parquet_options()) {
	if (inherits(file, "connection")) {
		tmp <- tempfile(fileext = ".parquet")
		dump_connection(file, tmp)
		file <- tmp
	}
  file <- path.expand(file)
  res <- .Call(nanoparquet_read2, file, options, sys.call())
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

  res
}

# dump the contents of a connection to path
dump_connection <- function(con, path) {
	if (!isOpen(con)) {
		on.exit(close(con), add = TRUE)
		open(con, "rb")
	}
	ocon <- file(path, open = "wb")
	# 10 MB buffer by default
	bs <- getOption("nanoparquet.con_buffer_size", 1024L * 1024L * 10)
	while (TRUE) {
		buf <- readBin(con, what = "raw", n = bs)
		if (length(buf) == 0) {
			break
		}
		writeBin(buf, path)
	}
	close(ocon)
}
