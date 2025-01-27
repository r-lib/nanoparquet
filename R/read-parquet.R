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
#' @param col_select Columns to read. It can be a numeric vector of column
#'   indices.
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

read_parquet <- function(file, col_select = NULL,
												 options = parquet_options()) {
	if (inherits(file, "connection")) {
		tmp <- tempfile(fileext = ".parquet")
		on.exit(unlink(tmp), add = TRUE)
		dump_connection(file, tmp)
		file <- tmp
	}
  file <- path.expand(file)

	if (!is.null(col_select)) {
		stopifnot(is.numeric(col_select))
		col_select <- as.integer(col_select)
		col_select <- col_select[!is.na(col_select)]
		col_select <- unique(col_select)
		stopifnot(all(col_select >= 1L))
	}

  res <- .Call(nanoparquet_read2, file, options, col_select, sys.call())
	post_process_read_result(res, file, options, col_select)
}

post_process_read_result <- function(res, file, options, col_select) {
	dicts <- res[[2]]
	types <- res[[3]]
	arrow_schema <- res[[4]]
	res <- res[[1]]
	if (options[["use_arrow_metadata"]] && !is.na(arrow_schema)) {
		res <- apply_arrow_schema(res, file, arrow_schema, dicts, types, col_select)
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

	if (length(options[["class"]]) > 0) {
		class(res) <- unique(c(options[["class"]], class(res)))
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

read_parquet_row_group <- function(file, row_group,
																	 options = parquet_options()) {
	if (inherits(file, "connection")) {
		tmp <- tempfile(fileext = ".parquet")
		on.exit(unlink(tmp), add = TRUE)
		dump_connection(file, tmp)
		file <- tmp
	}
  file <- path.expand(file)
  res <- .Call(nanoparquet_read_row_group, file, row_group,
							 options, sys.call())
	post_process_read_result(res, file, options, col_select = NULL)
}

# TODO: this does not work currently
read_parquet_column_chunk <- function(file, row_group = 0L, column = 0L,
																			options = parquet_options()) {
	if (inherits(file, "connection")) {
		tmp <- tempfile(fileext = ".parquet")
		on.exit(unlink(tmp), add = TRUE)
		dump_connection(file, tmp)
		file <- tmp
	}
  file <- path.expand(file)
  res <- .Call(nanoparquet_read_column_chunk, file, row_group, column,
							 options, sys.call())
	post_process_read_result(res, file, options)
}
