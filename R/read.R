#' Read Parquet file, next gen
#'
#' @inheritParams read_parquet
#' @export

read_parquet2 <- function(file, options = parquet_options()) {
  file <- path.expand(file)
  res <- .Call(nanoparquet_read2, file, options, sys.call())
	dicts <- res[[2]]
	types <- res[[3]]
	res <- res[[1]]
	if (options[["use_arrow_metadata"]]) {
		res <- apply_arrow_schema2(res, file, dicts, types)
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
