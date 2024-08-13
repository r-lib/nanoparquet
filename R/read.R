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

  res
}
