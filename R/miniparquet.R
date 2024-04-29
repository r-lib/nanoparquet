parquet_read <- function(file) {
	res <- .Call(miniparquet_read, file)
	# some data.frame dress up
	attr(res, "row.names") <- c(NA_integer_, as.integer(-1 * length(res[[1]])))
	class(res) <- "data.frame"
	res
}

read_parquet <- parquet_read

parquet_write <- function(x, file) {
	dim <- as.integer(dim(x))
	invisible(.Call(miniparquet_write, x, file, dim))
}

write_parquet <- parquet_write
