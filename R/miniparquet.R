#' Read a Parquet file into a data frame
#'
#' Converts the contents of the named Parquet file to a R data frame.
#'
#' @param file Path to a Parquet file.
#' @return A `data.frame` with the file's contents.
#' @export
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "miniparquet")
#' parquet_df <- miniparquet::parquet_read(file_name)
#' print(str(parquet_df))

parquet_read <- function(file) {
	res <- .Call(miniparquet_read, file)
	# some data.frame dress up
	attr(res, "row.names") <- c(NA_integer_, as.integer(-1 * length(res[[1]])))
	class(res) <- "data.frame"
	res
}

#' @export
#' @rdname parquet_read

read_parquet <- parquet_read

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
#' @examplesIf !miniparquet:::is_rcmd_check()
#' # add row names as a column, because `parquet_write()` ignores them.
#' mtcars2 <- cbind(name = rownames(mtcars), mtcars)
#' parquet_write(mtcars2, "mtcars.parquet")

parquet_write <- function(
	x,
	file,
	compression = c("snappy", "uncompressed")) {

	codecs <- c("uncompressed" = 0L, "snappy" = 1L)
	compression <- codecs[match.arg(compression)]
	dim <- as.integer(dim(x))
	invisible(.Call(miniparquet_write, x, file, dim, compression))
}

#' @export
#' @rdname parquet_write

write_parquet <- parquet_write
