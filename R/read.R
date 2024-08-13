
#' @export

read_parquet2 <- function(file, options = parquet_options()) {
  bits <- .Call(nanoparquet_read2, file, options, sys.call())
  bits
}
