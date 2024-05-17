parquet_pages <- function(file) {
	file <- path.expand(file)
	res <- .Call(miniparquet_read_pages, file)
	res$encoding <- names(encodings)[res$encoding + 1L]
	res$definition_level_encoding <-
		names(encodings)[res$definition_level_encoding + 1L]
	res$repetition_level_encoding <-
		names(encodings)[res$repetition_level_encoding + 1L]
	res$page_type <- names(page_types)[res$page_type + 1L]
	res <- as.data.frame(res)
	class(res) <- c("tbl", class(res))
	res
}

read_parquet_page <- function(file, offset) {
	file <- path.expand(file)
	res <- .Call(miniparquet_read_page, file, as.double(offset))
	res$page_type <- names(page_types)[res$page_type + 1L]
	res$codec <- names(codecs)[res$codec + 1L]
	res$encoding <- names(encodings)[res$encoding + 1L]
	res$definition_level_encoding <-
		names(encodings)[res$definition_level_encoding + 1L]
	res$repetition_level_encoding <-
		names(encodings)[res$repetition_level_encoding + 1L]
	res$data_type <- names(type_names)[res$data_type + 1L]
	res$repetition_type <- names(repetition_types)[res$repetition_type + 1L]
	res
}

snappy_compress <- function(buffer) {
  .Call(snappy_compress_raw, buffer)
}

snappy_uncompress <- function(buffer) {
  .Call(snappy_uncompress_raw, buffer)
}
