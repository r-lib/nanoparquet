parquet_pages <- function(file) {
	file <- path.expand(file)
	res <- .Call(nanoparquet_read_pages, file)
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
	res <- .Call(nanoparquet_read_page, file, as.double(offset))
	res$page_type <- names(page_types)[res$page_type + 1L]
	res$codec <- names(codecs)[res$codec + 1L]
	res$encoding <- names(encodings)[res$encoding + 1L]
	res$definition_level_encoding <-
		names(encodings)[res$definition_level_encoding + 1L]
	res$repetition_level_encoding <-
		names(encodings)[res$repetition_level_encoding + 1L]
	res$data_type <- names(type_names)[res$data_type + 1L]
	res$repetition_type <- names(repetition_types)[res$repetition_type + 1L]
	if (res$codec == "SNAPPY") {
		res$compressed_data <- res$data
		res$data <- snappy_uncompress(res$data)
	}
	res
}

snappy_compress <- function(buffer) {
  .Call(snappy_compress_raw, buffer)
}

snappy_uncompress <- function(buffer) {
  .Call(snappy_uncompress_raw, buffer)
}

rle_encode_int <- function(x) {
	bw <- if (length(x)) {
		max(as.integer(ceiling(log2(max(x) + 1L))), 1L)
	} else {
		1L
	}
	res <- .Call(nanoparquet_rle_encode_int, x, bw)
	attr(res, "bit_width") <- bw
	attr(res, "length") <- length(x)
	res
}

rle_decode_int <- function(x, bit_width, length = NA) {
	.Call(nanoparquet_rle_decode_int, x, bit_width, is.na(length), length)
}