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
	skip <- 0L
	copy <- 0L
	if (res$page_type == "DATA_PAGE_V2") {
		if (!is.na(res$repetition_levels_byte_length)) {
			skip <- res$repetition_levels_byte_length
		}
		if (!is.na(res$definition_levels_byte_length)) {
			copy <- res$definition_levels_byte_length
		}
	}
	if (res$codec == "SNAPPY") {
		res$compressed_data <- res$data
		res$data <- c(
			if (copy > 0) res$data[1:copy],
			snappy_uncompress(res$data[(skip+copy+1L):length(res$data)])
		)
	} else if (res$codec == "GZIP") {
		res$compressed_data <- res$data
		res$data <- c(
			if (copy > 0) res$data[1:copy],
			gzip_uncompress(
				res$data[(skip+copy+1L):length(res$data)],
				res$uncompressed_page_size - skip - copy
			)
		)
	}
	res
}

snappy_compress <- function(buffer) {
  .Call(snappy_compress_raw, buffer)
}

snappy_uncompress <- function(buffer) {
  .Call(snappy_uncompress_raw, buffer)
}

gzip_compress <- function(buffer) {
	.Call(gzip_compress_raw, buffer)
}

gzip_uncompress <- function(buffer, uncompressed_length) {
	.Call(gzip_uncompress_raw, buffer, uncompressed_length)
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

dict_encode <- function(x, n = length(x)) {
	.Call(nanoparquet_create_dict, x, n)
}

dict_encode_idx <- function(x) {
	.Call(nanoparquet_create_dict_idx, x)
}

lgl_avg_run_length <- function(x, n = length(x)) {
	.Call(nanoparquet_avg_run_length, x, n)
}
