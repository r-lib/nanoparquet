#' Metadata of all pages of a Parquet file
#'
#' @details
#' Reading all the page headers might be slow for large files, especially
#' if the file has many small pages.
#'
#' @param file Path to a Parquet file.
#' @return Data frame with columns:
#'   * `file_name`: file name.
#'   * `row_group`: id of the row group the page belongs to,
#'     an integer between 0 and the number of row groups
#'     minus one.
#'   * `column`: id of the column. An integer between the
#'     number of leaf columns minus one. Note that only leaf
#'     columns are considered, as non-leaf columns do not
#'     have any pages.
#'   * `page_type`: `DATA_PAGE`, `INDEX_PAGE`, `DICTIONARY_PAGE` or
#'     `DATA_PAGE_V2`.
#'   * `page_header_offset`: offset of the data page (its header) in the
#'     file.
#'   * `uncompressed_page_size`: does not include the page header, as per
#'     Parquet spec.
#'   * `compressed_page_size`: without the page header.
#'   * `crc`: integer, checksum, if present in the file, can be `NA`.
#'   * `num_values`: number of data values in this page, include
#'     `NULL` (`NA` in R) values.
#'   * `encoding`: encoding of the page, current possible encodings:
#'     `r paste0('"', names(encodings), '"', collapse = ", ")`.
#'   * `definition_level_encoding`: encoding of the definition levels,
#'     see `encoding` for possible values. This can be missing in V2 data
#'     pages, where they are always RLE encoded.
#'   * `repetition_level_encoding`: encoding of the repetition levels,
#'     see `encoding` for possible values. This can be missing in V2 data
#'     pages, where they are always RLE encoded.
#'   * `data_offset`: offset of the actual data in the file.
#'   * `page_header_length`: size of the page header, in bytes.
#'
#' @keywords internal
#' @seealso [read_parquet_page()] to read a page.
#' @examples
#' file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
#' nanoparquet:::read_parquet_pages(file_name)

read_parquet_pages <- function(file) {
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

#' Read a page from a Parquet file
#'
#' @param file Path to a Parquet file.
#' @param offset Integer offset of the start of the page in the file.
#'   See [read_parquet_pages()] for a list of all pages and their offsets.
#' @return Named list. Many entries correspond to the columns of
#'   the result of [read_parquet_pages()]. Additional entries are:
#'   * `codec`: compression codec. Possible values:
#'   * `has_repetition_levels`: whether the page has repetition levels.
#'   * `has_definition_levels`: whether the page has definition levels.
#'   * `schema_column`: which schema column the page corresponds to. Note
#'     that only leaf columns have pages.
#'   * `data_type`: low level Parquet data type. Possible values:
#'   * `repetition_type`: whether the column the page belongs to is
#'     `REQUIRED`, `OPTIONAL` or `REPEATED`.
#'   * `page_header`: the bytes of the page header in a raw vector.
#'   * `num_null`: number of missing (`NA`) values. Only set in V2 data
#'     pages.
#'   * `num_rows`: this is the same as `num_values` for flat tables, i.e.
#'     files without repetition levels.
#'   * `compressed_data`: the data of the page in a raw vector. It includes
#'     repetition and definition levels, if any.
#'   * `data`: the uncompressed data, if nanoparquet supports the
#'     compression codec of the file (GZIP and SNAPPY at the time of
#'     writing), or if the file is not compressed. In the latter case it
#'     is the same as `compressed_data`.
#'
#' @keywords internal
#' @seealso [read_parquet_pages()] for a summary of all pages.
#' @examplesIf Sys.getenv("IN_PKGDOWN") == "true"
#' file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
#' nanoparquet:::read_parquet_pages(file_name)
#' options(max.print = 100)  # otherwise long raw vector
#' nanoparquet:::read_parquet_page(file_name, 4L)

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
	res$compressed_data <- res$data
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
	} else if (res$codec == "ZSTD") {
		res$compressed_data <- res$data
		res$data <- c(
			if (copy > 0) res$data[1:copy],
			zstd_uncompress(
				res$data[(skip+copy+1L):length(res$data)],
				res$uncompressed_page_size - skip - copy
			)
		)
	} else if (res$codec == "UNCOMPRESSED") {
		# keep data
	} else {
		res$data <- NULL
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

zstd_compress <- function(buffer) {
	.Call(zstd_compress_raw, buffer)
}

zstd_uncompress <- function(buffer, uncompressed_length) {
	.Call(zstd_uncompress_raw, buffer, uncompressed_length)
}

#' RLE encode integers
#'
#' @param x Integer vector.
#' @return Raw vector, the encoded integers. It has two attributes:
#'   * `bit_length`: the number of bits needed to encode the input, and
#'   * `length`: length of the original integer input.
#'
#' @keywords internal
#' @seealso [rle_decode_int()]
#' @family encodings

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

#' RLE decode integers
#'
#' @param x Raw vector of the encoded integers.
#' @param bit_width Bit width used for the encoding.
#' @param length Length of the output. If `NA` then we assume that `x`
#'   starts with length of the output, encoded as a 4 byte integer.
#' @return The decoded integer vector.
#'
#' @keywords internal
#' @seealso [rle_encode_int()]
#' @family encodings

rle_decode_int <- function(x, bit_width = attr(x, "bit_width"),
													 length = attr(x, "length") %||% NA) {
	.Call(nanoparquet_rle_decode_int, x, bit_width, is.na(length), length)
}

# dbp_encode_int <- function(x) {
# 	.Call(nanoparquet_dbp_encode_int32, x)
# }

dbp_decode_int <- function(x) {
	.Call(nanoparquet_dbp_decode_int32, x)
}

# dbp_encode_int64 <- function(x) {
# 	.Call(nanoparquet_dbp_encode_int64, x)
# }

dbp_decode_int64 <- function(x) {
	.Call(nanoparquet_dbp_decode_int64, x)
}

unpack_bits <- function(x, bit_width, n) {
	.Call(nanoparquet_unpack_bits_int32, x, bit_width, n)
}

pack_bits <- function(x, bit_width = NULL) {
	bit_width <- bit_width %||% if (length(x)) {
		max(as.integer(ceiling(log2(max(x) + 1L))), 1L)
	} else {
		0L
	}
	.Call(nanoparquet_pack_bits_int32, x, bit_width)
}

dict_encode <- function(x, n = length(x)) {
	.Call(nanoparquet_create_dict, x, n)
}

dict_encode_idx <- function(x) {
	.Call(nanoparquet_create_dict_idx, x, sys.call())
}

lgl_avg_run_length <- function(x, n = length(x)) {
	.Call(nanoparquet_avg_run_length, x, n)
}
