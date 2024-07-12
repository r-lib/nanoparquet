#' Infer Parquet schema of a data frame
#'
#' @param df Data frame.
#' @param options Return value of [parquet_options()], may modify the
#' R to Parquet type mappings.
#'
#' @seealso [read_parquet_schema()] to the schema of a Parquet file,
#'   [parquet_schema()] to create a Parquet schema from scratch.
#' @export

infer_parquet_schema <- function(df, options) {
	types <- .Call(nanoparquet_map_to_parquet_types, df, options)
	type_tab <- data.frame(
		file_name = rep(NA_character_, length(df)),
		name = names(df),
		type = vapply(types, function(x) x[[1]], ""),
		r_type = vapply(types, function(x) x[[2]], ""),
		repetition_type = ifelse(vapply(df, anyNA, TRUE), "OPTIONAL", "REQUIRED"),
		logical_type = I(unname(lapply(types, function(x) x[[3]])))
	)

	rownames(type_tab) <- NULL
	class(type_tab) <- c("tbl", class(type_tab))
	type_tab
}
