#' Infer Parquet schema of a data frame
#'
#' @param df Data frame.
#' @param options Return value of [parquet_options()], may modify the
#'   R to Parquet type mappings.
#' @return Data frame, the inferred schema. It has the same columns as
#'   the return value of [read_parquet_schema()]:
#'   `r paste0("\u0060", names(infer_parquet_schema(mtcars)), "\u0060", collapse = ", ")`.
#'
#' @seealso [read_parquet_schema()] to read the schema of a Parquet file,
#'   [parquet_schema()] to create a Parquet schema from scratch.
#' @export

infer_parquet_schema <- function(df, options = parquet_options()) {
	types <- .Call(rf_nanoparquet_map_to_parquet_types, df, options)
	lt <- unname(lapply(types, function(x) x[[3]]))
	ct <- lapply(lt, function(x) if (!is.null(x)) logical_to_converted(x))
	type_tab <- data.frame(
		file_name = rep(NA_character_, length(df)),
		name = names(df),
		r_type = vapply(types, function(x) x[[2]], ""),
		type = vapply(types, function(x) x[[1]], ""),
		type_length = rep(NA_integer_, length(df)),
		repetition_type = ifelse(vapply(df, anyNA, TRUE), "OPTIONAL", "REQUIRED"),
		converted_type = map_chr(ct, function(x) {
			x[["converted_type"]] %||% NA_character_
		}),
		logical_type = I(lt),
		num_children = rep(NA_integer_, length(df)),
		scale = map_int(ct, function(x) {
			x[["scale"]] %||% NA_integer_
		}),
		precision = map_int(ct, function(x) {
			x[["precision"]] %||% NA_integer_
		}),
		field_id = rep(NA_integer_, length(df))
	)

	rownames(type_tab) <- NULL
	class(type_tab) <- c("tbl", class(type_tab))
	type_tab
}
