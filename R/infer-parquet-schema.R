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

  # types[[cl]] is a list of N 4-tuples: list(type, rtype, logical_type, name)
  nsels <- lengths(types)
  col_idx <- rep(seq_along(df), nsels)
  pos_in_col <- sequence(nsels)

  all_sels <- unlist(types, recursive = FALSE)
  lt <- unname(lapply(all_sels, function(x) x[[3]]))
  ct <- lapply(lt, function(x) if (!is.null(x)) logical_to_converted(x))

  repetition_type <- mapply(
    infer_repetition_type,
    col = df[col_idx],
    nsels = nsels[col_idx],
    pos = pos_in_col
  )

  type_tab <- data.frame(
    file_name = rep(NA_character_, length(all_sels)),
    r_col = as.integer(col_idx),
    name = vapply(all_sels, function(x) x[[4]], ""),
    r_type = vapply(all_sels, function(x) x[[2]], ""),
    type = vapply(all_sels, function(x) x[[1]], ""),
    type_length = rep(NA_integer_, length(all_sels)),
    repetition_type = repetition_type,
    converted_type = map_chr(ct, function(x) {
      x[["converted_type"]] %||% NA_character_
    }),
    logical_type = I(lt),
    num_children = ifelse(
      nsels[col_idx] == 3L & pos_in_col <= 2L,
      1L,
      NA_integer_
    ),
    scale = map_int(ct, function(x) {
      x[["scale"]] %||% NA_integer_
    }),
    precision = map_int(ct, function(x) {
      x[["precision"]] %||% NA_integer_
    }),
    field_id = rep(NA_integer_, length(all_sels))
  )

  rownames(type_tab) <- NULL
  class(type_tab) <- c("tbl", class(type_tab))
  type_tab
}

infer_repetition_type <- function(col, nsels, pos) {
  if (nsels == 1L) {
    if (anyNA(col)) "OPTIONAL" else "REQUIRED"
  } else {
    # LIST column: OPTIONAL, REPEATED, OPTIONAL
    c("OPTIONAL", "REPEATED", "OPTIONAL")[pos]
  }
}
