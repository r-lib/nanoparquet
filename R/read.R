
# TODO: rewrite in C, if possible
#' @export

read_parquet2 <- function(file, options = parquet_options()) {
  bits <- .Call(nanoparquet_read2, file)
  leaf_cols <- which(!vapply(bits[[2]], is.null, logical(1)))
  cnms <- bits[[1]][["col_name"]][leaf_cols]
  vals <- structure(vector("list", length(cnms)), names = cnms)
  for (idx in seq_along(vals)) {
    chunks <- bits[[2]][[leaf_cols[[idx]]]]
    type <- typeof(chunks[[1]][[1]] %||% chunks[[1]][[2]][[1]])
    val <- vector(type, bits[[1]][["num_rows"]])
    for (chunk in chunks) {
      if (is.null(chunk[[1]])) {
        # no dictionary
        # TODO: missing values
        for (pg in chunk[-1]) {
          if (is.null(pg)) break
          from <- pg[[3]] + 1L
          to <- pg[[4]]
          val[from:to] <- pg[[1]]
        }
      } else {
        # dictionary
        for (pg in chunk[-1]) {
          if (is.null(pg)) break
          from <- pg[[3]] + 1L
          to <- pg[[4]]
          val[from:to] <- chunk[[1]][pg[[1]] + 1L]
        }
      }
    }
    vals[[idx]] <- val
  }
	attr(vals, "row.names") <- c(NA_integer_, as.integer(-1 * length(vals[[1]])))
  class(vals) <- c("tbl", "data.frame")

  vals
}