#' @exportS3Method pillar::obj_sum
obj_sum.nanoparquet_logical_type <- function(x, ...) {
  type <- x$type
  paste0(
    type,
    if (type == "INT") sprintf("(%d, %s)", x$bit_width, x$is_signed),
    if (type == "DECIMAL") sprintf("(%d, %d)", x$precision, x$scale %||% 0),
    if (type %in% c("TIME", "TIMESTAMP")) {
      sprintf("(%s, %s)", x$is_adjusted_to_utc, x$unit)
    }
  )
}
