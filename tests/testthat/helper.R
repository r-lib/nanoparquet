test_df <- function(tibble = FALSE) {
  df <- cbind(nam = rownames(mtcars), mtcars)
  df$cyl <- as.integer(df$cyl)
  df$large <- df$cyl >= 6
  rownames(df) <- NULL
  if (tibble) {
    class(df) <- c("tbl_df", "tbl", "data.frame")
  }
  df
}
