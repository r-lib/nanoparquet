test_df <- function(tibble = FALSE, factor = FALSE) {
  df <- cbind(nam = rownames(mtcars), mtcars)
  df$cyl <- as.integer(df$cyl)
  df$large <- df$cyl >= 6
  if (factor) {
    df$fac <- as.factor(tolower(substr(df$nam, 1, 1)))
  }
  rownames(df) <- NULL
  if (tibble) {
    class(df) <- c("tbl_df", "tbl", "data.frame")
  } else {
    class(df) <- c("tbl", "data.frame")
  }
  df
}
