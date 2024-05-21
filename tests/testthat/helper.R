test_df <- function(tibble = FALSE, factor = FALSE, missing = FALSE) {
  df <- cbind(nam = rownames(mtcars), mtcars)
  df$cyl <- as.integer(df$cyl)
  df$large <- df$cyl >= 6
  if (factor) {
    df$fac <- as.factor(tolower(substr(df$nam, 1, 1)))
  }
  rownames(df) <- NULL

  if (missing) {
    for (i in seq_len(ncol(df))) {
      if (i <= nrow(df)) {
        df[i,i] <- NA
      } else {
        df[1,i] <- NA
      }
    }
  }

  if (tibble) {
    class(df) <- c("tbl_df", "tbl", "data.frame")
  } else {
    class(df) <- c("tbl", "data.frame")
  }
  df
}
