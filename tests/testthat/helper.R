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

test_package_root <- function() {
  skip_if_not_installed("rprojroot")
  x <- tryCatch(
    rprojroot::find_package_root_file(),
    error = function(e) NULL)

  if (!is.null(x)) return(x)

  pkg <- testthat::testing_package()
  x <- tryCatch(
    rprojroot::find_package_root_file(
      path = file.path("..", "..", "00_pkg_src", pkg)),
    error = function(e) NULL)

  if (!is.null(x)) return(x)

  stop("Cannot find package root")
}
