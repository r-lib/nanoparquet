#' @useDynLib nanoparquet, .registration=TRUE
#' @details
#' ```{r include = FALSE}
#' lines <- readLines("README.md")
#' end <- which(lines == "<!-- badges: end -->")
#' lines <- lines[-(1:end)]
#' readme <- tempfile()
#' writeLines(lines, readme)
#' ```
#' ```{r child = readme}
#' ```
"_PACKAGE"

#' @name nanoparquet-types
#' @title nanoparquet's type maps
#' @description
#' How nanoparquet maps R types to Parquet types.
#'
#' @details
#' ```{r, child = "tools/types.Rmd"}
#' ```
#' @seealso [nanoparquet-package] for options that modify the type
#' mappings.
NULL
