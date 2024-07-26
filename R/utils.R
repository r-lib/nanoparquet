`%||%` <- function(l, r) if (is.null(l)) r else l

is_rcmd_check <- function() {
  if (identical(Sys.getenv("NOT_CRAN"), "true")) {
    FALSE
  } else {
    Sys.getenv("_R_CHECK_PACKAGE_NAME_", "") != ""
  }
}

mkdirp <- function(x) {
  dir.create(x, showWarnings = FALSE, recursive = TRUE)
}

capitalize <- function(x) {
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  x
}

vcapply <- function(X, FUN, ...) {
  vapply(X, FUN, FUN.VALUE = character(1), ...)
}

is_asan <- function() {
  .Call(is_asan_)
}

is_ubsan <- function() {
  .Call(is_ubsan_)
}

is_flag <- function(x) {
  is.logical(x) && length(x) == 1 && !is.na(x)
}

is_string <- function(x) {
  is.character(x) && length(x) == 1 && !is.na(x)
}

is_uint32 <- function(x) {
  is.numeric(x) && length(x) == 1 && !is.na(x) &&
    round(x) == x && x >= 0 && x <= 4294967295
}

map_chr <- function(.x, .f, ...) {
  vapply(.x, .f, FUN.VALUE = character(1), ...)
}

map_int <- function(.x, .f, ...) {
  vapply(.x, .f, FUN.VALUE = integer(1), ...)
}

any_na <- function(x) {
  t <- typeof(x)
  if (t == "list") {
    .Call(nanoparquet_any_null, x)
  } else if (t == "double") {
    .Call(nanoparquet_any_na, x)
  } else {
    anyNA(x)
  }
}

is_named <- function(x) {
  nm <- names(x)
  !is.null(nm) && !anyNA(nm)
}
