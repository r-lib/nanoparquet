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

is_row_group_sequence <- function(x) {
  x <- tryCatch(as.integer(x), error = function(e) x)
  is.integer(x) && ! any(is.na(x)) && length(x) >= 1 && x[1] == 1L &&
    all(diff(x) > 0)
}

is_count <- function(x, min = 0L) {
  x <- tryCatch(as.integer(x), error = function(e) x)
  is.integer(x) && length(x) == 1 && !is.na(x) && x >= min
}
