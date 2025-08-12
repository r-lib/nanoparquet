`%||%` <- function(l, r) if (is.null(l)) r else l

`%&&%` <- function(l, r) if (is.null(l)) NULL else r

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

is_icount <- function(x, zero = FALSE) {
  is.integer(x) &&
    length(x) == 1 &&
    !is.na(x) &&
    ((zero && x >= 0L) || (!zero && x >= 1L))
}

is_dcount <- function(x, zero = FALSE) {
  is.double(x) &&
    length(x) == 1 &&
    !is.na(x) &&
    as.integer(x) == x &&
    ((zero && x >= 0) || (!zero && x >= 1))
}

as_count <- function(x, name = "x", zero = FALSE) {
  if (is_icount(x, zero)) {
    return(x)
  }
  if (is_dcount(x, zero)) {
    return(as.integer(x))
  }
  stop(name, " must be a count, i.e. an integer scalar")
}

as_integer_scalar <- function(x, name = "x") {
  if (is.integer(x) && length(x) == 1 && !is.na(x)) {
    return(x)
  }
  if (is.double(x) && length(x) == 1 && !is.na(x) && as.integer(x) == x) {
    return(as.integer(x))
  }
  stop(name, " must be an integer scalar")
}

is_uint32 <- function(x) {
  is.numeric(x) &&
    length(x) == 1 &&
    !is.na(x) &&
    round(x) == x &&
    x >= 0 &&
    x <= 4294967295
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
    .Call(rf_nanoparquet_any_null, x)
  } else if (t == "double") {
    .Call(rf_nanoparquet_any_na, x)
  } else {
    anyNA(x)
  }
}

is_named <- function(x) {
  nm <- names(x)
  !is.null(nm) && !anyNA(nm)
}
