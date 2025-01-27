# Load packages here, so the package loading time is not measured as
# part of the benchmark. nanoparquet loads very quickly, but duckdb and
# need to load their dependencies as well, and it might take up to 200ms,
# which is significant for the small data set.

loadNamespace("nanoparquet")
loadNamespace("arrow")
loadNamespace("duckdb")

me <- normalizePath(
  if (Sys.getenv("QUARTO_DOCUMENT_PATH") != "") {
    Sys.getenv("QUARTO_DOCUMENT_PATH")
  } else if (file.exists("benchmarks-funcs.R")) {
    getwd()
  } else if (file.exists("articles/benchmarks-funcs.R")) {
    "articles"
  } else {
    "vignettes/articles"
  })

data_sizes <- c("small", "medium", "large")
variants <- c(
  "nanoparquet", "arrow", "duckdb", "data.table",
  "data.table.gz", "readr"
)

gen_data <- function(size) {
  switch(
    size,
    small = nycflights13::flights,
    medium = vctrs::vec_rep(nycflights13::flights, 20),
    large = vctrs::vec_rep(nycflights13::flights, 200)
  )
}

read_nanoparquet <- function(path) {
  nanoparquet::read_parquet(path)
}

read_arrow <- function(path) {
  # do not use ALTREP for a fair (?) comparison
  options(arrow.use_altrep = FALSE)
  arrow::read_parquet(path)
}

read_duckdb <- function(path) {
  duckdb:::sql(sprintf("FROM '%s'", path))
}

read_data_table <- function(path) {
  data.table::fread(path)
}

read_readr <- function(path) {
  readr::read_csv(path)
}

write_nanoparquet <- function(x, path) {
  nanoparquet::write_parquet(x, path)
}

write_arrow <- function(x, path) {
  arrow::write_parquet(x, path)
}

write_duckdb <- function(x, path) {
  drv <- duckdb::duckdb()
  con <- DBI::dbConnect(drv)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbWriteTable(con, "tab", as.data.frame(x))
  DBI::dbExecute(con, DBI::sqlInterpolate(con,
    "COPY tab TO ?filename (FORMAT 'parquet', COMPRESSION 'snappy')",
    filename = path
  ))
}

write_data_table <- function(x, path) {
  data.table::fwrite(x, path)
}

write_readr <- function(x, path) {
  readr::write_csv(x, path)
}

measure <- function(
  variant = c("nanoparquet", "arrow", "duckdb", "data.table",
    "data.table.gz", "readr", "data"),
  size = c("small", "medium", "large")) {

  message("Measuring ", variant, ", ", size)

  variant <- match.arg(variant)
  size <- match.arg(size)

  ext <- switch(
    variant,
    readr = ,
    data.table = ".csv",
    data.table.gz = ".csv.gz",
    ".parquet"
  )
  tmp <- tempfile(fileext = ext)
  on.exit(unlink(tmp), add = TRUE)

  write_result <- callr::r(
    args = list(variant, size, tmp, me),
    function(variant, size, tmp, me) {

    source(file.path(me, "benchmarks-funcs.R"))
    test_data <- gen_data(size)

    write <- switch(variant,
      nanoparquet = write_nanoparquet,
      arrow = write_arrow,
      duckdb = write_duckdb,
      data.table =, data.table.gz = write_data_table,
      readr = write_readr
    )

    gc(); gc();
    mem_before <- ps::ps_memory_full_info()
    timing <- system.time(write(test_data, tmp))
    mem_after <- ps::ps_memory_full_info()
    list(mem_before = mem_before, mem_after = mem_after, timing = timing)
    })

  read_result <- callr::r(
    args = list(variant, tmp, me),
    function(variant, tmp, me) {

    source(file.path(me, "benchmarks-funcs.R"))
    read <- switch(variant,
      nanoparquet = read_nanoparquet,
      arrow = read_arrow,
      duckdb = read_duckdb,
      data.table =, data.table.gz = read_data_table,
      readr = read_readr
    )
    gc(); gc()
    mem_before <- ps::ps_memory_full_info()
    timing <- system.time(read(tmp))
    mem_after <- ps::ps_memory_full_info()
    list(mem_before = mem_before, mem_after = mem_after, timing = timing)
  })

  data.frame(
    software = variant,
    direction = c("read", "write"),
    data_size = size,
    time_user = c(read_result$timing["user.self"], write_result$timing["user.self"]),
    time_system = c(read_result$timing["sys.self"], write_result$timing["sys.self"]),
    time_elapsed = c(read_result$timing["elapsed"], write_result$timing["elapsed"]),
    mem_before = c(read_result$mem_before[["rss"]], write_result$mem_before[["rss"]]),
    mem_max_before = c(read_result$mem_before[["maxrss"]], write_result$mem_before[["maxrss"]]),
    mem_max_after = c(read_result$mem_after[["maxrss"]], write_result$mem_after[["maxrss"]]),
    file_size = c(NA_integer_, file.size(tmp))
  )
}
