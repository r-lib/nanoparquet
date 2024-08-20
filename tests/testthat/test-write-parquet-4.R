test_that("errors", {
  options <- parquet_options()
  expect_snapshot(error = TRUE, {
   .Call(
      nanoparquet_write, mtcars, tempfile(), dim(mtcars), 0L,
      list(character(), character()), rep(FALSE, ncol(mtcars)),
      options, map_schema_to_df(NULL, mtcars), rep(10L, ncol(mtcars)),
      sys.call()
   )
  })
})

test_that("force PLAIN / RLE", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  d <- data.frame(d1 = 1:100, d2 = rep(1L, 100))

  withr::local_envvar(NANOPARQUET_FORCE_PLAIN = "true")
  write_parquet(d, tmp)
  expect_snapshot({
    read_parquet_pages(tmp)[["encoding"]]
  })

  withr::local_envvar(
    NANOPARQUET_FORCE_PLAIN = NA_character_,
    NANOPARQUET_FORCE_RLE = "true"
  )
  write_parquet(d, tmp)
  expect_snapshot({
    read_parquet_pages(tmp)[["encoding"]]
  })
})

test_that("cutoff for dict encoding decision", {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  d <- data.frame(d1 = c(1:10000, rep(1L, 100000)))
  write_parquet(d, tmp)
  expect_snapshot({
    read_parquet_pages(tmp)[["encoding"]]
  })
})
