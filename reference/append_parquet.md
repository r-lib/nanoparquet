# Append a data frame to an existing Parquet file

The schema of the data frame must be compatible with the schema of the
file.

## Usage

``` r
append_parquet(
  x,
  file,
  compression = c("snappy", "gzip", "zstd", "uncompressed"),
  encoding = NULL,
  row_groups = NULL,
  options = parquet_options()
)
```

## Arguments

- x:

  Data frame to append.

- file:

  Path to the output file.

- compression:

  Compression algorithm to use for the newly written data. See
  [`write_parquet()`](https://nanoparquet.r-lib.org/reference/write_parquet.md).

- encoding:

  Encoding to use for the newly written data. It does not have to be the
  same as the encoding of data in `file`. See
  [`write_parquet()`](https://nanoparquet.r-lib.org/reference/write_parquet.md)
  for possible values.

- row_groups:

  Row groups of the new, extended Parquet file. `append_parquet()` can
  only change the last existing row group, and if `row_groups` is
  specified, it has respect this. I.e. if the existing file has `n`
  rows, and the last row group starts at `k` (`k <= n`), then the first
  row group in `row_groups` that refers to the new data must start at
  `k` or `n+1`. (It is simpler to specify `num_rows_per_row_group` in
  `options`, see
  [`parquet_options()`](https://nanoparquet.r-lib.org/reference/parquet_options.md)
  instead of `row_groups`. Only use `row_groups` if you need complete
  control.)

- options:

  Nanoparquet options, for the new data, see
  [`parquet_options()`](https://nanoparquet.r-lib.org/reference/parquet_options.md).
  The `keep_row_groups` option also affects whether `append_parquet()`
  overwrites existing row groups in `file`.

## Warning

This function is **not** atomic! If it is interrupted, it may leave the
file in a corrupt state. To work around this create a copy of the
original file, append the new data to the copy, and then rename the new,
extended file to the original one.

## About row groups

A Parquet file may be partitioned into multiple row groups, and indeed
most large Parquet files are. `append_parquet()` is only able to update
the existing file along the row group boundaries. There are two
possibilities:

- `append_parquet()` keeps all existing row groups in `file`, and
  creates new row groups for the new data. This mode can be forced by
  the `keep_row_groups` option in `options`, see
  [`parquet_options()`](https://nanoparquet.r-lib.org/reference/parquet_options.md).

- Alternatively, `write_parquet` will overwrite the *last* row group in
  file, with its existing contents plus the (beginning of) the new data.
  This mode makes more sense if the last row group is small, because
  many small row groups are inefficient.

By default `append_parquet` chooses between the two modes automatically,
aiming to create row groups with at least `num_rows_per_row_group` (see
[`parquet_options()`](https://nanoparquet.r-lib.org/reference/parquet_options.md))
rows. You can customize this behavior with the `keep_row_groups` options
and the `row_groups` argument.

## See also

[`write_parquet()`](https://nanoparquet.r-lib.org/reference/write_parquet.md).
