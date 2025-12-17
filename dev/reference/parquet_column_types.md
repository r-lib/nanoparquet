# Map between R and Parquet data types

Note that this function is now deprecated. Please use
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_schema.md)
for files, and
[`infer_parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/infer_parquet_schema.md)
for data frames.

## Usage

``` r
parquet_column_types(x, options = parquet_options())
```

## Arguments

- x:

  Path to a Parquet file, or a data frame.

- options:

  Nanoparquet options, see
  [`parquet_options()`](https://nanoparquet.r-lib.org/dev/reference/parquet_options.md).

## Value

Data frame with columns:

- `file_name`: file name.

- `name`: column name.

- `type`: (low level) Parquet data type.

- `r_type`: the R type that corresponds to the Parquet type. Might be
  `NA` if
  [`read_parquet()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet.md)
  cannot read this column. See
  [nanoparquet-types](https://nanoparquet.r-lib.org/dev/reference/nanoparquet-types.md)
  for the type mapping rules.

- `repetition_type`: whether the column in `REQUIRED` (cannot be `NA`)
  or `OPTIONAL` (may be `NA`). `REPEATED` columns are not currently
  supported by nanoparquet.

- `logical_type`: Parquet logical type in a list column. An element has
  at least an entry called `type`, and potentially additional entries,
  e.g. `bit_width`, `is_signed`, etc.

## Details

This function works two ways. It can map the R types of a data frame to
Parquet types, to see how
[`write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md)
would write out the data frame. It can also map the types of a Parquet
file to R types, to see how
[`read_parquet()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet.md)
would read the file into R.

## See also

[`read_parquet_metadata()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_metadata.md)
to read more metadata,
[`read_parquet_info()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_info.md)
for a very short summary.
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_schema.md)
for the complete Parquet schema.
[`read_parquet()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet.md),
[`write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md),
[nanoparquet-types](https://nanoparquet.r-lib.org/dev/reference/nanoparquet-types.md).
