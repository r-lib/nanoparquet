# Write a data frame to a Parquet file

Writes the contents of an R data frame into a Parquet file.

## Usage

``` r
write_parquet(
  x,
  file,
  schema = NULL,
  compression = c("snappy", "gzip", "zstd", "uncompressed"),
  encoding = NULL,
  metadata = NULL,
  row_groups = NULL,
  options = parquet_options()
)
```

## Arguments

- x:

  Data frame to write.

- file:

  Path to the output file. If this is the string `":raw:"`, then the
  data frame is written to a memory buffer, and the memory buffer is
  returned as a raw vector. If `":stdout:"`, then it is written to the
  standard output. (When writing to the standard output, special care is
  needed to make sure no regular R output gets mixed up with the Parquet
  bytes!)

- schema:

  Parquet schema. Specify a schema to tweak the default nanoparquet R
  -\> Parquet type mappings. Use
  [`parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/parquet_schema.md)
  to create a schema that you can use here, or
  [`read_parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_schema.md)
  to use the schema of a Parquet file.

- compression:

  Compression algorithm to use. Currently `"snappy"` (the default),
  `"gzip"`, `"zstd"`, and `"uncompressed"` are supported.

- encoding:

  Encoding to use. Possible values:

  - If `NULL`, the appropriate encoding is selected automatically: `RLE`
    or `PLAIN` for `BOOLEAN` columns, `RLE_DICTIONARY` for other columns
    with many repeated values, and `PLAIN` otherwise.

  - If It is a single (unnamed) character string, then it'll be used for
    all columns.

  - If it is an unnamed character vector of encoding names of the same
    length as the number of columns in the data frame, then those
    encodings will be used for each column.

  - If it is a named character vector, then the named must be unique and
    each name must match a column name, to specify the encoding of that
    column. The special empty name (`""`) applies to the rest of the
    columns. If there is no empty name, the rest of the columns will use
    the default encoding.

  If `NA_character_` is specified for a column, the default encoding is
  used for the column.

  If a specified encoding is invalid for a certain column type, or
  nanoparquet does not implement it, `write_parquet()` throws an error.

  Currently `write_parquet()` supports the following encodings:

  - `PLAIN` for all column types,

  - `PLAIN_DICTIONARY` and `RLE_DICTIONARY` for all column types,

  - `RLE` for BOOLEAN columns.

  See
  [parquet-encodings](https://nanoparquet.r-lib.org/dev/reference/parquet-encodings.md)
  for more about encodings.

- metadata:

  Additional key-value metadata to add to the file. This must be a named
  character vector, or a data frame with columns character columns
  called `key` and `value`.

- row_groups:

  Row groups of the Parquet file. If `NULL`, then the
  `num_rows_per_row_group` option is used from the `options` argument,
  see
  [`parquet_options()`](https://nanoparquet.r-lib.org/dev/reference/parquet_options.md).
  Otherwise it must be an integer vector, specifying the starts of the
  row groups.

- options:

  Nanoparquet options, see
  [`parquet_options()`](https://nanoparquet.r-lib.org/dev/reference/parquet_options.md).

## Value

`NULL`, unless `file` is `":raw:"`, in which case the Parquet file is
returned as a raw vector.

## Details

`write_parquet()` converts string columns to UTF-8 encoding by calling
[`base::enc2utf8()`](https://rdrr.io/r/base/Encoding.html). It does the
same for factor levels.

## See also

[`read_parquet_metadata()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_metadata.md),
[`read_parquet()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet.md).

## Examples

``` r
if (FALSE) {
# add row names as a column, because `write_parquet()` ignores them.
mtcars2 <- cbind(name = rownames(mtcars), mtcars)
write_parquet(mtcars2, "mtcars.parquet")
}
```
