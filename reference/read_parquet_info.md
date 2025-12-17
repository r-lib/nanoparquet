# Short summary of a Parquet file

Short summary of a Parquet file

## Usage

``` r
read_parquet_info(file)

parquet_info(file)
```

## Arguments

- file:

  Path to a Parquet file.

## Value

Data frame with columns:

- `file_name`: file name.

- `num_cols`: number of (leaf) columns.

- `num_rows`: number of rows.

- `num_row_groups`: number of row groups.

- `file_size`: file size in bytes.

- `parquet_version`: Parquet version.

- `created_by`: A string scalar, usually the name of the software that
  created the file. `NA` if not available.

## See also

[`read_parquet_metadata()`](https://nanoparquet.r-lib.org/reference/read_parquet_metadata.md)
to read more metadata,
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/reference/read_parquet_schema.md)
for column information.
[`read_parquet()`](https://nanoparquet.r-lib.org/reference/read_parquet.md),
[`write_parquet()`](https://nanoparquet.r-lib.org/reference/write_parquet.md),
[nanoparquet-types](https://nanoparquet.r-lib.org/reference/nanoparquet-types.md).
