# Infer Parquet schema of a data frame

Infer Parquet schema of a data frame

## Usage

``` r
infer_parquet_schema(df, options = parquet_options())
```

## Arguments

- df:

  Data frame.

- options:

  Return value of
  [`parquet_options()`](https://nanoparquet.r-lib.org/reference/parquet_options.md),
  may modify the R to Parquet type mappings.

## Value

Data frame, the inferred schema. It has the same columns as the return
value of
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/reference/read_parquet_schema.md):
`file_name`, `name`, `r_type`, `type`, `type_length`, `repetition_type`,
`converted_type`, `logical_type`, `num_children`, `scale`, `precision`,
`field_id`.

## See also

[`read_parquet_schema()`](https://nanoparquet.r-lib.org/reference/read_parquet_schema.md)
to read the schema of a Parquet file,
[`parquet_schema()`](https://nanoparquet.r-lib.org/reference/parquet_schema.md)
to create a Parquet schema from scratch.
