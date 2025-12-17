# Read the schema of a Parquet file

This function should work on all files, even if
[`read_parquet()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet.md)
is unable to read them, because of an unsupported schema, encoding,
compression or other reason.

## Usage

``` r
read_parquet_schema(file, options = parquet_options())
```

## Arguments

- file:

  Path to a Parquet file.

- options:

  Return value of
  [`parquet_options()`](https://nanoparquet.r-lib.org/dev/reference/parquet_options.md),
  options that potentially modify the Parquet to R type mappings.

## Value

    Data frame, the schema of the file. It has one row for
    each node (inner node or leaf node). For flat files this means one
    root node (inner node), always the first one, and then one row for
    each "real" column. For nested schemas, the rows are in depth-first
    search order. Most important columns are:
    - `file_name`: file name.
    - `name`: column name.
    - `r_type`: the R type that corresponds to the Parquet type.
      Might be `NA` if [read_parquet()] cannot read this column. See
      [nanoparquet-types] for the type mapping rules.
    - `type`: data type. One of the low level data types.
    - `type_length`: length for fixed length byte arrays.
    - `repettion_type`: character, one of `REQUIRED`, `OPTIONAL` or
      `REPEATED`.
    - `logical_type`: a list column, the logical types of the columns.
      An element has at least an entry called `type`, and potentially
      additional entries, e.g. `bit_width`, `is_signed`, etc.
    - `num_children`: number of child nodes. Should be a non-negative
      integer for the root node, and `NA` for a leaf node.

## See also

[`read_parquet_metadata()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_metadata.md)
to read more metadata,
[`read_parquet_info()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_info.md)
to show only basic information.
[`read_parquet()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet.md),
[`write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md),
[nanoparquet-types](https://nanoparquet.r-lib.org/dev/reference/nanoparquet-types.md).
