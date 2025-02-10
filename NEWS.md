# nanoparquet 0.4.1

* `write_parquet()` now correctly converts double `Date` columns
  to integer columns (@eitsupi, #116).

* `read_parquet()` now correctly reads `FLOAT` columns from files with
  multiple row groups.

* `read_parquet()` now correctly reads Parquet files that have column
  chunks with both dictionary encoded and not dictionary encoded
  pages (#110).

# nanoparquet 0.4.0

* API changes:

  - `parquet_schema()` is now called `read_parquet_schema()`. The new
    `parquet_schema()` function falls back to `read_parquet_schema()` if
    it is called with a single string argument, with a warning.

  - `parquet_info()` is now called `read_parquet_info()`. `parquet_info(`
    still works for now, with a warning.

  - `parquet_metadata()` is now called `read_parquet_metadata()`.
    `parquet_metadata()` still works, with a warning.

  - `parquet_column_types()` is now deprecated, and issues a warning. Use
    `read_parquet_schema()` or the new `infer_parquet_schema()` function
    instead.

* Other improvements:

  - The new `parquet_schema()` function creates a Parquet schema from
    scratch. You can use this schema as the new `schema` argument of
    `write_parquet()`, to specify how the columns of a data frame should
    be mapped to Parquet types.

  - New `append_parquet()` function to append a data frame to an
    existing Parquet file.

  - New `col_select` argument for `read_parquet()` to read a subset of
    columns from a Parquet file.

  - `write_parquet()` can now write multiple row groups. By default it puts
    at most 10 million rows into a single row group. You can choose the
    row groups manually with the `row_groups` argument.

  - `write_parquet()` now writes minimum and maximum values per row group
    for most types. See `?parquet_options()` for turning this off. It also
    writes out the number of non-missing values.

  - Newly supported type conversions in `write_parquet()` via the
    schema argument:

    - `integer` to `INT64`,
    - `integer` to `INT96`,
    - `double` to `INT96`,
    - `double` to `FLOAT`,
    - `character` to `BYTE_ARRAY`,
    - `character` to `FIXED_LEN_BYTE_ARRAY`,
    - `character` to `ENUM`,
    - `factor` to `ENUM`.
    - `integer` to `DECIMAL`, `INT32`,
    - `integer` to `DECIMAL`, `INT64`,
    - `double` to `DECIMAL`, `INT32`,
    - `double` to `DECIMAL`, `INT64`,
    - `integer` to `INT(8, *)`, `INT(16, *)`, `INT(32, signed)`,
    - `double` to `INT(*, *)`,
    - `character` to `UUID`,
    - `double` to `FLOAT16`,
    - `list` of `raw` vectors to `BYTE_ARRAY`,
    - `list` of `raw` vectors to `FIXED_LEN_BYTE_ARRAY`.

* `write_parquet()` can now write version 2 data pages. The default is
  still version 1, but it might change in the future.

* `write_parquet(file = ":raw:")` now works correctly for larger data
  frames (#77).

* New `compression_level` option to select the compression level
  manually. See `?parquet_options` for details. (#91).

* `read_parquet()` can now read from an R connection (#71).

* `read_parquet()` now reads `DECIMAL` values correctly from `INT32`
  and `INT64` columns if their `scale` is not zero.

* `read_parquet()` now reads `JSON` columns as character vectors, as
  documented.

* `read_parquet()` now reads the `FLOAT16` logical type as a real (double)
  vector.

* The `class` argument of `parquet_options()` and the `nanoparquet.class`
  option now work again (#104).

# nanoparquet 0.3.1

* This version fixes a `write_parquet()` crash (#73).

# nanoparquet 0.3.0

* `read_parquet()` type mapping changes:
  - The `STRING` logical type and the `UTF8` converted type are still read
    as a character vector, but `BYTE_ARRAY` types without a converted or
    logical types are not any more, and are read as a list of raw vectors.
    Missing values are indicated as `NULL` values.
  - The `DECIMAL` converted type is read as a `REALSXP` now, even if its
    type is `FIXED_LEN_BYTE_ARRAY`. (Not just if it is `BYTE_ARRAY`).
  - The `UUID` logical type is now read as a character vector, formatted as
    `00112233-4455-6677-8899-aabbccddeeff`.
  - `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` types without logical or
    converted types; or with unsupported ones: `FLOAT16`, `INTERVAL`; are
    now read into a list of raw vectors. Missing values are denoted by
    `NULL`.

* `write_parquet()` now automatically uses dictionary encoding for columns
  that have many repeated values. Only the first 10k rows are used to
  decide if dictionary will be used or not. Similarly, logical columns are
  written in RLE encoding if they contain runs of repeated values.
  `NA` values are ignored when selecting the encoding (#18).

* `write_parquet()` can now write a data frame to a memory buffer, returned
  as a raw vector, if the special `":raw:"` filename is used (#31).

* `read_parquet()` can now read Parquet files with V2 data pages (#37).

* Both `read_parquet()` and `write_parquet()` now support GZIP and ZSTD
  compressed Parquet files.

* `read_parquet()` now supports the `RLE` encoding for `BOOLEAN` columns
  and also supports the `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`,
  `DELTA_BYTE_ARRAY` and `BYTE_STREAM_SPLIT` encodings.

* The `parquet_columns()` function is now called `parquet_column_types()`
  and it can now map the column types of a data frame to Parquet types.

* `parquet_info()`, `parquet_metadata()` and `parquet_column_types()` now
  work if the `created_by` metadata field is unset.

* New `parquet_options()` function that you can use to set nanoparquet
  options for a single `read_parquet()` or `write_parquet()` call.

# nanoparquet 0.2.0

* First release on CRAN. It contains the Parquet reader from
  https://github.com/hannes/miniparquet, a Parquet writer,
  functions to read Parquet metadata, and many improvements.
