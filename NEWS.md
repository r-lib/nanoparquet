# nanoparquet (development version)

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
