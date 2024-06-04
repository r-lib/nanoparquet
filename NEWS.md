# nanoparquet (development version)

* `write_parquet()` now automatically uses dictionary encoding for columns
  that have many repeated values. Only the first 10k rows are use to
  decide if dictionary will be used or not. Similarly, logical columns are
  written in RLE encoding if they contain runs of repeated values.
  `NA` values are ignored when selecting the encoding (#18).

* `write_parquet()` can now write a data frame to a memory buffer, returned
  as a raw vector, if the special `":raw:"` filename is used (#31).

* `read_parquet()` can now read Parquet files with V2 data pages (#37).

* `read_parquet()` can now read GZIP compressed Parquet files.

* `read_parquet()` can now read Parquet files with `BOOLEAN` columns in
  `RLE` encoding.

* `parquet_info()`, `parquet_metadata()` and `parquet_columns()` now work
  if the `created_by` metadata field is unset.

# nanoparquet 0.2.0

* First release on CRAN. It contains the Parquet reader from
  https://github.com/hannes/miniparquet, a Parquet writer,
  functions to read Parquet metadata, and many improvements.
