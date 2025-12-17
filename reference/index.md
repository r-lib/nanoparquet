# Package index

## Parquet schemas and type mappings

How nanoparquet maps R data types to Parquet data types and vice versa.

- [`infer_parquet_schema()`](https://nanoparquet.r-lib.org/reference/infer_parquet_schema.md)
  : Infer Parquet schema of a data frame
- [`nanoparquet-types`](https://nanoparquet.r-lib.org/reference/nanoparquet-types.md)
  : nanoparquet's type maps
- [`parquet-encodings`](https://nanoparquet.r-lib.org/reference/parquet-encodings.md)
  : Parquet encodings
- [`parquet_schema()`](https://nanoparquet.r-lib.org/reference/parquet_schema.md)
  : Create a Parquet schema

## Read Parquet files

- [`read_parquet()`](https://nanoparquet.r-lib.org/reference/read_parquet.md)
  : Read a Parquet file into a data frame

## Write Parquet files

- [`append_parquet()`](https://nanoparquet.r-lib.org/reference/append_parquet.md)
  : Append a data frame to an existing Parquet file
- [`write_parquet()`](https://nanoparquet.r-lib.org/reference/write_parquet.md)
  : Write a data frame to a Parquet file

## Extract Parquet metadata

- [`read_parquet_info()`](https://nanoparquet.r-lib.org/reference/read_parquet_info.md)
  [`parquet_info()`](https://nanoparquet.r-lib.org/reference/read_parquet_info.md)
  : Short summary of a Parquet file
- [`read_parquet_metadata()`](https://nanoparquet.r-lib.org/reference/read_parquet_metadata.md)
  [`parquet_metadata()`](https://nanoparquet.r-lib.org/reference/read_parquet_metadata.md)
  : Read the metadata of a Parquet file
- [`read_parquet_schema()`](https://nanoparquet.r-lib.org/reference/read_parquet_schema.md)
  : Read the schema of a Parquet file

## Nanoparquet options

- [`parquet_options()`](https://nanoparquet.r-lib.org/reference/parquet_options.md)
  : Nanoparquet options

## Debugging Parquet files

These functions are useful for debugging possibly broken Parquet files
and for nanoparquet developers.

- [`read_parquet_pages()`](https://nanoparquet.r-lib.org/reference/read_parquet_pages.md)
  : Metadata of all pages of a Parquet file
- [`read_parquet_page()`](https://nanoparquet.r-lib.org/reference/read_parquet_page.md)
  : Read a page from a Parquet file
