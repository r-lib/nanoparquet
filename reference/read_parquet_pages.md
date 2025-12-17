# Metadata of all pages of a Parquet file

Metadata of all pages of a Parquet file

## Usage

``` r
read_parquet_pages(file)
```

## Arguments

- file:

  Path to a Parquet file.

## Value

Data frame with columns:

- `file_name`: file name.

- `row_group`: id of the row group the page belongs to, an integer
  between 0 and the number of row groups minus one.

- `column`: id of the column. An integer between the number of leaf
  columns minus one. Note that only leaf columns are considered, as
  non-leaf columns do not have any pages.

- `page_type`: `DATA_PAGE`, `INDEX_PAGE`, `DICTIONARY_PAGE` or
  `DATA_PAGE_V2`.

- `page_header_offset`: offset of the data page (its header) in the
  file.

- `uncompressed_page_size`: does not include the page header, as per
  Parquet spec.

- `compressed_page_size`: without the page header.

- `crc`: integer, checksum, if present in the file, can be `NA`.

- `num_values`: number of data values in this page, include `NULL` (`NA`
  in R) values.

- `encoding`: encoding of the page, current possible encodings: "PLAIN",
  "GROUP_VAR_INT", "PLAIN_DICTIONARY", "RLE", "BIT_PACKED",
  "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY",
  "RLE_DICTIONARY", "BYTE_STREAM_SPLIT".

- `definition_level_encoding`: encoding of the definition levels, see
  `encoding` for possible values. This can be missing in V2 data pages,
  where they are always RLE encoded.

- `repetition_level_encoding`: encoding of the repetition levels, see
  `encoding` for possible values. This can be missing in V2 data pages,
  where they are always RLE encoded.

- `data_offset`: offset of the actual data in the file.

- `page_header_length`: size of the page header, in bytes.

## Details

Reading all the page headers might be slow for large files, especially
if the file has many small pages.

## See also

[`read_parquet_page()`](https://nanoparquet.r-lib.org/reference/read_parquet_page.md)
to read a page.

## Examples

``` r
file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
nanoparquet:::read_parquet_pages(file_name)
#> # A data frame: 19 × 14
#>    file_name              row_group column page_type page_header_offset
#>    <chr>                      <int>  <int> <chr>                  <dbl>
#>  1 /home/runner/work/_te…         0      0 DATA_PAGE                  4
#>  2 /home/runner/work/_te…         0      1 DATA_PAGE               6741
#>  3 /home/runner/work/_te…         0      2 DICTIONA…              10766
#>  4 /home/runner/work/_te…         0      2 DATA_PAGE              12259
#>  5 /home/runner/work/_te…         0      3 DICTIONA…              13334
#>  6 /home/runner/work/_te…         0      3 DATA_PAGE              15211
#>  7 /home/runner/work/_te…         0      4 DATA_PAGE              16239
#>  8 /home/runner/work/_te…         0      5 DICTIONA…              31726
#>  9 /home/runner/work/_te…         0      5 DATA_PAGE              31759
#> 10 /home/runner/work/_te…         0      6 DATA_PAGE              32031
#> 11 /home/runner/work/_te…         0      7 DATA_PAGE              42952
#> 12 /home/runner/work/_te…         0      8 DICTIONA…              53749
#> 13 /home/runner/work/_te…         0      8 DATA_PAGE              55009
#> 14 /home/runner/work/_te…         0      9 DATA_PAGE              55925
#> 15 /home/runner/work/_te…         0     10 DATA_PAGE              59312
#> 16 /home/runner/work/_te…         0     11 DICTIONA…              65063
#> 17 /home/runner/work/_te…         0     11 DATA_PAGE              67026
#> 18 /home/runner/work/_te…         0     12 DICTIONA…              68019
#> 19 /home/runner/work/_te…         0     12 DATA_PAGE              71089
#> # ℹ 9 more variables: uncompressed_page_size <int>,
#> #   compressed_page_size <int>, crc <int>, num_values <int>,
#> #   encoding <chr>, definition_level_encoding <chr>,
#> #   repetition_level_encoding <chr>, data_offset <dbl>,
#> #   page_header_length <int>
```
