# Read a page from a Parquet file

Read a page from a Parquet file

## Usage

``` r
read_parquet_page(file, offset)
```

## Arguments

- file:

  Path to a Parquet file.

- offset:

  Integer offset of the start of the page in the file. See
  [`read_parquet_pages()`](https://nanoparquet.r-lib.org/reference/read_parquet_pages.md)
  for a list of all pages and their offsets.

## Value

Named list. Many entries correspond to the columns of the result of
[`read_parquet_pages()`](https://nanoparquet.r-lib.org/reference/read_parquet_pages.md).
Additional entries are:

- `codec`: compression codec. Possible values:

- `has_repetition_levels`: whether the page has repetition levels.

- `has_definition_levels`: whether the page has definition levels.

- `schema_column`: which schema column the page corresponds to. Note
  that only leaf columns have pages.

- `data_type`: low level Parquet data type. Possible values:

- `repetition_type`: whether the column the page belongs to is
  `REQUIRED`, `OPTIONAL` or `REPEATED`.

- `page_header`: the bytes of the page header in a raw vector.

- `num_null`: number of missing (`NA`) values. Only set in V2 data
  pages.

- `num_rows`: this is the same as `num_values` for flat tables, i.e.
  files without repetition levels.

- `compressed_data`: the data of the page in a raw vector. It includes
  repetition and definition levels, if any.

- `data`: the uncompressed data, if nanoparquet supports the compression
  codec of the file (GZIP and SNAPPY at the time of writing), or if the
  file is not compressed. In the latter case it is the same as
  `compressed_data`.

## See also

[`read_parquet_pages()`](https://nanoparquet.r-lib.org/reference/read_parquet_pages.md)
for a summary of all pages.

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
options(max.print = 100)  # otherwise long raw vector
nanoparquet:::read_parquet_page(file_name, 4L)
#> $page_type
#> [1] "DATA_PAGE"
#> 
#> $row_group
#> [1] 0
#> 
#> $column
#> [1] 0
#> 
#> $page_header_offset
#> [1] 4
#> 
#> $data_page_offset
#> [1] 24
#> 
#> $page_header_length
#> [1] 20
#> 
#> $compressed_page_size
#> [1] 6717
#> 
#> $uncompressed_page_size
#> [1] 8000
#> 
#> $codec
#> [1] "SNAPPY"
#> 
#> $num_values
#> [1] 1000
#> 
#> $encoding
#> [1] "PLAIN"
#> 
#> $definition_level_encoding
#> [1] "PLAIN"
#> 
#> $repetition_level_encoding
#> [1] "PLAIN"
#> 
#> $has_repetition_levels
#> [1] FALSE
#> 
#> $has_definition_levels
#> [1] FALSE
#> 
#> $schema_column
#> [1] 1
#> 
#> $data_type
#> [1] "INT64"
#> 
#> $repetition_type
#> [1] "REQUIRED"
#> 
#> $page_header
#>  [1] 15 00 15 80 7d 15 fa 68 2c 15 d0 0f 15 00 15 00 15 00 00 00
#> 
#> $data
#>   [1] 40 be 0c f1 d8 2a 05 00 c0 86 e0 9a e0 2a 05 00 c0 28 33 45 d3 2a
#>  [23] 05 00 40 2b 96 ce d2 2a 05 00 c0 9c 33 91 d6 2a 05 00 80 a2 54 7b
#>  [45] d8 2a 05 00 00 59 b2 77 d9 2a 05 00 80 ee 7d fc d7 2a 05 00 40 cf
#>  [67] 71 8d d5 2a 05 00 c0 bc 7b cd e1 2a 05 00 80 e4 da 72 d2 2a 05 00
#>  [89] 80 30 4d 73 e1 2a 05 00 40 fe a4 0f
#>  [ reached 'max' / getOption("max.print") -- omitted 7900 entries ]
#> 
#> $definition_levels_byte_length
#> [1] NA
#> 
#> $repetition_levels_byte_length
#> [1] NA
#> 
#> $num_nulls
#> [1] NA
#> 
#> $num_rows
#> [1] NA
#> 
#> $compressed_data
#>   [1] c0 3e 30 40 be 0c f1 d8 2a 05 00 c0 86 e0 9a e0 01 08 2c 28 33 45
#>  [23] d3 2a 05 00 40 2b 96 ce d2 01 10 28 9c 33 91 d6 2a 05 00 80 a2 54
#>  [45] 7b 01 28 10 00 59 b2 77 d9 01 10 0c ee 7d fc d7 01 28 0c cf 71 8d
#>  [67] d5 01 28 0c bc 7b cd e1 01 18 08 e4 da 72 01 38 0c 80 30 4d 73 01
#>  [89] 10 30 40 fe a4 0f e2 2a 05 00 00 eb
#>  [ reached 'max' / getOption("max.print") -- omitted 6617 entries ]
#> 
```
