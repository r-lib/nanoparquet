# Read the metadata of a Parquet file

This function should work on all files, even if
[`read_parquet()`](https://nanoparquet.r-lib.org/reference/read_parquet.md)
is unable to read them, because of an unsupported schema, encoding,
compression or other reason.

## Usage

``` r
read_parquet_metadata(file, options = parquet_options())

parquet_metadata(file)
```

## Arguments

- file:

  Path to a Parquet file.

- options:

  Options that potentially alter the default Parquet to R type mappings,
  see
  [`parquet_options()`](https://nanoparquet.r-lib.org/reference/parquet_options.md).

## Value

A named list with entries:

- `file_meta_data`: a data frame with file meta data:

  - `file_name`: file name.

  - `version`: Parquet version, an integer.

  - `num_rows`: total number of rows.

  - `key_value_metadata`: list column of a data frames with two
    character columns called `key` and `value`. This is the key-value
    metadata of the file. Arrow stores its schema here.

  - `created_by`: A string scalar, usually the name of the software that
    created the file.

- `schema`: data frame, the schema of the file. It has one row for each
  node (inner node or leaf node). For flat files this means one root
  node (inner node), always the first one, and then one row for each
  "real" column. For nested schemas, the rows are in depth-first search
  order. Most important columns are:

  - `file_name`: file name.

  - `name`: column name.

  - `r_type`: the R type that corresponds to the Parquet type. Might be
    `NA` if
    [`read_parquet()`](https://nanoparquet.r-lib.org/reference/read_parquet.md)
    cannot read this column. See
    [nanoparquet-types](https://nanoparquet.r-lib.org/reference/nanoparquet-types.md)
    for the type mapping rules.

  - `r_type`:

  - `type`: data type. One of the low level data types.

  - `type_length`: length for fixed length byte arrays.

  - `repettion_type`: character, one of `REQUIRED`, `OPTIONAL` or
    `REPEATED`.

  - `logical_type`: a list column, the logical types of the columns. An
    element has at least an entry called `type`, and potentially
    additional entries, e.g. `bit_width`, `is_signed`, etc.

  - `num_children`: number of child nodes. Should be a non-negative
    integer for the root node, and `NA` for a leaf node.

- `$row_groups`: a data frame, information about the row groups. Some
  important columns:

  - `file_name`: file name.

  - `id`: row group id, integer from zero to number of row groups minus
    one.

  - `total_byte_size`: total uncompressed size of all column data.

  - `num_rows`: number of rows.

  - `file_offset`: where the row group starts in the file. This is
    optional, so it might be `NA`.

  - `total_compressed_size`: total byte size of all compressed (and
    potentially encrypted) column data in this row group. This is
    optional, so it might be `NA`.

  - `ordinal`: ordinal position of the row group in the file, starting
    from zero. This is optional, so it might be `NA`. If `NA`, then the
    order of the row groups is as they appear in the metadata.

- `$column_chunks`: a data frame, information about all column chunks,
  across all row groups. Some important columns:

  - `file_name`: file name.

  - `row_group`: which row group this chunk belongs to.

  - `column`: which leaf column this chunks belongs to. The order is the
    same as in `$schema`, but only leaf columns (i.e. columns with `NA`
    children) are counted.

  - `file_path`: which file the chunk is stored in. `NA` means the same
    file.

  - `file_offset`: where the column chunk begins in the file.

  - `type`: low level parquet data type.

  - `encodings`: encodings used to store this chunk. It is a list column
    of character vectors of encoding names. Current possible encodings:
    "PLAIN", "GROUP_VAR_INT", "PLAIN_DICTIONARY", "RLE", "BIT_PACKED",
    "DELTA_BINARY_PACKED", "DELTA_LENGTH_BYTE_ARRAY",
    "DELTA_BYTE_ARRAY", "RLE_DICTIONARY", "BYTE_STREAM_SPLIT".

  - `path_in_scema`: list column of character vectors. It is simply the
    path from the root node. It is simply the column name for flat
    schemas.

  - `codec`: compression codec used for the column chunk. Possible
    values are: "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "BROTLI",
    "LZ4", "ZSTD".

  - `num_values`: number of values in this column chunk.

  - `total_uncompressed_size`: total uncompressed size in bytes.

  - `total_compressed_size`: total compressed size in bytes.

  - `data_page_offset`: absolute position of the first data page of the
    column chunk in the file.

  - `index_page_offset`: absolute position of the first index page of
    the column chunk in the file, or `NA` if there are no index pages.

  - `dictionary_page_offset`: absolute position of the first dictionary
    page of the column chunk in the file, or `NA` if there are no
    dictionary pages.

  - `null_count`: the number of missing values in the column chunk. It
    may be `NA`.

  - `min_value`: list column of raw vectors, the minimum value of the
    column, in binary. If `NULL`, then then it is not specified. This
    column is experimental.

  - `max_value`: list column of raw vectors, the maximum value of the
    column, in binary. If `NULL`, then then it is not specified. This
    column is experimental.

  - `is_min_value_exact`: whether the minimum value is an actual value
    of a column, or a bound. It may be `NA`.

  - `is_max_value_exact`: whether the maximum value is an actual value
    of a column, or a bound. It may be `NA`.

## See also

[`read_parquet_info()`](https://nanoparquet.r-lib.org/reference/read_parquet_info.md)
for a much shorter summary.
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/reference/read_parquet_schema.md)
for column information.
[`read_parquet()`](https://nanoparquet.r-lib.org/reference/read_parquet.md)
to read,
[`write_parquet()`](https://nanoparquet.r-lib.org/reference/write_parquet.md)
to write Parquet files,
[nanoparquet-types](https://nanoparquet.r-lib.org/reference/nanoparquet-types.md)
for the R \<-\> Parquet type mappings.

## Examples

``` r
file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
nanoparquet::read_parquet_metadata(file_name)
#> $file_meta_data
#> # A data frame: 1 × 5
#>   file_name              version num_rows key_value_metadata created_by
#>   <chr>                    <int>    <dbl> <I<list>>          <chr>     
#> 1 /home/runner/work/_te…       1     1000 <tbl [1 × 2]>      https://g…
#> 
#> $schema
#> # A data frame: 14 × 12
#>    file_name             name  r_type type  type_length repetition_type
#>    <chr>                 <chr> <chr>  <chr>       <int> <chr>          
#>  1 /home/runner/work/_t… sche… NA     NA             NA NA             
#>  2 /home/runner/work/_t… regi… POSIX… INT64          NA REQUIRED       
#>  3 /home/runner/work/_t… id    integ… INT32          NA REQUIRED       
#>  4 /home/runner/work/_t… firs… chara… BYTE…          NA OPTIONAL       
#>  5 /home/runner/work/_t… last… chara… BYTE…          NA REQUIRED       
#>  6 /home/runner/work/_t… email factor BYTE…          NA OPTIONAL       
#>  7 /home/runner/work/_t… gend… chara… BYTE…          NA OPTIONAL       
#>  8 /home/runner/work/_t… ip_a… chara… BYTE…          NA REQUIRED       
#>  9 /home/runner/work/_t… cc    chara… BYTE…          NA OPTIONAL       
#> 10 /home/runner/work/_t… coun… chara… BYTE…          NA REQUIRED       
#> 11 /home/runner/work/_t… birt… Date   INT32          NA OPTIONAL       
#> 12 /home/runner/work/_t… sala… double DOUB…          NA OPTIONAL       
#> 13 /home/runner/work/_t… title chara… BYTE…          NA OPTIONAL       
#> 14 /home/runner/work/_t… comm… chara… BYTE…          NA OPTIONAL       
#> # ℹ 6 more variables: converted_type <chr>, logical_type <I<list>>,
#> #   num_children <int>, scale <int>, precision <int>, field_id <int>
#> 
#> $row_groups
#> # A data frame: 1 × 7
#>   file_name                     id total_byte_size num_rows file_offset
#>   <chr>                      <int>           <dbl>    <dbl>       <dbl>
#> 1 /home/runner/work/_temp/L…     0           71427     1000          NA
#> # ℹ 2 more variables: total_compressed_size <dbl>, ordinal <int>
#> 
#> $column_chunks
#> # A data frame: 13 × 24
#>    file_name row_group column file_path file_offset offset_index_offset
#>    <chr>         <int>  <int> <chr>           <dbl>               <dbl>
#>  1 /home/ru…         0      0 NA                  4                  NA
#>  2 /home/ru…         0      1 NA               6741                  NA
#>  3 /home/ru…         0      2 NA              12259                  NA
#>  4 /home/ru…         0      3 NA              15211                  NA
#>  5 /home/ru…         0      4 NA              16239                  NA
#>  6 /home/ru…         0      5 NA              31759                  NA
#>  7 /home/ru…         0      6 NA              32031                  NA
#>  8 /home/ru…         0      7 NA              42952                  NA
#>  9 /home/ru…         0      8 NA              55009                  NA
#> 10 /home/ru…         0      9 NA              55925                  NA
#> 11 /home/ru…         0     10 NA              59312                  NA
#> 12 /home/ru…         0     11 NA              67026                  NA
#> 13 /home/ru…         0     12 NA              71089                  NA
#> # ℹ 18 more variables: offset_index_length <int>,
#> #   column_index_offset <dbl>, column_index_length <int>, type <chr>,
#> #   encodings <I<list>>, path_in_schema <I<list>>, codec <chr>,
#> #   num_values <dbl>, total_uncompressed_size <dbl>,
#> #   total_compressed_size <dbl>, data_page_offset <dbl>,
#> #   index_page_offset <dbl>, dictionary_page_offset <dbl>,
#> #   null_count <dbl>, min_value <I<list>>, max_value <I<list>>, …
#> 
```
