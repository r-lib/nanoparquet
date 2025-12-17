# Read a Parquet file into a data frame

Converts the contents of the named Parquet file to a R data frame.

## Usage

``` r
read_parquet(file, col_select = NULL, options = parquet_options())
```

## Arguments

- file:

  Path to a Parquet file. It may also be an R connection, in which case
  it first reads all data from the connection, writes it into a
  temporary file, then reads the temporary file, and deletes it. The
  connection might be open, it which case it must be a binary
  connection. If it is not open, then `read_parquet()` will open it and
  also close it in the end.

- col_select:

  Columns to read. It can be a numeric vector of column indices, or a
  character vector of column names. It is an error to select the same
  column multiple times. The order of the columns in the result is the
  same as the order in `col_select`.

- options:

  Nanoparquet options, see
  [`parquet_options()`](https://nanoparquet.r-lib.org/reference/parquet_options.md).

## Value

A `data.frame` with the file's contents.

## See also

See
[`write_parquet()`](https://nanoparquet.r-lib.org/reference/write_parquet.md)
to write Parquet files,
[nanoparquet-types](https://nanoparquet.r-lib.org/reference/nanoparquet-types.md)
for the R \<-\> Parquet type mapping. See
[`read_parquet_info()`](https://nanoparquet.r-lib.org/reference/read_parquet_info.md),
for general information,
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/reference/read_parquet_schema.md)
for information about the columns, and
[`read_parquet_metadata()`](https://nanoparquet.r-lib.org/reference/read_parquet_metadata.md)
for the complete metadata.

## Examples

``` r
file_name <- system.file("extdata/userdata1.parquet", package = "nanoparquet")
parquet_df <- nanoparquet::read_parquet(file_name)
print(str(parquet_df))
#> Classes ‘tbl’ and 'data.frame':  1000 obs. of  13 variables:
#>  $ registration: POSIXct, format: "2016-02-03 07:55:29" ...
#>  $ id          : int  1 2 3 4 5 6 7 8 9 10 ...
#>  $ first_name  : chr  "Amanda" "Albert" "Evelyn" "Denise" ...
#>  $ last_name   : chr  "Jordan" "Freeman" "Morgan" "Riley" ...
#>  $ email       : chr  "ajordan0@com.com" "afreeman1@is.gd" "emorgan2@altervista.org" "driley3@gmpg.org" ...
#>  $ gender      : Factor w/ 2 levels "Female","Male": 1 2 1 1 NA 1 2 2 2 1 ...
#>  $ ip_address  : chr  "1.197.201.2" "218.111.175.34" "7.161.136.94" "140.35.109.83" ...
#>  $ cc          : chr  "6759521864920116" NA "6767119071901597" "3576031598965625" ...
#>  $ country     : chr  "Indonesia" "Canada" "Russia" "China" ...
#>  $ birthdate   : Date, format: "1971-03-08" ...
#>  $ salary      : num  49757 150280 144973 90263 NA ...
#>  $ title       : chr  "Internal Auditor" "Accountant IV" "Structural Engineer" "Senior Cost Accountant" ...
#>  $ comments    : chr  "1E+02" NA NA NA ...
#> NULL
```
