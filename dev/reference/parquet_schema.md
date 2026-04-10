# Create a Parquet schema

You can use this schema to specify how to write out a data frame to a
Parquet file with
[`write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md).

## Usage

``` r
parquet_schema(...)
```

## Arguments

- ...:

  Parquet type specifications, see below. For backwards compatibility,
  you can supply a file name here, and then `parquet_schema` behaves as
  [`read_parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_schema.md).

## Value

Data frame with the same columns as
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_schema.md):
`file_name`, `r_col`, `name`, `r_type`, `type`, `type_length`,
`repetition_type`, `converted_type`, `logical_type`, `num_children`,
`scale`, `precision`, `field_id`.

## Details

A schema is a list of potentially named type specifications. A schema is
stored in a data frame. Each (potentially named) argument of
`parquet_schema` may be a character scalar, or a list. Parameterized
types need to be specified as a list. Primitive Parquet types may be
specified as a string or a list.

## Possible types:

Special type:

- `"AUTO"`: this is not a Parquet type, but it tells
  [`write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md)
  to map the R type to Parquet automatically, using the default mapping
  rules.

Primitive Parquet types:

- `"BOOLEAN"`

- `"INT32"`

- `"INT64"`

- `"INT96"`

- `"FLOAT"`

- `"DOUBLE"`

- `"BYTE_ARRAY"`

- `"FIXED_LEN_BYTE_ARRAY"`: fixed-length byte array. It needs a
  `type_length` parameter, an integer between 0 and 2^31-1.

Parquet logical types:

- `"STRING"`

- `"ENUM"`

- `"UUID"`

- `"INTEGER"`: signed or unsigned integer. It needs a `bit_width` and an
  `is_signed` parameter. `bit_width` must be 8, 16, 32 or 64.
  `is_signed` must be `TRUE` or `FALSE`.

- `"INT"`: same as `"INTEGER"`. The Parquet documentation uses `"INT"`,
  but the actual specification uses `"INTEGER"`. Both are supported in
  nanoparquet.

- `"DECIMAL"`: decimal number of specified scale and precision. It needs
  the `precision` and `primitive_type` parameters. Also supports the
  `scale` parameter, it defaults to zero if not specified.

- `"FLOAT16"`

- `"DATE"`

- `"TIME"`: needs an `is_adjusted_utc` (`TRUE` or `FALSE`) and a `unit`
  parameter. `unit` must be `"MILLIS"`, `"MICROS"` or `"NANOS"`.

- `"TIMESTAMP"`: needs an `is_adjusted_utc` (`TRUE` or `FALSE`) and a
  `unit` parameter. `unit` must be `"MILLIS"`, `"MICROS"` or `"NANOS"`.

- `"JSON"`

- `"BSON"`

- `"LIST"`: list of some other type. It needs an `element` parameter,
  which is a type specification for the list elements. Currently
  `element` can be only `"INT32"`, `"DOUBLE"` or `"STRING"`. Also,
  currently nanoparquet always write LIST columns that are `"OPTIONAL"`,
  both for the list elements and the list itself.

Logical types `MAP`, and `UNKNOWN` are not supported currently.

Converted types are deprecated in the Parquet specification in favor of
logical types, but `parquet_schema()` accepts some converted types as a
syntactic shortcut for the corresponding logical types:

- `INT_8` mean `list("INT", bit_width = 8, is_signed = TRUE)`.

- `INT_16` mean `list("INT", bit_width = 16, is_signed = TRUE)`.

- `INT_32` mean `list("INT", bit_width = 32, is_signed = TRUE)`.

- `INT_64` mean `list("INT", bit_width = 64, is_signed = TRUE)`.

- `TIME_MICROS` means
  `list("TIME", is_adjusted_utc = TRUE, unit = "MICROS")`.

- `TIME_MILLIS` means
  `list("TIME", is_adjusted_utc = TRUE, unit = "MILLIS")`.

- `TIMESTAMP_MICROS` means
  `list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MICROS")`.

- `TIMESTAMP_MILLIS` means
  `list("TIMESTAMP", is_adjusted_utc = TRUE, unit = "MILLIS")`.

- `UINT_8` means `list("INT", bit_width = 8, is_signed = FALSE)`.

- `UINT_16` means `list("INT", bit_width = 16, is_signed = FALSE)`.

- `UINT_32` means `list("INT", bit_width = 32, is_signed = FALSE)`.

- `UINT_64` means `list("INT", bit_width = 64, is_signed = FALSE)`.

### Missing values

Each type might also have a `repetition_type` parameter, with possible
values `"REQUIRED"`, `"OPTIONAL"` or `"REPEATED"`. `"REQUIRED"` columns
do not allow missing values. Missing values are allowed in `"OPTIONAL"`
columns. `"REPEATED"` columns are currently not supported in
[`write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md).

## Examples

``` r
parquet_schema(
  c1 = "INT32",
  c2 = list("INT", bit_width = 64, is_signed = TRUE),
  c3 = list("STRING", repetition_type = "OPTIONAL"),
  l = list("LIST", element = "DOUBLE")
)
#> # A data frame: 6 × 13
#>   file_name r_col name    r_type type       type_length repetition_type
#> * <chr>     <int> <chr>   <chr>  <chr>            <int> <chr>          
#> 1 NA            1 c1      NA     INT32               NA NA             
#> 2 NA            2 c2      NA     INT64               NA NA             
#> 3 NA            3 c3      NA     BYTE_ARRAY          NA OPTIONAL       
#> 4 NA            4 l       NA     NA                  NA OPTIONAL       
#> 5 NA            4 list    NA     NA                  NA REPEATED       
#> 6 NA            4 element NA     DOUBLE              NA OPTIONAL       
#> # ℹ 6 more variables: converted_type <chr>, logical_type <I<list>>,
#> #   num_children <int>, scale <int>, precision <int>, field_id <int>
```
