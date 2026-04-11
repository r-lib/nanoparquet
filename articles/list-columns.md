# List columns

Parquet has a `LIST` logical type for columns whose values are
variable-length sequences of a scalar type. nanoparquet maps these to
and from R list columns, i.e. R list vectors whose elements are atomic
vectors.

> **Note**
>
> Parquet supports nested lists (lists of lists), but nanoparquet cannot
> currently read or write them. Only flat lists with scalar elements are
> supported.

## Supported element types

When *reading*, nanoparquet supports any scalar element type that it can
read in general (see \[nanoparquet-types\]), including `INT32`,
`DOUBLE`, `STRING`, `DATE`, `TIMESTAMP`, `BOOLEAN`, and so on. Nested
`LIST` elements and unsupported types such as `MAP` are not supported.

When *writing*, nanoparquet currently supports three element types:

| Parquet element type | R element type |
|:---------------------|:---------------|
| `INT32`              | `integer`      |
| `DOUBLE`             | `double`       |
| `STRING`             | `character`    |

## Writing list columns

To write a data frame with a list column, put the column in the data
frame as a list. Each element of the list must be an atomic vector of a
supported type, `NULL` (missing value), or an empty vector. The elements
may contain missing values (`NA`).

``` r
df <- data.frame(id = 1:4)
df$ints <- list(1L, c(2L, 3L), NULL, c(4L, NA_integer_, 6L))
df$doubles <- list(1.5, c(2.5, 3.5), NULL, c(4.5, NA_real_, 6.5))
df$strings <- list("a", c("b", "c"), NULL, c("d", NA_character_, "f"))
df
```

    #>   id     ints      doubles  strings
    #> 1  1        1          1.5        a
    #> 2  2     2, 3     2.5, 3.5     b, c
    #> 3  3     NULL         NULL     NULL
    #> 4  4 4, NA, 6 4.5, NA, 6.5 d, NA, f

``` r
tmp <- tempfile(fileext = ".parquet")
write_parquet(df, tmp)
```

## Reading list columns

[`read_parquet()`](https://nanoparquet.r-lib.org/reference/read_parquet.md)
reads `LIST` Parquet columns back as R list columns:

``` r
as.data.frame(read_parquet(tmp))
```

    #>   id     ints      doubles  strings
    #> 1  1        1          1.5        a
    #> 2  2     2, 3     2.5, 3.5     b, c
    #> 3  3     NULL         NULL     NULL
    #> 4  4 4, NA, 6 4.5, NA, 6.5 d, NA, f

The round-trip preserves both the structure and the missing value
pattern.

## Missing values

nanoparquet distinguishes two kinds of missingness in list columns:

- `NULL` — the entire list entry is missing. In Parquet this is an
  absent (optional) outer list value.
- `NA` within an element vector — a missing scalar inside a present
  list. In Parquet this is an absent (optional) inner element value.
- An empty vector — the list is present but has zero elements.

``` r
df2 <- data.frame(id = 1:4)
df2$x <- list(
    c(1L, 2L, 3L), # normal, three elements
    integer(0), # present, but empty
    NULL, # entirely missing
    c(4L, NA, 6L) # present, with an NA element
)
df2
```

    #>   id        x
    #> 1  1  1, 2, 3
    #> 2  2
    #> 3  3     NULL
    #> 4  4 4, NA, 6

``` r
tmp2 <- tempfile(fileext = ".parquet")
write_parquet(df2, tmp2)
as.data.frame(read_parquet(tmp2))
```

    #>   id        x
    #> 1  1  1, 2, 3
    #> 2  2
    #> 3  3     NULL
    #> 4  4 4, NA, 6

## Inspecting the schema

[`infer_parquet_schema()`](https://nanoparquet.r-lib.org/reference/infer_parquet_schema.md)
shows how nanoparquet would encode each column:

``` r
infer_parquet_schema(df)
```

    #> # A data frame: 10 × 13
    #>    file_name r_col name    r_type          type       type_length repetition_type converted_type logical_type    num_children scale precision field_id
    #>    <chr>     <int> <chr>   <chr>           <chr>            <int> <chr>           <chr>          <I<list>>              <int> <int>     <int>    <int>
    #>  1 <NA>          1 id      integer         INT32               NA REQUIRED        INT_32         <INT(32, TRUE)>           NA    NA        NA       NA
    #>  2 <NA>          2 ints    list(integer)   <NA>                NA OPTIONAL        LIST           <LIST>                     1    NA        NA       NA
    #>  3 <NA>          2 list    <NA>            <NA>                NA REPEATED        <NA>           <NULL>                     1    NA        NA       NA
    #>  4 <NA>          2 element <NA>            INT32               NA OPTIONAL        INT_32         <INT(32, TRUE)>           NA    NA        NA       NA
    #>  5 <NA>          3 doubles list(double)    <NA>                NA OPTIONAL        LIST           <LIST>                     1    NA        NA       NA
    #>  6 <NA>          3 list    <NA>            <NA>                NA REPEATED        <NA>           <NULL>                     1    NA        NA       NA
    #>  7 <NA>          3 element <NA>            DOUBLE              NA OPTIONAL        <NA>           <NULL>                    NA    NA        NA       NA
    #>  8 <NA>          4 strings list(character) <NA>                NA OPTIONAL        LIST           <LIST>                     1    NA        NA       NA
    #>  9 <NA>          4 list    <NA>            <NA>                NA REPEATED        <NA>           <NULL>                     1    NA        NA       NA
    #> 10 <NA>          4 element <NA>            BYTE_ARRAY          NA OPTIONAL        UTF8           <STRING>                  NA    NA        NA       NA

A `LIST` column occupies three rows in the schema table: the outer list
node (logical type `LIST`), the middle repeated group node (`list`), and
the leaf element node (`element`). These three rows all share the same
`r_col` value.

To inspect the schema of an existing file use
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/reference/read_parquet_schema.md):

``` r
read_parquet_schema(tmp)
```

    #> # A data frame: 11 × 14
    #>    file_name                  r_col name  r_type type  type_length repetition_type converted_type logical_type    num_children scale precision field_id children
    #>    <chr>                      <int> <chr> <chr>  <chr>       <int> <chr>           <chr>          <I<list>>              <int> <int>     <int>    <int> <list>
    #>  1 /tmp/RtmpB0ZnZV/file1f672…    NA sche… <NA>   <NA>           NA <NA>            <NA>           <NULL>                     4    NA        NA       NA <int>
    #>  2 /tmp/RtmpB0ZnZV/file1f672…     1 id    integ… INT32          NA REQUIRED        INT_32         <INT(32, TRUE)>           NA    NA        NA       NA <int>
    #>  3 /tmp/RtmpB0ZnZV/file1f672…     2 ints  list(… <NA>           NA OPTIONAL        LIST           <LIST>                     1    NA        NA       NA <int>
    #>  4 /tmp/RtmpB0ZnZV/file1f672…     2 list  <NA>   <NA>           NA REPEATED        <NA>           <NULL>                     1    NA        NA       NA <int>
    #>  5 /tmp/RtmpB0ZnZV/file1f672…     2 elem… <NA>   INT32          NA OPTIONAL        INT_32         <INT(32, TRUE)>           NA    NA        NA       NA <int>
    #>  6 /tmp/RtmpB0ZnZV/file1f672…     3 doub… list(… <NA>           NA OPTIONAL        LIST           <LIST>                     1    NA        NA       NA <int>
    #>  7 /tmp/RtmpB0ZnZV/file1f672…     3 list  <NA>   <NA>           NA REPEATED        <NA>           <NULL>                     1    NA        NA       NA <int>
    #>  8 /tmp/RtmpB0ZnZV/file1f672…     3 elem… <NA>   DOUB…          NA OPTIONAL        <NA>           <NULL>                    NA    NA        NA       NA <int>
    #>  9 /tmp/RtmpB0ZnZV/file1f672…     4 stri… list(… <NA>           NA OPTIONAL        LIST           <LIST>                     1    NA        NA       NA <int>
    #> 10 /tmp/RtmpB0ZnZV/file1f672…     4 list  <NA>   <NA>           NA REPEATED        <NA>           <NULL>                     1    NA        NA       NA <int>
    #> 11 /tmp/RtmpB0ZnZV/file1f672…     4 elem… <NA>   BYTE…          NA OPTIONAL        UTF8           <STRING>                  NA    NA        NA       NA <int>

The `r_type` column shows `list(integer)`, `list(double)`, and
`list(character)` for the three list columns.

## Explicit schema with `parquet_schema()`

nanoparquet infers the element type from the R list contents
automatically. You can also specify it explicitly with
[`parquet_schema()`](https://nanoparquet.r-lib.org/reference/parquet_schema.md)
using `list("LIST", element = <type>)`:

``` r
schema <- parquet_schema(
    id     = "INT32",
    values = list("LIST", element = "DOUBLE")
)
schema
```

    #> # A data frame: 4 × 13
    #>   file_name r_col name    r_type type   type_length repetition_type converted_type logical_type num_children scale precision field_id
    #> * <chr>     <int> <chr>   <chr>  <chr>        <int> <chr>           <chr>          <I<list>>           <int> <int>     <int>    <int>
    #> 1 <NA>          1 id      <NA>   INT32           NA <NA>            <NA>           <NULL>                 NA    NA        NA       NA
    #> 2 <NA>          2 values  <NA>   <NA>            NA OPTIONAL        LIST           <LIST>                  1    NA        NA       NA
    #> 3 <NA>          2 list    <NA>   <NA>            NA REPEATED        <NA>           <NULL>                  1    NA        NA       NA
    #> 4 <NA>          2 element <NA>   DOUBLE          NA OPTIONAL        <NA>           <NULL>                 NA    NA        NA       NA

``` r
df3 <- data.frame(id = 1:3)
df3$values <- list(c(1.1, 2.2), c(3.3), NULL)
tmp3 <- tempfile(fileext = ".parquet")
write_parquet(df3, tmp3, schema = schema)
as.data.frame(read_parquet(tmp3))
```

    #>   id   values
    #> 1  1 1.1, 2.2
    #> 2  2      3.3
    #> 3  3     NULL

## Dictionary encoding

List columns support dictionary encoding, which can reduce file size
when list elements repeat frequently:

``` r
df4 <- data.frame(id = 1:6)
df4$x <- list(
    c(1L, 2L), c(2L, 3L), c(1L, 2L),
    c(2L, 3L), c(1L, 2L), c(2L, 3L)
)
tmp4 <- tempfile(fileext = ".parquet")
write_parquet(df4, tmp4, encoding = c(x = "RLE_DICTIONARY"))
as.data.frame(read_parquet(tmp4))
```

    #>   id    x
    #> 1  1 1, 2
    #> 2  2 2, 3
    #> 3  3 1, 2
    #> 4  4 2, 3
    #> 5  5 1, 2
    #> 6  6 2, 3

## Interoperability

nanoparquet can read standard `LIST` Parquet columns written by other
tools, such as Arrow or DuckDB. The standard three-layer representation
(`OPTIONAL` or `REQUIRED` list → `REPEATED` group → `OPTIONAL` or
`REQUIRED` element) is fully supported, for both data page version 1 and
version 2.

The four combinations of optional/required outer list and
optional/required element are all handled:

| Outer list | Element    | `NULL` entry? | `NA` element? |
|:-----------|:-----------|:--------------|:--------------|
| `OPTIONAL` | `OPTIONAL` | yes           | yes           |
| `OPTIONAL` | `REQUIRED` | yes           | no            |
| `REQUIRED` | `OPTIONAL` | no            | yes           |
| `REQUIRED` | `REQUIRED` | no            | no            |

Empty vectors (e.g. `integer(0)`) are supported in all four
combinations.

nanoparquet always writes `OPTIONAL`/`OPTIONAL` lists (i.e. both the
list entry and each element may be missing), which is the most general
case and is interoperable with all Parquet readers.
