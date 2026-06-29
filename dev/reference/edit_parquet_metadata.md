# Edit the key-value metadata of a Parquet file

Modify the file-level key-value metadata of a Parquet file in place,
without copying any row-group data. Individual entries can be added,
updated or removed.

## Usage

``` r
edit_parquet_metadata(file, metadata)
```

## Arguments

- file:

  Path to a Parquet file.

- metadata:

  Key-value pairs to add, update or remove. Accepted forms:

  - A **named character vector** – each name is a key and each element
    is its new value. Set a value to `NA` to **remove** that key.

  - A **data frame with two columns** – the first column contains keys
    and the second contains values. An `NA` value removes that key.

  - `NULL` – removes **all** key-value metadata from the file.

  Keys that are not mentioned in `metadata` are left unchanged.

## Value

`NULL`, invisibly. Called for its side effect.

## Warning

This function is **not** atomic! If it is interrupted it may leave the
file in a corrupt state. To work around this create a copy of the
original file, edit the copy, and then rename the copy to the original.

## See also

[`read_parquet_metadata()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_metadata.md)
to inspect metadata.
[`write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md)
for the `metadata` argument when writing.
[`append_parquet()`](https://nanoparquet.r-lib.org/dev/reference/append_parquet.md)
to append rows without copying.

## Examples

``` r
tmp <- tempfile(fileext = ".parquet")
write_parquet(data.frame(x = 1:5), tmp)

# Add a new entry
edit_parquet_metadata(tmp, c(created_by_user = "Alice"))
read_parquet_metadata(tmp)$file_meta_data$key_value_metadata
#> [[1]]
#> # A data frame: 2 × 2
#>   key             value                                                
#>   <chr>           <chr>                                                
#> 1 ARROW:schema    /////3gAAAAQAAAAAAAKAA4ADAALAAQACgAAABQAAAAAAAABBAAK…
#> 2 created_by_user Alice                                                
#> 

# Update an existing entry
edit_parquet_metadata(tmp, c(created_by_user = "Bob"))
read_parquet_metadata(tmp)$file_meta_data$key_value_metadata
#> [[1]]
#> # A data frame: 2 × 2
#>   key             value                                                
#>   <chr>           <chr>                                                
#> 1 ARROW:schema    /////3gAAAAQAAAAAAAKAA4ADAALAAQACgAAABQAAAAAAAABBAAK…
#> 2 created_by_user Bob                                                  
#> 

# Remove an entry
edit_parquet_metadata(tmp, c(created_by_user = NA_character_))
read_parquet_metadata(tmp)$file_meta_data$key_value_metadata
#> [[1]]
#> # A data frame: 1 × 2
#>   key          value                                                   
#>   <chr>        <chr>                                                   
#> 1 ARROW:schema /////3gAAAAQAAAAAAAKAA4ADAALAAQACgAAABQAAAAAAAABBAAKAAw…
#> 
```
