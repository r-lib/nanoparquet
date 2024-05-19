# nanoparquet

<div>
<!-- badges: start -->
[![R-CMD-check](https://github.com/gaborcsardi/nanoparquet/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/gaborcsardi/nanoparquet/actions/workflows/R-CMD-check.yaml)
[![CRAN status](https://www.r-pkg.org/badges/version/nanoparquet)](https://cran.r-project.org/package=nanoparquet)
[![](http://cranlogs.r-pkg.org/badges/nanoparquet)](https://dgrtwo.shinyapps.io/cranview/)
<!-- badges: end -->
</div>

`nanoparquet` is a reader and writer for a common subset of Parquet files.
nanoparquet only supports rectangular-shaped data structures
(no nested tables) and only the Snappy compression scheme.
nanoparquet has no (zero, none, 0)
[external dependencies](https://research.swtch.com/deps) and is very
lightweight. It compiles in seconds to a binary size of about 2 MB.

## Installation

Install the R package from CRAN:

```r
install.packages("nanoparquet")
```

## Usage

### Read

Call `read_parquet()` to read a Parquet file:
```r
df <- nanoparquet::read_parquet("example.parquet")
```

Folders of similar-structured Parquet files (e.g. produced by Spark)
can be read like this:

```r
df <- data.table::rbindlist(lapply(
  Sys.glob("some-folder/part-*.parquet"),
  nanoparquet::read_parquet
))
```

### Write

Call `write_parquet()` to write a data frame to a Parquet file:
```{r}
nanoparquet::write_parquet(mtcars, "mtcars.parquet")
```

### Inspect

Call `parquet_info()`, `parquet_columns()`, `parquet_schema()` or
`parquet_metadata()` to see various kinds of metadata from a Parquet
file:
* `parquet_info()` shows a basic summary of the file.
* `parquet_columns()` shows the leaf columns, these are are the ones
  that `read_parquet()` reads into R.
* `parquet_schema()` shows all columns, including non-leaf columns.
* `parquet_metadata()` shows the most complete metadata information:
  file meta data, the schema, the row groups and column chunks of the
  file.

```r
nanoparquet::parquet_info("mtcars.parquet")
nanoparquet::parquet_columns("mtcars.parquet")
nanoparquet::parquet_schema("mtcars.parquet")
nanoparquet::parquet_metadata("mtcars.parquet")
```

If you find a file that should be supported but isn't, please open an
issue here with a link to the file.

## Options

* `nanoparquet.use_arrow_metadata`: unless this is set to `FALSE`,
  `read_parquet()` will make use of Arrow metadata in the Parquet file.
  Currently this is used to detect factor columns.
* `nanoparquet.write_arrow_metadata`: unless this is set to `FALSE`,
  `write_parquet()` will add Arrow metadata to the Parquet file.
  This helps preserving classes of columns, e.g. factors will be read
  back as factors, both by nanoparquet and Arrow.

## License

MIT
