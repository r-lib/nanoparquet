# miniparquet

<div>
<!-- badges: start -->
[![R-CMD-check](https://github.com/gaborcsardi/miniparquet/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/gaborcsardi/miniparquet/actions/workflows/R-CMD-check.yaml)
[![CRAN status](https://www.r-pkg.org/badges/version/miniparquet)](https://cran.r-project.org/package=miniparquet)
[![](http://cranlogs.r-pkg.org/badges/miniparquet)](https://dgrtwo.shinyapps.io/cranview/)
<!-- badges: end -->
</div>

`miniparquet` is a reader and writer for a common subset of Parquet files.
miniparquet only supports rectangular-shaped data structures
(no nested tables) and only the Snappy compression scheme.
miniparquet has no (zero, none, 0)
[external dependencies](https://research.swtch.com/deps) and is very
lightweight. It compiles in seconds to a binary size of about 2 MB.

## Installation

Install the R package from CRAN:

```r
install.packages("miniparquet")
```

## Usage

Call `read_parquet()` to read a Parquet file:
```r
df <- miniparquet::read_parquet("example.parquet")
```

Folders of similar-structured Parquet files (e.g. produced by Spark)
can be read like this:

```r
df <- data.table::rbindlist(lapply(
  Sys.glob("some-folder/part-*.parquet"),
  miniparquet::read_parquet
))
```

Call `parquet_schema()` to show the schema (i.e. the column names and
types) of a Parquet file, or `parquet_metadata()` to show the complete
 metadata, without reading the whole file into memory:

```r
miniparquet::parquet_schema("example.parquet")
miniparquet::parquet_metadata("example.parquet")
```

Call `write_parquet()` to write a data frame to a Parquet file:
```r
write_parquet(mtcars, "mtcars.parquet")
```

If you find a file that should be supported but isn't, please open an
issue here with a link to the file.

## Options

* `miniparquet.use_arrow_metadata`: unless this is set to `FALSE`,
  `read_parquet()` will make use of Arrow metadata in the Parquet file.
  Currently this is used to detect factor columns.
* `miniparquet.write_arrow_metadata`: unless this is set to `FALSE`,
  `write_parquet()` will add Arrow metadata to the Parquet file.
  This helps preserving classes of columns, e.g. factors will be read
  back as factors, both by miniparquet and Arrow.

## License

MIT
