# miniparquet

<!-- badges: start -->
[![R-CMD-check](https://github.com/gaborcsardi/miniparquet/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/gaborcsardi/miniparquet/actions/workflows/R-CMD-check.yaml)
[![CRAN status](https://www.r-pkg.org/badges/version/miniparquet)](https://cran.r-project.org/package=miniparquet)
[![](http://cranlogs.r-pkg.org/badges/miniparquet)](https://dgrtwo.shinyapps.io/cranview/)
<!-- badges: end -->

`miniparquet` is a reader and writer for a common subset of Parquet files.
miniparquet only supports rectangular-shaped data structures
(no nested tables) and only the Snappy compression scheme.
miniparquet has no (zero, none, 0)
[external dependencies](https://research.swtch.com/deps) and is very
lightweight. It compiles in seconds to a binary size of under 1 MB.

## Installation

Install the R package from CRAN:

```r
install.packages("miniparquet")
```

## Usage

Call `parquet_read()` to read a Parquet file:
```r
df <- miniparquet::parquet_read("example.parquet")
```

Folders of similar-structured Parquet files (e.g. produced by Spark)
can be read like this:

```r
df <- data.table::rbindlist(lapply(
  Sys.glob("some-folder/part-*.parquet"),
  miniparquet::parquet_read
))
```

Call `parquet_read_metadata()` to show the metadata, e.g. the column
types, of a Parquet file, without reading the whole file into memory:

```r
miniparquet::parquet_read_metadata("example.parquet")
```

Call `parquet_write()` to write a data frame to a Parquet file:
```r
parquet_write(mtcars, "mtcars.parquet")

If you find a file that should be supported but isn't, please open an
issue here with a link to the file.
