# nanoparquet

<!-- badges: start -->
[![R-CMD-check](https://github.com/r-lib/nanoparquet/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/r-lib/nanoparquet/actions/workflows/R-CMD-check.yaml)
[![CRAN status](https://www.r-pkg.org/badges/version/nanoparquet)](https://cran.r-project.org/package=nanoparquet)
[![](http://cranlogs.r-pkg.org/badges/nanoparquet)](https://r-pkg.org/pkg/nanoparquet)
<!-- badges: end -->

`nanoparquet` is a reader and writer for a common subset of Parquet files.

## Features:

* Read and write flat (i.e. non-nested) Parquet files.
* Can read most [Parquet data types](https://r-lib.github.io/nanoparquet/reference/nanoparquet-types.html).
* Can write many R data types, including factors and temporal types
  to Parquet.
* Completely dependency free.
* Supports Snappy, Gzip and Zstd compression.

## Limitations:

* Nested Parquet types are not supported.
* Some Parquet logical types are not supported: `FLOAT16`, `INTERVAL`,
  `UNKNOWN`.
* Only Snappy, Gzip and Zstd compression is supported.
* Encryption is not supported.
* Reading files from URLs is not supported.
* Being single-threaded and not fully optimized, nanoparquet is probably
  not suited well for large data sets. It should be fine for a couple of
  gigabytes. Reading or writing a ~250MB file that has 32 million rows and 14 columns takes about 10-15 seconds on an M2 MacBook Pro.
  For larger files, use Apache Arrow or DuckDB.

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

To see the columns of a Parquet file and how their types are mapped to
R types by `read_parquet()`, call `read_parquet_schema()` first:
```r
nanoparquet::read_parquet_schema("example.parquet")
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
```r
nanoparquet::write_parquet(mtcars, "mtcars.parquet")
```

To see how the columns of the data frame will be mapped to Parquet types
by `write_parquet()`, call `infer_parquet_schema()` first:
```r
nanoparquet::infer_parquet_schema(mtcars)
```

### Inspect

Call `read_parquet_info()`, `read_parquet_schema()`, or
`read_parquet_metadata()` to see various kinds of metadata from a Parquet
file:

* `read_parquet_info()` shows a basic summary of the file.
* `read_parquet_schema()` shows all columns, including non-leaf columns,
  and how they are mapped to R types by `read_parquet()`.
* `read_parquet_metadata()` shows the most complete metadata information:
  file meta data, the schema, the row groups and column chunks of the
  file.

```r
nanoparquet::read_parquet_info("mtcars.parquet")
nanoparquet::read_parquet_schema("mtcars.parquet")
nanoparquet::read_parquet_metadata("mtcars.parquet")
```

If you find a file that should be supported but isn't, please open an
issue here with a link to the file.

## Options

See also `?parquet_options()`.

* `nanoparquet.class`: extra class to add to data frames returned by
  `read_parquet()`. If it is not defined, the default is `"tbl"`,
  which changes how the data frame is printed if the pillar package is
  loaded.
* `nanoparquet.use_arrow_metadata`: unless this is set to `FALSE`,
  `read_parquet()` will make use of Arrow metadata in the Parquet file.
  Currently this is used to detect factor columns.
* `nanoparquet.write_arrow_metadata`: unless this is set to `FALSE`,
  `write_parquet()` will add Arrow metadata to the Parquet file.
  This helps preserving classes of columns, e.g. factors will be read
  back as factors, both by nanoparquet and Arrow.

## License

MIT
