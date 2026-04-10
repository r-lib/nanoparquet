# nanoparquet's type maps

How nanoparquet maps R types to Parquet types.

## R's data types

When writing out a data frame, nanoparquet maps R's data types to
Parquet logical types. The following table is a summary of the mapping.
For the details see below.

|           |                         |         |                                                                              |
|-----------|-------------------------|---------|------------------------------------------------------------------------------|
| R type    | Parquet type            | Default | Notes                                                                        |
| character | STRING(BYTE_ARRAY)      | x       | I.e. STRSXP. Converted to UTF-8.                                             |
| "         | BYTE_ARRAY              |         |                                                                              |
| "         | FIXED_LEN_BYTE_ARRAY    |         |                                                                              |
| "         | ENUM                    |         |                                                                              |
| "         | UUID                    |         |                                                                              |
| Date      | DATE                    | x       |                                                                              |
| difftime  | INT64                   | x       | If not hms::hms. Arrow metadata marks it as Duration(NS).                    |
| factor    | STRING                  | x       | Arrow metadata marks it as a factor.                                         |
| "         | ENUM                    |         |                                                                              |
| hms::hms  | TIME(true, MILLIS)      | x       | Sub-milliseconds precision is lost.                                          |
| integer   | INT(32, true)           | x       | I.e. INTSXP.                                                                 |
| "         | INT64                   |         |                                                                              |
| "         | INT96                   |         |                                                                              |
| "         | DECIMAL(INT32)          |         |                                                                              |
| "         | DECIMAL(INT64)          |         |                                                                              |
| "         | INT(8, \*)              |         |                                                                              |
| "         | INT(16, \*)             |         |                                                                              |
| "         | INT(32, signed)         |         |                                                                              |
| list      | LIST(INT32 elements)    | x       | List of integer vectors. `NULL` entries and `NA` elements are supported.     |
| "         | LIST(DOUBLE elements)   | x       | List of double vectors. `NULL` entries and `NA` elements are supported.      |
| "         | LIST(STRING elements)   | x       | List of character vectors. `NULL` entries and `NA` elements are supported.   |
| "         | BYTE_ARRAY              |         | Must be a list of raw vectors. Missing values are `NULL`.                    |
| "         | FIXED_LEN_BYTE_ARRAY    |         | Must be a list of raw vectors of the same length. Missing values are `NULL`. |
| logical   | BOOLEAN                 | x       | I.e. LGLSXP.                                                                 |
| numeric   | DOUBLE                  | x       | I.e. REALSXP.                                                                |
| "         | INT96                   |         |                                                                              |
| "         | FLOAT                   |         |                                                                              |
| "         | DECIMAL(INT32)          |         |                                                                              |
| "         | DECIMAL(INT64)          |         |                                                                              |
| "         | INT(\*, \*)             |         |                                                                              |
| "         | FLOAT16                 |         |                                                                              |
| POSIXct   | TIMESTAMP(true, MICROS) | x       | Sub-microsecond precision is lost.                                           |

The non-default mappings can be selected via the `schema` argument. E.g.
to write out a factor column called 'name' as `ENUM`, use

    write_parquet(..., schema = parquet_schema(name = "ENUM"))

The detailed mapping rules are listed below, in order of preference.
These rules will likely change until nanoparquet reaches version 1.0.0.

1.  Factors (i.e. vectors that inherit the *factor* class) are converted
    to character vectors using
    [`as.character()`](https://rdrr.io/r/base/character.html), then
    written as a `STRSXP` (character vector) type. The fact that a
    column is a factor is stored in the Arrow metadata (see below),
    unless the `nanoparquet.write_arrow_metadata` option is set to
    `FALSE`.

2.  Dates (i.e. the `Date` class) is written as `DATE` logical type,
    which is an `INT32` type internally.

3.  `hms` objects (from the hms package) are written as
    `TIME(true, MILLIS)`. logical type, which is internally the `INT32`
    Parquet type. Sub-milliseconds precision is lost.

4.  `POSIXct` objects are written as `TIMESTAMP(true, MICROS)` logical
    type, which is internally the `INT64` Parquet type. Sub-microsecond
    precision is lost.

5.  `difftime` objects (that are not `hms` objects, see above), are
    written as an `INT64` Parquet type, and noting in the Arrow metadata
    (see below) that this column has type `Duration` with `NANOSECONDS`
    unit.

6.  Integer vectors (`INTSXP`) are written as `INT(32, true)` logical
    type, which corresponds to the `INT32` type.

7.  Real vectors (`REALSXP`) are written as the `DOUBLE` type.

8.  Character vectors (`STRSXP`) are written as the `STRING` logical
    type, which has the `BYTE_ARRAY` type. They are always converted to
    UTF-8 before writing.

9.  Logical vectors (`LGLSXP`) are written as the `BOOLEAN` type.

10. Other vectors error currently.

You can use
[`infer_parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/infer_parquet_schema.md)
on a data frame to map R data types to Parquet data types.

To change the default R to Parquet mapping, use
[`parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/parquet_schema.md)
and the `schema` argument of
[`write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md).
Currently supported non-default mappings are:

- `integer` to `INT64`,

- `integer` to `INT96`,

- `double` to `INT96`,

- `double` to `FLOAT`,

- `character` to `BYTE_ARRAY`,

- `character` to `FIXED_LEN_BYTE_ARRAY`,

- `character` to `ENUM`,

- `factor` to `ENUM`,

- `integer` to `DECIAML` & `INT32`,

- `integer` to `DECIAML` & `INT64`,

- `double` to `DECIAML` & `INT32`,

- `double` to `DECIAML` & `INT64`,

- `integer` to `INT(8, *)`, `INT(16, *)`, `INT(32, signed)`,

- `double` to `INT(*, *)`,

- `character` to `UUID`,

- `double` to `FLOAT16`,

- `list` of `integer` vectors to `LIST` with `INT32` elements,

- `list` of `double` vectors to `LIST` with `DOUBLE` elements,

- `list` of `character` vectors to `LIST` with `STRING` elements,

- `list` of `raw` vectors to `BYTE_ARRAY`,

- `list` of `raw` vectors to `FIXED_LEN_BYTE_ARRAY`.

## Parquet's data types

When reading a Parquet file nanoparquet also relies on logical types and
the Arrow metadata (if present, see below) in addition to the low level
data types. The following table summarizes the mappings. See more
details below.

|                      |           |                                                   |
|----------------------|-----------|---------------------------------------------------|
| Parquet type         | R type    | Notes                                             |
| *Logical types*      |           |                                                   |
| BSON                 | character |                                                   |
| DATE                 | Date      |                                                   |
| DECIMAL              | numeric   | REALSXP, potentially losing precision.            |
| ENUM                 | character |                                                   |
| FLOAT16              | numeric   | REALSXP                                           |
| INT(8, \*)           | integer   |                                                   |
| INT(16, \*)          | integer   |                                                   |
| INT(32, \*)          | integer   | Large unsigned values may overflow!               |
| INT(64, \*)          | numeric   | REALSXP                                           |
| INTERVAL             | list(raw) | Missing values are `NULL`.                        |
| JSON                 | character |                                                   |
| LIST                 | list      | Elements are read as their corresponding R type.  |
| MAP                  |           | Not supported.                                    |
| STRING               | factor    | If Arrow metadata says it is a factor. Also UTF8. |
| "                    | character | Otherwise. Also UTF8.                             |
| TIME                 | hms::hms  | Also TIME_MILLIS and TIME_MICROS.                 |
| TIMESTAMP            | POSIXct   | Also TIMESTAMP_MILLIS and TIMESTAMP_MICROS.       |
| UUID                 | character | In `00112233-4455-6677-8899-aabbccddeeff` form.   |
| UNKNOWN              |           | Not supported.                                    |
| *Primitive types*    |           |                                                   |
| BOOLEAN              | logical   |                                                   |
| BYTE_ARRAY           | factor    | If Arrow metadata says it is a factor.            |
| "                    | list(raw) | Otherwise. Missing values are `NULL`.             |
| DOUBLE               | numeric   | REALSXP                                           |
| FIXED_LEN_BYTE_ARRAY | list(raw) | Missing values are `NULL`.                        |
| FLOAT                | numeric   | REALSXP                                           |
| INT32                | integer   |                                                   |
| INT64                | numeric   | REALSXP                                           |
| INT96                | POSIXct   |                                                   |

The exact rules are below. These rules will likely change until
nanoparquet reaches version 1.0.0.

1.  The `BOOLEAN` type is read as a logical vector (`LGLSXP`).

2.  The `STRING` logical type and the `UTF8` converted type is read as a
    character vector with UTF-8 encoding.

3.  The `DATE` logical type and the `DATE` converted type are read as a
    `Date` R object.

4.  The `TIME` logical type and the `TIME_MILLIS` and `TIME_MICROS`
    converted types are read as an `hms` object, see the hms package.

5.  The `TIMESTAMP` logical type and the `TIMESTAMP_MILLIS` and
    `TIMESTAMP_MICROS` converted types are read as `POSIXct` objects. If
    the logical type has the `UTC` flag set, then the time zone of the
    `POSIXct` object is set to `UTC`.

6.  `INT32` is read as an integer vector (`INTSXP`).

7.  `INT64`, `DOUBLE` and `FLOAT` are read as real vectors (`REALSXP`).

8.  `INT96` is read as a `POSIXct` read vector with the `tzone`
    attribute set to `"UTC"`. It was an old convention to store time
    stamps as `INT96` objects.

9.  The `DECIMAL` converted type (`FIXED_LEN_BYTE_ARRAY` or `BYTE_ARRAY`
    type) is read as a real vector (`REALSXP`), potentially losing
    precision.

10. The `ENUM` logical type is read as a character vector.

11. The `UUID` logical type is read as a character vector that uses the
    `00112233-4455-6677-8899-aabbccddeeff` form.

12. The `FLOAT16` logical type is read as a real vector (`REALSXP`).

13. `BYTE_ARRAY` is read as a *factor* object if the file was written by
    Arrow and the original data type of the column was a factor. (See
    'The Arrow metadata below.)

14. Otherwise `BYTE_ARRAY` is read a list of raw vectors, with missing
    values denoted by `NULL`.

Other logical and converted types are read as their annotated low level
types:

1.  `INT(8, true)`, `INT(16, true)` and `INT(32, true)` are read as
    integer vectors because they are `INT32` internally in Parquet.

2.  `INT(64, true)` is read as a real vector (`REALSXP`).

3.  Unsigned integer types `INT(8, false)`, `INT(16, false)` and
    `INT(32, false)` are read as integer vectors (`INTSXP`). Large
    positive values may overflow into negative values, this is a known
    issue that we will fix.

4.  `INT(64, false)` is read as a real vector (`REALSXP`). Large
    positive values may overflow into negative values, this is a known
    issue that we will fix.

5.  `INTERVAL` is a fixed length byte array, and nanoparquet reads it as
    a list of raw vectors. Missing values are denoted by `NULL`.

6.  `JSON` columns are read as character vectors (`STRSXP`).

7.  `BSON` columns are read as raw vectors (`RAWSXP`).

These types are not yet supported:

1.  Nested `LIST` types (lists of lists) are not supported.

2.  The `MAP` logical type is not supported.

3.  The `UNKNOWN` logical type is not supported.

You can use the
[`read_parquet_schema()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet_schema.md)
function to see how R would read the columns of a Parquet file. Look at
the `r_type` column.

## The Arrow metadata

Apache Arrow (i.e. the arrow R package) adds additional metadata to
Parquet files when writing them in
[`arrow::write_parquet()`](https://arrow.apache.org/docs/r/reference/write_parquet.html).
Then, when reading the file in
[`arrow::read_parquet()`](https://arrow.apache.org/docs/r/reference/read_parquet.html),
it uses this metadata to recreate the same Arrow and R data types as
before writing.

[`nanoparquet::write_parquet()`](https://nanoparquet.r-lib.org/dev/reference/write_parquet.md)
also adds the Arrow metadata to Parquet files, unless the
`nanoparquet.write_arrow_metadata` option is set to `FALSE`.

Similarly,
[`nanoparquet::read_parquet()`](https://nanoparquet.r-lib.org/dev/reference/read_parquet.md)
uses the Arrow metadata in the Parquet file (if present), unless the
`nanoparquet.use_arrow_metadata` option is set to FALSE.

The Arrow metadata is stored in the file level key-value metadata, with
key `ARROW:schema`.

Currently nanoparquet uses the Arrow metadata for two things:

- It uses it to detect factors. Without the Arrow metadata factors are
  read as string vectors.

- It uses it to detect `difftime` objects. Without the arrow metadata
  these are read as `INT64` columns, containing the time difference in
  nanoseconds.

## See also

[nanoparquet-package](https://nanoparquet.r-lib.org/dev/reference/nanoparquet-package.md)
for options that modify the type mappings.
