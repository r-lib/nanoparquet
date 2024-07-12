# deprecated warnings

    Code
      sch <- parquet_schema(pf)
    Condition
      Warning in `parquet_schema()`:
      Using `parquet_schema()` to read the schema from a file is deprecated. Use `read_parquet_schema()` instead.

---

    Code
      pct <- parquet_column_types(mtcars)
    Condition
      Warning in `parquet_column_types()`:
      `parquet_column_types()` is deprecated, please use `read_parquet_schema()` or `parquet_schema()` instead.

---

    Code
      mtd <- parquet_metadata(pf)
    Condition
      Warning in `parquet_metadata()`:
      `parquet_metadata()` is deprecated. Please use `read_parquet_metadata()` instead.

---

    Code
      info <- parquet_info(pf)
    Condition
      Warning in `parquet_info()`:
      `parquet_info()` is deprecated, please use `read_parquet_info() instead.

