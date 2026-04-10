# infer_parquet_schema

    Code
      as.data.frame(infer_parquet_schema(test_df(missing = FALSE)))
    Output
         file_name r_col  name    r_type       type type_length repetition_type
      1       <NA>     1   nam character BYTE_ARRAY          NA        REQUIRED
      2       <NA>     2   mpg    double     DOUBLE          NA        REQUIRED
      3       <NA>     3   cyl   integer      INT32          NA        REQUIRED
      4       <NA>     4  disp    double     DOUBLE          NA        REQUIRED
      5       <NA>     5    hp    double     DOUBLE          NA        REQUIRED
      6       <NA>     6  drat    double     DOUBLE          NA        REQUIRED
      7       <NA>     7    wt    double     DOUBLE          NA        REQUIRED
      8       <NA>     8  qsec    double     DOUBLE          NA        REQUIRED
      9       <NA>     9    vs    double     DOUBLE          NA        REQUIRED
      10      <NA>    10    am    double     DOUBLE          NA        REQUIRED
      11      <NA>    11  gear    double     DOUBLE          NA        REQUIRED
      12      <NA>    12  carb    double     DOUBLE          NA        REQUIRED
      13      <NA>    13 large   logical    BOOLEAN          NA        REQUIRED
         converted_type logical_type num_children scale precision field_id
      1            UTF8       STRING           NA    NA        NA       NA
      2            <NA>                        NA    NA        NA       NA
      3          INT_32 INT, 32,....           NA    NA        NA       NA
      4            <NA>                        NA    NA        NA       NA
      5            <NA>                        NA    NA        NA       NA
      6            <NA>                        NA    NA        NA       NA
      7            <NA>                        NA    NA        NA       NA
      8            <NA>                        NA    NA        NA       NA
      9            <NA>                        NA    NA        NA       NA
      10           <NA>                        NA    NA        NA       NA
      11           <NA>                        NA    NA        NA       NA
      12           <NA>                        NA    NA        NA       NA
      13           <NA>                        NA    NA        NA       NA

---

    Code
      as.data.frame(infer_parquet_schema(test_df(missing = TRUE)))
    Output
         file_name r_col  name    r_type       type type_length repetition_type
      1       <NA>     1   nam character BYTE_ARRAY          NA        OPTIONAL
      2       <NA>     2   mpg    double     DOUBLE          NA        OPTIONAL
      3       <NA>     3   cyl   integer      INT32          NA        OPTIONAL
      4       <NA>     4  disp    double     DOUBLE          NA        OPTIONAL
      5       <NA>     5    hp    double     DOUBLE          NA        OPTIONAL
      6       <NA>     6  drat    double     DOUBLE          NA        OPTIONAL
      7       <NA>     7    wt    double     DOUBLE          NA        OPTIONAL
      8       <NA>     8  qsec    double     DOUBLE          NA        OPTIONAL
      9       <NA>     9    vs    double     DOUBLE          NA        OPTIONAL
      10      <NA>    10    am    double     DOUBLE          NA        OPTIONAL
      11      <NA>    11  gear    double     DOUBLE          NA        OPTIONAL
      12      <NA>    12  carb    double     DOUBLE          NA        OPTIONAL
      13      <NA>    13 large   logical    BOOLEAN          NA        OPTIONAL
         converted_type logical_type num_children scale precision field_id
      1            UTF8       STRING           NA    NA        NA       NA
      2            <NA>                        NA    NA        NA       NA
      3          INT_32 INT, 32,....           NA    NA        NA       NA
      4            <NA>                        NA    NA        NA       NA
      5            <NA>                        NA    NA        NA       NA
      6            <NA>                        NA    NA        NA       NA
      7            <NA>                        NA    NA        NA       NA
      8            <NA>                        NA    NA        NA       NA
      9            <NA>                        NA    NA        NA       NA
      10           <NA>                        NA    NA        NA       NA
      11           <NA>                        NA    NA        NA       NA
      12           <NA>                        NA    NA        NA       NA
      13           <NA>                        NA    NA        NA       NA

# logical_to_converted

    Code
      logical_to_converted(list("DECIMAL"))
    Condition
      Error:
      ! Parquet decimal logical type needs scale and precision
    Code
      logical_to_converted(list("TIME", TRUE, "SECS"))
    Condition
      Error:
      ! Unknown TIME time unit: SECS
    Code
      logical_to_converted(list("TIMESTAMP", TRUE, "SECS"))
    Condition
      Error:
      ! Unknown TIMESTAMP time unit: SECS
    Code
      logical_to_converted(list("INT"))
    Condition
      Error:
      ! Parquet integer logical type needs bit width and signedness
    Code
      logical_to_converted(list("FOOBAR"))
    Condition
      Error:
      ! Unknown Parquet logical type: FOOBAR

