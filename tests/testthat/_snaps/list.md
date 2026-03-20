# LIST schema

    Code
      as.data.frame(read_parquet_schema(pf))
    Output
                file_name    name  r_type   type type_length repetition_type
      1 data/list.parquet  schema    <NA>   <NA>          NA        REQUIRED
      2 data/list.parquet       a integer  INT32          NA        OPTIONAL
      3 data/list.parquet       l    list   <NA>          NA        OPTIONAL
      4 data/list.parquet    list    <NA>   <NA>          NA        REPEATED
      5 data/list.parquet element  double DOUBLE          NA        OPTIONAL
        converted_type logical_type num_children scale precision field_id
      1           <NA>                         2    NA        NA       NA
      2           <NA>                        NA    NA        NA       NA
      3           LIST         LIST            1    NA        NA       NA
      4           <NA>                         1    NA        NA       NA
      5           <NA>                        NA    NA        NA       NA

